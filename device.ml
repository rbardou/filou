open Misc

type mode = RW | RO

type location =
  | Local of mode * Path.absolute_dir

let parse_location mode string =
  match Path.parse string with
    | None ->
        failed [ "invalid location: " ^ string ]
    | Some path ->
        let path = Path.Any_relativity.to_absolute (Path.Any.to_dir path) in
        OK (Local (mode, path))

let show_location = function
  | Local (_, path) ->
      Path.show path

let sublocation location filename =
  match location with
    | Local (mode, path) ->
        Local (mode, Path.Dir (filename, path))

type path = Path.Filename.t list

type file_path = path * Path.Filename.t

let path_of_repository_path rpath =
  match List.rev rpath with
    | [] ->
        Path.(D Current)
    | head :: tail ->
        let rec dir = function
          | [] ->
              Path.Current
          | head :: tail ->
              Path.Dir (head, dir tail)
        in
        Path.F (Path.File (head, dir tail))

let repository_path_of_path full_path =
  let rec gather: 'a. _ -> (Path.relative, 'a) Path.t -> _ =
    fun (type a) acc (path: (Path.relative, a) Path.t) ->
      match path with
        | Current ->
            OK acc
        | Parent _ ->
            failed [ "path contains parent directories (..): " ^ Path.show full_path ]
        | File (filename, parent) ->
            gather (filename :: acc) parent
        | Dir (filename, parent) ->
            gather (filename :: acc) parent
  in
  gather [] full_path

let parse_path location string =
  match location with
    | Local (_, repository_root) ->
        match Path.parse string with
          | None ->
              failed [ "invalid path: \"" ^ String.escaped string ^ "\"" ]
          | Some path ->
              match
                Path.Any.to_absolute path
                |> Path.Any_kind.to_relative ~from: repository_root
              with
                | R_empty ->
                    OK []
                | R_none ->
                    failed [
                      "path denotes a file that is outside of the repository directory: "
                      ^ string
                    ]
                | R_some (D path) ->
                    repository_path_of_path path
                | R_some (F path) ->
                    repository_path_of_path path

let show_path (path: path) =
  match path with
    | [] -> "."
    | _ -> String.concat "/" (List.map Path.Filename.show path)

let path_of_file_path ((path, file): file_path) =
  path @ [ file ]

let file_path_of_path (path: path) =
  match List.rev path with
    | [] ->
        None
    | head :: tail ->
        Some (List.rev tail, head)

let parse_file_path location string =
  let* path = parse_path location string in
  match file_path_of_path path with
    | None ->
        failed [ string ^ " is a directory" ]
    | Some file_path ->
        ok file_path

let show_file_path path =
  show_path (path_of_file_path path)

let compare_paths a b = String.compare (show_path a) (show_path b)
let compare_file_paths a b = compare_paths (path_of_file_path a) (path_of_file_path b)
let same_paths a b = compare_paths a b = 0
let same_file_paths a b = same_paths (path_of_file_path a) (path_of_file_path b)

let failed_to_read_directory path msg =
  failed (("failed to read directory: " ^ (Path.show path)) :: msg)

let failed_to_write_file path msg =
  failed (("failed to write file: " ^ path) :: msg)

let failed_to_read_file path msg =
  failed (("failed to read file: " ^ path) :: msg)

let dot_part (path: (_, Path.file) Path.t) = Path.show path ^ ".part"

let local_path (local_device_path: Path.absolute_dir) (path: path) =
  Path.Any_kind.concat local_device_path (path_of_repository_path path)

let local_dir_path local_device_path path =
  local_path local_device_path path |> Path.Any_kind.to_dir

let local_file_path local_device_path ((dir_path, filename): file_path) =
  Path.File (filename, local_dir_path local_device_path dir_path)

let close fd =
  try Unix.close fd with Unix.Unix_error _ -> ()

let close_dir fd =
  try Unix.closedir fd with Unix.Unix_error _ -> ()

let with_lock location (path: file_path) f =
  match location with
    | Local (RO, _) ->
        f ()
    | Local (RW, device_path) ->
        let full_path = local_file_path device_path path in
        let full_path_string = Path.show full_path in
        let flags: Unix.open_flag list =
          [
            O_WRONLY;
            O_CREAT;
            O_EXCL;
            O_CLOEXEC;
          ]
        in
        match Unix.openfile full_path_string flags 0o640 with
          | exception Unix.Unix_error (EEXIST, _, _) ->
              failed [
                "lock is already taken, remove file manually if it shouldn't: " ^
                full_path_string;
              ]
          | exception Unix.Unix_error (error, _, _) ->
              failed [
                "failed to take lock: " ^ full_path_string;
                Unix.error_message error;
              ]
          | fd ->
              close fd;
              let unlock () =
                try
                  Unix.unlink full_path_string
                with Unix.Unix_error _ ->
                  ()
              in
              match f () with
                | exception exn ->
                    unlock ();
                    raise exn
                | result ->
                    unlock ();
                    result

let iter_read_dir location (path: path) f =
  match location with
    | Local (_, device_path) ->
        let full_path = local_dir_path device_path path in
        match Unix.opendir (Path.show full_path) with
          | exception Unix.Unix_error (ENOENT, _, _) ->
              error `no_such_file [ "no such file or directory: " ^ Path.show full_path ]
          | exception Unix.Unix_error (error, _, _) ->
              failed_to_read_directory full_path [ Unix.error_message error ]
          | dir_handle ->
              let rec loop () =
                match Unix.readdir dir_handle with
                  | exception End_of_file ->
                      OK ()
                  | exception Unix.Unix_error (error, _, _) ->
                      failed_to_read_directory full_path [ Unix.error_message error ]
                  | entry ->
                      if
                        String.equal entry Filename.current_dir_name ||
                        String.equal entry Filename.parent_dir_name
                      then
                        loop ()
                      else
                        match Path.Filename.parse entry with
                          | None ->
                              failed_to_read_directory full_path [
                                sf "invalid filename: %S" entry;
                              ]
                          | Some filename ->
                              let* () = f filename in
                              loop ()
              in
              match loop () with
                | exception exn ->
                    close_dir dir_handle;
                    raise exn
                | result ->
                    close_dir dir_handle;
                    result

let read_dir location path =
  let list = ref [] in
  let* () = iter_read_dir location path (fun filename -> list := filename :: !list; unit) in
  ok !list

let check_directory_is_empty_or_inexistant location (path: path) =
  let exception Not_empty in
  match
    iter_read_dir location path @@ fun _ ->
    raise Not_empty
  with
    | exception Not_empty ->
        failed [
          sf "directory is not empty: %s in %s" (show_path path)
            (show_location location);
        ]
    | ERROR { code = `no_such_file; _ } -> unit
    | ERROR { code = `failed; _ } | OK () as x -> x

(* Helper for [write_file]. *)
let make_parent_directories (path: (Path.absolute, _) Path.t) =
  let rec make (dir: (Path.absolute, Path.dir) Path.t) =
    let dir_string = Path.show dir in
    match Unix.stat dir_string with
      | exception Unix.Unix_error (ENOENT, _, _) ->
          (
            match Path.parent dir with
              | None ->
                  OK ()
              | Some parent ->
                  let* () = make parent in
                  match Unix.mkdir dir_string 0o750 with
                    | exception Unix.Unix_error (error, _, _) ->
                        failed_to_write_file (Path.show path) [
                          ("failed to create parent directory: " ^ dir_string);
                          Unix.error_message error;
                        ]
                    | () ->
                        OK ()
          )
      | exception Unix.Unix_error (error, _, _) ->
          failed_to_write_file (Path.show path) [
            ("failed to read parent directory: " ^ dir_string);
            Unix.error_message error;
          ]
      | { st_kind = S_DIR; _ } ->
          OK ()
      | { st_kind = (S_REG | S_CHR | S_BLK | S_LNK | S_FIFO | S_SOCK); _ } ->
          failed_to_write_file (Path.show path) [ ("not a directory: " ^ dir_string) ]
  in
  match Path.parent path with
    | None ->
        OK ()
    | Some parent ->
        make parent

let read_only () =
  failed [ "read-only mode is active" ]

let write_file_incrementally_gen unix_write location path write_contents =
  match location with
    | Local (RO, _) ->
        read_only ()
    | Local (RW, device_path) ->
        let full_path = local_file_path device_path path in
        let* () = make_parent_directories full_path in
        let temp_path = dot_part full_path in
        let flags: Unix.open_flag list =
          [
            O_WRONLY;
            O_CREAT;
            O_CLOEXEC;
            O_EXCL; (* if the .part already exists, someone else is writing it? *)
          ]
        in
        match Unix.openfile temp_path flags 0o640 with
          | exception Unix.Unix_error (error, _, _) ->
              failed_to_write_file temp_path [ Unix.error_message error ]
          | fd ->
              let result =
                try
                  write_contents @@ fun string ofs len ->
                  match unix_write fd string ofs len with
                    | exception Unix.Unix_error (error, _, _) ->
                        failed_to_write_file temp_path [ Unix.error_message error ]
                    | written_len ->
                        if written_len <> len then
                          failed_to_write_file temp_path
                            [ sf "wrote only %d bytes out of %d" written_len len ]
                        else
                          OK ()
                with exn ->
                  failed_to_write_file temp_path [ Printexc.to_string exn ]
              in
              close fd;
              match result with
                | OK () ->
                    (
                      let full_path_string = Path.show full_path in
                      match Unix.rename temp_path full_path_string with
                        | exception Unix.Unix_error (error, _, _) ->
                            failed_to_write_file full_path_string [
                              "failed to rename: " ^ temp_path;
                              Unix.error_message error;
                            ]
                        | () ->
                            OK ()
                    )
                | ERROR _ as error ->
                    (
                      try
                        Unix.unlink temp_path
                      with Unix.Unix_error _ ->
                        ()
                    );
                    error

let write_file_incrementally location path write_contents =
  write_file_incrementally_gen Unix.write_substring location path write_contents

let write_file_incrementally_bytes location path write_contents =
  write_file_incrementally_gen Unix.write location path write_contents

let write_file location path contents =
  write_file_incrementally location path @@ fun write ->
  write contents 0 (String.length contents)

let read_file_incrementally location path read_contents =
  match location with
    | Local (_, device_path) ->
        let full_path = local_file_path device_path path in
        let full_path_string = Path.show full_path in
        let flags: Unix.open_flag list =
          [
            O_RDONLY;
            O_CLOEXEC;
          ]
        in
        match Unix.openfile full_path_string flags 0o640 with
          | exception Unix.Unix_error (ENOENT, _, _) ->
              ERROR {
                code = `no_such_file;
                msg = [ "no such file: " ^ full_path_string ];
              }
          | exception Unix.Unix_error (error, _, _) ->
              failed_to_read_file full_path_string [ Unix.error_message error ]
          | fd ->
              let result =
                try
                  read_contents @@ fun bytes ofs len ->
                  match Unix.read fd bytes ofs len with
                    | exception Unix.Unix_error (error, _, _) ->
                        failed_to_read_file full_path_string [ Unix.error_message error ]
                    | written_len ->
                        OK written_len
                with exn ->
                  failed_to_read_file full_path_string [ Printexc.to_string exn ]
              in
              close fd;
              result

let read_file location path =
  read_file_incrementally location path @@ fun read ->
  let buffer_size = 4096 in
  let buffer = Buffer.create buffer_size in
  let bytes = Bytes.create buffer_size in
  let rec loop () =
    match read bytes 0 buffer_size with
      | OK 0 ->
          OK (Buffer.contents buffer)
      | OK len ->
          Buffer.add_subbytes buffer bytes 0 len;
          loop ()
      | ERROR _ as x ->
          x
  in
  loop ()

type stat =
  | File of { path: file_path; size: int }
  | Dir

let stat location (path: path) =
  match List.rev path with
    | [] ->
        OK Dir
    | head :: tail ->
        let file_path = List.rev tail, head in
        match location with
          | Local (_, device_path) ->
              let full_path = local_file_path device_path file_path in
              let full_path_string = Path.show full_path in
              match Unix.stat full_path_string with
                | exception Unix.Unix_error (ENOENT, _, _) ->
                    ERROR {
                      code = `no_such_file;
                      msg = [ "no such file: " ^ full_path_string ];
                    }
                | exception Unix.Unix_error (error, _, _) ->
                    failed_to_read_file full_path_string [ Unix.error_message error ]
                | { st_kind = S_DIR; _ } ->
                    OK Dir
                | { st_kind = S_REG; st_size; _ } ->
                    OK (File { path = file_path; size = st_size })
                | { st_kind = (S_CHR | S_BLK | S_LNK | S_FIFO | S_SOCK); _ } ->
                    failed [ "not a regular file or a directory: " ^ full_path_string ]

let exists location (path: path) =
  match stat location path with
    | ERROR { code = `no_such_file; _ } ->
        ok false
    | ERROR { code = `failed; _ } as x ->
        x
    | OK _ ->
        ok true

let file_exists location (path: file_path) =
  match stat location (path_of_file_path path) with
    | ERROR { code = `no_such_file; _ } ->
        ok false
    | ERROR { code = `failed; _ } as x ->
        x
    | OK Dir ->
        ok false
    | OK (File _) ->
        ok true

let dir_exists location (path: path) =
  match stat location path with
    | ERROR { code = `no_such_file; _ } ->
        ok false
    | ERROR { code = `failed; _ } as x ->
        x
    | OK Dir ->
        ok true
    | OK (File _) ->
        ok false

let hash location path =
  read_file_incrementally location path @@ fun read ->
  let bytes = Bytes.create 4096 in
  let rec loop partial total_length =
    let* len = read bytes 0 4096 in
    let total_length = total_length + len in
    if len <= 0 then
      ok (Hash.finish partial, total_length)
    else
      loop (Hash.feed_bytes partial bytes 0 len) total_length
  in
  loop (Hash.start ()) 0

let copy_file
    ~on_progress
    ~source: (source_location, source_path)
    ~target: (target_location, target_path) =
  read_file_incrementally source_location source_path @@ fun read ->
  write_file_incrementally_bytes target_location target_path @@ fun write ->
  let written_byte_count = ref 0 in
  let buffer_size = 4096 in
  let bytes = Bytes.create buffer_size in
  let rec loop () =
    match read bytes 0 buffer_size with
      | OK 0 ->
          unit
      | OK len ->
          let* () = write bytes 0 len in
          written_byte_count := !written_byte_count + len;
          on_progress !written_byte_count;
          loop ()
      | ERROR _ as x ->
          x
  in
  loop ()

(* Should not be exposed in the .mli: it does not recurse. *)
let remove_directory location (path: path) =
  match location with
    | Local (RO, _) ->
        read_only ()
    | Local (RW, device_path) ->
        let full_path = local_path device_path path in
        let full_path_string = Path.Any_kind.show full_path in
        match Unix.rmdir full_path_string with
          | exception Unix.Unix_error (ENOENT, _, _) ->
              error `no_such_file [ "no such file or directory: " ^ full_path_string ]
          | exception Unix.Unix_error (error, _, _) ->
              failed [
                "failed to remove directory: " ^ full_path_string;
                Unix.error_message error;
              ]
          | () ->
              unit

let remove_file location ((dir_path, _) as file_path: file_path) =
  let* () =
    match location with
      | Local (RO, _) ->
          read_only ()
      | Local (RW, device_path) ->
          let full_path = local_file_path device_path file_path in
          let full_path_string = Path.show full_path in
          try
            Unix.unlink full_path_string;
            unit
          with
            | Unix.Unix_error (ENOENT, _, _) ->
                ERROR {
                  code = `no_such_file;
                  msg = [ "no such file: " ^ full_path_string ];
                }
            | Unix.Unix_error (error, _, _) ->
                failed [
                  "failed to remove file: " ^ full_path_string;
                  Unix.error_message error;
                ]
  in
  (* If directory becomes empty, remove it, recursively. *)
  let rec remove_if_empty (dir_path: path) =
    (* Note: we could actually just [rmdir] without [readdir] first.
       But... just in case there is a system where [rmdir] removes recursively... *)
    let* is_empty =
      let exception Not_empty in
      match
        iter_read_dir location dir_path @@ fun _ ->
        raise Not_empty
      with
        | exception Not_empty ->
            ok false
        | ERROR { code = (`no_such_file | `failed); msg } ->
            failed (
              ("failed to test whether directory became empty: " ^ show_path dir_path) ::
              msg
            )
        | OK () ->
            ok true
    in
    if not is_empty then
      unit
    else
      let* () =
        match remove_directory location dir_path with
          | ERROR { code = `no_such_file; _ } -> unit
          | ERROR { code = `failed; _ } | OK () as x -> x
      in
      match file_path_of_path dir_path with
        | None ->
            unit
        | Some (parent, _) ->
            remove_if_empty parent
  in
  remove_if_empty dir_path
