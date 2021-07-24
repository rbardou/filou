open Misc
open Device_common

type location = Path.absolute_dir

let local_path (local_device_path: Path.absolute_dir) (path: path) =
  Path.Any_kind.concat local_device_path (path_of_device_path path)

let local_dir_path local_device_path path =
  local_path local_device_path path |> Path.Any_kind.to_dir

let local_file_path local_device_path ((dir_path, filename): file_path) =
  Path.File (filename, local_dir_path local_device_path dir_path)

let close fd =
  try Unix.close fd with Unix.Unix_error _ -> ()

let close_dir fd =
  try Unix.closedir fd with Unix.Unix_error _ -> ()

let dot_part (path: (_, Path.file) Path.t) = Path.show path ^ ".part"

let lock (mode: mode) (location: location) (path: file_path) =
  match mode with
    | RO ->
        unit
    | RW ->
        let full_path = local_file_path location path in
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
              unit

let unlock (mode: mode) (location: location) (path: file_path) =
  match mode with
    | RO ->
        unit
    | RW ->
        let full_path = local_file_path location path in
        let full_path_string = Path.show full_path in
        match Unix.unlink full_path_string with
          | exception Unix.Unix_error (error, _, _) ->
              failed [
                "failed to unlock: " ^ full_path_string;
                Unix.error_message error;
              ]
          | () ->
              unit

let iter_read_dir (location: location) (path: path) f =
  let full_path = local_dir_path location path in
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

let write_file_incrementally_gen unix_write (mode: mode) (location: location)
    (path: file_path) write_contents =
  match mode with
    | RO ->
        read_only ()
    | RW ->
        let full_path = local_file_path location path in
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

let read_file_incrementally (location: location) (path: file_path) read_contents =
  let full_path = local_file_path location path in
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

let stat (location: location) (path: path) =
  match List.rev path with
    | [] ->
        OK Dir
    | head :: tail ->
        let file_path = List.rev tail, head in
        let full_path = local_file_path location file_path in
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
              OK (File { size = st_size })
          | { st_kind = (S_CHR | S_BLK | S_LNK | S_FIFO | S_SOCK); _ } ->
              failed [ "not a regular file or a directory: " ^ full_path_string ]

let remove_file (mode: mode) (location: location) ((dir_path, _) as file_path: file_path) =
  let* () =
    match mode with
      | RO ->
          read_only ()
      | RW ->
          let full_path = local_file_path location file_path in
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
        let full_dir_path = local_dir_path location dir_path in
        let full_dir_path_string = Path.show full_dir_path in
        match Unix.rmdir full_dir_path_string with
          | exception Unix.Unix_error (ENOENT, _, _) ->
              unit
          | exception Unix.Unix_error (error, _, _) ->
              failed [
                "failed to remove directory: " ^ full_dir_path_string;
                Unix.error_message error;
              ]
          | () ->
              unit
      in
      match file_path_of_path dir_path with
        | None ->
            unit
        | Some (parent, _) ->
            remove_if_empty parent
  in
  remove_if_empty dir_path
