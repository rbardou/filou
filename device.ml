open Misc

include Device_common

type location =
  | Local of mode * Device_local.location
  | SSH_filou of mode * Device_ssh_filou.location

let parse_location mode string =
  match Device_location_lexer.location (Lexing.from_string string) with
    | `invalid ->
        failed [ "invalid location: " ^ string ]
    | `local path ->
        ok (Local (mode, path |> Path.Any.to_absolute |> Path.Any_kind.to_dir))
    | `ssh_filou (username, hostname, port, path) ->
        let location =
          Device_ssh_filou.make_location
            ?username
            ~hostname
            ?port
            ~path: (Path.Any.to_dir path)
            ()
        in
        ok (SSH_filou (mode, location))

let show_location = function
  | Local (_, path) ->
      (* TODO: if path starts with something that looks like a protocol, prepend "file://" *)
      Path.show path
  | SSH_filou (
      _,
      { username; hostname; port; path; connection = _ }
    ) ->
      sf "filou+ssh://%s%s%s/%s"
        (match username with None -> "" | Some u -> u ^ "@")
        hostname
        (match port with None -> "" | Some p -> ":" ^ string_of_int p)
        (Path.Any_relativity.show path)

let sublocation location filename =
  match location with
    | Local (mode, path) ->
        Local (mode, Path.Dir (filename, path))
    | SSH_filou (mode, location) ->
        SSH_filou (mode, Device_ssh_filou.sublocation location filename)

type lock = file_path

let lock location path =
  let* () =
    match location with
      | Local (mode, location) ->
          Device_local.lock mode location path
      | SSH_filou (mode, location) ->
          Device_ssh_filou.lock mode location path
  in
  ok (path: lock)

let unlock location (lock: lock) =
  match location with
    | Local (mode, location) ->
        Device_local.unlock mode location lock
    | SSH_filou (mode, location) ->
        Device_ssh_filou.unlock mode location

let with_lock location path f =
  let* lock = lock location path in
  match f () with
    | exception exn ->
        let* () = unlock location lock in
        raise exn
    | result ->
        let* () = unlock location lock in
        result

let iter_read_dir location path f =
  match location with
    | Local (_, location) ->
        Device_local.iter_read_dir location path f
    | SSH_filou (_, location) ->
        Device_ssh_filou.iter_read_dir location path f

let read_dir location path =
  let list = ref [] in
  let* () = iter_read_dir location path (fun filename -> list := filename :: !list; unit) in
  ok !list

let write_file_incrementally location path write_contents =
  match location with
    | Local (mode, location) ->
        Device_local.write_file_incrementally mode location path write_contents
    | SSH_filou (mode, location) ->
        Device_ssh_filou.write_file_incrementally mode location path write_contents

let write_file_incrementally_bytes location path write_contents =
  match location with
    | Local (mode, location) ->
        Device_local.write_file_incrementally_bytes mode location path write_contents
    | SSH_filou (mode, location) ->
        Device_ssh_filou.write_file_incrementally_bytes mode location path write_contents

let write_file location path contents =
  write_file_incrementally location path @@ fun write ->
  write contents 0 (String.length contents)

let read_file_incrementally location path read_contents =
  match location with
    | Local (_, location) ->
        Device_local.read_file_incrementally location path read_contents
    | SSH_filou (_, location) ->
        Device_ssh_filou.read_file_incrementally location path read_contents

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

let stat location path =
  match location with
    | Local (_, location) ->
        Device_local.stat location path
    | SSH_filou (_, location) ->
        Device_ssh_filou.stat location path

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

let remove_file location file_path =
  match location with
    | Local (mode, location) ->
        Device_local.remove_file mode location file_path
    | SSH_filou (mode, location) ->
        Device_ssh_filou.remove_file mode location file_path
