open Misc

type path = Device_common.path
type file_path = Device_common.file_path
type stat = Device_common.stat

type query =
  | Q_hello of Path.dir Path.any_relativity
  | Q_lock of file_path
  | Q_unlock
  | Q_read_dir of path
  | Q_write_file of file_path
  | Q_write_file_chunk of string
  | Q_write_file_eof
  | Q_read_file of file_path
  | Q_stat of path
  | Q_remove_file of file_path

let show_query = function
  | Q_hello path ->
      "Q_hello " ^ Path.Any_relativity.show path
  | Q_lock path ->
      "Q_lock " ^ Device_common.show_file_path path
  | Q_unlock ->
      "Q_unlock"
  | Q_read_dir path ->
      "Q_read_dir " ^ Device_common.show_path path
  | Q_write_file path ->
      "Q_write_file " ^ Device_common.show_file_path path
  | Q_write_file_chunk data ->
      "Q_write_file_chunk (" ^ string_of_int (String.length data) ^ " bytes)"
  | Q_write_file_eof ->
      "Q_write_file_eof"
  | Q_read_file path ->
      "Q_read_file " ^ Device_common.show_file_path path
  | Q_stat path ->
      "Q_stat " ^ Device_common.show_path path
  | Q_remove_file path ->
      "Q_remove_file " ^ Device_common.show_file_path path

type response =
  | R_failed of string list
  | R_no_such_file of string list
  | R_ok_hello
  | R_ok_lock
  | R_ok_unlock
  | R_ok_read_dir of Path.Filename.t list
  | R_ok_write_file_eof
  | R_ok_read_file_chunk of string
  | R_ok_read_file_eof
  | R_ok_stat of stat
  | R_ok_remove_file

let show_response = function
  | R_failed _ ->
      "R_failed"
  | R_no_such_file _ ->
      "R_no_such_file"
  | R_ok_hello ->
      "R_ok_hello"
  | R_ok_lock ->
      "R_ok_lock"
  | R_ok_unlock ->
      "R_ok_unlock"
  | R_ok_read_dir _ ->
      "R_ok_read_dir"
  | R_ok_write_file_eof ->
      "R_ok_write_file_eof"
  | R_ok_read_file_chunk data ->
      "R_ok_read_file_chunk (" ^ string_of_int (String.length data) ^ " bytes)"
  | R_ok_read_file_eof ->
      "R_ok_read_file_eof"
  | R_ok_stat (File { size; mtime }) ->
      "R_ok_stat (File { size = " ^ string_of_int size ^
      "; mtime = " ^ string_of_float mtime ^ " })"
  | R_ok_stat Dir ->
      "R_ok_stat Dir"
  | R_ok_remove_file ->
      "R_ok_remove_file"

(* TODO: share the following functions that were copy-pasted from State?
   Or maybe it's safer that way, in case we want to change it for one protocol only? *)

let write_filename buffer (value: Path.Filename.t) =
  W.(string int_u16) buffer (Path.Filename.show value)

let read_filename buffer: Path.Filename.t =
  let filename = R.(string int_u16) buffer in
  match Path.Filename.parse filename with
    | None ->
        raise (Failed [ sf "invalid filename: %S" filename ])
    | Some filename ->
        filename

let write_path buffer (path: Device_common.path) =
  W.(list int_u16) write_filename buffer path

let read_path buffer: Device_common.path =
  R.(list int_u16) read_filename buffer

let write_file_path buffer ((dir_path, filename): Device_common.file_path) =
  write_path buffer dir_path;
  write_filename buffer filename

let read_file_path buffer: Device_common.file_path =
  let dir_path = read_path buffer in
  let filename = read_filename buffer in
  dir_path, filename

let write_query buffer = function
  | Q_hello path ->
      W.(string @@ fun _ _ -> ()) buffer "FQhello1";
      W.(string int_u16) buffer (Path.Any_relativity.show path)
  | Q_lock file_path ->
      W.(string @@ fun _ _ -> ()) buffer "FQlock01";
      write_file_path buffer file_path
  | Q_unlock ->
      W.(string @@ fun _ _ -> ()) buffer "FQunlok1"
  | Q_read_dir path ->
      W.(string @@ fun _ _ -> ()) buffer "FQrddir1";
      write_path buffer path
  | Q_write_file file_path ->
      W.(string @@ fun _ _ -> ()) buffer "FQwrfil1";
      write_file_path buffer file_path
  | Q_write_file_chunk chunk ->
      W.(string @@ fun _ _ -> ()) buffer "FQwfchk1";
      W.(string int_32) buffer chunk
  | Q_write_file_eof ->
      W.(string @@ fun _ _ -> ()) buffer "FQwfeof1"
  | Q_read_file file_path ->
      W.(string @@ fun _ _ -> ()) buffer "FQrdfil1";
      write_file_path buffer file_path
  | Q_stat path ->
      W.(string @@ fun _ _ -> ()) buffer "FQstat01";
      write_path buffer path
  | Q_remove_file file_path ->
      W.(string @@ fun _ _ -> ()) buffer "FQrmfil1";
      write_file_path buffer file_path

let read_query buffer =
  let tag = R.(string @@ fun _ -> 8) buffer in
  match tag with
    | "FQhello1" ->
        let path = R.(string int_u16) buffer in
        let path =
          match Path.parse path with
            | None ->
                raise (Failed [ sf "invalid path: %S" path ])
            | Some path ->
                Path.Any.to_dir path
        in
        Q_hello path
    | "FQlock01" ->
        let file_path = read_file_path buffer in
        Q_lock file_path
    | "FQunlok1" ->
        Q_unlock
    | "FQrddir1" ->
        let path = read_path buffer in
        Q_read_dir path
    | "FQwrfil1" ->
        let file_path = read_file_path buffer in
        Q_write_file file_path
    | "FQwfchk1" ->
        let chunk = R.(string int_32) buffer in
        Q_write_file_chunk chunk
    | "FQwfeof1" ->
        Q_write_file_eof
    | "FQrdfil1" ->
        let file_path = read_file_path buffer in
        Q_read_file file_path
    | "FQstat01" ->
        let path = read_path buffer in
        Q_stat path
    | "FQrmfil1" ->
        let file_path = read_file_path buffer in
        Q_remove_file file_path
    | _ ->
        raise (Failed [ sf "unknown query tag: %S" tag ])

let write_stat buffer (stat: Device_common.stat) =
  match stat with
    | File { size; mtime } ->
        W.int_u8 buffer 0;
        W.int_64 buffer size;
        W.float_64 buffer mtime
    | Dir ->
        W.int_u8 buffer 1

let read_stat buffer: Device_common.stat =
  match R.int_u8 buffer with
    | 0 ->
        let size = R.int_64 buffer in
        let mtime = R.float_64 buffer in
        File { size; mtime }
    | 1 ->
        Dir
    | tag ->
        raise (Failed [ sf "unknown stat tag: %d" tag ])

let write_response buffer = function
  | R_failed msg ->
      W.(string @@ fun _ _ -> ()) buffer "FRfaild1";
      W.(list int_u8 (string int_u16)) buffer msg
  | R_no_such_file msg ->
      W.(string @@ fun _ _ -> ()) buffer "FRnofil1";
      W.(list int_u8 (string int_u16)) buffer msg
  | R_ok_hello ->
      W.(string @@ fun _ _ -> ()) buffer "FRhello1"
  | R_ok_lock ->
      W.(string @@ fun _ _ -> ()) buffer "FRlock01"
  | R_ok_unlock ->
      W.(string @@ fun _ _ -> ()) buffer "FRunlok1"
  | R_ok_read_dir filenames ->
      W.(string @@ fun _ _ -> ()) buffer "FRrddir1";
      W.(list int_u16 write_filename) buffer filenames
  | R_ok_write_file_eof ->
      W.(string @@ fun _ _ -> ()) buffer "FRwreof1"
  | R_ok_read_file_chunk chunk  ->
      W.(string @@ fun _ _ -> ()) buffer "FRrfchk1";
      W.(string int_32) buffer chunk
  | R_ok_read_file_eof ->
      W.(string @@ fun _ _ -> ()) buffer "FRrdeof1"
  | R_ok_stat stat ->
      W.(string @@ fun _ _ -> ()) buffer "FRstat01";
      write_stat buffer stat
  | R_ok_remove_file ->
      W.(string @@ fun _ _ -> ()) buffer "FRrmfil1"

let read_response buffer =
  let tag = R.(string @@ fun _ -> 8) buffer in
  match tag with
    | "FRfaild1" ->
        let msg = R.(list int_u8 (string int_u16)) buffer in
        R_failed msg
    | "FRnofil1" ->
        let msg = R.(list int_u8 (string int_u16)) buffer in
        R_no_such_file msg
    | "FRhello1" ->
        R_ok_hello
    | "FRlock01" ->
        R_ok_lock
    | "FRunlok1" ->
        R_ok_unlock
    | "FRrddir1" ->
        let filenames = R.(list int_u16 read_filename) buffer in
        R_ok_read_dir filenames
    | "FRwreof1" ->
        R_ok_write_file_eof
    | "FRrfchk1" ->
        let chunk = R.(string int_32) buffer in
        R_ok_read_file_chunk chunk
    | "FRrdeof1" ->
        R_ok_read_file_eof
    | "FRstat01" ->
        let stat = read_stat buffer in
        R_ok_stat stat
    | "FRrmfil1" ->
        R_ok_remove_file
    | _ ->
        raise (Failed [ sf "unknown response tag: %S" tag ])

let encode_query query =
  W.to_string @@ fun buffer ->
  write_query buffer query

let decode_query buffer =
  decode_rawbin_or_eof read_query buffer

let encode_response response =
  W.to_string @@ fun buffer ->
  write_response buffer response

let decode_response buffer =
  decode_rawbin_or_eof read_response buffer
