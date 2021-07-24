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
  | R_ok_stat (File { size }) ->
      "R_ok_stat (File { size = " ^ string_of_int size ^ " })"
  | R_ok_stat Dir ->
      "R_ok_stat Dir"
  | R_ok_remove_file ->
      "R_ok_remove_file"

open Protype
open Device_common.T

let dir_path: Path.dir Path.any_relativity t =
  let decode string =
    Option.map' (Path.parse string) @@ fun path ->
    path |> Path.Any.to_dir
  in
  convert_partial string ~encode: Path.Any_relativity.show ~decode

let query: query t =
  let q_hello = case "Q_hello" dir_path (fun x -> Q_hello x) in
  let q_lock = case "Q_lock" file_path (fun x -> Q_lock x) in
  let q_unlock = case "Q_unlock" unit (fun () -> Q_unlock) in
  let q_read_dir = case "Q_read_dir" path (fun x -> Q_read_dir x) in
  let q_write_file = case "Q_write_file" file_path (fun x -> Q_write_file x) in
  let q_write_file_chunk = case "Q_write_file_chunk" string (fun x -> Q_write_file_chunk x) in
  let q_write_file_eof = case "Q_write_file_eof" unit (fun () -> Q_write_file_eof) in
  let q_read_file = case "Q_read_file" file_path (fun x -> Q_read_file x) in
  let q_stat = case "Q_stat" path (fun x -> Q_stat x) in
  let q_remove_file = case "Q_remove_file" file_path (fun x -> Q_remove_file x) in
  variant [
    Case q_hello;
    Case q_lock;
    Case q_unlock;
    Case q_read_dir;
    Case q_write_file;
    Case q_write_file_chunk;
    Case q_write_file_eof;
    Case q_read_file;
    Case q_stat;
    Case q_remove_file;
  ] @@
  function
  | Q_hello x -> value q_hello x
  | Q_lock x -> value q_lock x
  | Q_unlock -> value q_unlock ()
  | Q_read_dir x -> value q_read_dir x
  | Q_write_file x -> value q_write_file x
  | Q_write_file_chunk x -> value q_write_file_chunk x
  | Q_write_file_eof -> value q_write_file_eof ()
  | Q_read_file x -> value q_read_file x
  | Q_stat x -> value q_stat x
  | Q_remove_file x -> value q_remove_file x

let stat: stat t =
  let file = case "File" int (fun x -> Device_common.File { size = x }) in
  let dir = case "Dir" unit (fun () -> Device_common.Dir) in
  variant [ Case file; Case dir ] @@
  function File { size = x } -> value file x | Dir -> value dir ()

let response: response t =
  let r_failed = case "R_failed" (list string) (fun x -> R_failed x) in
  let r_no_such_file = case "R_no_such_file" (list string) (fun x -> R_no_such_file x) in
  let r_ok_hello = case "R_ok_hello" unit (fun () -> R_ok_hello) in
  let r_ok_lock = case "R_ok_lock" unit (fun () -> R_ok_lock) in
  let r_ok_unlock = case "R_ok_unlock" unit (fun () -> R_ok_unlock) in
  let r_ok_read_dir = case "R_ok_read_dir" (list filename) (fun x -> R_ok_read_dir x) in
  let r_ok_write_file_eof = case "R_ok_write_file_eof" unit (fun () -> R_ok_write_file_eof) in
  let r_ok_read_file_chunk =
    case "R_ok_read_file_chunk" string (fun x -> R_ok_read_file_chunk x)
  in
  let r_ok_read_file = case "R_ok_read_file_eof" unit (fun () -> R_ok_read_file_eof) in
  let r_ok_stat = case "R_ok_stat" stat (fun x -> R_ok_stat x) in
  let r_ok_remove_file = case "R_ok_remove_file" unit (fun () -> R_ok_remove_file) in
  variant [
    Case r_failed;
    Case r_no_such_file;
    Case r_ok_hello;
    Case r_ok_lock;
    Case r_ok_unlock;
    Case r_ok_read_dir;
    Case r_ok_write_file_eof;
    Case r_ok_read_file_chunk;
    Case r_ok_read_file;
    Case r_ok_stat;
    Case r_ok_remove_file;
  ] @@
  function
  | R_failed x -> value r_failed x
  | R_no_such_file x -> value r_no_such_file x
  | R_ok_hello -> value r_ok_hello ()
  | R_ok_lock -> value r_ok_lock ()
  | R_ok_unlock -> value r_ok_unlock ()
  | R_ok_read_dir x -> value r_ok_read_dir x
  | R_ok_write_file_eof -> value r_ok_write_file_eof ()
  | R_ok_read_file_chunk x -> value r_ok_read_file_chunk x
  | R_ok_read_file_eof -> value r_ok_read_file ()
  | R_ok_stat x -> value r_ok_stat x
  | R_ok_remove_file -> value r_ok_remove_file ()
