(* TODO: remove what's not actually used *)

include Mysc

module W = Rawbin.Write
module R = Rawbin.Read

(* We don't want to use [echo] but [Prout.echo]. *)
type do_not_use = Do_not_use
let echo = Do_not_use

module Path = Typath.UNIX

module Hash_set = Set.Make (Hash)

let (//) = Path.concat

type ('a, 'e) r =
  | OK of 'a
  | ERROR of { code: 'e; msg: string list }

let unit = OK ()
let ok x = OK x
let error code msg = ERROR { code; msg }
let failed = error `failed

let (let*) r f =
  match r with
    | OK x -> f x
    | ERROR _ as x -> x

let rec list_iter_e l f =
  match l with
    | [] ->
        OK ()
    | head :: tail ->
        let* () = f head in
        list_iter_e tail f

let list_map_e l f =
  let rec map acc = function
    | [] ->
        OK (List.rev acc)
    | head :: tail ->
        let* x = f head in
        map (x :: acc) tail
  in
  map [] l

let rec list_fold_e acc l f =
  match l with
    | [] ->
        ok acc
    | head :: tail ->
        let* acc = f acc head in
        list_fold_e acc tail f

let rec list_filter_e ?(acc = []) l f =
  match l with
    | [] ->
        ok (List.rev acc)
    | head :: tail ->
        let* keep = f head in
        if keep then
          list_filter_e ~acc: (head :: acc) tail f
        else
          list_filter_e ~acc tail f

let rec list_filter_map_e ?(acc = []) l f =
  match l with
    | [] ->
        ok (List.rev acc)
    | head :: tail ->
        let* mapped = f head in
        match mapped with
          | None ->
              list_filter_map_e ~acc tail f
          | Some mapped ->
              list_filter_map_e ~acc: (mapped :: acc) tail f

let opt_map_e o f =
  match o with
    | None -> ok None
    | Some x -> let* y = f x in ok (Some y)

let trace error_message = function
  | OK _ as x -> x
  | ERROR { code; msg } -> ERROR { code; msg = error_message :: msg }

let dot_filou = Path.Filename.parse_exn ".filou"
let config_path_in_dot_filou = [], Path.Filename.parse_exn "config"

exception Failed of string list

let decode_rawbin_or_eof read buffer =
  try
    ok (read buffer)
  with
    | Failed msg ->
        failed msg
    | End_of_file ->
        error `end_of_file [ "end of file" ]

let decode_rawbin read buffer =
  try
    ok (read buffer)
  with
    | Failed msg ->
        failed msg
    | End_of_file ->
        failed [ "end of file" ]

let decode_rawbin_string read string =
  let buffer = R.from_string string in
  decode_rawbin read buffer

let warn x = Printf.ksprintf (Prout.echo "Warning: %s") x

let warn_msg error_msg x =
  Printf.ksprintf
    (fun s -> Prout.echo "Warning: %s: %s" s (String.concat ": " error_msg))
    x

let rec list_take ?(acc = []) n l =
  if n <= 0 then
    List.rev acc
  else
    match l with
      | [] ->
          List.rev acc
      | head :: tail ->
          list_take ~acc: (head :: acc) (n - 1) tail

let show_size size =
  (* By using string_of_int instead of divisions and modulos we
     are more compatible with 32bit architectures. *)
  let str = string_of_int size in
  let len = String.length str in
  if len <= 3 then
    Printf.sprintf "%s B" str
  else
    let with_unit unit =
      let f =
        match String.length str mod 3 with
          | 1 -> Printf.sprintf "%c.%c%c %s"
          | 2 -> Printf.sprintf "%c%c.%c %s"
          | _ -> Printf.sprintf "%c%c%c %s"
      in
      f str.[0] str.[1] str.[2] unit
    in
    if len <= 6 then with_unit "kB" else
    if len <= 9 then with_unit "MB" else
    if len <= 12 then with_unit "GB" else
    if len <= 14 then with_unit "TB" else
      Printf.sprintf "%d TB" (size / 1_000_000_000_000)
