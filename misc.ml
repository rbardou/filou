(* TODO: remove what's not actually used *)

(* We use version numbers starting from 0xF1100, which is supposed to look like "FILOU".
   This acts as a kind of magic number. *)
let protocol_version = 0xF1100

include Mysc

(* We don't want to use [echo] but [Prout.echo]. *)
type do_not_use = Do_not_use
let echo = Do_not_use

module Path = Typath.UNIX

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

let opt_map_e o f =
  match o with
    | None -> ok None
    | Some x -> let* y = f x in ok (Some y)

let trace error_message = function
  | OK _ as x -> x
  | ERROR { code; msg } -> ERROR { code; msg = error_message :: msg }

let dot_filou = Path.Filename.parse_exn ".filou"
let dot_filou_config = [ dot_filou ], Path.Filename.parse_exn "config"

let decode_robin_string typ string =
  match
    Protype_robin.Decode.from_string ~ignore_unknown_fields: true typ string
  with
    | Ok value ->
        ok value
    | Error error ->
        failed [ Protype_robin.Decode.show_error error ]

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
