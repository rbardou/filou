(* TODO: remove what's not actually used *)

include Mysc

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

let warn x = Printf.ksprintf (echo "Warning: %s") x
