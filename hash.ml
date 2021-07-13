type t = string

let hex_char_of_int = function
  | 0 -> '0'
  | 1 -> '1'
  | 2 -> '2'
  | 3 -> '3'
  | 4 -> '4'
  | 5 -> '5'
  | 6 -> '6'
  | 7 -> '7'
  | 8 -> '8'
  | 9 -> '9'
  | 10 -> 'a'
  | 11 -> 'b'
  | 12 -> 'c'
  | 13 -> 'd'
  | 14 -> 'e'
  | 15 -> 'f'
  | i -> invalid_arg ("hex_char_of_int " ^ string_of_int i)

let int_of_hex_char = function
  | '0' -> 0
  | '1' -> 1
  | '2' -> 2
  | '3' -> 3
  | '4' -> 4
  | '5' -> 5
  | '6' -> 6
  | '7' -> 7
  | '8' -> 8
  | '9' -> 9
  | 'a' -> 10
  | 'b' -> 11
  | 'c' -> 12
  | 'd' -> 13
  | 'e' -> 14
  | 'f' -> 15
  | c -> invalid_arg (Printf.sprintf "int_of_hex_char: %C" c)

let hex_of_string s =
  String.init (String.length s * 2) @@ fun i ->
  if i mod 2 = 0 then
    hex_char_of_int (Char.code s.[i / 2] lsr 4)
  else
    hex_char_of_int (Char.code s.[i / 2] land 0xF)

let string_of_hex s =
  if String.length s mod 2 <> 0 then invalid_arg "string_of_hex: odd length";
  String.init (String.length s / 2) @@ fun i ->
  Char.chr ((int_of_hex_char s.[2 * i] lsl 4) lor (int_of_hex_char s.[2 * i + 1]))

let bin_length = 32
let hex_length = bin_length * 2

let to_hex = hex_of_string
let to_bin = Fun.id

let of_hex s =
  if String.length s <> hex_length then
    None
  else
    Some (string_of_hex s)

let of_bin s =
  if String.length s <> bin_length then
    None
  else
    Some s

let compare = String.compare

let string s = Digestif.SHA256.digest_string s |> Digestif.SHA256.to_raw_string

type partial = Digestif.SHA256.ctx

let start = Digestif.SHA256.init

let feed_bytes partial string off len = Digestif.SHA256.feed_bytes ~off ~len partial string

let finish partial = Digestif.SHA256.get partial |> Digestif.SHA256.to_raw_string
