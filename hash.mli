type t

val bin_length: int

val hex_length: int

val to_hex: t -> string

val to_bin: t -> string

val of_hex: string -> t option

val of_bin: string -> t option

val compare: t -> t -> int

val hex_of_string: string -> string

val string_of_hex: string -> string option

val string: string -> t

type partial

val start: unit -> partial

val feed_bytes: partial -> bytes -> int -> int -> partial

val finish: partial -> t
