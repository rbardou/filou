type t

val bin_length: int

val hex_length: int

val to_hex: t -> string

val to_bin: t -> string

val of_hex: string -> t option

val of_bin: string -> t option

val compare: t -> t -> int

val string: string -> t

val hex_of_string: string -> string
