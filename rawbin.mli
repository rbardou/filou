module Write:
sig
  type buffer
  val flush: buffer -> unit
  val to_custom: ?buffer_capacity: int -> ?blit_threshold: int -> ?flush: (unit -> unit) ->
    (bytes -> int -> int -> unit) -> (string -> int -> int -> unit) -> buffer
  val to_buffer: ?buffer_capacity: int -> Buffer.t -> buffer
  val to_string: ?buffer_capacity: int -> (buffer -> unit) -> string
  val to_channel: ?buffer_capacity: int -> ?blit_threshold: int -> out_channel -> buffer
  val to_file: ?buffer_capacity: int -> ?blit_threshold: int -> string ->
    (buffer -> unit) -> unit

  val unit: buffer -> unit -> unit
  val bool: buffer -> bool -> unit
  val char: buffer -> char -> unit
  val int_8: buffer -> int -> unit
  val int_u8: buffer -> int -> unit
  val int_16: buffer -> int -> unit
  val int_u16: buffer -> int -> unit
  val int_32: buffer -> int -> unit
  val int_64: buffer -> int -> unit
  val int32: buffer -> Int32.t -> unit
  val int64: buffer -> Int64.t -> unit
  val float_32: buffer -> float -> unit
  val float_64: buffer -> float -> unit
  val string: (buffer -> int -> unit) -> buffer -> string -> unit
  val substring: (buffer -> int -> unit) -> buffer -> string -> int -> int -> unit
  val bytes: (buffer -> int -> unit) -> buffer -> bytes -> unit
  val subbytes: (buffer -> int -> unit) -> buffer -> bytes -> int -> int -> unit
  val option: (buffer -> 'a -> unit) -> buffer -> 'a option -> unit
  val array: (buffer -> int -> unit) -> (buffer -> 'a -> unit) -> buffer -> 'a array -> unit
  val list: (buffer -> int -> unit) -> (buffer -> 'a -> unit) -> buffer -> 'a list -> unit
end

module Read:
sig
  type buffer
  val from_string: ?ofs: int -> ?len: int -> string -> buffer
  val from_bytes: ?ofs: int -> ?len: int -> bytes -> buffer
  val from_input: ?buffer_capacity: int -> ?strict: bool ->
    (bytes -> int -> int -> int) -> buffer
  val from_channel: ?buffer_capacity: int -> ?strict: bool -> in_channel -> buffer
  val from_file: ?buffer_capacity: int -> string -> (buffer -> 'a) -> 'a

  (** All functions below may raise [End_of_file]. *)

  val unit: buffer -> unit
  val bool: buffer -> bool
  val char: buffer -> char
  val int_8: buffer -> int
  val int_u8: buffer -> int
  val int_16: buffer -> int
  val int_u16: buffer -> int
  val int_32: buffer -> int
  val int_64: buffer -> int
  val int32: buffer -> Int32.t
  val int64: buffer -> Int64.t
  val float_32: buffer -> float
  val float_64: buffer -> float
  val string: (buffer -> int) -> buffer -> string
  val bytes: (buffer -> int) -> buffer -> bytes
  val option: (buffer -> 'a) -> buffer -> 'a option
  val array: (buffer -> int) -> (buffer -> 'a) -> buffer -> 'a array
  val list: (buffer -> int) -> (buffer -> 'a) -> buffer -> 'a list
end
