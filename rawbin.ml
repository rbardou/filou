(* Minimum capacity to handle atomic writes and reads of int64 values. *)
let min_capacity = 8
let default_capacity = 512
let default_blit_threshold = 32 (* TODO: experiment on this *)

module Write =
struct

  type buffer =
    {
      bytes: bytes;
      capacity: int;
      blit_threshold: int;
      mutable position: int;
      flush: unit -> unit;
      write_bytes: bytes -> int -> int -> unit;
      write_string: string -> int -> int -> unit;
    }

  let partial_flush buffer =
    buffer.write_bytes buffer.bytes 0 buffer.position;
    buffer.position <- 0

  let flush buffer =
    partial_flush buffer;
    buffer.flush ()

  let to_custom
      ?(buffer_capacity = default_capacity)
      ?(blit_threshold = default_blit_threshold)
      ?(flush = fun () -> ())
      write_bytes write_string =
    let capacity = max min_capacity buffer_capacity in
    let blit_threshold = min capacity blit_threshold in
    {
      bytes = Bytes.create capacity;
      capacity;
      position = 0;
      flush;
      write_bytes;
      write_string;
      blit_threshold;
    }

  let to_buffer ?buffer_capacity buffer =
    to_custom ?buffer_capacity (Buffer.add_subbytes buffer) (Buffer.add_substring buffer)
      ~blit_threshold: 0

  let to_string ?(buffer_capacity = default_capacity) f =
    let buf = Buffer.create buffer_capacity in
    let buffer = to_buffer ~buffer_capacity buf in
    f buffer;
    flush buffer;
    Buffer.contents buf

  let to_channel ?buffer_capacity ?blit_threshold channel =
    to_custom ?buffer_capacity ?blit_threshold (output channel) (output_substring channel)

  let to_file ?buffer_capacity ?blit_threshold path f =
    let channel = open_out path in
    let buffer = to_channel ?buffer_capacity ?blit_threshold channel in
    match f buffer with
      | exception exn ->
          close_out channel;
          raise exn
      | () ->
          flush buffer;
          close_out channel

  (* Ensure that [buffer] contains at least [count] free bytes, by flushing if needed.
     Maximum value for [count] is [buffer.capacity]. *)
  let require count buffer =
    if count > buffer.capacity - buffer.position then
      partial_flush buffer

  let unit (_: buffer) () = ()

  let char buffer value =
    require 1 buffer;
    Bytes.set buffer.bytes buffer.position value;
    buffer.position <- buffer.position + 1

  let bool buffer value =
    char buffer (if value then '\001' else '\000')

  let int_8 buffer value =
    if value < -0x80 || value > 0x7F then
      invalid_arg ("Write.int_8: out of range (" ^ string_of_int value ^ ")");
    require 1 buffer;
    Bytes.set_int8 buffer.bytes buffer.position value;
    buffer.position <- buffer.position + 1

  let int_u8 buffer value =
    if value < 0 || value > 0xFF then
      invalid_arg ("Write.int_u8: out of range (" ^ string_of_int value ^ ")");
    require 1 buffer;
    Bytes.set_uint8 buffer.bytes buffer.position value;
    buffer.position <- buffer.position + 1

  let int_16 buffer value =
    if value < -0x8000 || value > 0x7FFF then
      invalid_arg ("Write.int_16: out of range (" ^ string_of_int value ^ ")");
    require 2 buffer;
    Bytes.set_int16_le buffer.bytes buffer.position value;
    buffer.position <- buffer.position + 2

  let int_u16 buffer value =
    if value < 0 || value > 0xFFFF then
      invalid_arg ("Write.int_u16: out of range (" ^ string_of_int value ^ ")");
    require 2 buffer;
    Bytes.set_uint16_le buffer.bytes buffer.position value;
    buffer.position <- buffer.position + 2

  let int32 buffer value =
    require 4 buffer;
    Bytes.set_int32_le buffer.bytes buffer.position value;
    buffer.position <- buffer.position + 4

  let int64 buffer value =
    require 8 buffer;
    Bytes.set_int64_le buffer.bytes buffer.position value;
    buffer.position <- buffer.position + 8

  let int_32 buffer value =
    if Int32.to_int (Int32.of_int value) <> value then
      invalid_arg ("Write.int_32: out of range (" ^ string_of_int value ^ ")");
    int32 buffer (Int32.of_int value)

  let int_64 buffer value =
    int64 buffer (Int64.of_int value)

  let float_32 buffer value =
    int32 buffer (Int32.bits_of_float value)

  let float_64 buffer value =
    int64 buffer (Int64.bits_of_float value)

  let substring size buffer value ofs len =
    size buffer len;
    if len <= buffer.blit_threshold then
      (
        (* [blit_threshold <= capacity] so we can write to [bytes]. *)
        require len buffer;
        Bytes.blit_string value ofs buffer.bytes buffer.position len;
        buffer.position <- buffer.position + len
      )
    else
      (
        partial_flush buffer;
        buffer.write_string value ofs len;
      )

  let string size buffer value =
    substring size buffer value 0 (String.length value)

  let subbytes size buffer value ofs len =
    size buffer len;
    if len <= buffer.blit_threshold then
      (
        (* [blit_threshold <= capacity] so we can write to [bytes]. *)
        require len buffer;
        Bytes.blit value ofs buffer.bytes buffer.position len;
        buffer.position <- buffer.position + len
      )
    else
      (
        partial_flush buffer;
        buffer.write_bytes value ofs len;
      )

  let bytes size buffer value =
    subbytes size buffer value 0 (Bytes.length value)

  let option contents buffer = function
    | None ->
        char buffer '\000'
    | Some value ->
        char buffer '\001';
        contents buffer value

  let array size contents buffer value =
    let len = Array.length value in
    size buffer len;
    for i = 0 to len - 1 do
      contents buffer value.(i)
    done

  let list size contents buffer value =
    size buffer (List.length value);
    List.iter (contents buffer) value

end

module Read =
struct

  type refill =
    | No_refill
    | Refill of { capacity: int; strict: bool; refill: bytes -> int -> int -> int }

  type buffer =
    {
      bytes: bytes;
      mutable position: int;
      mutable used: int;
      refill: refill;
    }

  (* We won't modify [bytes] since we set [refill] to [None]. *)
  let from_bytes ?(ofs = 0) ?len bytes =
    {
      bytes;
      position = ofs;
      used = (match len with None -> Bytes.length bytes | Some len -> ofs + len);
      refill = No_refill;
    }

  (* We use [unsafe_of_string] because [bytes] will not be modified
     because [from_bytes] sets [refill] to [None]. *)
  let from_string ?ofs ?len string =
    from_bytes ?ofs ?len (Bytes.unsafe_of_string string)

  let from_input ?(buffer_capacity = default_capacity) ?(strict = false) refill =
    {
      bytes = Bytes.create buffer_capacity;
      position = 0;
      used = 0;
      refill =
        Refill {
          capacity = buffer_capacity;
          strict;
          refill;
        };
    }

  let from_channel ?buffer_capacity ?strict channel =
    from_input ?buffer_capacity ?strict (input channel)

  let from_file ?buffer_capacity path f =
    let channel = open_in path in
    match f (from_channel ?buffer_capacity ~strict: false channel) with
      | exception exn ->
          close_in channel;
          raise exn
      | x ->
          close_in channel;
          x

  (* Ensure that [buffer] contains at least [count] bytes, by flushing if needed.
     Maximum value for [count] is [min_capacity]. *)
  let require count buffer =
    let remainder = buffer.used - buffer.position in
    if count > remainder then
      match buffer.refill with
        | No_refill ->
            raise End_of_file
        | Refill { capacity; strict; refill } ->
            (* Move remaining bytes to the beginning of the buffer. *)
            Bytes.blit
              buffer.bytes buffer.position
              buffer.bytes 0
              remainder;
            buffer.position <- 0;
            buffer.used <- remainder;
            (* Refill until there are at least [count] bytes (or until end of file). *)
            let refill_once () =
              let query_len = (if strict then count else capacity) - buffer.used in
              let filled_len = refill buffer.bytes buffer.used query_len in
              if filled_len <= 0 then raise End_of_file;
              buffer.used <- buffer.used + filled_len
            in
            refill_once ();
            while count > buffer.used do
              refill_once ()
            done

  let unit (_: buffer) = ()

  let char buffer =
    require 1 buffer;
    let value = Bytes.unsafe_get buffer.bytes buffer.position in
    buffer.position <- buffer.position + 1;
    value

  let bool buffer =
    char buffer <> '\000'

  let int_8 buffer =
    require 1 buffer;
    let value = Bytes.get_int8 buffer.bytes buffer.position in
    buffer.position <- buffer.position + 1;
    value

  let int_u8 buffer =
    require 1 buffer;
    let value = Bytes.get_uint8 buffer.bytes buffer.position in
    buffer.position <- buffer.position + 1;
    value

  let int_16 buffer =
    require 2 buffer;
    let value = Bytes.get_int16_le buffer.bytes buffer.position in
    buffer.position <- buffer.position + 2;
    value

  let int_u16 buffer =
    require 2 buffer;
    let value = Bytes.get_uint16_le buffer.bytes buffer.position in
    buffer.position <- buffer.position + 2;
    value

  let int32 buffer =
    require 4 buffer;
    let value = Bytes.get_int32_le buffer.bytes buffer.position in
    buffer.position <- buffer.position + 4;
    value

  let int64 buffer =
    require 8 buffer;
    let value = Bytes.get_int64_le buffer.bytes buffer.position in
    buffer.position <- buffer.position + 8;
    value

  let int_32 buffer =
    let int32 = int32 buffer in
    let value = Int32.to_int int32 in
    if Int32.of_int value <> int32 then
      failwith ("Read.int_32: out of range (" ^ Int32.to_string int32 ^ ")");
    value

  let int_64 buffer =
    let int64 = int64 buffer in
    let value = Int64.to_int int64 in
    if Int64.of_int value <> int64 then
      failwith ("Read.int_64: out of range (" ^ Int64.to_string int64 ^ ")");
    value

  let float_32 buffer =
    Int32.float_of_bits (int32 buffer)

  let float_64 buffer =
    Int64.float_of_bits (int64 buffer)

  let bytes size buffer =
    let len = size buffer in
    let bytes = Bytes.create len in
    if len <= buffer.used - buffer.position then
      (
        Bytes.blit buffer.bytes buffer.position bytes 0 len;
        buffer.position <- buffer.position + len;
      )
    else
      (
        match buffer.refill with
          | No_refill ->
              raise End_of_file
          | Refill { refill; _ } ->
              let remainder = buffer.used - buffer.position in
              Bytes.blit buffer.bytes buffer.position bytes 0 remainder;
              buffer.position <- 0;
              buffer.used <- 0;
              let ofs = ref remainder in
              while !ofs < len do
                let added_len = refill bytes !ofs (len - !ofs) in
                if added_len = 0 then raise End_of_file;
                ofs := !ofs + added_len
              done
      );
    bytes

  let string size buffer =
    bytes size buffer |> Bytes.unsafe_to_string

  let option contents buffer =
    if bool buffer then
      Some (contents buffer)
    else
      None

  let array size contents buffer =
    let len = size buffer in
    if len <= 0 then
      [||]
    else
      let first = contents buffer in
      let value = Array.make len first in
      for i = 1 to len - 1 do
        value.(i) <- contents buffer
      done;
      value

  let list size contents buffer =
    array size contents buffer |> Array.to_list

end
