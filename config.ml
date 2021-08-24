open Misc

type clone =
  {
    main_location: Device.location;
  }

type t =
  | Main
  | Clone of clone

let write buffer = function
  | Main ->
      W.int_u8 buffer 0
  | Clone { main_location } ->
      W.int_u8 buffer 1;
      W.(string int_u16) buffer (Device.show_location main_location)

let read buffer =
  (* TODO: expect eof? *)
  match R.int_u8 buffer with
    | 0 ->
        Main
    | 1 ->
        let main_location = R.(string int_u16) buffer in
        (
          match Device.parse_location RW main_location with
            | ERROR { code = `failed; msg } ->
                raise (Failed msg)
            | OK main_location ->
                Clone { main_location }
        )
    | tag ->
        raise (Failed [ sf "unexpected config type: %d" tag ])
