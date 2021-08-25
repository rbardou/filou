open Misc

type clone =
  {
    main_location: Device.location;
    no_cache: bool;
  }

type t =
  | Main
  | Clone of clone

let write buffer config =
  W.(string @@ fun _ _ -> ()) buffer "Fconfig1";
  match config with
    | Main ->
        W.int_u8 buffer 0
    | Clone { main_location; no_cache } ->
        W.int_u8 buffer 1;
        W.(string int_u16) buffer (Device.show_location main_location);
        W.bool buffer no_cache

let read buffer =
  let typ = R.(string @@ fun _ -> 8) buffer in
  if typ <> "Fconfig1" then
    raise (Failed [ sf "unknown config file type: %S" typ ]);
  (* TODO: expect eof? *)
  match R.int_u8 buffer with
    | 0 ->
        Main
    | 1 ->
        let main_location = R.(string int_u16) buffer in
        let main_location =
          match Device.parse_location RW main_location with
            | ERROR { code = `failed; msg } ->
                raise (Failed msg)
            | OK main_location ->
                main_location
        in
        let no_cache = R.bool buffer in
        Clone { main_location; no_cache }
    | tag ->
        raise (Failed [ sf "unexpected config type: %d" tag ])
