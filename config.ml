open Misc

type clone =
  {
    main_location: Device.location;
    no_cache: bool;
    ignore_filters: string list;
    ignore_filters_rex: Re.re;
  }

let match_nothing = Re.(compile empty)

type t =
  | Main
  | Clone of clone

let write buffer config =
  W.(string @@ fun _ _ -> ()) buffer "Fconfig2";
  match config with
    | Main ->
        W.int_u8 buffer 0
    | Clone { main_location; no_cache; ignore_filters; ignore_filters_rex = _ } ->
        W.int_u8 buffer 1;
        W.(string int_u16) buffer (Device.show_location main_location);
        W.bool buffer no_cache;
        W.(list int_32 (string int_u16)) buffer ignore_filters

let read_fconfig1 buffer =
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
        Clone {
          main_location;
          no_cache;
          ignore_filters = [];
          ignore_filters_rex = match_nothing;
        }
    | tag ->
        raise (Failed [ sf "unexpected config type: %d" tag ])

let read_fconfig2 buffer =
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
        let ignore_filters = R.(list int_32 (string int_u16)) buffer in
        let ignore_filters_rex =
          match ignore_filters with
            | [] ->
                (* Re's doc does not specify what [alt []] does. *)
                match_nothing
            | _ ->
                List.map (fun filter -> Re.Pcre.re filter) ignore_filters
                |> Re.alt |> Re.compile
        in
        Clone {
          main_location;
          no_cache;
          ignore_filters;
          ignore_filters_rex;
        }
    | tag ->
        raise (Failed [ sf "unexpected config type: %d" tag ])

(* TODO: expect eof? *)
let read buffer =
  match R.(string @@ fun _ -> 8) buffer with
    | "Fconfig1" -> read_fconfig1 buffer
    | "Fconfig2" -> read_fconfig2 buffer
    | tag -> raise (Failed [ sf "unsupported config file format: %s" tag ])
