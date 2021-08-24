open Misc

type clone =
  {
    main_location: Device.location;
  }

let write_clone buffer { main_location } =
  W.(string int_u16) buffer (Device.show_location main_location)

let read_clone buffer =
  let main_location = R.(string int_u16) buffer in
  match Device.parse_location RW main_location with
    | ERROR { code = `failed; msg } ->
        raise (Failed msg)
    | OK main_location ->
        { main_location }
