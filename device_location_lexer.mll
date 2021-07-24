{
  open Misc

  let with_int string f =
    match int_of_string_opt string with
      | None ->
          `invalid
      | Some int ->
          f int

  let with_path string f =
    match Path.parse string with
      | None ->
          `invalid
      | Some path ->
          f path

  let with_option with_converter option f =
    match option with
      | None ->
          f None
      | Some string ->
          with_converter string @@ fun value -> f (Some value)
}

let username = ['a'-'z' 'A'-'Z' '0'-'9' '_' '-']+

let hostname = ['a'-'z' 'A'-'Z' '0'-'9' '_' '-' '.']+

let path = [^'\n' '\000']+

(* To ensure that we don't accidentally read ssh+filou://something as a local path,
   we first parse the protocol. If there is no protocol, then we parse as a local path. *)
rule location = parse
  | "file://"
      { location_file None lexbuf }
  | "filou+ssh://"
  | "ssh+filou://"
      { location_ssh_filou lexbuf }
  | _ as c
      { location_file (Some c) lexbuf }
  | eof
      { `invalid }

and location_file first_char = parse
  | (path as path) eof
      {
        let path = match first_char with None -> path | Some c -> String.make 1 c ^ path in
        with_path path @@ fun path -> `local path
      }

and location_ssh_filou = parse
  | ((username as user) '@')?
      (hostname as host)
      (':' (['0'-'9']+ as port))?
      '/' (path as path) eof
      {
        with_path path @@ fun path ->
        with_option with_int port @@ fun port ->
        `ssh_filou (user, host, port, path)
      }
