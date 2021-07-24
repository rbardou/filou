open Misc
open Device_common

let debug = Sys.getenv_opt "FILOU_DEBUG" = Some "yes"

type connection =
  {
    ssh_pid: int;
    stdin: out_channel;
    stdout: in_channel;
  }

(* Must be in a separate record so that we can copy [location] in [sublocation]. *)
type connection_status =
  {
    mutable status: connection option;
  }

type location =
  {
    username: string option;
    hostname: string;
    port: int option;
    path: Path.dir Path.any_relativity;
    connection: connection_status;
  }

let make_location ?username ~hostname ?port ~path () =
  { username; hostname; port; path; connection = { status = None } }

let sublocation (location: location) (filename: Path.Filename.t) =
  {
    location with
      path = Path.Any_relativity.concat location.path (Path.Dir (filename, Current));
  }

let send connection (query: Protocol.query) =
  match connection.status with
    | None ->
        failed [ "cannot send"; "connection is closed" ]
    | Some status ->
        let string =
          Protype_robin.Encode.to_string ~version: protocol_version Protocol.query query
        in
        match output_string status.stdin string with
          | exception Sys_error message ->
              failed [ "failed to send query through SSH"; message ]
          | () ->
              flush status.stdin;
              unit

let receive_bytes = Bytes.create 8192

let receive ?query connection filter =
  match connection.status with
    | None ->
        failed [ "cannot receive"; "connection is closed" ]
    | Some status ->
        match
          Protype_robin.Decode.value ~ignore_unknown_fields: true Protocol.response
          @@ fun len ->
          let len =
            input status.stdout receive_bytes 0 (min (Bytes.length receive_bytes) len)
          in
          Bytes.sub_string receive_bytes 0 len
        with
          | exception Sys_error message ->
              failed [ "failed to receive response through SSH"; message ]
          | Error { error = Robin_error End_of_file; _ } ->
              (
                match Unix.waitpid [] status.ssh_pid with
                  | exception Unix.Unix_error (error, _, _) ->
                      failed [
                        "end of file";
                        "additionally, failed to get SSH process status";
                        Unix.error_message error;
                      ]
                  | _, status ->
                      connection.status <- None;
                      let status =
                        match status with
                          | WEXITED code -> sf "SSH exited with code %d" code
                          | WSIGNALED n -> sf "SSH was killed by signal %d" n
                          | WSTOPPED n -> sf "SSH was stopped by signal %d" n
                      in
                      failed [ "end of file"; status ]
              )
          | Error error ->
              failed [ "failed to decode response"; Protype_robin.Decode.show_error error ]
          | Ok response ->
              match response with
                | R_failed message ->
                    failed ("remote responded with an error" :: message)
                | _ ->
                    match filter response with
                      | None ->
                          failed (
                            "remote responded with an unexpected response" ::
                            (
                              match query with
                                | None ->
                                    []
                                | Some query ->
                                    [ "query: " ^ Protocol.show_query query ]
                            ) @
                            [ "response: " ^ Protocol.show_response response ]
                          )
                      | Some value ->
                          ok value

let send_and_receive connection query filter =
  let* () = send connection query in
  receive ~query connection filter

let send_without_response connection query =
  let* () = send connection query in
  match connection.status with
    | None ->
        unit
    | Some status ->
        (* This is a bit hackish. But we check that there is no response yet. *)
        match Unix.select [ Unix.descr_of_in_channel status.stdout ] [] [] 0. with
          | exception Unix.Unix_error (EINTR, _, _) ->
              unit
          | [ _ ], _, _ ->
              let* response = receive connection (fun x -> Some x) in
              failed [
                "expected no response, but received " ^ Protocol.show_response response;
              ]
          | _ ->
              unit

let connections: connection_status list ref = ref []

let pipe () =
  match Unix.pipe () with
    | exception Unix.Unix_error (error, _, _) ->
        failed [
          "failed to create pipe";
          Unix.error_message error;
        ]
    | pipe_exit, pipe_entrance ->
        ok (pipe_exit, pipe_entrance)

let set_close_on_exec fd =
  try Unix.set_close_on_exec fd with Unix.Unix_error _ -> ()

let create_process command argv stdin stdout stderr =
  match Unix.create_process command argv stdin stdout stderr with
    | exception Unix.Unix_error (error, _, _) ->
        failed [
          "failed to create process";
          Unix.error_message error;
        ]
    | pid ->
        ok pid

let close fd =
  try Unix.close fd with Unix.Unix_error _ -> ()

let spawn command args =
  trace (
    sf "failed to spawn: %s"
      (String.concat " " (List.map Filename.quote_if_needed (command :: args)))
  ) @@
  let argv = Array.of_list (command :: args) in
  let* stdin_exit, stdin_entrance = pipe () in
  let* stdout_exit, stdout_entrance = pipe () in
  set_close_on_exec stdin_entrance;
  set_close_on_exec stdout_exit;
  let result = create_process command argv stdin_exit stdout_entrance Unix.stderr in
  close stdin_exit;
  close stdout_entrance;
  match result with
    | ERROR _ as x ->
        close stdin_entrance;
        close stdout_exit;
        x
    | OK pid ->
        ok (
          pid,
          Unix.out_channel_of_descr stdin_entrance,
          Unix.in_channel_of_descr stdout_exit
        )

let connect location =
  match location.connection.status with
    | Some _ ->
        ok location.connection
    | None ->
        let url =
          "ssh://" ^
          (
            match location.username with
              | None -> ""
              | Some username -> username ^ "@"
          ) ^
          location.hostname ^
          (
            match location.port with
              | None -> ""
              | Some port -> ":" ^ string_of_int port
          )
        in
        let args =
          if debug then
            [ url; "FILOU_DEBUG=yes"; "filou"; "listen" ]
          else
            [ url; "filou"; "listen" ]
        in
        (* TODO: when SSH exits (e.g. if we Ctrl+C the host Filou),
           it doesn't kill the remote filou *)
        let* ssh_pid, stdin, stdout = spawn "ssh" args in
        let connection =
          {
            ssh_pid;
            stdin;
            stdout;
          }
        in
        location.connection.status <- Some connection;
        connections := location.connection :: !connections;
        let* () =
          send_and_receive location.connection (Q_hello location.path) @@ function
          | R_ok_hello -> Some ()
          | _ -> None
        in
        ok location.connection

let close_all_connections () =
  let all_connections = !connections in
  connections := [];
  List.iter' all_connections @@ fun connection ->
  match connection.status with
    | None ->
        ()
    | Some status ->
        close_in status.stdout;
        (try close_out status.stdin with Sys_error _ -> ());
        try
          Unix.kill status.ssh_pid Sys.sigkill
        with Unix.Unix_error (error, _, _) ->
          warn "failed to kill SSH: %s" (Unix.error_message error)

let () = at_exit close_all_connections

let send_and_receive_loc location query filter =
  let* connection = connect location in
  send_and_receive connection query filter

let send_without_response_loc location query =
  let* connection = connect location in
  send_without_response connection query

let lock (mode: mode) (location: location) (path: file_path) =
  match mode with
    | RO ->
        unit
    | RW ->
        send_and_receive_loc location (Q_lock path) @@ function
        | R_ok_lock -> Some ()
        | _ -> None

let unlock (mode: mode) (location: location) =
  match mode with
    | RO ->
        unit
    | RW ->
        send_and_receive_loc location Q_unlock @@ function
        | R_ok_unlock -> Some ()
        | _ -> None

let iter_read_dir (location: location) (path: path) f =
  let* response =
    send_and_receive_loc location (Q_read_dir path) @@ function
    | R_no_such_file message -> Some (`no_such_file message)
    | R_ok_read_dir contents -> Some (`ok contents)
    | _ -> None
  in
  match response with
    | `no_such_file message ->
        error `no_such_file message
    | `ok contents ->
        list_iter_e contents f

let write_file_incrementally_gen sub (mode: mode) (location: location) (path: file_path)
    write_contents =
  match mode with
    | RO ->
        read_only ()
    | RW ->
        let* () = send_without_response_loc location (Q_write_file path) in
        let* () =
          write_contents @@ fun string ofs len ->
          send_without_response_loc location (Q_write_file_chunk (sub string ofs len))
        in
        send_and_receive_loc location Q_write_file_eof @@ function
        | R_ok_write_file_eof -> Some ()
        | _ -> None

let write_file_incrementally location path write_contents =
  write_file_incrementally_gen String.sub location path write_contents

let write_file_incrementally_bytes location path write_contents =
  write_file_incrementally_gen Bytes.sub_string location path write_contents

type read_state =
  | RS_empty
  | RS_bytes of string * int (* int = offset in string of next byte to read *)
  | RS_eof

let read_file_incrementally (location: location) (path: file_path) read_contents =
  let* connection = connect location in
  let* first_response =
    send_and_receive connection (Q_read_file path) @@ function
    | R_no_such_file message ->
        Some (`no_such_file message)
    | R_ok_read_file_chunk data ->
        Some (`ok (RS_bytes (data, 0)))
    | R_ok_read_file_eof ->
        Some (`ok RS_eof)
    | _ ->
        None
  in
  match first_response with
    | `no_such_file message ->
        error `no_such_file message
    | `ok state ->
        let state = ref state in
        let* result =
          read_contents @@ fun bytes ofs len ->
          match !state with
            | RS_empty ->
                (
                  receive connection @@ function
                  | R_ok_read_file_chunk data ->
                      let data_len = String.length data in
                      if data_len <= len then
                        (
                          Bytes.blit_string data 0 bytes ofs data_len;
                          Some data_len
                        )
                      else
                        (
                          Bytes.blit_string data 0 bytes ofs len;
                          state := RS_bytes (data, len);
                          Some len
                        )
                  | R_ok_read_file_eof ->
                      state := RS_eof;
                      Some 0
                  | _ ->
                      None
                )
            | RS_bytes (data, data_ofs) ->
                let remaining_len = String.length data - data_ofs in
                if remaining_len <= len then
                  (
                    Bytes.blit_string data data_ofs bytes ofs remaining_len;
                    state := RS_empty;
                    ok remaining_len
                  )
                else
                  (
                    Bytes.blit_string data data_ofs bytes ofs len;
                    state := RS_bytes (data, data_ofs + len);
                    ok len
                  )
            | RS_eof ->
                ok 0
        in
        let rec read_until_eof () =
          let* continue =
            receive connection @@ function
            | R_ok_read_file_chunk _ ->
                Some true
            | R_ok_read_file_eof ->
                Some false
            | _ ->
                None
          in
          if continue then read_until_eof () else unit
        in
        let* () =
          match !state with
            | RS_empty | RS_bytes _ ->
                read_until_eof ()
            | RS_eof ->
                unit
        in
        ok result

let stat (location: location) (path: path) =
  let* result =
    send_and_receive_loc location (Q_stat path) @@ function
    | R_no_such_file message -> Some (`no_such_file message)
    | R_ok_stat stat -> Some (`ok stat)
    | _ -> None
  in
  match result with
    | `no_such_file message ->
        error `no_such_file message
    | `ok stat ->
        ok stat

let remove_file (mode: mode) (location: location) (file_path: file_path) =
  match mode with
    | RO ->
        read_only ()
    | RW ->
        let* result =
          send_and_receive_loc location (Q_remove_file file_path) @@ function
          | R_no_such_file message -> Some (`no_such_file message)
          | R_ok_remove_file -> Some `ok
          | _ -> None
        in
        match result with
          | `no_such_file message ->
              error `no_such_file message
          | `ok ->
              unit
