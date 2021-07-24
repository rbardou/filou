open Misc

let debug = Sys.getenv_opt "FILOU_DEBUG" = Some "yes"

(* Can't print on stdout as we use it to output responses. *)
let debug_log x = Printf.ksprintf prerr_endline x

let read_bytes = Bytes.create 8192

let read_query () =
  match
    Protype_robin.Decode.value ~ignore_unknown_fields: true Protocol.query @@ fun len ->
    let len =
      input stdin read_bytes 0 (min (Bytes.length read_bytes) len)
    in
    Bytes.sub_string read_bytes 0 len
  with
    | exception Sys_error message ->
        failed [ "failed to receive response through SSH"; message ]
    | Error ({ error = Robin_error End_of_file; _ } as e) ->
        error `query_end_of_file [ Protype_robin.Decode.show_error e ]
    | Error error ->
        failed [ "failed to decode response"; Protype_robin.Decode.show_error error ]
    | Ok query ->
        ok query

let read_query () =
  if debug then
    match read_query () with
      | ERROR { msg; _ } as x ->
          debug_log "read_query: error: %s" (String.concat ": " msg);
          x
      | OK query as x ->
          debug_log "read_query: ok: %s" (Protocol.show_query query);
          x
  else
    read_query ()

let respond response =
  if debug then debug_log "responding with: %s" (Protocol.show_response response);
  let string =
    Protype_robin.Encode.to_string ~version: protocol_version Protocol.response response
  in
  match
    print_string string;
    flush stdout
  with
    | exception Sys_error message ->
        error `failed_to_respond [ "failed to respond"; message ]
    | () ->
        unit

let respond_failed message =
  respond (R_failed message)

let read_file_bytes = Bytes.create 4096

let rec loop current_lock location =
  let* query = read_query () in
  let unlock () =
    match !current_lock with
      | None ->
          unit
      | Some lock ->
          let* () = Device.unlock location lock in
          current_lock := None;
          unit
  in
  let response =
    match query with
      | Q_hello _ | Q_write_file_chunk _ | Q_write_file_eof ->
          failed [
            sf "unexpected query %s" (Protocol.show_query query)
          ]
      | Q_lock path ->
          (
            match !current_lock with
              | None ->
                  let* lock = Device.lock location path in
                  current_lock := Some lock;
                  ok Protocol.R_ok_lock
              | Some _ ->
                  failed [ "cannot have multiple locks at the same time" ]
          )
      | Q_unlock ->
          (
            match !current_lock with
              | None ->
                  failed [ "no current lock" ]
              | Some lock ->
                  let* () = Device.unlock location lock in
                  current_lock := None;
                  ok Protocol.R_ok_unlock
          )
      | Q_read_dir path ->
          (
            match Device.read_dir location path with
              | ERROR { code = `failed; _ } as x ->
                  x
              | ERROR { code = `no_such_file; msg } ->
                  ok (Protocol.R_no_such_file msg)
              | OK contents ->
                  ok (Protocol.R_ok_read_dir contents)
          )
      | Q_write_file file_path ->
          let* () =
            Device.write_file_incrementally location file_path @@ fun write ->
            let rec receive_data () =
              let* query = read_query () in
              match query with
                | Q_write_file_chunk data ->
                    let* () = write data 0 (String.length data) in
                    receive_data ()
                | Q_write_file_eof ->
                    unit
                | _ ->
                    failed [
                      sf "unexpected query %s while writing file" (Protocol.show_query query)
                    ]
            in
            receive_data ()
          in
          ok Protocol.R_ok_write_file_eof
      | Q_read_file file_path ->
          let result =
            Device.read_file_incrementally location file_path @@ fun read ->
            let rec read_and_send () =
              let* len = read read_file_bytes 0 (Bytes.length read_file_bytes) in
              if len > 0 then
                let* () =
                  respond (R_ok_read_file_chunk (Bytes.sub_string read_file_bytes 0 len))
                in
                read_and_send ()
              else
                ok Protocol.R_ok_read_file_eof
            in
            read_and_send ()
          in
          (
            match result with
              | ERROR { code = `no_such_file; msg } ->
                  ok (Protocol.R_no_such_file msg)
              | ERROR { code = (`failed | `failed_to_respond); _ } | OK _ as x ->
                  x
          )
      | Q_stat path ->
          (
            match Device.stat location path with
              | ERROR { code = `no_such_file; msg } ->
                  ok (Protocol.R_no_such_file msg)
              | ERROR { code = `failed; _ } as x ->
                  x
              | OK stat ->
                  ok (Protocol.R_ok_stat stat)
          )
      | Q_remove_file file_path ->
          (
            match Device.remove_file location file_path with
              | ERROR { code = `no_such_file; msg } ->
                  ok (Protocol.R_no_such_file msg)
              | ERROR { code = `failed; _ } as x ->
                  x
              | OK () ->
                  ok Protocol.R_ok_remove_file
          )
  in
  match response with
    | ERROR { code = `failed; msg } ->
        (
          match respond_failed msg with
            | ERROR { code = `failed_to_respond; _ } ->
                unlock ()
            | OK () ->
                loop current_lock location
        )
    | ERROR { code = (`query_end_of_file | `failed_to_respond); _ } ->
        unlock ()
    | OK response ->
        match respond response with
          | ERROR { code = `failed_to_respond; _ } ->
              unlock ()
          | OK () ->
              loop current_lock location

let run () =
  (* TODO: version number? *)
  let result =
    if debug then debug_log "filou listen: debug mode is active";
    let* query = read_query () in
    match query with
      | Q_hello path ->
          let path = Path.Any_relativity.to_absolute path in
          (* TODO: support RO mode in hello or on the CLI?… *)
          let location = Device.Local (RW, path) in
          let* () = respond R_ok_hello in
          loop (ref None) location
      | _ ->
          respond_failed [
            sf "unexpected query %s (expected Q_hello)"
              (Protocol.show_query query)
          ]
  in
  match result with
    | ERROR { code = (`query_end_of_file | `failed_to_respond | `failed); _ } | OK () ->
        unit
