open Misc

type setup =
  {
    main: Device.location;
    clone_root: Device.location;
    clone_dot_filou: Device.location;
    mutable clone_root_is_up_to_date: bool;
  }

let setup ~main ~clone =
  {
    main;
    clone_root = clone;
    clone_dot_filou = Device.sublocation clone dot_filou;
    clone_root_is_up_to_date = false;
  }

let main { main; _ } = main
let clone { clone_root; _ } = clone_root

module Make (R: Repository.S with type t = Device.location): Repository.S
  with type root = R.root and type t = setup =
struct
  type root = R.root
  type t = setup

  let set_read_only () =
    R.set_read_only ()

  (* TODO: we encode and hash twice, this is unnecessary. *)
  let store_now setup typ value =
    let* hash = R.store_now setup.main typ value in
    let* _ = R.store_now setup.clone_dot_filou typ value in
    ok hash

  (* TODO: we encode and hash twice, this is unnecessary. *)
  let store_later ?on_stored setup typ value =
    R.store_later setup.main typ value
      ~on_stored: (
        fun () ->
          let* _ = R.store_now setup.clone_dot_filou typ value in
          match on_stored with
            | None ->
                unit
            | Some on_stored ->
                on_stored ()
      )

  let fetch setup typ hash =
    match R.fetch setup.clone_dot_filou typ hash with
      | ERROR { code = `not_available; _ } ->
          R.fetch setup.main typ hash
      | ERROR { code = `failed; _ } as x ->
          x
      | OK _ as x ->
          x

  let store_file ~source ~source_path ~target =
    R.store_file ~source ~source_path ~target: target.main

  let fetch_file ~source hash ~target ~target_path =
    R.fetch_file ~source: source.main hash ~target ~target_path

  let get_file_size setup hash =
    match R.get_file_size setup.main hash with
      | ERROR { code = `not_available; _ } ->
          R.get_file_size setup.main hash
      | ERROR { code = `failed; _ } as x ->
          x
      | OK _ as x ->
          x

  let store_root setup root =
    setup.clone_root_is_up_to_date <- false;
    let* () = R.store_root setup.main root in
    let* () = R.store_root setup.clone_dot_filou root in
    setup.clone_root_is_up_to_date <- true;
    unit

  let fetch_root setup =
    R.fetch_root (
      if setup.clone_root_is_up_to_date then
        setup.clone_dot_filou
      else
        setup.main
    )

  let garbage_collect setup ~everything_except =
    let* () = R.garbage_collect setup.clone_dot_filou ~everything_except in
    R.garbage_collect setup.main ~everything_except
end
