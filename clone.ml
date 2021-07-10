open Misc

(* If [main] is [None], only operations that do not need [main] can be done. *)
type setup =
  {
    main: Device.location option;
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
    match setup.main with
      | None ->
          failed [ "cannot store objects in clone-only mode" ]
      | Some main ->
          let* hash = R.store_now main typ value in
          let* _ = R.store_now setup.clone_dot_filou typ value in
          ok hash

  (* TODO: we encode and hash twice, this is unnecessary. *)
  let store_later ?on_stored setup typ value =
    match setup.main with
      | None ->
          failed [ "cannot store objects in clone-only mode" ]
      | Some main ->
          R.store_later main typ value
            ~on_stored: (
              fun () ->
                let* _ = R.store_now setup.clone_dot_filou typ value in
                match on_stored with
                  | None ->
                      unit
                  | Some on_stored ->
                      on_stored ()
            )

  let fetch setup hash =
    match R.fetch setup.clone_dot_filou hash with
      | ERROR { code = `not_available; _ } as error ->
          (
            match setup.main with
              | None ->
                  error
              | Some main ->
                  R.fetch main hash
          )
      | ERROR { code = `failed; _ } as x ->
          x
      | OK _ as x ->
          x

  let store_file ~source ~source_path ~target ~on_progress =
    match target.main with
      | None ->
          failed [ "cannot store files in clone-only mode" ]
      | Some main ->
          R.store_file ~source ~source_path ~target: main ~on_progress

  let fetch_file ~source hash ~target ~target_path ~on_progress =
    match source.main with
      | None ->
          failed [ "cannot fetch files in clone-only mode" ]
      | Some main ->
          R.fetch_file ~source: main hash ~target ~target_path ~on_progress

  let get_file_size setup hash =
    match setup.main with
      | None ->
          failed [ "cannot get file sizes in clone-only mode" ]
      | Some main ->
          (* Files are not stored in clones, at least not with their hash. *)
          R.get_file_size main hash

  let store_root setup root =
    match setup.main with
      | None ->
          failed [ "cannot store roots in clone-only mode" ]
      | Some main ->
          setup.clone_root_is_up_to_date <- false;
          let* () = R.store_root main root in
          let* () = R.store_root setup.clone_dot_filou root in
          setup.clone_root_is_up_to_date <- true;
          unit

  let fetch_root setup =
    match setup.main with
      | None ->
          R.fetch_root setup.clone_dot_filou
      | Some main ->
          R.fetch_root (
            if setup.clone_root_is_up_to_date then
              setup.clone_dot_filou
            else
              main
          )

  let garbage_collect setup =
    (* TODO: also GC the clone *)
    match setup.main with
      | None ->
          ok (0, 0)
      | Some main ->
          R.garbage_collect main
end
