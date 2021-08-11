open Misc

(* If [main] is [None], only operations that do not need [main] can be done. *)
type setup =
  {
    main: Device.location option;
    workdir: Device.location;
    clone_dot_filou: Device.location;
    mutable clone_root_is_up_to_date: bool;
  }

let setup ~main ~workdir ?clone_dot_filou () =
  {
    main;
    workdir;
    clone_dot_filou = clone_dot_filou |> default (Device.sublocation workdir dot_filou);
    clone_root_is_up_to_date = false;
  }

let main { main; _ } = main
let clone_dot_filou { clone_dot_filou; _ } = clone_dot_filou
let workdir { workdir; _ } = workdir

module type S =
sig
  include Repository.S

  (** Transfer all reachable objects from main to clone, as well as the root. *)
  val update:
    on_availability_check_progress: (checked: int -> count: int -> unit) ->
    on_copy_progress: (transferred: int -> count: int -> unit) ->
    t -> (int, [> `failed ]) r
end

module Make (R: Repository.S with type t = Device.location): S
  with type root = R.root and type t = setup =
struct

  type root = R.root
  type t = setup

  let set_read_only () =
    R.set_read_only ()

  let is_read_only () =
    R.is_read_only ()

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
                  let* () =
                    R.transfer ~source: main ~target: setup.clone_dot_filou
                      (Repository.hash_of_hash hash)
                  in
                  R.fetch setup.clone_dot_filou hash
          )
      | ERROR { code = `failed; _ } as x ->
          x
      | OK _ as x ->
          x

  let fetch_raw setup hash =
    match R.fetch_raw setup.clone_dot_filou hash with
      | ERROR { code = `not_available; _ } as error ->
          (
            match setup.main with
              | None ->
                  error
              | Some main ->
                  let* () = R.transfer ~source: main ~target: setup.clone_dot_filou hash in
                  R.fetch_raw setup.clone_dot_filou hash
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

  let fetch_root_gen fetch setup =
    let* () =
      match setup.main with
        | None ->
            unit
        | Some main ->
            if setup.clone_root_is_up_to_date then
              unit
            else
              R.transfer_root ~source: main ~target: setup.clone_dot_filou ()
    in
    fetch setup.clone_dot_filou

  let fetch_root setup = fetch_root_gen R.fetch_root setup
  let fetch_root_raw setup = fetch_root_gen R.fetch_root_raw setup

  let reachable ?files setup =
    match setup.main with
      | None ->
          R.reachable ?files setup.clone_dot_filou
      | Some main ->
          R.reachable ?files main

  let available setup hash =
    let* available = R.available setup.clone_dot_filou hash in
    if available then
      ok true
    else
      match setup.main with
        | None ->
            ok false
        | Some main ->
            R.available main hash

  let transfer ?on_progress: _ ~source: _ ~target: _ _hash =
    (* Would we want to transfer to target.main or target.clone_dot_filou? *)
    failed [ "Clone.transfer is not implemented" ]

  let transfer_root ?on_progress: _ ~source: _ ~target: _ () =
    (* Would we want to transfer to target.main or target.clone_dot_filou? *)
    failed [ "Clone.transfer is not implemented" ]

  (* TODO: not very efficient: we read once to decode and compute dependencies,
     and then we read again to copy. We could have R.iter_reachable instead
     for instance. *)
  let update ~on_availability_check_progress ~on_copy_progress setup =
    match setup.main with
      | None ->
          failed [ "cannot update in clone-only mode" ]
      | Some main ->
          let* hashes = R.reachable ~files: false main in
          let hashes = Repository.Hash_set.elements hashes in
          let count = List.length hashes in
          let index = ref 0 in
          let* hashes =
            list_filter_e hashes @@ fun hash ->
            let* available = R.available setup.clone_dot_filou hash in
            incr index;
            on_availability_check_progress ~checked: !index ~count;
            ok (not available)
          in
          let count = List.length hashes in
          index := 0;
          let* () =
            list_iter_e hashes @@ fun hash ->
            let* () =
              trace (sf "failed to copy %s" (Hash.to_hex hash)) @@
              match R.transfer ~source: main ~target: setup.clone_dot_filou hash with
              | ERROR { code = (`not_available | `failed); msg } ->
                  failed msg
              | OK () as x ->
                  x
            in
            incr index;
            on_copy_progress ~transferred: !index ~count;
            unit
          in
          let* () = R.transfer_root ~source: main ~target: setup.clone_dot_filou () in
          ok count

  let garbage_collect ?reachable setup =
    match setup.main with
      | None ->
          R.garbage_collect ?reachable setup.clone_dot_filou
      | Some main ->
          let* reachable =
            match reachable with
              | None ->
                  R.reachable main
              | Some set ->
                  ok set
          in
          let* (main_count, main_size) =
            R.garbage_collect ~reachable main
          in
          let* (clone_count, clone_size) =
            R.garbage_collect ~reachable setup.clone_dot_filou
          in
          ok (main_count + clone_count, main_size + clone_size)

  let check_hash setup hash =
    let* () =
      match setup.main with
        | None ->
            unit
        | Some main ->
            R.check_hash main hash
    in
    match R.check_hash setup.clone_dot_filou hash with
      | ERROR { code = `not_available; _ } ->
          unit
      | x ->
          x

  let get_object_size setup hash =
    match R.get_object_size setup.clone_dot_filou hash with
      | OK _ | ERROR { code = `failed; _ } as x ->
          x
      | ERROR { code = `not_available; _ } as not_available ->
          match setup.main with
            | None ->
                not_available
            | Some main ->
                R.get_object_size main hash

end
