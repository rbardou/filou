open Misc

(* TODO: [store_later] is nice to avoid creating too many useless files, but:
   - it can be good to add files iteratively and still update the root for each file;
   - and it needs O(n) memory where n is the number of added files.
   So do we actually want to use it?
   - but it is also convenient to be able to cancel a push
     => [store_later] is not useful for that, the only important thing is to have
        a journal with relevant checkpoints
   - it is good to avoid creating many useless files (O(n×m) where n is the number
     of files added at the same times and m the average directory depth + the depth
     of the hash index)
   We can still keep it and activate / deactive it using a flag or heuristics… *)

type 'a hash_status =
  | Stored
  | Not_stored of {
      (* TODO: the encoded value is also available in the closure, this uses
         twice the needed amount of memory. Having [value] makes it faster to fetch though. *)
      value: 'a;
      store: unit -> unit;
    }

type file

type 'a hash =
  | Value_hash of {
      hash: Hash.t;
      typ: 'a Protype.t;
      mutable status: 'a hash_status;
    }
  | File_hash: Hash.t -> file hash
  | File_hash_with_size: Hash.t * int -> file hash (* for read-only mode *)

type packed_hash = H: 'a hash -> packed_hash

type path_kind = Meta | Data

let store_not_stored_hash_when_encoding = ref false

let with_store_not_stored_hash_when_encoding value f =
  let old_value = !store_not_stored_hash_when_encoding in
  (* If the flag was already set, we must keep it. *)
  store_not_stored_hash_when_encoding := old_value || value;
  try
    let r = f () in
    store_not_stored_hash_when_encoding := old_value;
    r
  with exn ->
    store_not_stored_hash_when_encoding := old_value;
    raise exn

type _ Protype.annotation +=
  | Hash: 'a Protype.t -> 'a hash Protype.annotation
  | File_hash: file hash Protype.annotation

let raw_hash_type =
  let open Protype in
  convert_partial string ~encode: Hash.to_bin ~decode: Hash.of_bin

let hash_type (type a) (typ: a Protype.t) =
  let open Protype in
  let encode (hash: a hash) =
    match hash with
      | Value_hash hash ->
          if !store_not_stored_hash_when_encoding then (
            match hash.status with
              | Stored ->
                  ()
              | Not_stored { store; _ } ->
                  store ();
                  hash.status <- Stored
          );
          hash.hash
      | File_hash hash | File_hash_with_size (hash, _) ->
          hash
  in
  let decode hash =
    Some (
      Value_hash {
        hash;
        typ;
        status = Stored;
      }
    )
  in
  (* Cannot use [convert_partial] because of the value restriction, but this is equivalent. *)
  Annotate (Hash typ, Convert { typ = raw_hash_type; encode; decode })

let hash_of_hash (type a) (hash: a hash) =
  match hash with
    | Value_hash { hash; _ }
    | File_hash hash
    | File_hash_with_size (hash, _) -> hash

let hex_of_hash hash = Hash.to_hex (hash_of_hash hash)
let bin_of_hash hash = Hash.to_bin (hash_of_hash hash)
let compare_hashes a b = Hash.compare (hash_of_hash a) (hash_of_hash b)

let file_hash_type =
  let open Protype in
  annotate File_hash @@
  convert raw_hash_type ~encode: hash_of_hash ~decode: (fun h -> File_hash h)

module Hash_map = Map.Make (Hash)

module type ROOT =
sig
  type t
  val version: int
  val typ: t Protype.t
end

module type S =
sig
  type t
  type root

  val set_read_only: unit -> unit

  val is_read_only: unit -> bool

  val store_now: t -> 'a Protype.t -> 'a ->
    ('a hash, [> `failed ]) r

  val store_later: ?on_stored: (unit -> (unit, [ `failed ] as 'e) r) ->
    t -> 'a Protype.t -> 'a -> ('a hash, [> `failed ]) r

  val fetch: t -> 'a hash ->
    ('a, [> `failed | `not_available ]) r

  val fetch_raw: t -> Hash.t ->
    (string, [> `failed | `not_available ]) r

  val store_file:
    on_hash_progress: (bytes: int -> size: int -> unit) ->
    on_copy_progress: (bytes: int -> size: int -> unit) ->
    source: Device.location -> source_path: Device.file_path -> target: t ->
    (file hash * int, [> `failed ]) r

  val fetch_file:
    on_progress: (bytes: int -> size: int -> unit) ->
    source: t -> file hash -> target: Device.location -> target_path: Device.file_path ->
    (unit, [> `failed | `not_available | `already_exists ]) r

  val get_file_size: t -> file hash ->
    (int, [> `failed | `not_available ]) r

  val store_root: t -> root ->
    (unit, [> `failed ]) r

  val fetch_root: t ->
    (root, [> `failed ]) r

  val fetch_root_raw: t ->
    (string, [> `failed ]) r

  val reachable: ?files: bool -> t -> (path_kind Hash_map.t, [> `failed ]) r

  val garbage_collect: ?reachable: path_kind Hash_map.t -> t -> (int * int, [> `failed ]) r

  val available: t -> path_kind -> Hash.t -> (bool, [> `failed ]) r

  val transfer: ?on_progress: (int -> unit) -> source: t -> target: t ->
    path_kind -> Hash.t ->
    (unit, [> `failed | `not_available ]) r

  val transfer_root: ?on_progress: (int -> unit) -> source: t -> target: t -> unit ->
    (unit, [> `failed ]) r

  val check_hash: on_progress: (bytes: int -> size: int -> unit) ->
    t -> path_kind -> Hash.t ->
    (unit, [> `failed | `corrupted | `not_available ]) r

  val get_object_size: t -> path_kind -> Hash.t ->
    (int, [> `failed | `not_available ]) r
end

exception Failed_to_store_later of string list

module Make (Root: ROOT): S with type root = Root.t and type t = Device.location =
struct

  type t = Device.location
  type root = Root.t

  let read_only = ref false

  let set_read_only () = read_only := true

  let is_read_only () = !read_only

  (* As encoding hashes can raise [Failed_to_store_later], we need to catch it. *)
  let encode ~trigger_hash_storing typ value =
    with_store_not_stored_hash_when_encoding trigger_hash_storing @@ fun () ->
    try
      ok (Protype_robin.Encode.to_string ~version: Root.version typ value)
    with
      | Failed_to_store_later msg ->
          failed msg

  let hash_part_length = 2

  let meta_filename = Path.Filename.parse_exn "meta"
  let data_filename = Path.Filename.parse_exn "data"

  let file_path_of_hash kind hash =
    let kind =
      match kind with
        | Meta -> meta_filename
        | Data -> data_filename
    in
    let hash_string = Hash.to_hex hash in
    (* [Hash.to_hex] always returns a string of length >= 4,
       composed only of valid characters for filenames. *)
    let prefix1 = String.sub hash_string 0 hash_part_length in
    let prefix2 = String.sub hash_string hash_part_length hash_part_length in
    [ kind; Path.Filename.parse_exn prefix1; Path.Filename.parse_exn prefix2 ],
    Path.Filename.parse_exn hash_string

  let store_raw_now location hash encoded_value =
    trace ("failed to store object with hash " ^ Hash.to_hex hash) @@
    let path = file_path_of_hash Meta hash in
    let* already_exists = Device.file_exists location path in
    if already_exists then
      unit
    else if !read_only then
      (* This function is never actually called in read-only mode,
         except if read-only mode was set *after* some calls to [store_later]. *)
      unit
    else
      Device.write_file location path encoded_value

  let store_now location typ value =
    let* encoded_value = encode ~trigger_hash_storing: true typ value in
    let hash = Hash.string encoded_value in
    if !read_only then
      ok (Value_hash { hash; typ; status = Not_stored { value; store = fun () -> () } })
    else
      let* () = store_raw_now location hash encoded_value in
      ok (Value_hash { hash; typ; status = Stored })

  let store_later ?on_stored location typ value =
    let* encoded_value = encode ~trigger_hash_storing: false typ value in
    let hash = Hash.string encoded_value in
    let store () =
      match
        (* TODO: We need to re-encode, just to trigger hash storing. This is inefficient. *)
        let* encoded_value = encode ~trigger_hash_storing: true typ value in
        let* () =
          if !read_only then
            unit
          else
            store_raw_now location hash encoded_value
        in
        match on_stored with
          | None -> unit
          | Some on_stored -> on_stored ()
      with
        | OK () ->
            ()
        | ERROR { code = `failed; msg } ->
            raise (Failed_to_store_later msg)
    in
    ok (Value_hash { hash; typ; status = Not_stored { value; store } })

  let fetch (type a) location (hash: a hash) =
    trace ("failed to fetch object with hash " ^ hex_of_hash hash) @@
    match hash with
      | File_hash _ | File_hash_with_size _ ->
          failed [ "this is a file, not an object" ]
      | Value_hash { status = Not_stored { value; _ }; _ } ->
          ok value
      | Value_hash { status = Stored; hash; typ } ->
          let path = file_path_of_hash Meta hash in
          match Device.read_file location path with
            | ERROR { code = `no_such_file; msg } ->
                ERROR { code = `not_available; msg }
            | ERROR { code = `failed; _ } as x ->
                x
            | OK encoded_value ->
                decode_robin_string typ encoded_value

  let fetch_raw location (hash: Hash.t) =
    trace ("failed to fetch object with hash " ^ Hash.to_hex hash) @@
    let path = file_path_of_hash Meta hash in
    match Device.read_file location path with
      | ERROR { code = `no_such_file; msg } ->
          ERROR { code = `not_available; msg }
      | ERROR { code = `failed; _ } | OK _ as x ->
          x

  let store_file ~on_hash_progress ~on_copy_progress ~source ~source_path ~target =
    trace ("failed to store " ^ Device.show_file_path source_path) @@
    let* hash, size =
      match
        Device.hash
          ~on_progress: on_hash_progress
          source source_path
      with
        | ERROR { code = (`no_such_file | `failed); msg } ->
            failed msg
        | OK _ as x ->
            x
    in
    let hash_path = file_path_of_hash Data hash in
    let* already_exists = Device.file_exists target hash_path in
    if already_exists || !read_only then
      ok (File_hash_with_size (hash, size), size)
    else
      match
        Device.copy_file ~on_progress: (fun bytes -> on_copy_progress ~bytes ~size)
          ~source: (source, source_path) ~target: (target, hash_path)
      with
        | ERROR { code = (`no_such_file | `failed); msg } ->
            failed msg
        | OK () ->
            ok (File_hash_with_size (hash, size), size)

  let fetch_file ~on_progress ~source hash ~target ~target_path =
    trace ("failed to fetch file " ^ Device.show_file_path target_path) @@
    match hash with
    | Value_hash _ ->
        (* The only way for this to happen is if the user stored a value
           of type [file], which requires a value of type [file Protype.t],
           and the only way for the user to get such a type is to make it
           using [Protype.convert] with encoding functions that raise exceptions. *)
        failed [ "this is not a file but a value" ]
    | File_hash hash | File_hash_with_size (hash, _) ->
        let hash_path = file_path_of_hash Data hash in
        match Device.stat source (Device.path_of_file_path hash_path) with
          | ERROR { code = `no_such_file; msg } ->
              error `not_available msg
          | ERROR { code = `failed; _ } as x ->
              x
          | OK Dir ->
              failed [ Device.show_file_path hash_path ^ " is a directory" ]
          | OK (File { size; _ }) ->
              let* already_exists = Device.file_exists target target_path in
              if already_exists then
                error `already_exists [
                  sf "file %s already exists in %s"
                    (Device.show_file_path target_path)
                    (Device.show_location target);
                ]
              else if !read_only then
                unit
              else
                match
                  Device.copy_file ~on_progress: (fun bytes -> on_progress ~bytes ~size)
                    ~source: (source, hash_path) ~target: (target, target_path)
                with
                  | ERROR { code = (`no_such_file | `failed); msg } ->
                      failed msg
                  | OK () as x ->
                      x

  (* TODO: share code with [get_object_size]? *)
  let get_file_size location hash =
    trace ("failed to get file size for " ^ hex_of_hash hash) @@
    let hash_path = file_path_of_hash Data (hash_of_hash hash) in
    match Device.stat location (Device.path_of_file_path hash_path) with
      | ERROR { code = `no_such_file; msg } ->
          if !read_only then
            match hash with
              | File_hash_with_size (_, size) ->
                  (* TODO: we could also do that in the beginning and
                     thus use this as a cache. *)
                  ok size
              | File_hash _ | Value_hash _ ->
                  error `not_available msg
          else
            error `not_available msg
      | ERROR { code = `failed; _ } as x ->
          x
      | OK Dir ->
          failed [ Device.show_file_path hash_path ^ " is a directory" ]
      | OK (File { size; _ }) ->
          ok size

  let root_path = [], Path.Filename.parse_exn "root"

  let current_root = ref None

  let store_root location root =
    trace "failed to store root" @@
    let* encoded_root = encode ~trigger_hash_storing: true Root.typ root in
    if !read_only then
      (
        current_root := Some root;
        unit
      )
    else
      Device.write_file location root_path encoded_root

  let fetch_root location =
    let actually_fetch_root () =
      trace "failed to fetch root" @@
      match Device.read_file location root_path with
      | ERROR { code = (`no_such_file | `failed); msg } ->
          failed msg
      | OK encoded_root ->
          decode_robin_string Root.typ encoded_root
    in
    if !read_only then
      match !current_root with
        | None ->
            actually_fetch_root ()
        | Some root ->
            ok root
    else
      actually_fetch_root ()

  let fetch_root_raw location =
    trace "failed to fetch root" @@
    match Device.read_file location root_path with
    | ERROR { code = (`no_such_file | `failed); msg } ->
        failed msg
    | OK _ as x ->
        x

  let direct_value_dependencies ~files typ value =
    let equal: 'a. 'a -> 'a Protype.annotation -> packed_hash option =
      fun (type a) (x: a) (annotation: a Protype.annotation): packed_hash option ->
        match annotation with
          | Hash _ -> Some (H x)
          | File_hash -> if files then Some (H x) else None
          | _ -> None
    in
    Protype.find_all { equal } typ value

  let direct_hash_dependencies (type a) ~files location (hash: a hash) =
    match hash with
      | File_hash _ | File_hash_with_size _ ->
          ok []
      | Value_hash { typ; _ } ->
          match fetch location hash with
            | ERROR { code = (`failed | `not_available); msg } ->
                failed msg
            | OK value ->
                ok (direct_value_dependencies ~files typ value)

  let rec reachable_hashes_from_hash: 'a. files: bool -> _ -> _ -> 'a hash -> _ =
    fun (type a) ~files acc location (hash: a hash) ->
    let h = hash_of_hash hash in
    if Hash_map.mem h acc then
      ok acc
    else
      let hash_kind =
        match hash with
          | Value_hash _ -> Meta
          | File_hash _ | File_hash_with_size _ -> Data
      in
      let acc = Hash_map.add h hash_kind acc in
      let* direct_deps = direct_hash_dependencies ~files location hash in
      list_fold_e acc direct_deps @@ fun acc (H dep_hash) ->
      reachable_hashes_from_hash ~files acc location dep_hash

  let reachable ?(files = true) location =
    let* root = fetch_root location in
    let direct_deps = direct_value_dependencies ~files Root.typ root in
    list_fold_e Hash_map.empty direct_deps @@ fun acc (H dep_hash) ->
    reachable_hashes_from_hash ~files acc location dep_hash

  let garbage_collect ?reachable: reachable_set location =
    let removed_object_count = ref 0 in
    let removed_object_total_size = ref 0 in
    let* reachable =
      match reachable_set with
        | None ->
            reachable location
        | Some set ->
            ok set
    in
    match
      let rec garbage_collect_dir dir_path =
        Device.iter_read_dir location dir_path @@ fun filename ->
        let filename_string = Path.Filename.show filename in
        let sub_path = dir_path @ [ filename ] in
        match Device.stat location sub_path with
          | ERROR { code = `no_such_file; _ } ->
              unit
          | ERROR { code = `failed; _ } as x ->
              x
          | OK Dir ->
              if String.length filename_string <> hash_part_length then
                unit
              else
                let is_hex =
                  let rec loop i =
                    if i >= hash_part_length then
                      true
                    else
                      match filename_string.[i] with
                        | '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' ->
                            loop (i + 1)
                        | _ ->
                            false
                  in
                  loop 0
                in
                if is_hex then
                  garbage_collect_dir sub_path
                else
                  unit
          | OK (File { size; _ }) ->
              match Hash.of_hex filename_string with
                | None ->
                    unit
                | Some hash ->
                    match Device.file_path_of_path sub_path with
                      | None ->
                          unit
                      | Some file_path ->
                          if Hash_map.mem hash reachable || !read_only then
                            unit
                          else
                            let* () = Device.remove_file location file_path in
                            incr removed_object_count;
                            removed_object_total_size := !removed_object_total_size + size;
                            unit
      in
      let* () =
        match
          let* () = garbage_collect_dir [ meta_filename ] in
          garbage_collect_dir [ data_filename ]
        with
          | ERROR { code = `no_such_file; _ } ->
              unit
          | ERROR { code = `failed; _ } as x ->
              x
          | OK () ->
              unit
      in
      ok (!removed_object_count, !removed_object_total_size)
    with
      | ERROR { code = `failed; msg } ->
          failed msg
      | OK _ as x ->
          x

  let available location kind hash =
    let path = file_path_of_hash kind hash in
    match Device.stat location (Device.path_of_file_path path) with
      | ERROR { code = `no_such_file; _ } ->
          ok false
      | ERROR { code = `failed; _ } as x ->
          x
      | OK Dir ->
          failed [ sf "%s is a directory" (Device.show_file_path path) ]
      | OK (File _) ->
          ok true

  let transfer ?(on_progress = fun _ -> ()) ~source ~target kind (hash: Hash.t) =
    trace ("failed to fetch object with hash " ^ Hash.to_hex hash) @@
    let* available = available target kind hash in
    if available || !read_only then
      unit
    else
      let path = file_path_of_hash kind hash in
      match
        Device.copy_file ~on_progress ~source: (source, path) ~target: (target, path)
      with
        | ERROR { code = `no_such_file; msg } ->
            error `not_available msg
        | ERROR { code = `failed; _ } | OK () as x ->
            x

  let transfer_root ?(on_progress = fun _ -> ()) ~source ~target () =
    trace "failed to fetch root" @@
    if !read_only then
      unit
    else
      match
        Device.copy_file ~source: (source, root_path) ~target: (target, root_path)
          ~on_progress
      with
        | ERROR { code = (`no_such_file | `failed); msg } ->
            failed msg
        | OK () as x ->
            x

  let check_hash ~on_progress location kind (expected_hash: Hash.t) =
    let file_path = file_path_of_hash kind expected_hash in
    let* hash, _ =
      trace (sf "failed to check hash of object %s" (Hash.to_hex expected_hash)) @@
      match
        Device.hash
          ~on_progress
          location file_path
      with
        | ERROR { code = `no_such_file; msg } ->
            error `not_available msg
        | ERROR { code = `failed; _ } | OK _ as x ->
            x
    in
    if Hash.compare hash expected_hash <> 0 then
      error `corrupted [
        sf "hash of file %s is not %s but %s"
          (Device.show_file_path file_path)
          (Hash.to_hex expected_hash)
          (Hash.to_hex hash)
      ]
    else
      unit

  let get_object_size location kind hash =
    trace ("failed to get object size for " ^ Hash.to_hex hash) @@
    let hash_path = file_path_of_hash kind hash in
    match Device.stat location (Device.path_of_file_path hash_path) with
      | ERROR { code = `no_such_file; msg } ->
          error `not_available msg
      | ERROR { code = `failed; _ } as x ->
          x
      | OK Dir ->
          failed [ Device.show_file_path hash_path ^ " is a directory" ]
      | OK (File { size; _ }) ->
          ok size

end
