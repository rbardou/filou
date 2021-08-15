open Misc

type 'a hash_status =
  | Stored of Hash.t
  | Not_stored of 'a

type 'a hash =
  {
    mutable status: 'a hash_status;
  }

let concrete_hash hash =
  match hash.status with
    | Stored hash ->
        Some hash
    | Not_stored _ ->
        None

let concrete_hash_or_fail hash =
  match hash.status with
    | Stored hash ->
        ok hash
    | Not_stored _ ->
        failed [ "object not stored" ]

type file_hash =
  {
    hash: Hash.t;
    mutable size: int option; (* for read-only mode and as a cache *)
  }

let concrete_file_hash { hash; _ } = hash

let stored_hash hash = { status = Stored hash }

let stored_file_hash hash = { hash; size = None }

module type OBJECT =
sig
  type 'a t
  type root
  val root: root t
  val encode: 'a t -> 'a -> string
  val decode: 'a t -> string -> ('a, [> `failed ]) r
  type dep =
    | Object: 'a t * 'a hash -> dep
    | File: file_hash -> dep
  val iter_deps: 'a t -> 'a -> (dep -> (unit, [> `failed ] as 'e) r) -> (unit, 'e) r
end

module type S =
sig
  type 'a object_type
  type root

  val set_read_only: unit -> unit

  val is_read_only: unit -> bool

  val hash: 'a -> 'a hash

  val store: Device.location -> 'a object_type -> 'a hash -> (unit, [> `failed ]) r

  val fetch: Device.location -> 'a object_type -> 'a hash ->
    ('a, [> `failed | `not_available ]) r

  val fetch_raw: Device.location -> Hash.t -> (string, [> `failed | `not_available ]) r

  val store_file:
    on_hash_progress: (bytes: int -> size: int -> unit) ->
    on_copy_progress: (bytes: int -> size: int -> unit) ->
    source: Device.location ->
    source_path: Device.file_path ->
    target: Device.location ->
    (file_hash * int, [> `failed ]) r

  val fetch_file:
    on_progress: (bytes: int -> size: int -> unit) ->
    source: Device.location ->
    file_hash ->
    target: Device.location ->
    target_path: Device.file_path ->
    (unit, [> `already_exists | `failed | `not_available ]) r

  val get_object_size: Device.location -> Hash.t -> (int, [> `failed | `not_available ]) r

  val get_file_size: Device.location -> file_hash -> (int, [> `failed ]) r

  val write_root: Device.location -> root hash -> (unit, [> `failed ]) r

  val read_root: Device.location -> (root hash option, [> `failed ]) r

  val object_is_available: Device.location -> Hash.t -> (bool, [> `failed ]) r

  val file_is_available: Device.location -> Hash.t -> (bool, [> `failed ]) r

  val transfer_object:
    on_progress: (bytes: int -> unit) ->
    source: Device.location ->
    target: Device.location ->
    Hash.t ->
    (unit, [> `failed | `not_available ]) r

  val iter_objects: Device.location -> (Hash.t -> (unit, [< `failed ]) r) ->
    (unit, [> `failed ]) r

  val iter_files: Device.location -> (Hash.t -> (unit, [< `failed ]) r) ->
    (unit, [> `failed ]) r

  val check_object_hash:
    Device.location ->
    on_progress: (bytes: int -> size: int -> unit) ->
    Hash.t -> (unit, [> `failed ]) r

  val check_file_hash:
    Device.location ->
    on_progress: (bytes: int -> size: int -> unit) ->
    Hash.t -> (unit, [> `failed ]) r

  val get_reachable_hashes:
    on_progress: (object_count: int -> file_count: int -> unit) ->
    Device.location ->
    (Hash_set.t * Hash_set.t, [> `failed | `no_root ]) r

  val remove_object: Device.location -> Hash.t -> (unit, [> `failed ]) r

  val remove_file: Device.location -> Hash.t -> (unit, [> `failed ]) r
end

module Make (Object: OBJECT): S
  with type 'a object_type = 'a Object.t
   and type root = Object.root =
struct

  type 'a object_type = 'a Object.t
  type root = Object.root

  let read_only = ref false

  let set_read_only () = read_only := true

  let is_read_only () = !read_only

  let meta_filename = Path.Filename.parse_exn "meta"
  let data_filename = Path.Filename.parse_exn "data"

  let hash_prefix_length = 2

  let file_path_of_object_hash hash =
    let hash_string = Hash.to_hex hash in
    (* [Hash.to_hex] always returns a string of length >= 4,
       composed only of valid characters for filenames. *)
    let prefix = String.sub hash_string 0 hash_prefix_length in
    [ meta_filename; Path.Filename.parse_exn prefix ],
    Path.Filename.parse_exn hash_string

  (* Same as [file_path_of_object_hash] but with 2 levels. *)
  let file_path_of_file_hash hash =
    let hash_string = Hash.to_hex hash in
    let prefix1 = String.sub hash_string 0 hash_prefix_length in
    let prefix2 = String.sub hash_string hash_prefix_length hash_prefix_length in
    [ data_filename; Path.Filename.parse_exn prefix1; Path.Filename.parse_exn prefix2 ],
    Path.Filename.parse_exn hash_string

  let hash value =
    { status = Not_stored value }

  let rec store: 'a. _ -> 'a Object.t -> 'a hash -> _ =
    fun (type a) location (typ: a Object.t) (hash: a hash) ->
    match hash.status with
      | Stored _ ->
          unit
      | Not_stored value ->
          (* Store dependencies.
             We may need their hash before encoding. *)
          let* () =
            Object.iter_deps typ value @@ fun dep ->
            match dep with
              | File _ ->
                  (* Files are always stored immediately. *)
                  unit
              | Object (typ, hash) ->
                  store location typ hash
          in
          (* Encode, hash and write the file. *)
          let encoded = Object.encode typ value in
          let concrete_hash = Hash.string encoded in
          let path = file_path_of_object_hash concrete_hash in
          let* already_exists = Device.file_exists location path in
          let* () =
            if already_exists || !read_only then
              unit
            else
              Device.write_file location path encoded
          in
          (* Done storing. *)
          if not !read_only then hash.status <- Stored concrete_hash;
          unit

  let fetch_raw location concrete_hash =
    match Device.read_file location (file_path_of_object_hash concrete_hash) with
      | ERROR { code = `no_such_file; msg } ->
          error `not_available msg
      | ERROR { code = `failed; _ } | OK _ as x ->
          x

  let fetch_concrete location (typ: 'a Object.t) (concrete_hash: Hash.t) =
    let* encoded = fetch_raw location concrete_hash in
    Object.decode typ encoded

  let fetch location (typ: 'a Object.t) (hash: 'a hash) =
    match hash.status with
      | Not_stored value ->
          ok value
      | Stored concrete_hash ->
          fetch_concrete location typ concrete_hash

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
    let hash_path = file_path_of_file_hash hash in
    let* already_exists = Device.file_exists target hash_path in
    if already_exists || !read_only then
      ok ({ hash; size = Some size }, size)
    else
      match
        Device.copy_file ~on_progress: (fun bytes -> on_copy_progress ~bytes ~size)
          ~source: (source, source_path) ~target: (target, hash_path)
      with
        | ERROR { code = (`no_such_file | `failed); msg } ->
            failed msg
        | OK () ->
            ok ({ hash; size = Some size }, size)

  let fetch_file ~on_progress ~source { hash; _ } ~target ~target_path =
    trace ("failed to fetch file " ^ Device.show_file_path target_path) @@
    let hash_path = file_path_of_file_hash hash in
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

  let get_object_size location hash =
    let file_path = file_path_of_object_hash hash in
    match Device.stat location (Device.path_of_file_path file_path) with
      | ERROR { code = `no_such_file; msg } ->
          error `not_available msg
      | ERROR { code = `failed; _ } as x ->
          x
      | OK (File { size; _ }) ->
          ok size
      | OK Dir ->
          failed [
            "failed to get file size";
            sf "%s is a directory" (Device.show_file_path file_path);
          ]

  let get_file_size location ({ hash; size } as file_hash) =
    match size with
      | Some size ->
          ok size
      | None ->
          let file_path = file_path_of_file_hash hash in
          match Device.stat location (Device.path_of_file_path file_path) with
            | ERROR { code = `no_such_file | `failed; msg } ->
                failed ("failed to get file size" :: msg)
            | OK (File { size; _ }) ->
                file_hash.size <- Some size;
                ok size
            | OK Dir ->
                failed [
                  "failed to get file size";
                  sf "%s is a directory" (Device.show_file_path file_path);
                ]

  let root_path = [], Path.Filename.parse_exn "root"

  (* For read-only mode only.
     TODO: turn into a map from device location? *)
  let current_root = ref None

  let write_root location (hash: Object.root hash) =
    if !read_only then
      (
        current_root := Some hash;
        unit
      )
    else
      let* () = store location Object.root hash in
      let* concrete_hash = concrete_hash_or_fail hash in
      Device.write_file location root_path (Hash.to_hex concrete_hash)

  let read_root location: (Object.root hash option, _) r =
    match !current_root with
      | Some _ as x ->
          assert !read_only;
          ok x
      | None ->
          match Device.read_file location root_path with
            | ERROR { code = `failed; _ } as x ->
                x
            | ERROR { code = `no_such_file; _ } ->
                ok None
            | OK hash_hex ->
                match Hash.of_hex hash_hex with
                  | None ->
                      failed [
                        sf "invalid hash in %s: %s"
                          (Device.show_file_path root_path) hash_hex;
                      ]
                  | Some concrete_hash ->
                      (* Don't set it to [Some] if we're not read-only. *)
                      current_root := None;
                      ok (Some { status = Stored concrete_hash })

  let is_available location path =
    match Device.stat location (Device.path_of_file_path path) with
      | ERROR { code = `no_such_file; _ } ->
          ok false
      | ERROR { code = `failed; _ } as x ->
          x
      | OK Dir ->
          failed [ sf "%s is a directory" (Device.show_file_path path) ]
      | OK (File _) ->
          ok true

  let object_is_available location hash =
    is_available location (file_path_of_object_hash hash)

  let file_is_available location hash =
    is_available location (file_path_of_file_hash hash)

  let transfer_object ~on_progress ~source ~target (hash: Hash.t) =
    trace ("failed to fetch object with hash " ^ Hash.to_hex hash) @@
    let* available = object_is_available target hash in
    if available || !read_only then
      unit
    else
      let path = file_path_of_object_hash hash in
      match
        Device.copy_file ~on_progress: (fun bytes -> on_progress ~bytes)
          ~source: (source, path) ~target: (target, path)
      with
        | ERROR { code = `no_such_file; msg } ->
            error `not_available msg
        | ERROR { code = `failed; _ } | OK () as x ->
            x

  (* Iter on files with a name that looks like a hash. *)
  let iter_hash_files location dir_path f =
    match
      Device.iter_read_dir location dir_path @@ fun filename ->
      match Hash.of_hex (Path.Filename.show filename) with
        | None ->
            unit
        | Some hash ->
            match Device.stat location (dir_path @ [ filename ]) with
              | ERROR { code = `no_such_file; _ } ->
                  unit
              | ERROR { code = `failed; _ } as x ->
                  x
              | OK Dir ->
                  unit
              | OK (File _) ->
                  match f hash with
                    | ERROR { code = `failed; _ } | OK () as x ->
                        x
    with
      | ERROR { code = `no_such_file; _ } ->
          unit
      | ERROR { code = `failed; _ } | OK () as x ->
          x

  (* Iter on directories with a name that looks like part of a hash. *)
  let iter_hash_dirs location dir_path f =
    match
      Device.iter_read_dir location dir_path @@ fun filename ->
      let filename_string = Path.Filename.show filename in
      if String.length filename_string <> hash_prefix_length then
        unit
      else
        match Hash.string_of_hex filename_string with
          | None ->
              unit
          | Some _ ->
              let subdir_path = dir_path @ [ filename ] in
              match Device.stat location subdir_path with
                | ERROR { code = `no_such_file; _ } ->
                    unit
                | ERROR { code = `failed; _ } as x ->
                    x
                | OK (File _) ->
                    unit
                | OK Dir ->
                    match f subdir_path with
                      | ERROR { code = `failed; _ } | OK () as x ->
                          x
    with
      | ERROR { code = `no_such_file; _ } ->
          unit
      | ERROR { code = `failed; _ } | OK () as x ->
          x

  let iter_objects location f =
    iter_hash_dirs location [ meta_filename ] @@ fun subdir_path ->
    iter_hash_files location subdir_path f

  let iter_files location f =
    iter_hash_dirs location [ data_filename ] @@ fun subdir_path ->
    iter_hash_dirs location subdir_path @@ fun subdir_path ->
    iter_hash_files location subdir_path f

  let check_path_hash location ~on_progress file_path expected_hash =
    let* actual_hash, _ =
      trace (sf "failed to check hash of %s" (Device.show_file_path file_path)) @@
      match
        Device.hash
          ~on_progress
          location file_path
      with
        | ERROR { code = `no_such_file | `failed; msg } ->
            failed msg
        | OK _ as x ->
            x
    in
    if Hash.compare actual_hash expected_hash <> 0 then
      failed [
        sf "hash of %s is not %s but %s"
          (Device.show_file_path file_path)
          (Hash.to_hex expected_hash)
          (Hash.to_hex actual_hash)
      ]
    else
      unit

  let check_object_hash location ~on_progress hash =
    let file_path = file_path_of_object_hash hash in
    check_path_hash location ~on_progress file_path hash

  let check_file_hash location ~on_progress hash =
    let file_path = file_path_of_file_hash hash in
    check_path_hash location ~on_progress file_path hash

  let get_reachable_hashes ~on_progress location =
    let object_count = ref 0 in
    let file_count = ref 0 in
    let objects = ref Hash_set.empty in
    let files = ref Hash_set.empty in
    trace "cannot compute the set of reachable hashes" @@
    let rec gather: 'a. 'a Object.t -> 'a hash -> _ =
      fun (type a) (typ: a Object.t) (hash: a hash) ->
        match hash.status with
          | Not_stored _ ->
              error `failed [ "object is not stored" ]
          | Stored concrete_hash ->
              if Hash_set.mem concrete_hash !objects then
                unit
              else (
                objects := Hash_set.add concrete_hash !objects;
                incr object_count;
                on_progress ~object_count: !object_count ~file_count: !file_count;
                let* obj =
                  match fetch location typ hash with
                    | ERROR { code = `failed | `not_available; msg } ->
                        failed msg
                    | OK _ as x ->
                        x
                in
                Object.iter_deps typ obj @@ fun dep ->
                match dep with
                  | Object (dep_type, dep_hash) ->
                      gather dep_type dep_hash
                  | File file_hash ->
                      let hash = concrete_file_hash file_hash in
                      if Hash_set.mem hash !files then
                        unit
                      else (
                        files := Hash_set.add hash !files;
                        incr file_count;
                        on_progress ~object_count: !object_count ~file_count: !file_count;
                        unit
                      )
              )
    in
    let* root_hash = read_root location in
    match root_hash with
      | None ->
          error `no_root [ "no root" ]
      | Some root_hash ->
          let* () = gather Object.root root_hash in
          ok (!objects, !files)

  let remove_object location hash =
    let path = file_path_of_object_hash hash in
    match Device.remove_file location path with
      | ERROR { code = `no_such_file; _ } ->
          unit
      | ERROR { code = `failed; _ } | OK () as x ->
          x

  let remove_file location hash =
    let path = file_path_of_file_hash hash in
    match Device.remove_file location path with
      | ERROR { code = `no_such_file; _ } ->
          unit
      | ERROR { code = `failed; _ } | OK () as x ->
          x

end
