open Misc

type _ hash = Hash.t

let hash_type =
  let open Protype in
  convert_partial string
    ~encode: Hash.to_bin
    ~decode: Hash.of_bin

let hex_of_hash = Hash.to_hex
let bin_of_hash = Hash.to_bin
let compare_hashes = Hash.compare

module Raw_hash_set = Set.Make (Hash)

type file

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

  val store: t -> 'a Protype.t -> 'a ->
    ('a hash, [> `failed ]) r

  val fetch: t -> 'a Protype.t -> 'a hash ->
    ('a, [> `failed | `not_available ]) r

  val store_file: source: Device.location -> source_path: Device.file_path -> target: t ->
    on_progress: (bytes: int -> size: int -> unit) ->
    (file hash * int, [> `failed ]) r

  val fetch_file: source: t -> file hash ->
    target: Device.location -> target_path: Device.file_path ->
    on_progress: (bytes: int -> size: int -> unit) ->
    (unit, [> `failed | `not_available | `already_exists ]) r

  val store_root: t -> root ->
    (unit, [> `failed ]) r

  val fetch_root: t ->
    (root, [> `failed ]) r

  val garbage_collect: t -> everything_except: Raw_hash_set.t ->
    (unit, [> `failed ]) r
end

module Make (Root: ROOT): S with type root = Root.t and type t = Device.location =
struct
  type t = Device.location
  type root = Root.t

  let hash_part_length = 2

  let file_path_of_hash hash =
    let hash_string = Hash.to_hex hash in
    (* [Hash.to_hex] always returns a string of length >= 4,
       composed only of valid characters for filenames. *)
    let prefix1 = String.sub hash_string 0 hash_part_length in
    let prefix2 = String.sub hash_string hash_part_length hash_part_length in
    [ Path.Filename.parse_exn prefix1; Path.Filename.parse_exn prefix2 ],
    Path.Filename.parse_exn hash_string

  let store location typ value =
    let encoded_value = Protype_robin.Encode.to_string ~version: Root.version typ value in
    let hash = Hash.string encoded_value in
    trace ("failed to store object with hash " ^ hex_of_hash hash) @@
    let path = file_path_of_hash hash in
    let* already_exists = Device.file_exists location path in
    if already_exists then
      ok hash
    else
      let* () = Device.write_file location path encoded_value in
      ok hash

  let fetch location typ hash =
    trace ("failed to fetch object with hash " ^ hex_of_hash hash) @@
    let path = file_path_of_hash hash in
    match Device.read_file location path with
      | ERROR { code = `no_such_file; msg } ->
          ERROR { code = `not_available; msg }
      | ERROR { code = `failed; _ } as x ->
          x
      | OK encoded_value ->
          decode_robin_string typ encoded_value

  let store_file ~source ~source_path ~target ~on_progress =
    trace ("failed to store " ^ Device.show_file_path source_path) @@
    match Device.stat source (Device.path_of_file_path source_path) with
      | ERROR { code = (`no_such_file | `failed); msg } ->
          failed msg
      | OK Dir ->
          failed [ Device.show_file_path source_path ^ " is a directory" ]
      | OK (File { size; _ }) ->
          let* hash = Device.hash source source_path in
          let hash_path = file_path_of_hash hash in
          let* already_exists = Device.file_exists target hash_path in
          if already_exists then
            ok (hash, size)
          else
            match
              Device.copy_file ~on_progress: (fun bytes -> on_progress ~bytes ~size)
                ~source: (source, source_path) ~target: (target, hash_path)
            with
              | ERROR { code = (`no_such_file | `failed); msg } ->
                  failed msg
              | OK () ->
                  ok (hash, size)

  let fetch_file ~source hash ~target ~target_path ~on_progress =
    trace ("failed to fetch file " ^ Device.show_file_path target_path) @@
    let hash_path = file_path_of_hash hash in
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
          else
            match
              Device.copy_file ~on_progress: (fun bytes -> on_progress ~bytes ~size)
                ~source: (source, hash_path) ~target: (target, target_path)
            with
              | ERROR { code = (`no_such_file | `failed); msg } ->
                  failed msg
              | OK () as x ->
                  x

  let root_path = [], Path.Filename.parse_exn "root"

  let store_root location root =
    trace "failed to store root" @@
    let encoded_root = Protype_robin.Encode.to_string ~version: Root.version Root.typ root in
    Device.write_file location root_path encoded_root

  let fetch_root location =
    trace "failed to fetch root" @@
    match Device.read_file location root_path with
      | ERROR { code = (`no_such_file | `failed); msg } ->
          failed msg
      | OK encoded_root ->
          decode_robin_string Root.typ encoded_root

  let garbage_collect location ~everything_except =
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
          | OK (File { path = file_path; _ }) ->
              match Hash.of_hex filename_string with
                | None ->
                    unit
                | Some hash ->
                    if Raw_hash_set.mem hash everything_except then
                      unit
                    else
                      Device.remove_file location file_path
      in
      garbage_collect_dir []
    with
      | ERROR { code = `no_such_file; _ } ->
          unit
      | ERROR { code = `failed; _ } | OK () as x ->
          x
end
