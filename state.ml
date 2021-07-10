open Misc

type 'a hash = 'a Repository.hash

module Filename_map = Map.Make (Path.Filename)

module File_hash =
struct
  type t = Repository.file hash
  let compare = Repository.compare_hashes
end

module File_hash_map = Map.Make (File_hash)

type dir = dir_entry Filename_map.t

and dir_entry =
  | Dir of { hash: dir hash; total_size: int; total_file_count: int }
  | File of { hash: Repository.file hash; size: int }

module File_path =
struct
  type t = Device.file_path
  let compare = Device.compare_file_paths
end

module File_path_set = Set.Make (File_path)

type hash_index_2 = File_path_set.t File_hash_map.t

type hash_index_1 = hash_index_2 hash Map.Char.t

type hash_index = hash_index_1 hash Map.Char.t

type non_empty_root =
  {
    root_dir: dir hash;
    hash_index: hash_index hash;
  }

type root =
  | Empty
  | Non_empty of non_empty_root

type clone_config =
  {
    main_location: Device.location;
  }

module T =
struct
  open Protype

  let hash = Repository.hash_type
  let file_hash = Repository.file_hash_type

  let filename: Path.Filename.t t =
    convert_partial string
      ~encode: Path.Filename.show
      ~decode: Path.Filename.parse

  let dir: dir t =
    recursive @@ fun dir ->
    let entry =
      let dir_record =
        record @@
        ("name", filename, fun (x, _, _, _) -> x) @
        ("hash", hash dir, fun (_, x, _, _) -> x) @ (* TODO *)
        ("size", int, fun (_, _, x, _) -> x) @
        ("count", int, fun (_, _, _, x) -> x) @:
        fun name hash size count -> name, hash, size, count
      in
      let file_record =
        record @@
        ("name", filename, fun (x, _, _) -> x) @
        ("hash", file_hash, fun (_, x, _) -> x) @
        ("size", int, fun (_, _, x) -> x) @:
        fun name hash size -> name, hash, size
      in
      let dir = case "Dir" dir_record (fun x -> `dir x) in
      let file = case "File" file_record (fun x -> `file x) in
      variant [ Case dir; Case file ] @@
      function `dir x -> value dir x | `file x -> value file x
    in
    let encode map =
      List.map' (Filename_map.bindings map) @@ fun (filename, entry) ->
      match entry with
        | Dir { hash; total_size; total_file_count } ->
            `dir (filename, hash, total_size, total_file_count)
        | File { hash; size } ->
            `file (filename, hash, size)
    in
    let decode list =
      Some (
        List.fold_left' Filename_map.empty list @@ fun acc entry ->
        match entry with
          | `dir (filename, hash, total_size, total_file_count) ->
              Filename_map.add filename (Dir { hash; total_size; total_file_count }) acc
          | `file (filename, hash, size) ->
              Filename_map.add filename (File { hash; size }) acc
      )
    in
    Convert { typ = list entry; encode; decode }

  let file_path: Device.file_path t =
    convert_partial (list filename)
      ~encode: (fun (dir, file) -> List.append dir [ file ])
      ~decode: (
        fun list ->
          match List.rev list with
            | [] ->
                None
            | file :: dir_rev ->
                Some (List.rev dir_rev, file)
      )

  let file_path_set =
    convert (list file_path)
      ~encode: File_path_set.elements
      ~decode: File_path_set.of_list

  let hash_index_2: hash_index_2 t =
    let entry =
      record @@
      ("hash", file_hash, fst) @
      ("paths", file_path_set, snd) @:
      fun hash paths -> hash, paths
    in
    convert (list entry) ~encode: File_hash_map.bindings ~decode: File_hash_map.of_list

  let char_to_hash_map typ =
    let entry =
      record @@
      ("key", char, fst) @
      ("hash", hash typ, snd) @:
      fun key hash -> key, hash
    in
    convert (list entry) ~encode: Map.Char.bindings ~decode: Map.Char.of_list

  let hash_index_1: hash_index_1 t = char_to_hash_map hash_index_2

  let hash_index: hash_index t = char_to_hash_map hash_index_1

  let non_empty_root: non_empty_root t =
    record @@
    ("root_dir", hash dir, fun x -> x.root_dir) @
    ("hash_index", hash hash_index, fun x -> x.hash_index) @:
    fun root_dir hash_index -> { root_dir; hash_index }

  let root: root t =
    let empty = case "Empty" unit (fun () -> Empty) in
    let non_empty = case "Non_empty" non_empty_root (fun x -> Non_empty x) in
    variant [ Case empty; Case non_empty ] @@
    function Empty -> value empty () | Non_empty x -> value non_empty x

  let location: Device.location t =
    convert_partial string
      ~encode: Device.show_location
      ~decode: (
        fun s ->
          (* TODO: do we want to be able to configure main repositories in read-only? *)
          match Device.parse_location RW s with
            | ERROR _ -> None
            | OK x -> Some x
      )

  let clone_config: clone_config t =
    let open Protype in
    record @@
    ("main", location, fun r -> r.main_location) @:
    fun main_location -> { main_location }
end

module Root =
struct
  type t = root

  (* We use version numbers starting from 0xF1100, which is supposed to look like "FILOU".
     This acts as a kind of magic number. *)
  let version = 0xF1100

  let typ = T.root
end

module Bare = Repository.Make (Root)

module Repository =
struct
  include Repository
  include Clone.Make (Bare)
end
