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

type hash_index_entry =
  {
    hash: Repository.file hash;
    paths: File_path_set.t;
  }

type hash_index = hash_index_node Map.Char.t

and hash_index_node =
  | Node of hash_index hash
  | Entry of hash_index_entry

type non_empty_root =
  {
    root_dir: dir hash;
    hash_index: hash_index hash;
  }

type root =
  | Empty
  | Non_empty of non_empty_root

type journal_entry =
  {
    command: string;
    root: root;
  }

type journal =
  {
    redo: journal_entry list; (* from oldest to newest *)
    head: journal_entry;
    undo: journal_entry list; (* from newest to oldest *)
  }

type clone_config =
  {
    main_location: Device.location;
  }

module T =
struct
  open Protype
  open Device_common.T

  let hash = Repository.hash_type
  let file_hash = Repository.file_hash_type

  let dir: dir t =
    recursive @@ fun dir ->
    let entry =
      let dir_record =
        record @@
        ("name", filename, fun (x, _, _, _) -> x) @
        ("hash", hash dir, fun (_, x, _, _) -> x) @
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

  let file_path_set =
    convert (list file_path)
      ~encode: File_path_set.elements
      ~decode: File_path_set.of_list

  let hash_index_entry: hash_index_entry t =
    record @@
    ("hash", file_hash, fun r -> r.hash) @
    ("paths", file_path_set, fun r -> r.paths) @:
    fun hash paths -> { hash; paths }

  let hash_index: hash_index t =
    recursive @@ fun hash_index ->
    let hash_index_node: hash_index_node t =
      let hi_node = case "Node" (hash hash_index) (fun x -> Node x) in
      let hi_entry = case "Entry" hash_index_entry (fun x -> Entry x) in
      variant [ Case hi_node; Case hi_entry ] @@
      function Node x -> value hi_node x | Entry x -> value hi_entry x
    in
    let entry: (char * hash_index_node) t =
      record @@
      ("key", char, fst) @
      ("node", hash_index_node, snd) @:
      fun key node -> key, node
    in
    convert (list entry) ~encode: Map.Char.bindings ~decode: Map.Char.of_list

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

  let journal_entry: journal_entry t =
    record @@
    ("command", string, fun x -> x.command) @
    ("root", root, fun x -> x.root) @:
    fun command root -> { command; root }

  let journal: journal t =
    record @@
    ("redo", list journal_entry, fun x -> x.redo) @
    ("head", journal_entry, fun x -> x.head) @
    ("undo", list journal_entry, fun x -> x.undo) @:
    fun redo head undo -> { redo; head; undo }

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
  type t = journal
  let version = protocol_version
  let typ = T.journal
end

module Bare = Repository.Make (Root)

module Repository =
struct
  include Repository
  include Clone.Make (Bare)
end
