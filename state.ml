open Misc

type 'a hash = 'a Repository.hash

module Filename_set = Set.Make (Path.Filename)
module Filename_map = Map.Make (Path.Filename)

module File_hash =
struct
  type t = Repository.file_hash

  let compare a b =
    Hash.compare (Repository.concrete_file_hash a) (Repository.concrete_file_hash b)
end

module File_hash_map = Map.Make (File_hash)

type dir = dir_entry Filename_map.t

and dir_entry =
  | Dir of { hash: dir hash; total_size: int; total_file_count: int }
  | File of { hash: Repository.file_hash; size: int }

module File_path =
struct
  type t = Device.file_path
  let compare = Device.compare_file_paths
end

module File_path_set = Set.Make (File_path)

type hash_index_bucket = File_path_set.t File_hash_map.t

type hash_index =
  | Leaf of hash_index_bucket hash
  | Node of { zero: hash_index; one: hash_index }

type non_empty_state =
  {
    root_dir: dir hash;
    hash_index: hash_index hash;
  }

type state =
  | Empty
  | Non_empty of non_empty_state

type journal_entry =
  {
    command: string;
    state: state;
  }

type journal =
  {
    redo: journal_entry list; (* from oldest to newest *)
    head: journal_entry;
    undo: journal_entry list; (* from newest to oldest *)
  }

module Object =
struct

  type _ t =
    | Journal: journal t
    | Dir: dir t
    | Hash_index: hash_index t
    | Hash_index_bucket: hash_index_bucket t

  let describe_type (type a) (typ: a t) =
    match typ with
      | Journal -> "journal"
      | Dir -> "directory"
      | Hash_index -> "hash index"
      | Hash_index_bucket -> "hash index bucket"

  type e = E: _ t -> e [@@unboxed]

  type v = V: 'a t * 'a -> v

  type root = journal

  let root = Journal

  type dep =
    | Object: 'a t * 'a hash -> dep
    | File: Repository.file_hash -> dep

  let write_type (type a) (buffer: W.buffer) (typ: a t) =
    W.(string @@ fun _ _ -> ()) buffer @@
    match typ with
      | Journal ->
          "FJourna1"
      | Dir ->
          "FDirect1"
      | Hash_index ->
          "FHIndex1"
      | Hash_index_bucket ->
          "FHBuket1"

  let read_type (buffer: R.buffer) =
    match R.(string @@ fun _ -> 8) buffer with
      | "FJourna1" ->
          E Journal
      | "FDirect1" ->
          E Dir
      | "FHIndex1" ->
          E Hash_index
      | "FHBuket1" ->
          E Hash_index_bucket
      | s ->
          raise (Failed [ sf "unknown object type: %S" s ])

  let () =
    assert (Hash.bin_length = 32)

  let write_concrete_hash buffer (hash: Hash.t) =
    W.(string @@ fun _ _ -> ()) buffer (Hash.to_bin hash)

  let read_concrete_hash buffer: Hash.t =
    let hash_bin = R.(string @@ fun _ -> Hash.bin_length) buffer in
    match hash_bin |> Hash.of_bin with
      | None ->
          raise (Failed [ sf "invalid hash: %s" (Hash.hex_of_string hash_bin) ])
      | Some hash ->
          hash

  let write_hash (type a) buffer (value: a Repository.hash) =
    match Repository.concrete_hash value with
      | None ->
          raise (Failed [ "cannot write hash"; "object is not stored yet" ])
      | Some hash ->
          write_concrete_hash buffer hash

  let read_hash (type a) buffer: a Repository.hash =
    let hash = read_concrete_hash buffer in
    Repository.stored_hash hash

  let write_file_hash buffer (value: Repository.file_hash) =
    write_concrete_hash buffer (Repository.concrete_file_hash value)

  let read_file_hash buffer: Repository.file_hash =
    let hash = read_concrete_hash buffer in
    Repository.stored_file_hash hash

  (* TODO: review all sizes to use unsigned ints *)
  let write_filename buffer (value: Path.Filename.t) =
    W.(string int_u16) buffer (Path.Filename.show value)

  let read_filename buffer: Path.Filename.t =
    let filename = R.(string int_u16) buffer in
    match Path.Filename.parse filename with
      | None ->
          raise (Failed [ sf "invalid filename: %S" filename ])
      | Some filename ->
          filename

  let write_path buffer (path: Device.path) =
    W.(list int_u16) write_filename buffer path

  let write_file_path buffer ((dir_path, filename): Device.file_path) =
    write_path buffer dir_path;
    write_filename buffer filename

  let read_path buffer: Device.path =
    R.(list int_u16) read_filename buffer

  let read_file_path buffer: Device.file_path =
    let dir_path = read_path buffer in
    let filename = read_filename buffer in
    dir_path, filename

  let write_journal buffer ({ redo; head; undo }: journal) =
    let write_journal_entry buffer ({ command; state }: journal_entry) =
      W.(string int_u16) buffer command;
      match state with
        | Empty ->
            W.int_u8 buffer 0
        | Non_empty { root_dir; hash_index } ->
            W.int_u8 buffer 1;
            write_hash buffer root_dir;
            write_hash buffer hash_index
    in
    write_journal_entry buffer head;
    W.(list int_32) write_journal_entry buffer redo;
    W.(list int_32) write_journal_entry buffer undo

  let read_journal buffer: journal =
    let read_journal_entry buffer =
      let command = R.(string int_u16) buffer in
      let state =
        match R.int_u8 buffer with
          | 0 ->
              Empty
          | 1 ->
              let root_dir = read_hash buffer in
              let hash_index = read_hash buffer in
              Non_empty { root_dir; hash_index }
          | x ->
              raise (Failed [ sf "invalid journal entry state: %d" x ])
      in
      { command; state }
    in
    let head = read_journal_entry buffer in
    let redo = R.(list int_32) read_journal_entry buffer in
    let undo = R.(list int_32) read_journal_entry buffer in
    { redo; head; undo }

  let iter_deps_journal ({ redo; head; undo }: journal) f =
    let iter_journal_entry { command = (_: string); state } =
      match state with
        | Empty ->
            unit
        | Non_empty { root_dir; hash_index } ->
            let* () = f (Object (Dir, root_dir)) in
            f (Object (Hash_index, hash_index))
    in
    let* () = iter_journal_entry head in
    let* () = list_iter_e redo iter_journal_entry in
    list_iter_e undo iter_journal_entry

  let write_dir buffer (value: dir) =
    W.int_32 buffer (Filename_map.cardinal value);
    Filename_map.iter' value @@ fun filename dir_entry ->
    write_filename buffer filename;
    match dir_entry with
      | Dir { hash; total_size; total_file_count } ->
          W.int_u8 buffer 0;
          write_hash buffer hash;
          W.int_64 buffer total_size;
          W.int_32 buffer total_file_count
      | File { hash; size } ->
          W.int_u8 buffer 1;
          write_file_hash buffer hash;
          W.int_64 buffer size

  let read_dir buffer: dir =
    let count = R.int_32 buffer in
    let result = ref Filename_map.empty in
    for _ = 1 to count do
      let filename = read_filename buffer in
      let dir_entry: dir_entry =
        match R.int_u8 buffer with
          | 0 ->
              let hash = read_hash buffer in
              let total_size = R.int_64 buffer in
              let total_file_count = R.int_32 buffer in
              Dir { hash; total_size; total_file_count }
          | 1 ->
              let hash = read_file_hash buffer in
              let size = R.int_64 buffer in
              File { hash; size }
          | x ->
              raise (Failed [ sf "invalid directory entry: %d" x ])
      in
      result := Filename_map.add filename dir_entry !result
    done;
    !result

  let iter_deps_dir (value: dir) f =
    list_iter_e (Filename_map.bindings value) @@ fun ((_: Path.Filename.t), dir_entry) ->
    match dir_entry with
      | Dir { hash; total_size = (_: int); total_file_count = (_: int) } ->
          f (Object (Dir, hash))
      | File { hash; size = (_: int) } ->
          f (File hash)

  let rec write_hash_index buffer (value: hash_index) =
    match value with
      | Leaf bucket_hash ->
          W.int_u8 buffer 0;
          write_hash buffer bucket_hash
      | Node { zero; one } ->
          W.int_u8 buffer 1;
          write_hash_index buffer zero;
          write_hash_index buffer one

  let rec read_hash_index buffer: hash_index =
    match R.int_u8 buffer with
      | 0 ->
          let bucket_hash = read_hash buffer in
          Leaf bucket_hash
      | 1 ->
          let zero = read_hash_index buffer in
          let one = read_hash_index buffer in
          Node { zero; one }
      | x ->
          raise (Failed [ sf "invalid hash index node: %d" x ])

  let rec iter_deps_hash_index (value: hash_index) f =
    match value with
      | Leaf bucket_hash ->
          f (Object (Hash_index_bucket, bucket_hash))
      | Node { zero; one } ->
          let* () = iter_deps_hash_index zero f in
          iter_deps_hash_index one f

  let write_hash_index_bucket buffer (value: hash_index_bucket) =
    W.int_32 buffer (File_hash_map.cardinal value);
    File_hash_map.iter' value @@ fun file_hash file_path_set ->
    write_file_hash buffer file_hash;
    W.int_u16 buffer (File_path_set.cardinal file_path_set);
    File_path_set.iter' file_path_set (write_file_path buffer)

  let read_hash_index_bucket buffer: hash_index_bucket =
    let count = R.int_32 buffer in
    let result = ref File_hash_map.empty in
    for _ = 1 to count do
      let file_hash = read_file_hash buffer in
      let file_path_count = R.int_u16 buffer in
      let paths = ref File_path_set.empty in
      for _ = 1 to file_path_count do
        let path = read_file_path buffer in
        paths := File_path_set.add path !paths
      done;
      result := File_hash_map.add file_hash !paths !result
    done;
    !result

  let iter_deps_hash_index_bucket (value: hash_index_bucket) f =
    list_iter_e (File_hash_map.bindings value) @@ fun (file_hash, (_: File_path_set.t)) ->
    f (File file_hash)

  let encode (type a) (typ: a t) (value: a) =
    W.to_string @@ fun buffer ->
    write_type buffer typ;
    match typ with
      | Journal ->
          write_journal buffer value
      | Dir ->
          write_dir buffer value
      | Hash_index ->
          write_hash_index buffer value
      | Hash_index_bucket ->
          write_hash_index_bucket buffer value

  let read_any buffer =
    let E typ = read_type buffer in
    let value (type a) (typ: a t): a =
      match typ with
        | Journal ->
            read_journal buffer
        | Dir ->
            read_dir buffer
        | Hash_index ->
            read_hash_index buffer
        | Hash_index_bucket ->
            read_hash_index_bucket buffer
    in
    V (typ, value typ)

  let decode_any string =
    decode_rawbin_string read_any string

  let decode (type a) (typ: a t) string =
    let* V (actual_type, value) = decode_any string in
    let wrong_type () =
      failed [
        sf "expected %s, got %s" (describe_type typ) (describe_type actual_type)
      ]
    in
    match actual_type, typ with
      | Journal, Journal ->
          ok (value: a)
      | Journal, _ ->
          wrong_type ()
      | Dir, Dir ->
          ok (value: a)
      | Dir, _ ->
          wrong_type ()
      | Hash_index, Hash_index ->
          ok (value: a)
      | Hash_index, _ ->
          wrong_type ()
      | Hash_index_bucket, Hash_index_bucket ->
          ok (value: a)
      | Hash_index_bucket, _ ->
          wrong_type ()

  let iter_deps (type a) (typ: a t) (value: a) f =
    match typ with
      | Journal ->
          iter_deps_journal value f
      | Dir ->
          iter_deps_dir value f
      | Hash_index ->
          iter_deps_hash_index value f
      | Hash_index_bucket ->
          iter_deps_hash_index_bucket value f

end

module Repo = Repository.Make (Object)
