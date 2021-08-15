open Misc
open State

type value =
  | Hash of Hash.t option
  | String of string
  | Int of int
  | List of value list
  | Record of (string * value) list
  | Variant of string * (string * value) list
  | Map of (value * value) list

let value_of_hash hash =
  Hash (Repository.concrete_hash hash)

let value_of_file_hash hash =
  Hash (Some (Repository.concrete_file_hash hash))

let value_of_state = function
  | Empty ->
      Variant ("Empty", [])
  | Non_empty { hash_index; root_dir } ->
      Variant (
        "Non_empty",
        [
          "hash_index", value_of_hash hash_index;
          "root_dir", value_of_hash root_dir;
        ]
      )

let value_of_journal_entry { command; state } =
  Record [
    "command", String command;
    "state", value_of_state state;
  ]

let value_of_journal { redo; head; undo } =
  Record [
    "redo", List (List.map value_of_journal_entry redo);
    "head", value_of_journal_entry head;
    "undo", List (List.map value_of_journal_entry undo);
  ]

let value_of_dir_entry = function
  | Dir { hash; total_size; total_file_count } ->
      Variant (
        "Dir",
        [
          "hash", value_of_hash hash;
          "total_size", Int total_size;
          "total_file_count", Int total_file_count;
        ]
      )
  | File { hash; size } ->
      Variant (
        "File",
        [
          "hash", value_of_file_hash hash;
          "size", Int size;
        ]
      )

let value_of_dir (dir: dir) =
  Map (
    List.map' (Filename_map.bindings dir) @@ fun (filename, dir_entry) ->
    String (Path.Filename.show filename),
    value_of_dir_entry dir_entry
  )

let rec value_of_hash_index = function
  | Leaf hash ->
      value_of_hash hash
  | Node { zero; one } ->
      Map [
        Int 0, value_of_hash_index zero;
        Int 1, value_of_hash_index one;
      ]

let value_of_file_path path =
  String (Device.show_file_path path)

let value_of_file_path_set paths =
  List (List.map value_of_file_path (File_path_set.elements paths))

let value_of_hash_index_bucket (bucket: hash_index_bucket) =
  Map (
    List.map' (File_hash_map.bindings bucket) @@ fun (hash, paths) ->
    value_of_file_hash hash, value_of_file_path_set paths
  )

let value_of_object (Object.V (typ, value)) =
  match typ with
    | Journal ->
        value_of_journal value
    | Dir ->
        value_of_dir value
    | Hash_index ->
        value_of_hash_index value
    | Hash_index_bucket ->
        value_of_hash_index_bucket value

let show ?(indent = 0) value =
  let indent = ref indent in
  let buffer = Buffer.create 512 in
  let adds = Buffer.add_string buffer in
  let addc = Buffer.add_char buffer in
  let nl () =
    addc '\n';
    adds (String.make (2 * !indent) ' ')
  in
  let addl left right list add_item =
    match list with
      | [] ->
          addc left;
          addc right
      | [ head ] ->
          addc left;
          addc ' ';
          incr indent;
          add_item head;
          decr indent;
          addc ' ';
          addc right
      | items ->
          addc left;
          incr indent;
          (
            List.iter' items @@ fun item ->
            nl ();
            add_item item;
            addc ';';
          );
          decr indent;
          nl ();
          addc right
  in
  let rec addv value =
    match value with
      | Hash None ->
          adds "(not stored yet)"
      | Hash (Some hash) ->
          adds (Hash.to_hex hash)
      | String s ->
          addc '"';
          adds (String.escaped s);
          addc '"'
      | Int i ->
          adds (string_of_int i)
      | List l ->
          addl '[' ']' l addv
      | Record l ->
          addr l
      | Variant (name, []) ->
          adds name
      | Variant (name, l) ->
          adds name;
          addc ' ';
          addr l
      | Map l ->
          addl '{' '}' l @@ fun (k, v) ->
          addv k;
          adds " = ";
          addv v
  and addr l =
    addl '{' '}' l @@ fun (k, v) ->
    adds k;
    adds " = ";
    addv v
  in
  addv value;
  Buffer.contents buffer
