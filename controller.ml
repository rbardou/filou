open Misc
open State

(* TODO: use [Device.with_lock] for read-write operations?
   Note that the damage that can be done if we don't is not terrible:
   it just means that if there are multiple concurrent accesses, the last
   one wins, but the repository is not corrupted: it only contains some
   unreachable objects. *)

let empty_dir: dir = Filename_map.empty

let fetch_or_fail setup hash =
  match Repository.fetch setup hash with
    | ERROR { code = (`not_available | `failed); msg } ->
        failed msg
    | OK _ as x ->
        x

let on_copy_progress prefix ~bytes ~size =
  Prout.minor (fun () -> sf "%s (%d / %d) (%d%%)" prefix bytes size (bytes * 100 / size))

let init (location: Device.location) =
  Bare.store_root location {
    redo = [];
    head = { command = "init"; root = Empty };
    undo = [];
  }

let write_clone_config ~clone_location config =
  Device.write_file clone_location dot_filou_config
    (Protype_robin.Encode.to_string ~version: Root.version T.clone_config config)

let read_clone_config ~clone_location =
  trace "failed to read configuration file" @@
  let* contents = Device.read_file clone_location dot_filou_config in
  decode_robin_string T.clone_config contents

let clone ~(main_location: Device.location) ~(clone_location: Device.location) =
  Prout.echo "Cloning %s into: %s" (Device.show_location main_location)
    (Device.show_location clone_location);
  (* Check that the remote repository is readable. *)
  let* _ = Bare.fetch_root main_location in
  (* Initialize the clone. *)
  write_clone_config ~clone_location { main_location }

let find_local_clone_config mode =
  let rec find current =
    let clone_location = Device.Local (mode, current) in
    match read_clone_config ~clone_location with
      | OK config ->
          ok (clone_location, config)
      | ERROR { code = (`no_such_file | `failed); _ } ->
          match Path.parent current with
            | None -> failed [ "not in a clone repository" ]
            | Some parent -> find parent
  in
  find (Path.get_cwd ())

let find_local_clone ~clone_only mode =
  let* clone_location, config = find_local_clone_config mode in
  let main =
    if clone_only then
      None
    else
      Some config.main_location
  in
  ok (Clone.setup ~main ~workdir: clone_location ())

let store_later = Repository.store_later

let fetch_root setup =
  let* journal = Repository.fetch_root setup in
  ok journal.head.root

let store_root setup root command =
  (* TODO: store journal in memory to avoid re-reading it *)
  let* journal = Repository.fetch_root setup in
  let new_journal =
    {
      redo = [];
      head = { command; root };
      undo = journal.head :: journal.undo;
    }
  in
  Repository.store_root setup new_journal

let fetch_root_dir setup (root: root) =
  match root with
    | Empty ->
        ok Filename_map.empty
    | Non_empty root ->
        fetch_or_fail setup root.root_dir

module Hash_index =
struct

  let max_bucket_size = 512

  (* With 1M hashes and buckets of size 512, assuming buckets are half full,
     we need around 4000 buckets. So the file that stores the [hash_index]
     itself can be about, what, 200KB? Maybe up to 1MB? Which is fine?
     We could add a [Big_node] constructor to split the [hash_index] itself
     once it gets too big. *)

  (* Return [false] for [0], [true] for [1].
     If [bit] is outside [string], returns [0] as if it was padded with zeros. *)
  let get_bit string bit =
    let char_index = bit / 8 in
    if char_index >= String.length string then
      false
    else
      let byte = Char.code string.[char_index] in
      let bit = bit mod 8 in
      (byte lsr (7 - bit)) land 1 <> 0

  let rec check_prefix ?(from = 0) hash_bin = function
    | [] ->
        unit
    | head :: tail ->
        if get_bit hash_bin from <> head then
          failed [ sf "%s is in the wrong bucket" (Hash.hex_of_string hash_bin) ]
        else
          check_prefix ~from: (from + 1) hash_bin tail

  (* Read the entire hash index and return a single map containing all of it.
     One can expect less than 200 bytes per hash, so 200MB for 1M files.
     It starts to be too big in memory after 10M files? *)
  let fetch_all setup
      (hash_index_hash: hash_index hash): (File_path_set.t File_hash_map.t, [> `failed ]) r =
    let* hash_index = fetch_or_fail setup hash_index_hash in
    let rec gather prefix_rev acc (hash_index: hash_index) =
      match hash_index with
        | Leaf bucket_hash ->
            let* bucket = fetch_or_fail setup bucket_hash in
            list_fold_e acc (File_hash_map.bindings bucket) @@ fun acc (hash, paths) ->
            (* This helper is used by [check], so we can fail if we see something weird.
               Otherwise we could assume that hashes appear only in one bucket. *)
            let* () = check_prefix (Repository.bin_of_hash hash) (List.rev prefix_rev) in
            if File_hash_map.mem hash acc then
              failed [ sf "hash %s appears in two buckets" (Repository.hex_of_hash hash) ]
            else
              ok (File_hash_map.add hash paths acc)
        | Node { zero; one } ->
            let* acc = gather (false :: prefix_rev) acc zero in
            gather (true :: prefix_rev) acc one
    in
    gather [] File_hash_map.empty hash_index

  let get setup (root: root) (hash: Repository.file hash): (File_path_set.t, [> `failed ]) r =
    match root with
      | Empty ->
          ok File_path_set.empty
      | Non_empty root ->
          let* hash_index = fetch_or_fail setup root.hash_index in
          let hash_bin = Repository.bin_of_hash hash in
          let rec find bit (hash_index: hash_index) =
            match hash_index with
              | Leaf bucket_hash ->
                  let* bucket = fetch_or_fail setup bucket_hash in
                  File_hash_map.find_opt hash bucket |> default File_path_set.empty |> ok
              | Node { zero; one } ->
                  find (bit + 1) (if get_bit hash_bin bit then zero else one)
          in
          find 0 hash_index

  (* Also returns the old list of paths that were associated to this hash. *)
  let add setup (root: root) (added_file_hash: Repository.file hash)
      (path: Device.file_path): (hash_index hash * File_path_set.t, [> `failed ]) r =
    match root with
      | Empty ->
          let bucket =
            File_hash_map.singleton added_file_hash (File_path_set.singleton path)
          in
          let* bucket_hash = store_later setup T.hash_index_bucket bucket in
          let hash_index = Leaf bucket_hash in
          let* hash_index_hash = store_later setup T.hash_index hash_index in
          ok (hash_index_hash, File_path_set.empty)
      | Non_empty root ->
          let* hash_index = fetch_or_fail setup root.hash_index in
          let hash_bin = Repository.bin_of_hash added_file_hash in
          let add_to_bucket bucket =
            let old_paths =
              File_hash_map.find_opt added_file_hash bucket
              |> default File_path_set.empty
            in
            let new_paths = File_path_set.add path old_paths in
            File_hash_map.add added_file_hash new_paths bucket, old_paths
          in
          let rec add_to bit (hash_index: hash_index): (hash_index * File_path_set.t, _) r =
            match hash_index with
              | Leaf bucket_hash ->
                  let* bucket = fetch_or_fail setup bucket_hash in
                  (* One could think that storing the size of each bucket in
                     the [hash_index] would allow to make the following test
                     without having to read the bucket, but we'll have to read it
                     anyway to add the path to it or to split it if it's too big. *)
                  if File_hash_map.cardinal bucket < max_bucket_size then
                    (* Bucket has room for more hashes. *)
                    let new_bucket, old_paths = add_to_bucket bucket in
                    let* new_bucket_hash = store_later setup T.hash_index_bucket new_bucket in
                    ok (Leaf new_bucket_hash, old_paths)
                  else
                    (* Bucket is too big, split it.
                       In theory we should split recursively in case one of the two
                       resulting buckets has 0 entries. But the probability of this
                       is 1/2^max_bucket_size, i.e. very, VERY low. MUCH lower than
                       the probability of having hash collisions. And even if this
                       happens, nothing will break anyway. *)
                    let one, zero =
                      File_hash_map.partition' bucket @@ fun hash _ ->
                      get_bit (Repository.bin_of_hash hash) bit
                    in
                    let one, zero, old_paths =
                      if get_bit hash_bin bit then
                        let one, old_paths = add_to_bucket one in
                        one, zero, old_paths
                      else
                        let zero, old_paths = add_to_bucket zero in
                        one, zero, old_paths
                    in
                    let* one_hash = store_later setup T.hash_index_bucket one in
                    let* zero_hash = store_later setup T.hash_index_bucket zero in
                    ok (Node { zero = Leaf zero_hash; one = Leaf one_hash }, old_paths)
              | Node { zero; one } ->
                  if get_bit hash_bin bit then
                    let* new_one, old_paths = add_to (bit + 1) one in
                    ok (Node { zero; one = new_one }, old_paths)
                  else
                    let* new_zero, old_paths = add_to (bit + 1) zero in
                    ok (Node { zero = new_zero; one }, old_paths)
          in
          let* new_hash_index, old_paths = add_to 0 hash_index in
          let* new_hash_index_hash = store_later setup T.hash_index new_hash_index in
          ok (new_hash_index_hash, old_paths)

  (* Does not actually store the result so that one can bulk remove. *)
  let remove setup (hash_index: hash_index) (hash: Repository.file hash)
      (path: Device.file_path): (hash_index, [> `failed ]) r =
    let hash_bin = Repository.bin_of_hash hash in
    let rec remove_from bit (hash_index: hash_index): (hash_index, _) r =
      match hash_index with
        | Leaf bucket_hash ->
            let* bucket = fetch_or_fail setup bucket_hash in
            let old_paths =
              File_hash_map.find_opt hash bucket |> default File_path_set.empty
            in
            let new_paths = File_path_set.remove path old_paths in
            let new_bucket =
              if File_path_set.is_empty new_paths then
                File_hash_map.remove hash bucket
              else
                File_hash_map.add hash new_paths bucket
            in
            (* TODO: if bucket becomes too small, merge it with its neighbor, if any. *)
            let* new_bucket_hash = store_later setup T.hash_index_bucket new_bucket in
            ok (Leaf new_bucket_hash)
        | Node { zero; one } ->
            if get_bit hash_bin bit then
              let* new_one = remove_from (bit + 1) one in
              ok (Node { zero; one = new_one })
            else
              let* new_zero = remove_from (bit + 1) zero in
              ok (Node { zero = new_zero; one })
    in
    remove_from 0 hash_index

end

module Dir =
struct

  type add_result =
    | Already_exists
    | Added of {
        new_dir_hash: dir hash;
        added_file_hash: Repository.file hash;
        added_file_size: int;
      }

  let add setup (root_dir: dir) ((dir_path, filename): Device.file_path)
      (get_hash_and_size: unit -> (Repository.file Repository.hash * int, _) r):
    (add_result, [> `failed ]) r =
    let rec update_dir dir_path_rev (dir: dir) = function
      | head :: tail ->
          let* head_dir, head_dir_total_size, head_dir_total_file_count =
            match Filename_map.find_opt head dir with
              | None ->
                  ok (empty_dir, 0, 0)
              | Some (File _) ->
                  failed [
                    "parent directory already exists as a file: " ^
                    (Device.show_file_path (List.rev dir_path_rev, head));
                  ]
              | Some (Dir { hash = head_dir_hash; total_size; total_file_count }) ->
                  let* dir = fetch_or_fail setup head_dir_hash in
                  ok (dir, total_size, total_file_count)
          in
          let* update_result =
            update_dir (head :: dir_path_rev) head_dir tail
          in
          (
            match update_result with
              | Already_exists ->
                  ok Already_exists
              | Added {
                  new_dir_hash = new_head_dir_hash;
                  added_file_hash;
                  added_file_size;
                } ->
                  let head_dir_entry =
                    Dir {
                      hash = new_head_dir_hash;
                      total_size = head_dir_total_size + added_file_size;
                      total_file_count = head_dir_total_file_count + 1;
                    }
                  in
                  let new_dir = Filename_map.add head head_dir_entry dir in
                  let* new_dir_hash = store_later setup T.dir new_dir in
                  ok (Added { new_dir_hash; added_file_hash; added_file_size })
          )
      | [] ->
          match Filename_map.find_opt filename dir with
            | Some (Dir _) ->
                failed [ "file already exists as a directory" ]
            | Some (File _) ->
                (* TODO: check if hashes are the same,
                   without calling Repository.store_file *)
                ok Already_exists
            | None ->
                let* added_file_hash, added_file_size = get_hash_and_size () in
                let dir_entry =
                  File {
                    hash = added_file_hash;
                    size = added_file_size;
                  }
                in
                let new_dir = Filename_map.add filename dir_entry dir in
                let* new_dir_hash = store_later setup T.dir new_dir in
                ok (Added { new_dir_hash; added_file_hash; added_file_size })
    in
    update_dir [] root_dir dir_path

end

(* Recursively check sizes, file counts, and whether reachable files are available. *)
let rec check_dir ~clone_only expected_hash_index setup
    (path: Device.path) (dir: dir hash) =
  let* dir = fetch_or_fail setup dir in
  if Filename_map.is_empty dir then
    warn "directory %s is empty, it should just not exist"
      (Device.show_path path);
  let path_size = ref 0 in
  let path_file_count = ref 0 in
  let* () =
    list_iter_e (Filename_map.bindings dir) @@ fun (filename, dir_entry) ->
    let entry_path = path @ [ filename ] in
    match dir_entry with
      | Dir { hash; total_size = expected_size; total_file_count = expected_file_count } ->
          let* size, file_count =
            check_dir ~clone_only expected_hash_index setup entry_path hash
          in
          if size <> expected_size then
            failed [
              sf "wrong size for %s (%s): expected %d, found %d"
                (Device.show_path entry_path) (Repository.hex_of_hash hash)
                expected_size size;
            ]
          else if file_count <> expected_file_count then
            failed [
              sf "wrong file count for %s (%s): expected %d, found %d"
                (Device.show_path entry_path) (Repository.hex_of_hash hash)
                expected_file_count file_count;
            ]
          else (
            path_size := !path_size + size;
            path_file_count := !path_file_count + file_count;
            unit
          )
      | File { hash; size = expected_size } ->
          match
            if clone_only then
              (* Cannot get file sizes in clone-only mode. *)
              ok expected_size
            else
              Repository.get_file_size setup hash
          with
            | ERROR { code = `not_available; msg } ->
                failed (
                  sf "file %s (%s) is unvailable"
                    (Device.show_path entry_path) (Repository.hex_of_hash hash) ::
                  msg
                )
            | ERROR { code = `failed; _ } as x ->
                x
            | OK size ->
                if size <> expected_size then
                  failed [
                    sf "wrong size for %s (%s): expected %d, found %d"
                      (Device.show_path entry_path) (Repository.hex_of_hash hash)
                      expected_size size;
                  ]
                else (
                  path_size := !path_size + expected_size;
                  path_file_count := !path_file_count + 1;
                  expected_hash_index := (
                    let previous =
                      File_hash_map.find_opt hash !expected_hash_index
                      |> default File_path_set.empty
                    in
                    File_hash_map.add hash (File_path_set.add (path, filename) previous)
                      !expected_hash_index
                  );
                  unit
                )
  in
  ok (!path_size, !path_file_count)

let check ~clone_only ~hash setup =
  let* () =
    match hash with
      | `no ->
          unit
      | `metadata | `all as x ->
          let files = match x with `metadata -> false | `all -> true in
          let count = ref 0 in
          let* () =
            Prout.major_s "Checking hashes: computing the set of reachable objects...";
            let* hashes = Repository.reachable ~files setup in
            let hashes = Repository.Hash_set.elements hashes in
            count := List.length hashes;
            let index = ref 0 in
            let progress () =
              Prout.minor @@ fun () ->
              sf "Checking hashes (%d / %d) (%d%%)" !index !count (!index * 100 / !count)
            in
            progress ();
            list_iter_e hashes @@ fun hash ->
            let* () = Repository.check_hash setup hash in
            incr index;
            progress ();
            unit
          in
          Prout.echo "No reachable object is corrupted (object count: %d)." !count;
          unit
  in
  let* root = fetch_root setup in
  match root with
    | Empty ->
        Prout.echo_s "Repository is empty.";
        unit
    | Non_empty root ->
        let expected_hash_index = ref File_hash_map.empty in
        let* size, count = check_dir ~clone_only expected_hash_index setup [] root.root_dir in
        let expected_hash_index = !expected_hash_index in
        Prout.echo "Directory structure looks ok (total size: %d, file count: %d)."
          size count;
        Prout.echo_s "All files are available.";
        let* actual_hash_index = Hash_index.fetch_all setup root.hash_index in
        let* () =
          let exception Inconsistent of string in
          let inconsistent x = Printf.ksprintf (fun s -> raise (Inconsistent s)) x in
          try
            let _ =
              File_hash_map.merge' expected_hash_index actual_hash_index @@
              fun hash expected actual ->
              let expected = expected |> default File_path_set.empty in
              let actual = actual |> default File_path_set.empty in
              let missing = File_path_set.diff expected actual in
              let unexpected = File_path_set.diff actual expected in
              if not (File_path_set.is_empty missing) then
                inconsistent "missing path(s) %s for %s"
                  (String.concat ", "
                     (List.map Device.show_file_path
                        (File_path_set.elements missing)))
                  (Repository.hex_of_hash hash)
              else if not (File_path_set.is_empty unexpected) then
                inconsistent "unexpected path(s) %s for %s"
                  (String.concat ", "
                     (List.map Device.show_file_path
                        (File_path_set.elements unexpected)))
                  (Repository.hex_of_hash hash)
              else
                None
            in
            unit
          with Inconsistent msg ->
            failed [
              "hash index is inconsistent";
              msg;
            ]
        in
        Prout.echo_s "Hash index is consistent with the directory structure.";
        unit

let show_size size =
  (* By using string_of_int instead of divisions and modulos we
     are more compatible with 32bit architectures. *)
  let str = string_of_int size in
  let len = String.length str in
  if len <= 3 then
    Printf.sprintf "%s B" str
  else
    let with_unit unit =
      let f =
        match String.length str mod 3 with
          | 1 -> Printf.sprintf "%c.%c%c %s"
          | 2 -> Printf.sprintf "%c%c.%c %s"
          | _ -> Printf.sprintf "%c%c%c %s"
      in
      f str.[0] str.[1] str.[2] unit
    in
    if len <= 6 then with_unit "kB" else
    if len <= 9 then with_unit "MB" else
    if len <= 12 then with_unit "GB" else
    if len <= 14 then with_unit "TB" else
      Printf.sprintf "%d TB" (size / 1_000_000_000_000)

type merged_dir_entry =
  | MDE_main of State.dir_entry
  | MDE_clone of Device.stat
  | MDE_both of State.dir_entry * Device.stat

type tree_entry_name =
  | TEN_path of Device.path
  | TEN_filename of Path.Filename.t

let tree ~color ~max_depth ~only_main ~only_dirs
    ~print_size ~print_file_count ~print_duplicates ~full_dir_paths
    (setup: Clone.setup) (paths: Device.path list) =
  let workdir = Clone.workdir setup in
  let* root = fetch_root setup in
  let* root_dir = fetch_root_dir setup root in
  let find_path path =
    let* dir_entry =
      match root with
        | Empty ->
            ok None
        | Non_empty root ->
            let rec find dir = function
              | [] ->
                  (* [dir = root_dir], we have to compute its stats. *)
                  let total_size =
                    Filename_map.fold' dir 0 @@ fun _ dir_entry acc ->
                    match dir_entry with
                      | Dir { total_size; _ } -> acc + total_size
                      | File { size; _ } -> acc + size
                  in
                  let total_file_count =
                    Filename_map.fold' dir 0 @@ fun _ dir_entry acc ->
                    match dir_entry with
                      | Dir { total_file_count; _ } -> acc + total_file_count
                      | File _ -> acc + 1
                  in
                  ok (Some (Dir { hash = root.root_dir; total_size; total_file_count }))
              | head :: tail ->
                  match Filename_map.find_opt head dir with
                    | None ->
                        ok None
                    | Some (File _) as dir_entry ->
                        (
                          match tail with
                            | [] ->
                                ok dir_entry
                            | _ :: _ ->
                                (* [tail] is not empty, but [head] is a file:
                                   [path] does not exist. *)
                                ok None
                        )
                    | Some (Dir { hash; _ }) as dir_entry ->
                        match tail with
                          | [] ->
                              ok dir_entry
                          | _ :: _ ->
                              let* subdir =
                                trace (
                                  sf "failed to fetch directory object %s"
                                    (Repository.hex_of_hash hash)
                                ) @@
                                fetch_or_fail setup hash
                              in
                              find subdir tail
            in
            find root_dir path
    in
    let* stat =
      match Device.stat workdir path with
        | ERROR { code = `failed; msg } ->
            failed (
              sf "failed to get path type for %s in %s"
                (Device.show_path path)
                (Device.show_location workdir)
              :: msg
            )
        | ERROR { code = `no_such_file; _ } ->
            ok None
        | OK stat ->
            ok (Some stat)
    in
    ok (path, dir_entry, stat)
  in
  (* [filename] is [None] for root entries (for which we print [path] itself). *)
  let rec print_entry prefix ~last ~is_root dir_path dir_depth entry_name
      merged_dir_entry =
    let print_prefix kind =
      if not is_root then (
        print_string prefix;
        print_string (if last then "└"  else "├");
        match kind with
          | `dir | `file | `inconsistent ->
              print_string "── ";
          | `dir_not_pulled | `file_not_pulled ->
              print_string "-- "
          | `dir_not_pushed | `file_not_pushed ->
              print_string "++ "
          | `file_differs ->
              print_string "≠≠ "
      )
    in
    let print_prefix_and_filename kind =
      print_prefix kind;
      let need_to_reset_color =
        color && match kind with
          | `dir -> print_string "\027[1m\027[34m"; true
          | `dir_not_pulled -> print_string "\027[1m\027[33m"; true
          | `dir_not_pushed -> print_string "\027[1m\027[31m"; true
          | `file -> false
          | `file_not_pulled -> print_string "\027[33m"; true
          | `file_not_pushed -> print_string "\027[31m"; true
          | `file_differs -> print_string "\027[35m"; true
          | `inconsistent -> print_string "\027[1m\027[35m"; true
      in
      print_string (
        match entry_name with
          | TEN_path path ->
              Device.show_path path
          | TEN_filename filename ->
              match kind, full_dir_paths with
                | (`dir | `dir_not_pulled | `dir_not_pushed), true ->
                    Device.show_file_path (dir_path, filename)
                | (`file | `file_not_pulled | `file_not_pushed |
                   `file_differs | `inconsistent), true
                | _, false ->
                    Path.Filename.show filename
      );
      if need_to_reset_color then print_string "\027[0m";
      match kind with
        | `dir | `dir_not_pulled | `dir_not_pushed ->
            print_char '/'
        | `file | `file_not_pulled | `file_not_pushed | `file_differs ->
            ()
        | `inconsistent ->
            print_char '?'
    in
    let print_size size =
      if print_size then
        Printf.printf " (%s)" (show_size size)
    in
    let print_file_count count =
      if print_file_count then
        if count = 1 then
          print_string " (1 file)"
        else
          Printf.printf " (%d files)" count
    in
    let duplicates hash =
      if not print_duplicates then
        ok File_path_set.empty
      else
        let file_path =
          match entry_name with
            | TEN_path path ->
                Device.file_path_of_path path
            | TEN_filename filename ->
                Some (dir_path, filename)
        in
        match file_path with
          | None ->
              (* Root path: not a file. *)
              ok File_path_set.empty
          | Some file_path ->
              trace (
                sf "failed to look up duplicates for %s" (Device.show_file_path file_path)
              ) @@
              let* paths = Hash_index.get setup root hash in
              ok (File_path_set.remove file_path paths)
    in
    let print_duplicate_paths duplicates =
      File_path_set.iter' duplicates @@ fun duplicate_path ->
      print_string prefix;
      if last then
        print_string "    = /"
      else
        print_string "│   = /";
      print_string (Device.show_file_path duplicate_path);
      print_newline ();
    in
    let recurse hash =
      let subdir_path =
        match entry_name with
          | TEN_path path ->
              path
          | TEN_filename filename ->
              dir_path @ [ filename ]
      in
      let* dir =
        match hash with
          | None ->
              ok Filename_map.empty
          | Some hash ->
              trace (sf "failed to recurse into %s" (Device.show_path subdir_path)) @@
              fetch_or_fail setup hash
      in
      tree_dir
        (if is_root then "" else if last then prefix ^ "    " else prefix ^ "│   ")
        (dir_depth + 1)
        subdir_path dir
    in
    let* () =
      match merged_dir_entry with
        | MDE_main (File { hash; size }) ->
            print_prefix_and_filename `file_not_pulled;
            print_size size;
            let* duplicates = duplicates hash in
            print_newline ();
            print_duplicate_paths duplicates;
            unit
        | MDE_clone (File { size; _ }) ->
            if only_main then unit else (
              print_prefix_and_filename `file_not_pushed;
              print_size size;
              print_newline ();
              unit
            )
        | MDE_both (File { hash; size }, File { size = clone_size; _ }) ->
            (* TODO: also check hash *)
            if size = clone_size then
              (
                print_prefix_and_filename `file;
                print_size size;
                let* duplicates = duplicates hash in
                print_newline ();
                print_duplicate_paths duplicates;
                unit
              )
            else
              (
                print_prefix_and_filename `file_differs;
                print_newline ();
                unit
              )
        | MDE_main (Dir { hash; total_size; total_file_count }) ->
            print_prefix_and_filename `dir_not_pulled;
            print_size total_size;
            print_file_count total_file_count;
            print_newline ();
            recurse (Some hash)
        | MDE_clone Dir ->
            if only_main then unit else
            if is_root then
              (
                (* Consider "." to always be part of the main repository. *)
                print_prefix_and_filename `dir;
                print_size 0;
                print_file_count 0;
                print_newline ();
                recurse None
              )
            else
              (
                print_prefix_and_filename `dir_not_pushed;
                print_newline ();
                unit
              )
        | MDE_both (Dir { hash; total_size; total_file_count }, Dir) ->
            print_prefix_and_filename `dir;
            print_size total_size;
            print_file_count total_file_count;
            print_newline ();
            recurse (Some hash)
        | MDE_both (Dir _, File _)
        | MDE_both (File _, Dir) ->
            print_prefix_and_filename `inconsistent;
            print_newline ();
            unit
    in
    unit
  and tree_dir prefix dir_depth dir_path dir =
    if
      match max_depth with
        | None ->
            false
        | Some max_depth ->
            dir_depth >= max_depth
    then
      unit
    else
      let* work_dir =
        let* filenames =
          match Device.read_dir workdir dir_path with
            | ERROR { code = `failed; _ } | OK _ as x ->
                x
            | ERROR { code = `no_such_file; _ } ->
                ok []
        in
        let* list =
          list_map_e filenames @@ fun filename ->
          let path = dir_path @ [ filename ] in
          match Device.stat workdir path with
            | ERROR { code = (`no_such_file | `failed); msg } ->
                failed (
                  sf "failed to get path type for %s in %s"
                    (Device.show_path path)
                    (Device.show_location workdir)
                  :: msg
                )
            | OK stat ->
                ok (filename, stat)
        in
        let list =
          if Device.same_paths dir_path [] then
            List.filter (fun (f, _) -> Path.Filename.compare f dot_filou <> 0) list
          else
            list
        in
        ok (Filename_map.of_list list)
      in
      let merged_dir =
        Filename_map.merge' dir work_dir @@ fun _ dir_entry stat ->
        match dir_entry, stat with
          | None, None ->
              None
          | Some main, None ->
              Some (MDE_main main)
          | None, Some clone ->
              Some (MDE_clone clone)
          | Some main, Some clone ->
              Some (MDE_both (main, clone))
      in
      let filtered_dir =
        if only_dirs then
          Filename_map.filter' merged_dir @@ fun _ -> function
            | MDE_main (Dir _) | MDE_clone Dir | MDE_both (Dir _, _) | MDE_both (_, Dir) ->
                true
            | MDE_main (File _) | MDE_clone (File _) | MDE_both (File _, File _) ->
                false
        else
          merged_dir
      in
      match List.rev (Filename_map.bindings filtered_dir) with
        | [] ->
            unit
        | (head_filename, head_entry) :: tail ->
            let* () =
              list_iter_e (List.rev tail) @@ fun (filename, entry) ->
              print_entry prefix ~last: false ~is_root: false dir_path dir_depth
                (TEN_filename filename) entry
            in
            print_entry prefix ~last: true ~is_root: false dir_path dir_depth
              (TEN_filename head_filename) head_entry
  in
  let* found_paths = list_map_e paths find_path in
  let merged_entries =
    List.filter_map' found_paths @@ fun (path, dir_entry, stat) ->
    match dir_entry, stat with
      | None, None ->
          None
      | Some dir_entry, None ->
          Some (path, MDE_main dir_entry)
      | None, Some stat ->
          Some (path, MDE_clone stat)
      | Some dir_entry, Some stat ->
          Some (path, MDE_both (dir_entry, stat))
  in
  let print_merged_entry ~last (path, merged_dir_entry) =
    print_entry "" ~last ~is_root: (path = []) [] (-1) (TEN_path path) merged_dir_entry
  in
  match List.rev merged_entries with
    | [] ->
        unit
    | head :: tail ->
        let* () =
          list_iter_e (List.rev tail) (print_merged_entry ~last: false)
        in
        print_merged_entry ~last: true head

let push_file ~verbose (setup: Clone.setup) (root: root) (path: Device.file_path) =
  trace ("failed to push file: " ^ Device.show_file_path path) @@
  let* root_dir = fetch_root_dir setup root in
  let* add_result =
    Dir.add setup root_dir path @@ fun () ->
    Repository.store_file
      ~source: (Clone.workdir setup)
      ~source_path: path
      ~target: setup
      ~on_progress: (
        on_copy_progress ("Pushing: " ^ Device.show_file_path path)
      )
  in
  match add_result with
    | Already_exists ->
        if verbose then Prout.echo "Already exists: %s" (Device.show_file_path path);
        ok root
    | Added { new_dir_hash = new_root_dir_hash; added_file_hash; _ } ->
        let* new_hash_index_hash, old_paths =
          Hash_index.add setup root added_file_hash path
        in
        if verbose then (
          if File_path_set.is_empty old_paths then
            Prout.echo "Pushed: %s" (Device.show_file_path path)
          else
            Prout.echo "Pushed: %s (duplicate of: %s)" (Device.show_file_path path) (
              File_path_set.elements old_paths
              |> List.map Device.show_file_path
              |> String.concat ", "
            );
        );
        ok (
          Non_empty {
            root_dir = new_root_dir_hash;
            hash_index = new_hash_index_hash;
          }
        )

let push ~verbose setup (paths: Device.path list) =
  let rec push root path =
    if Device.same_paths path [ dot_filou ] then
      ok root
    else
      let* stat = Device.stat (Clone.workdir setup) path in
      match stat with
        | File _ ->
            (
              match Device.file_path_of_path path with
                | None ->
                    ok root
                | Some path ->
                    push_file ~verbose setup root path
            )
        | Dir ->
            let new_root = ref root in
            let* () =
              Device.iter_read_dir (Clone.workdir setup) path @@ fun filename ->
              let* root = push !new_root (path @ [ filename ]) in
              new_root := root;
              unit
            in
            ok !new_root
  in
  let* root = fetch_root setup in
  let* root = list_fold_e root paths push in
  store_root setup root (String.concat " " ("push" :: List.map Device.show_path paths))

let pull ~verbose setup (paths: Device.path list) =
  let rec pull (dir: dir) dir_path_rev (path: Device.path) =
    match path with
      | [] ->
          list_iter_e (Filename_map.bindings dir) @@ fun (filename, _) ->
          pull dir dir_path_rev [ filename ]
      | head :: tail ->
          match Filename_map.find_opt head dir with
            | None ->
                failed [
                  "no such file or directory: " ^ Device.show_path (List.rev dir_path_rev);
                ]
            | Some (Dir { hash; _ }) ->
                let* dir = fetch_or_fail setup hash in
                pull dir (head :: dir_path_rev) tail
            | Some (File { hash; _ }) ->
                let target_path = List.rev dir_path_rev, head in
                trace (sf "failed to pull %s" (Device.show_file_path target_path)) @@
                match tail with
                  | _ :: _ ->
                      failed [
                        "no such file or directory: " ^
                        Device.show_path (List.rev_append dir_path_rev path);
                        Device.show_path (List.rev dir_path_rev) ^
                        " is a file";
                      ]
                  | [] ->
                      match
                        Repository.fetch_file ~source: setup hash
                          ~target: (Clone.workdir setup)
                          ~target_path
                          ~on_progress: (
                            on_copy_progress
                              ("Pulling: " ^ Device.show_file_path target_path)
                          )
                      with
                        | ERROR { code = `already_exists; _ } ->
                            if verbose then
                              Prout.echo "Already exists: %s"
                                (Device.show_file_path target_path);
                            unit
                        | ERROR { code = `failed | `not_available; msg } ->
                            failed msg
                        | OK () ->
                            if verbose then
                              Prout.echo "Pulled: %s" (Device.show_file_path target_path);
                            unit
  in
  let* root = fetch_root setup in
  let* root_dir = fetch_root_dir setup root in
  list_iter_e paths (pull root_dir [])

type remove_result =
  | Unchanged
  | Removed_whole_dir
  | Dir_not_empty of {
      new_dir_hash: dir Repository.hash;
      size_diff: int; (* positive, to be subtracted *)
      file_count_diff: int; (* positive, to be subtracted *)
    }

let remove ~recursive setup (paths: Device.path list) =
  let removed_paths = ref [] in
  let rec remove_from_dir (dir: dir) dir_path_rev (path: Device.path) =
    match path with
      | [] ->
          (* Fully followed the requested path: remove where we are, i.e. [dir]. *)
          if recursive then
            (* We could return [Removed_whole_dir], but we need to update [removed_paths]. *)
            let* () =
              list_iter_e (Filename_map.bindings dir) @@ fun (filename, _) ->
              let* (_: remove_result) = remove_from_dir dir dir_path_rev [ filename ] in
              unit
            in
            ok Removed_whole_dir
          else
            failed [
              sf "%s is a directory, use --recursive (or -r) to remove it and its contents"
                (Device.show_path (List.rev dir_path_rev))
            ]
      | head :: tail ->
          match Filename_map.find_opt head dir with
            | None ->
                (* Requested to remove something that doesn't exist. *)
                ok Unchanged
            | Some (Dir { hash; total_size; total_file_count }) ->
                (* Requested to remove [head/tail] from [dir]. *)
                (* Find [subdir] named [head] in [dir]. *)
                let* subdir = fetch_or_fail setup hash in
                (* Remove [tail] from [subdir]. *)
                let* remove_result =
                  remove_from_dir subdir (head :: dir_path_rev) tail
                in
                (* Update [dir]. *)
                (
                  match remove_result with
                    | Unchanged ->
                        ok Unchanged
                    | Removed_whole_dir ->
                        (* Removed [subdir], i.e. [head], completely. *)
                        let new_dir = Filename_map.remove head dir in
                        if Filename_map.is_empty new_dir then
                          ok Removed_whole_dir
                        else
                          let* new_dir_hash = store_later setup T.dir new_dir in
                          (* TODO: share with below? *)
                          ok (
                            Dir_not_empty {
                              new_dir_hash;
                              size_diff = total_size;
                              file_count_diff = total_file_count;
                            }
                          )
                    | Dir_not_empty { new_dir_hash; size_diff; file_count_diff } ->
                        (* Removed part of [head], which still contains files. *)
                        let new_dir =
                          Filename_map.add head
                            (
                              Dir {
                                hash = new_dir_hash;
                                total_size = total_size - size_diff;
                                total_file_count = total_file_count - file_count_diff;
                              }
                            )
                            dir
                        in
                        let* new_dir_hash = store_later setup T.dir new_dir in
                        ok (
                          Dir_not_empty {
                            new_dir_hash;
                            size_diff;
                            file_count_diff;
                          }
                        )
                )
            | Some (File { hash; size }) ->
                match tail with
                  | _ :: _ ->
                      failed [
                        "no such file or directory: " ^
                        Device.show_path (List.rev_append dir_path_rev path);
                        Device.show_path (List.rev dir_path_rev) ^
                        " is a file";
                      ]
                  | [] ->
                      (* Remove file [head] from current [dir]. *)
                      let new_dir = Filename_map.remove head dir in
                      removed_paths :=
                        (hash, (List.rev dir_path_rev, head)) :: !removed_paths;
                      if Filename_map.is_empty new_dir then
                        ok Removed_whole_dir
                      else
                        let* new_dir_hash = store_later setup T.dir new_dir in
                        ok (
                          Dir_not_empty {
                            new_dir_hash;
                            size_diff = size;
                            file_count_diff = 1;
                          }
                        )
  in
  let* root = fetch_root setup in
  match root, paths with
    | Empty, [] ->
        unit
    | Empty, _ :: _ ->
        failed [ "repository is empty" ]
    | Non_empty { hash_index; _ }, _ ->
        (* Update [root_dir]. *)
        let* root =
          list_fold_e root paths @@ fun root path ->
          let* root_dir = fetch_root_dir setup root in
          let* remove_result = remove_from_dir root_dir [] path in
          match remove_result with
            | Removed_whole_dir ->
                ok Empty
            | Unchanged ->
                ok root
            | Dir_not_empty { new_dir_hash; _ } ->
                ok (Non_empty { root_dir = new_dir_hash; hash_index })
        in
        let* root =
          match root with
            | Empty ->
                ok Empty
            | Non_empty root ->
                (* Update [hash_index].
                   Note: removing files with no pathes that lead to them
                   is a task for the garbage collector. *)
                (* TODO: this is probably inefficient as we re-fetch stuff
                   that we just stored. *)
                let* hash_index = fetch_or_fail setup root.hash_index in
                let* new_hash_index =
                  list_fold_e hash_index !removed_paths @@
                  fun hash_index (hash, path) ->
                  Hash_index.remove setup hash_index hash path
                in
                let* new_hash_index_hash = store_later setup T.hash_index new_hash_index in
                ok (
                  Non_empty {
                    root_dir = root.root_dir;
                    hash_index = new_hash_index_hash;
                  }
                )
        in
        store_root setup root @@
        String.concat " " (
          "rm" :: (if recursive then [ "-r" ] else []) @ List.map Device.show_path paths
        )

let update setup =
  let* count =
    Prout.major_s "Computing reachable object set...";
    let on_availability_check_progress ~checked ~count =
      Prout.minor @@ fun () ->
      sf "Checking availability (%d / %d) (%d%%)" checked count (checked * 100 / count)
    in
    let on_copy_progress ~transferred ~count =
      Prout.minor @@ fun () ->
      sf "Copying (%d / %d) (%d%%)" transferred count (transferred * 100 / count)
    in
    Repository.update ~on_availability_check_progress ~on_copy_progress setup
  in
  Prout.echo "Copied %d objects." count;
  Prout.echo_s "Clone is up-to-date.";
  unit

let print_journal { redo; head; undo } =
  let redo = List.mapi (fun i x -> - i - 1, x) redo in
  (
    List.iter' (List.rev redo) @@ fun (i, { command; _ }) ->
    Prout.echo "    %2d %s" i command;
  );
  Prout.echo "-->  0 %s" head.command;
  List.iteri' undo @@ fun i { command; _ } ->
  Prout.echo "    %2d %s" (i + 1) command

let prune ~yes ~keep_undo ~keep_redo setup =
  let* journal = Repository.fetch_root setup in
  let new_journal =
    {
      redo = list_take keep_redo journal.redo;
      head = journal.head;
      undo = list_take keep_undo journal.undo;
    }
  in
  let* yes =
    if yes then
      ok true
    else (
      Prout.echo_s "/!\\ THIS OPERATION CANNOT BE UNDONE /!\\";
      Prout.echo_s "After this, the history will look like this:";
      Prout.echo_s "";
      print_journal new_journal;
      Prout.echo_s "";
      Prout.echo_s "Repositories:";
      Prout.echo_s "";
      (
        Option.iter' (Clone.main setup) @@ fun location ->
        Prout.echo "  Main repository: %s" (Device.show_location location);
      );
      Prout.echo "            Clone: %s" (Device.show_location (Clone.workdir setup));
      Prout.echo "";
      Prout.print_s "Prune main repository and its clone? [y/N] ";
      flush stdout;
      let response = read_line () in
      match String.lowercase_ascii response with
        | "y" | "yes" ->
            ok true
        | "n" | "no" | "" ->
            Prout.echo_s "Operation was canceled.";
            ok false
        | _ ->
            failed [ "please answer yes or no" ]
    )
  in
  if not yes then
    unit
  else
    let* () = Repository.store_root setup new_journal in
    let* (count, size) = Repository.garbage_collect setup in
    Prout.echo "Removed %d objects totalling %s." count (show_size size);
    unit

let log setup =
  let* journal = Repository.fetch_root setup in
  print_journal journal;
  unit

let undo setup ~count =
  let undo count =
    let* journal = Repository.fetch_root setup in
    if List.compare_length_with journal.undo count < 0 then
      failed [ sf "journal has less than %d undo entries" count ]
    else
      let rec undo journal count =
        if count > 0 then
          match journal.undo with
            | [] ->
                assert false (* we checked lengths above *)
            | head :: tail ->
                let journal =
                  {
                    redo = journal.head :: journal.redo;
                    head;
                    undo = tail;
                  }
                in
                undo journal (count - 1)
        else
          journal
      in
      let journal = undo journal count in
      Repository.store_root setup journal
  in
  let redo count =
    let* journal = Repository.fetch_root setup in
    if List.compare_length_with journal.redo count < 0 then
      failed [ sf "journal has less than %d redo entries" count ]
    else
      let rec redo journal count =
        if count > 0 then
          match journal.redo with
            | [] ->
                assert false (* we checked lengths above *)
            | head :: tail ->
                let journal =
                  {
                    redo = tail;
                    head;
                    undo = journal.head :: journal.undo;
                  }
                in
                redo journal (count - 1)
        else
          journal
      in
      let journal = redo journal count in
      Repository.store_root setup journal
  in
  if count > 0 then undo count else
  if count < 0 then redo (- count) else
    unit

let diff ~color setup ~before ~after =
  let* journal = Repository.fetch_root setup in
  let get_root index =
    if index > 0 then
      match List.nth_opt journal.undo (index - 1) with
        | None ->
            failed [ sf "journal has less than %d undo entries" index ]
        | Some root ->
            ok root
    else if index < 0 then
      match List.nth_opt journal.redo (- index - 1) with
        | None ->
            failed [ sf "journal has less than %d redo entries" index ]
        | Some root ->
            ok root
    else
      ok journal.head
  in
  let* root_before = get_root before in
  let* root_after = get_root after in
  let* dir_before = fetch_root_dir setup root_before.root in
  let* dir_after = fetch_root_dir setup root_after.root in
  let rec diff_dirs dir_path (before: dir) (after: dir) =
    let merged =
      Filename_map.merge' before after @@ fun _ before after ->
      match before, after with
        | None, None -> None
        | _ -> Some (before, after)
    in
    list_iter_e (Filename_map.bindings merged) @@ fun (filename, merged) ->
    let path = dir_path @ [ filename ] in
    let status status kind =
      let text_color =
        match status with
          | `removed -> "\027[31m"
          | `added -> "\027[32m"
          | `differs -> "\027[35m"
      in
      let text_color =
        match kind with
          | `file | `inconsistent -> text_color
          | `dir -> "\027[1m" ^ text_color
      in
      Prout.echo "%s%s %s%s%s"
        (if color then text_color else "")
        (match status with `differs -> "? " | `added -> "+ " | `removed -> "- ")
        (Device.show_path path)
        (match kind with `file -> "" | `dir -> "/" | `inconsistent -> "?")
        (if color then "\027[0m" else "");
      unit
    in
    match merged with
      | None, None ->
          unit
      | Some (File { hash = hash_before; _ }), Some (File { hash = hash_after; _ }) ->
          if Repository.compare_hashes hash_before hash_after = 0 then
            unit
          else
            status `differs `file
      | Some (Dir { hash = hash_before; _ }), Some (Dir { hash = hash_after; _ }) ->
          if Repository.compare_hashes hash_before hash_after = 0 then
            unit
          else
            let* dir_before = fetch_or_fail setup hash_before in
            let* dir_after = fetch_or_fail setup hash_after in
            diff_dirs path dir_before dir_after
      | Some (File _), Some (Dir _) ->
          status `differs `inconsistent
      | Some (Dir _), Some (File _) ->
          status `differs `inconsistent
      | None, Some (Dir _) ->
          status `added `dir
      | None, Some (File _) ->
          status `added `file
      | Some (Dir _), None ->
          status `removed `dir
      | Some (File _), None ->
          status `removed `file
  in
  diff_dirs [] dir_before dir_after

(* Round to the highest multiple of 4096.
   TODO: Not sure this is very accurate: does a file that is close to 4096 bytes
   fit in one block, or is there some kind of header? *)
let disk_usage size =
  max 4096 (size / 4096 * 4096 + (if size mod 4096 = 0 then 0 else 4096))

let stats setup =
  let* reachable_count, total_bytes, total_disk_usage =
    Prout.major_s "Computing the set of reachable objects...";
    let* hashes = Repository.reachable ~files: false setup in
    let hashes = Repository.Hash_set.elements hashes in
    let reachable_count = List.length hashes in
    let* hashes =
      let index = ref 0 in
      list_map_e hashes @@ fun hash ->
      incr index;
      (
        Prout.minor @@ fun () ->
        sf "Computing stats... (%d / %d) (%d%%)"
          !index reachable_count (!index * 100 / reachable_count)
      );
      let* size = Repository.get_object_size setup hash in
      ok (hash, size)
    in
    let total_bytes, total_disk_usage =
      List.fold_left' (0, 0) hashes @@
      fun (acc_bytes, acc_disk_usage) (_, size) ->
      acc_bytes + size, acc_disk_usage + disk_usage size
    in
    ok (reachable_count, total_bytes, total_disk_usage)
  in
  Prout.echo "Reachable object count: %d" reachable_count;
  Prout.echo "Total size of reachable objects: %s" (show_size total_bytes);
  Prout.echo "Disk usage of reachable objects: %s" (show_size total_disk_usage);
  Prout.echo "Efficiency (total size / disk usage): %d%%"
    (total_bytes * 100 / total_disk_usage);
  unit

let show setup obj ~max_depth =
  let* obj =
    match obj with
      | "journal" -> ok `journal
      | "hash_index" -> ok `hash_index
      | "root_dir" -> ok `root_dir
      | _ ->
          match Hash.of_hex obj with
            | None ->
                failed [ "invalid object hash or alias: " ^ obj ]
            | Some hash ->
                ok (`hash hash)
  in
  let rec pp_value ~indent ~max_depth fmt (value: Robin.Value.t) =
    Protype_robin.Explore.pp_value_gen ~custom ~indent ?max_depth () fmt value
  and custom ~indent ~max_depth fmt (value: Robin.Value.t) =
    match value with
      | String s ->
          (
            match Hash.of_bin s with
              | None ->
                  false
              | Some hash ->
                  match Repository.fetch_raw setup hash with
                    | ERROR _ ->
                        false
                    | OK encoded_value ->
                        match Robin.Decode.from_string encoded_value with
                          | Error _ ->
                              false
                          | Ok value ->
                              (* TODO: indentation is broken here *)
                              Format.fprintf fmt "%s =\n%s%a" (Hash.to_hex hash)
                                (String.make indent ' ')
                                (pp_value ~indent ~max_depth) value;
                              true
          )
      | _ ->
          false
  in
  let show_encoded_object encoded_value =
    match Robin.Decode.from_string encoded_value with
      | Error _ ->
          print_string encoded_value;
          flush stdout
      | Ok value ->
          Format.printf "%a@." (pp_value ~indent: 0 ~max_depth: (Some max_depth)) value
  in
  match obj with
    | `hash hash ->
        let* encoded_object = Repository.fetch_raw setup hash in
        show_encoded_object encoded_object;
        unit
    | `journal ->
        let* encoded_object = Repository.fetch_root_raw setup in
        show_encoded_object encoded_object;
        unit
    | `root_dir | `hash_index as obj ->
        let* root = Repository.fetch_root setup in
        match root.head.root with
          | Empty ->
              Prout.echo_s "empty";
              unit
          | Non_empty non_empty_root ->
              let hash =
                match obj with
                  | `root_dir -> Repository.hash_of_hash non_empty_root.root_dir
                  | `hash_index -> Repository.hash_of_hash non_empty_root.hash_index
              in
              let* encoded_object = Repository.fetch_raw setup hash in
              show_encoded_object encoded_object;
              unit

let config ~mode ~main_location =
  let* clone_location, config = find_local_clone_config mode in
  match main_location with
    | None ->
        Prout.echo "clone repository: %s" (Device.show_location clone_location);
        Prout.echo "main repository: %s" (Device.show_location config.main_location);
        unit
    | Some main_location ->
        write_clone_config ~clone_location { main_location }
