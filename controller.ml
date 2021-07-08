open Misc
open State

let empty_dir: dir = Filename_map.empty

let fetch_or_fail setup typ hash =
  match Repository.fetch setup typ hash with
    | ERROR { code = (`not_available | `failed); msg } ->
        failed msg
    | OK _ as x ->
        x

let with_progress f =
  Progress_bar.start ();
  match f () with
    | exception exn ->
        Progress_bar.set "";
        raise exn
    | result ->
        Progress_bar.set "";
        result

let on_copy_progress prefix ~bytes ~size =
  Progress_bar.set "%s (%d / %d) (%d%%)" prefix bytes size (bytes * 100 / size)

let init (location: Device.location) =
  let* root_dir_hash = Bare.store_now location T.dir Filename_map.empty in
  let* hash_index_hash = Bare.store_now location T.hash_index Map.Char.empty in
  Bare.store_root location {
    root_dir = root_dir_hash;
    hash_index = hash_index_hash;
  }

let write_clone_config ~clone_location config =
  Device.write_file clone_location dot_filou_config
    (Protype_robin.Encode.to_string ~version: Root.version T.clone_config config)

let read_clone_config ~clone_location =
  trace "failed to read configuration file" @@
  let* contents = Device.read_file clone_location dot_filou_config in
  decode_robin_string T.clone_config contents

let clone ~(main_location: Device.location) ~(clone_location: Device.location) =
  (* Check that the remote repository is readable. *)
  let* _ = Bare.fetch_root main_location in
  (* Initialize the clone. *)
  write_clone_config ~clone_location { main_location }

let find_local_clone () =
  let rec find current =
    let clone_location = Device.Local current in
    match read_clone_config ~clone_location with
      | OK config ->
          OK (Clone.setup ~main: config.main_location ~clone: clone_location)
      | ERROR { code = (`no_such_file | `failed); _ } ->
          match Path.parent current with
            | None -> failed [ "not in a clone repository" ]
            | Some parent -> find parent
  in
  find (Path.get_cwd ())

let store_later = Repository.store_later

(* Recursively check sizes, file counts,
   and whether reachable files are available. *)
let rec check_dir ~all_files_must_be_available expected_hash_index
    (location: Device.location) (path: Device.path) (dir: dir hash) =
  let* dir = Bare.fetch location T.dir dir in
  let path_size = ref 0 in
  let path_file_count = ref 0 in
  let* () =
    list_iter_e (Filename_map.bindings dir) @@ fun (filename, dir_entry) ->
    let entry_path = path @ [ filename ] in
    match dir_entry with
      | Dir { hash; total_size = expected_size; total_file_count = expected_file_count } ->
          let* size, file_count =
            check_dir ~all_files_must_be_available expected_hash_index
              location entry_path hash
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
          let good () =
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
          in
          match Bare.get_file_size location hash with
            | ERROR { code = `not_available; msg } ->
                if all_files_must_be_available then
                  failed (
                    sf "file %s (%s) is unvailable"
                      (Device.show_path entry_path) (Repository.hex_of_hash hash) ::
                    msg
                  )
                else
                  good ()
            | ERROR { code = `failed; _ } as x ->
                x
            | OK size ->
                if size <> expected_size then
                  failed [
                    sf "wrong size for %s (%s): expected %d, found %d"
                      (Device.show_path entry_path) (Repository.hex_of_hash hash)
                      expected_size size;
                  ]
                else
                  good ()
  in
  ok (!path_size, !path_file_count)

let check (location: Device.location) =
  let* is_main =
    match read_clone_config ~clone_location: location with
      | OK _ ->
          ok false
      | ERROR { code = `no_such_file; _ } ->
          ok true
      | ERROR { code = `failed; _ } as x ->
          x
  in
  if is_main then
    echo "Checking main repository at: %s" (Device.show_location location)
  else
    echo "Checking clone repository at: %s" (Device.show_location location);
  let location =
    if is_main then
      location
    else
      Device.sublocation location dot_filou
  in
  let* root = Bare.fetch_root location in
  let expected_hash_index = ref File_hash_map.empty in
  let* size, count =
    check_dir ~all_files_must_be_available: is_main expected_hash_index
      location [] root.root_dir
  in
  let expected_hash_index = !expected_hash_index in
  echo "Directory structure looks ok (total size: %d, file count: %d)." size count;
  if is_main then echo "All files are available.";
  let actual_hash_index = ref File_hash_map.empty in
  let* () =
    let* hash_index = Bare.fetch location T.hash_index root.hash_index in
    list_iter_e (Map.Char.bindings hash_index) @@ fun (_, hash_index_1_hash) ->
    let* hash_index_1 = Bare.fetch location T.hash_index_1 hash_index_1_hash in
    list_iter_e (Map.Char.bindings hash_index_1) @@ fun (_, hash_index_2_hash) ->
    let* hash_index_2 = Bare.fetch location T.hash_index_2 hash_index_2_hash in
    list_iter_e (File_hash_map.bindings hash_index_2) @@ fun (hash, paths) ->
    actual_hash_index := File_hash_map.add hash paths !actual_hash_index;
    unit
  in
  let actual_hash_index = !actual_hash_index in
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
  echo "Hash index is consistent with the directory structure.";
  unit

type merged_dir_entry =
  | MDE_main of State.dir_entry
  | MDE_clone of Device.stat
  | MDE_both of State.dir_entry * Device.stat

let tree ~color ~max_depth ~only_main ~only_dirs
    ~print_size ~print_file_count ~print_duplicates
    (setup: Clone.setup) (dir_path: Device.path) =
  let* root = Repository.fetch_root setup in
  let* root_dir = fetch_or_fail setup T.dir root.root_dir in
  let rec find_dir dir_path_rev dir = function
    | [] ->
        ok dir
    | head :: tail ->
        let dir_path_rev = head :: dir_path_rev in
        match Filename_map.find_opt head dir with
          | None ->
              failed [ sf "no such directory: %s" (Device.show_path (List.rev dir_path_rev)) ]
          | Some (File _) ->
              failed [ sf "%s is a file" (Device.show_path (List.rev dir_path_rev)) ]
          | Some (Dir { hash; _ }) ->
              let* subdir = fetch_or_fail setup T.dir hash in
              find_dir dir_path_rev subdir tail
  in
  let* dir = find_dir [] root_dir dir_path in
  (
    let total_size =
      if print_size then
        let size =
          Filename_map.fold' dir 0 @@ fun _ dir_entry acc ->
          match dir_entry with
            | Dir { total_size; _ } -> acc + total_size
            | File { size; _ } -> acc + size
        in
        sf " (%d B)" size
      else
        ""
    in
    let total_file_count =
      if print_file_count then
        let size =
          Filename_map.fold' dir 0 @@ fun _ dir_entry acc ->
          match dir_entry with
            | Dir { total_file_count; _ } -> acc + total_file_count
            | File _ -> acc + 1
        in
        sf " (%d files)" size
      else
        ""
    in
    echo "%s%s%s" (Device.show_path dir_path) total_size total_file_count;
  );
  let rec tree_dir prefix dir_depth dir_path_rev dir =
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
        let clone_location = Clone.clone setup in
        let dir_path = List.rev dir_path_rev in
        let* filenames =
          match Device.read_dir clone_location dir_path with
            | ERROR { code = `failed; _ } | OK _ as x ->
                x
            | ERROR { code = `no_such_file; _ } ->
                ok []
        in
        let* list =
          list_map_e filenames @@ fun filename ->
          let path = dir_path @ [ filename ] in
          match Device.stat clone_location path with
            | ERROR { code = (`no_such_file | `failed); msg } ->
                failed (
                  sf "failed to get path type for %s in %s"
                    (Device.show_path path)
                    (Device.show_location clone_location)
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
      let print_entry ?(last = false) (filename, merged_dir_entry) =
        let print_prefix () =
          print_string prefix;
          print_string (if last then "└── "  else "├── ");
        in
        let print_filename kind =
          let need_to_reset_color =
            color && match kind with
              | `dir -> print_string "\027[1m\027[34m"; true
              | `dir_not_pulled -> print_string "\027[1m\027[33m"; true
              | `dir_not_pushed -> print_string "\027[1m\027[31m"; true
              | `file -> false
              | `file_not_pulled -> print_string "\027[33m"; true
              | `file_not_pushed -> print_string "\027[31m"; true
              | `inconsistent -> print_string "\027[1m\027[35m"; true
          in
          print_string (Path.Filename.show filename);
          if need_to_reset_color then print_string "\027[0m";
          match kind with
            | `dir | `dir_not_pulled | `dir_not_pushed ->
                print_char '/'
            | `file | `file_not_pulled | `file_not_pushed ->
                ()
            | `inconsistent ->
                print_char '?'
        in
        let print_size size =
          if print_size then
            (* By using string_of_int instead of divisions and modulos we
               are more compatible with 32bit architectures. *)
            let str = string_of_int size in
            let len = String.length str in
            if len <= 3 then
              Printf.printf " (%s B)" str
            else
              let with_unit unit =
                let f =
                  match String.length str mod 3 with
                    | 1 -> Printf.printf " (%c.%c%c %s)"
                    | 2 -> Printf.printf " (%c%c.%c %s)"
                    | _ -> Printf.printf " (%c%c%c %s)"
                in
                f str.[0] str.[1] str.[2] unit
              in
              if len <= 6 then with_unit "kB" else
              if len <= 9 then with_unit "MB" else
              if len <= 12 then with_unit "GB" else
              if len <= 14 then with_unit "TB" else
                Printf.printf " (%d TB)" (size / 1_000_000_000_000)
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
            trace (
              sf "failed to look up duplicates for %s"
                (Device.show_file_path (List.rev dir_path_rev, filename))
            ) @@
            let hash_bin = Repository.bin_of_hash hash in
            let char0 = hash_bin.[0] in
            let char1 = hash_bin.[1] in
            let* hash_index = fetch_or_fail setup T.hash_index root.hash_index in
            let* hash_index_1 =
              match Map.Char.find_opt char0 hash_index with
                | None ->
                    ok Map.Char.empty
                | Some hash_index_1_hash ->
                    fetch_or_fail setup T.hash_index_1 hash_index_1_hash
            in
            let* hash_index_2 =
              match Map.Char.find_opt char1 hash_index_1 with
                | None ->
                    ok File_hash_map.empty
                | Some hash_index_2_hash ->
                    fetch_or_fail setup T.hash_index_2 hash_index_2_hash
            in
            match File_hash_map.find_opt hash hash_index_2 with
              | None ->
                  failed [ "%s is missing from the hash index"; ]
              | Some set ->
                  ok set
        in
        let print_duplicate_count duplicates =
          let count = File_path_set.cardinal duplicates in
          if count >= 2 then Printf.printf " [%d]" count
        in
        let recurse hash =
          let* dir = fetch_or_fail setup T.dir hash in
          tree_dir (if last then prefix ^ "    " else prefix ^ "│   ") (dir_depth + 1)
            (filename :: dir_path_rev) dir
        in
        print_prefix ();
        let* () =
          match merged_dir_entry with
            | MDE_main (File { hash; size }) ->
                print_filename `file_not_pulled;
                print_size size;
                let* duplicates = duplicates hash in
                print_duplicate_count duplicates;
                print_newline ();
                unit
            | MDE_clone (File { size; _ }) ->
                if only_main then unit else (
                  print_filename `file_not_pushed;
                  print_size size;
                  print_newline ();
                  unit
                )
            | MDE_both (File { hash; size }, File { size = clone_size; _ }) ->
                (* TODO: also check hash *)
                if size = clone_size then
                  (
                    print_filename `file;
                    print_size size;
                    let* duplicates = duplicates hash in
                    print_duplicate_count duplicates;
                    print_newline ();
                    unit
                  )
                else
                  (
                    print_filename `inconsistent;
                    print_newline ();
                    unit
                  )
            | MDE_main (Dir { hash; total_size; total_file_count }) ->
                print_filename `dir_not_pulled;
                print_size total_size;
                print_file_count total_file_count;
                print_newline ();
                recurse hash
            | MDE_clone Dir ->
                if only_main then unit else (
                  print_filename `dir_not_pushed;
                  print_newline ();
                  unit
                )
            | MDE_both (Dir { hash; total_size; total_file_count }, Dir) ->
                print_filename `dir;
                print_size total_size;
                print_file_count total_file_count;
                print_newline ();
                recurse hash
            | MDE_both (Dir _, File _)
            | MDE_both (File _, Dir) ->
                print_filename `inconsistent;
                print_newline ();
                unit
        in
        unit
      in
      match List.rev (Filename_map.bindings filtered_dir) with
        | [] ->
            unit
        | head :: tail ->
            let* () = list_iter_e (List.rev tail) print_entry in
            print_entry ~last: true head
  in
  tree_dir "" 0 (List.rev dir_path) dir

let push_file ~verbose (setup: Clone.setup) root
    (((dir_path, filename) as path): Device.file_path) =
  trace ("failed to push file: " ^ Device.show_file_path path) @@
  let* root_dir = fetch_or_fail setup T.dir root.root_dir in
  let rec update_dir dir_path_rev (dir: dir) = function
    | head :: tail ->
        let* head_dir, head_dir_total_size, head_dir_total_file_count =
          match Filename_map.find_opt head dir with
            | None ->
                ok (empty_dir, 0, 0)
            | Some (File _) ->
                failed [
                  "parent directory already exists as a file: " ^
                  (Device.show_path (List.rev dir_path_rev));
                ]
            | Some (Dir { hash = head_dir_hash; total_size; total_file_count }) ->
                let* dir = fetch_or_fail setup T.dir head_dir_hash in
                ok (dir, total_size, total_file_count)
        in
        let* update_result =
          update_dir (head :: dir_path_rev) head_dir tail
        in
        (
          match update_result with
            | None ->
                ok None
            | Some (new_head_dir_hash, added_file_hash, size_diff, file_count_diff) ->
                let head_dir_entry =
                  Dir {
                    hash = new_head_dir_hash;
                    total_size = head_dir_total_size + size_diff;
                    total_file_count = head_dir_total_file_count + file_count_diff;
                  }
                in
                let new_dir = Filename_map.add head head_dir_entry dir in
                let* new_dir_hash = store_later setup T.dir new_dir in
                ok (Some (new_dir_hash, added_file_hash, size_diff, file_count_diff))
        )
    | [] ->
        match Filename_map.find_opt filename dir with
          | Some (Dir _) ->
              failed [ "file already exists as a directory" ]
          | Some (File _) ->
              (* TODO: check if hashes are the same *)
              ok None
          | None ->
              let* file_hash, file_size =
                with_progress @@ fun () ->
                Repository.store_file
                  ~source: (Clone.clone setup)
                  ~source_path: path
                  ~target: setup
                  ~on_progress: (on_copy_progress ("Pushing: " ^ Device.show_file_path path))
              in
              let dir_entry =
                File {
                  hash = file_hash;
                  size = file_size;
                }
              in
              let new_dir = Filename_map.add filename dir_entry dir in
              let* new_dir_hash = store_later setup T.dir new_dir in
              ok (Some (new_dir_hash, file_hash, file_size, 1))
  in
  let* update_result = update_dir [] root_dir dir_path in
  match update_result with
    | None ->
        if verbose then echo "Already exists: %s" (Device.show_file_path path);
        ok root
    | Some (new_root_dir_hash, added_file_hash, _, _) ->
        let* new_hash_index_hash, old_paths =
          let added_file_hash_bin = Repository.bin_of_hash added_file_hash in
          let char0 = added_file_hash_bin.[0] in
          let char1 = added_file_hash_bin.[1] in
          let* hash_index = fetch_or_fail setup T.hash_index root.hash_index in
          let* hash_index_1 =
            match Map.Char.find_opt char0 hash_index with
              | None ->
                  ok Map.Char.empty
              | Some hash_index_1_hash ->
                  fetch_or_fail setup T.hash_index_1 hash_index_1_hash
          in
          let* hash_index_2 =
            match Map.Char.find_opt char1 hash_index_1 with
              | None ->
                  ok File_hash_map.empty
              | Some hash_index_2_hash ->
                  fetch_or_fail setup T.hash_index_2 hash_index_2_hash
          in
          let old_paths =
            File_hash_map.find_opt added_file_hash hash_index_2
            |> default File_path_set.empty
          in
          let new_hash_index_2 =
            let new_paths = File_path_set.add path old_paths in
            File_hash_map.add added_file_hash new_paths hash_index_2
          in
          let* new_hash_index_2_hash = store_later setup T.hash_index_2 new_hash_index_2 in
          let new_hash_index_1 = Map.Char.add char1 new_hash_index_2_hash hash_index_1 in
          let* new_hash_index_1_hash = store_later setup T.hash_index_1 new_hash_index_1 in
          let new_hash_index = Map.Char.add char0 new_hash_index_1_hash hash_index in
          let* new_hash_index_hash = store_later setup T.hash_index new_hash_index in
          ok (new_hash_index_hash, old_paths)
        in
        if File_path_set.is_empty old_paths then
          echo "Pushed: %s" (Device.show_file_path path)
        else
          echo "Pushed: %s (duplicate of: %s)" (Device.show_file_path path) (
            File_path_set.elements old_paths
            |> List.map Device.show_file_path
            |> String.concat ", "
          );
        ok {
          root_dir = new_root_dir_hash;
          hash_index = new_hash_index_hash;
        }

let push ~verbose setup (paths: Device.path list) =
  let* root = Repository.fetch_root setup in
  let rec push root path =
    if Device.same_paths path [ dot_filou ] then
      ok root
    else
      let* stat = Device.stat (Clone.clone setup) path in
      match stat with
        | File { path; _ } ->
            push_file ~verbose setup root path
        | Dir ->
            let new_root = ref root in
            let* () =
              Device.iter_read_dir (Clone.clone setup) path @@ fun filename ->
              let* root = push !new_root (path @ [ filename ]) in
              new_root := root;
              unit
            in
            ok !new_root
  in
  let* root = list_fold_e root paths push in
  Repository.store_root setup root

(* TODO:
- pull
- a command to list the duplicates of a file (or of several files, or a dir)
  (or modify tree to make [2] the default and extend with └ = or something *)
