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

let tree =
  unit (* TODO *)

let push_file (setup: Clone.setup) root (((dir_path, filename) as path): Device.file_path) =
  trace ("failed to push file: " ^ Device.show_file_path path) @@
  let* root_dir = fetch_or_fail setup T.dir root.root_dir in
  let* hash_index = fetch_or_fail setup T.hash_index root.hash_index in
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
        let* new_head_dir_hash, added_file_hash, size_diff, file_count_diff =
          update_dir (head :: dir_path_rev) head_dir tail
        in
        let head_dir_entry =
          Dir {
            hash = new_head_dir_hash;
            total_size = head_dir_total_size + size_diff;
            total_file_count = head_dir_total_file_count + file_count_diff;
          }
        in
        let new_dir = Filename_map.add head head_dir_entry dir in
        let* new_dir_hash = store_later setup T.dir new_dir in
        ok (new_dir_hash, added_file_hash, size_diff, file_count_diff)
    | [] ->
        match Filename_map.find_opt filename dir with
          | Some (Dir _) ->
              failed [ "file already exists as a directory" ]
          | Some (File _) ->
              (* TODO: check if hashes are the same *)
              failed [ "file already exists" ]
          | None ->
              let* file_hash, file_size =
                with_progress @@ fun () ->
                Repository.store_file
                  ~source: (Clone.clone setup)
                  ~source_path: path
                  ~target: setup
                  ~on_progress: (
                    on_copy_progress ("[   PUSH   ] " ^ (Device.show_file_path path))
                  )
              in
              let dir_entry =
                File {
                  hash = file_hash;
                  size = file_size;
                }
              in
              let new_dir = Filename_map.add filename dir_entry dir in
              let* new_dir_hash = store_later setup T.dir new_dir in
              ok (new_dir_hash, file_hash, file_size, 1)
  in
  let* new_root_dir_hash, added_file_hash, _, _ = update_dir [] root_dir dir_path in
  let* new_hash_index_hash =
    let added_file_hash_bin = Repository.bin_of_hash added_file_hash in
    let char0 = added_file_hash_bin.[0] in
    let char1 = added_file_hash_bin.[1] in
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
    let new_hash_index_2 =
      let new_paths =
        File_path_set.add path (
          File_hash_map.find_opt added_file_hash hash_index_2
          |> default File_path_set.empty
        )
      in
      File_hash_map.add added_file_hash new_paths hash_index_2
    in
    let* new_hash_index_2_hash = store_later setup T.hash_index_2 new_hash_index_2 in
    let new_hash_index_1 = Map.Char.add char1 new_hash_index_2_hash hash_index_1 in
    let* new_hash_index_1_hash = store_later setup T.hash_index_1 new_hash_index_1 in
    let new_hash_index = Map.Char.add char0 new_hash_index_1_hash hash_index in
    store_later setup T.hash_index new_hash_index
  in
  ok {
    root_dir = new_root_dir_hash;
    hash_index = new_hash_index_hash;
  }

let push setup (paths: Device.path list) =
  let* root = Repository.fetch_root setup in
  let rec push root path =
    if Device.same_paths path [ dot_filou ] then
      ok root
    else
      let* stat = Device.stat (Clone.clone setup) path in
      match stat with
        | File { path; _ } ->
            push_file setup root path
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
