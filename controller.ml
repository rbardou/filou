open Misc
open State

(* TODO: use [Device.with_lock] for read-write operations?
   Note that the damage that can be done if we don't is not terrible:
   it just means that if there are multiple concurrent accesses, the last
   one wins, but the repository is not corrupted: it only contains some
   unreachable objects. *)

let prompt_for_confirmation prompt =
  Prout.print "%s [y/N] " prompt;
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

let empty_dir: dir = Filename_map.empty

let operation_not_available msg =
  failed ("operation not available" :: msg)

let fetch_or_fail (setup: Setup.t) typ hash =
  match setup with
    | { clone_dot_filou = None; main_dot_filou = None; _ } ->
        operation_not_available [ "no repository is configured" ]
    | { clone_dot_filou = Some repository_dot_filou; main_dot_filou = None; _ }
    | { clone_dot_filou = None; main_dot_filou = Some repository_dot_filou; _ } ->
        (
          match Repo.fetch repository_dot_filou typ hash with
            | OK _ as x ->
                x
            | ERROR { code = `failed | `not_available; msg } ->
                failed msg
        )
    | { clone_dot_filou = Some clone_dot_filou; main_dot_filou = Some _; _ } ->
        match Repo.fetch clone_dot_filou typ hash with
          | OK _ as x ->
              x
          | ERROR { code = `failed; _ } as x ->
              x
          | ERROR { code = `not_available; msg } ->
              match setup.main_dot_filou with
                | None ->
                    failed msg
                | Some main_dot_filou ->
                    (* If [R.fetch] failed, the object is stored, so [concrete_hash_or_fail]
                       will not fail. *)
                    let* concrete_hash = Repository.concrete_hash_or_fail hash in
                    let* () =
                      let on_progress ~bytes =
                        Prout.minor @@ fun () ->
                        sf "Transferring object: %s (%s)" (Hash.to_hex concrete_hash)
                          (show_size bytes)
                      in
                      (* TODO: do we really want [transfer_object] to
                         manipulate concrete hashes? *)
                      Repo.transfer_object ~source: main_dot_filou ~target: clone_dot_filou
                        concrete_hash ~on_progress
                    in
                    Repo.fetch clone_dot_filou typ hash

let fetch_raw_or_fail (setup: Setup.t) hash =
  match setup with
    | { clone_dot_filou = None; main_dot_filou = None; _ } ->
        operation_not_available [ "no repository is configured" ]
    | { clone_dot_filou = Some repository_dot_filou; main_dot_filou = None; _ }
    | { clone_dot_filou = None; main_dot_filou = Some repository_dot_filou; _ } ->
        (
          match Repo.fetch_raw repository_dot_filou hash with
            | OK _ as x ->
                x
            | ERROR { code = `failed | `not_available; msg } ->
                failed msg
        )
    | { clone_dot_filou = Some clone_dot_filou; main_dot_filou = Some _; _ } ->
        match Repo.fetch_raw clone_dot_filou hash with
          | OK _ as x ->
              x
          | ERROR { code = `failed; _ } as x ->
              x
          | ERROR { code = `not_available; msg } ->
              match setup.main_dot_filou with
                | None ->
                    failed msg
                | Some main_dot_filou ->
                    let* () =
                      let on_progress ~bytes =
                        Prout.minor @@ fun () ->
                        sf "Transferring object: %s (%s)" (Hash.to_hex hash)
                          (show_size bytes)
                      in
                      (* TODO: do we really want [transfer_object] to
                         manipulate concrete hashes? *)
                      Repo.transfer_object ~source: main_dot_filou ~target: clone_dot_filou
                        hash ~on_progress
                    in
                    Repo.fetch_raw clone_dot_filou hash

let write_config ~dot_filou config =
  let encoded = W.to_string (fun buffer -> Config.write buffer config) in
  Device.write_file dot_filou config_path_in_dot_filou encoded

let read_config ~dot_filou =
  trace "failed to read configuration file" @@
  let* encoded = Device.read_file dot_filou config_path_in_dot_filou in
  decode_rawbin_string Config.read encoded

let init (location: Device.location) =
  let dot_filou = Device.sublocation location dot_filou in
  let* () = write_config ~dot_filou Main in
  Repo.write_root [ dot_filou ] @@ Repo.hash {
    redo = [];
    head = { command = "init"; state = Empty };
    undo = [];
  }

let read_root_hash (setup: Setup.t) =
  match setup with
    | { clone_dot_filou = None; main_dot_filou = None; _ } ->
        operation_not_available [ "no repository is configured" ]
    | { clone_dot_filou = Some repository_dot_filou; main_dot_filou = None; _ }
    | { clone_dot_filou = None; main_dot_filou = Some repository_dot_filou; _ } ->
        let* root_hash_option = Repo.read_root repository_dot_filou in
        (
          match root_hash_option with
            | None ->
                failed [ "failed to read root"; "no root" ]
            | Some root_hash ->
                ok root_hash
        )
    | { clone_dot_filou = Some clone_dot_filou; main_dot_filou = Some main_dot_filou; _ } ->
        let* root_hash_option = Repo.read_root main_dot_filou in
        match root_hash_option with
          | None ->
              failed [ "failed to read root"; "no root" ]
          | Some root_hash ->
              let* () = Repo.write_root [ clone_dot_filou ] root_hash in
              ok root_hash

let store_journal (setup: Setup.t) journal =
  let journal_hash = Repo.hash journal in
  match setup.main_dot_filou with
    | None ->
        operation_not_available [ "no main repository is configured" ]
    | Some main_dot_filou ->
        match setup.clone_dot_filou with
          | None ->
              Repo.write_root [ main_dot_filou ] journal_hash
          | Some clone_dot_filou ->
              Repo.write_root [ main_dot_filou; clone_dot_filou ] journal_hash

let get_object_size (setup: Setup.t) (hash: Hash.t) =
  match setup with
    | { clone_dot_filou = None; main_dot_filou = None; _ } ->
        operation_not_available [ "no repository is configured" ]
    | { clone_dot_filou = Some repository_dot_filou; main_dot_filou = None; _ }
    | { clone_dot_filou = None; main_dot_filou = Some repository_dot_filou; _ } ->
        Repo.get_object_size repository_dot_filou hash
    | { clone_dot_filou = Some clone_dot_filou; main_dot_filou = Some main_dot_filou; _ } ->
        match Repo.get_object_size clone_dot_filou hash with
          | ERROR { code = `failed; _ } | OK _ as x ->
              x
          | ERROR { code = `not_available; _ } ->
              Repo.get_object_size main_dot_filou hash

let get_file_size (setup: Setup.t) (hash: Repository.file_hash) =
  match setup.main_dot_filou with
    | None ->
        failed [ "cannot get file sizes in clone-only mode" ]
    | Some main_dot_filou ->
        (* Files are not stored in clones, at least not with their hash. *)
        Repo.get_file_size main_dot_filou hash

let print_journal ?(only_redo = false) { redo; head; undo } =
  let redo = List.mapi (fun i x -> - i - 1, x) redo in
  (
    List.iter' (List.rev redo) @@ fun (i, { command; _ }) ->
    Prout.echo "    %2d %s" i command;
  );
  if not only_redo then (
    Prout.echo "-->  0 %s" head.command;
    List.iteri' undo @@ fun i { command; _ } ->
    Prout.echo "    %2d %s" (i + 1) command
  )

let clone ~no_cache ~(main_location: Device.location) ~(clone_location: Device.location) =
  Prout.major "Checking %s..." (Device.show_location main_location);
  let main_dot_filou = Device.sublocation main_location dot_filou in
  let* main_config = read_config ~dot_filou: main_dot_filou in
  match main_config with
    | Clone _ ->
        failed [ "cannot clone %s"; "%s is itself a clone repository" ]
    | Main ->
        let* root_hash_option = Repo.read_root main_dot_filou in
        match root_hash_option with
          | None ->
              failed [ sf "%s has no root" (Device.show_location main_dot_filou) ]
          | Some root ->
              Prout.major "Creating %s..." (Device.show_location clone_location);
              let clone_dot_filou = Device.sublocation clone_location dot_filou in
              let* () =
                write_config ~dot_filou: clone_dot_filou
                  (Clone { main_location; no_cache })
              in
              let* () = Repo.write_root [ clone_dot_filou ] root in
              Prout.echo "Cloned %s into: %s" (Device.show_location main_location)
                (Device.show_location clone_location);
              unit

let find_local_config mode =
  let rec find current =
    let location = Device.Local (mode, current) in
    let dot_filou = Device.sublocation location dot_filou in
    match read_config ~dot_filou with
      | OK config ->
          ok (location, dot_filou, config)
      | ERROR { code = (`no_such_file | `failed); _ } ->
          match Path.parent current with
            | None -> failed [ "not in a repository" ]
            | Some parent -> find parent
  in
  find (Path.get_cwd ())

let find_setup ~repository ~no_main ~no_cache mode =
  let* location, location_dot_filou, config =
    match repository with
      | None ->
          find_local_config mode
      | Some repository ->
          let* location = Device.parse_location mode repository in
          let dot_filou = Device.sublocation location dot_filou in
          match read_config ~dot_filou with
            | OK config ->
                ok (location, dot_filou, config)
            | ERROR { code = (`no_such_file | `failed); msg } ->
                failed ("not a repository" :: msg)
  in
  match config with
    | Main ->
        if no_main then
          failed [
            sf "%s is a main repository but --no-main was specified"
              (Device.show_location location)
          ]
        else
          let setup: Setup.t =
            {
              main_dot_filou = Some location_dot_filou;
              clone_dot_filou = None;
              workdir = None; (* TODO: could actually use main's *)
            }
          in
          ok setup
    | Clone config ->
        let main_dot_filou =
          if no_main then
            None
          else
            Some (Device.sublocation config.main_location dot_filou)
        in
        let setup: Setup.t =
          {
            main_dot_filou;
            clone_dot_filou = (
              if no_cache || config.no_cache then
                None
              else
                Some location_dot_filou
            );
            workdir = Some location;
          }
        in
        ok setup

module Check_redo:
sig
  type t

  (* [yes] is the [--yes] CLI argument. *)
  val check: yes: bool -> journal -> (t, [> `failed ]) r
end =
struct
  type t = unit

  let check ~yes journal =
    if yes then
      unit
    else match journal.redo with
      | [] ->
          unit
      | _ :: _ ->
          Prout.echo_s "/!\\ YOU WILL LOSE YOUR REDO HISTORY /!\\";
          Prout.echo_s "After this, the following redo history will no longer be available:";
          Prout.echo_s "";
          print_journal ~only_redo: true journal;
          Prout.echo_s "";
          let* yes = prompt_for_confirmation "Continue anyway?" in
          if not yes then exit 0;
          unit
end

let fetch_journal setup =
  let* root_hash = read_root_hash setup in
  fetch_or_fail setup Journal root_hash

let store_state ~checked_redo: (_: Check_redo.t) setup state command =
  (* TODO: store journal in memory to avoid re-reading it *)
  let* journal = fetch_journal setup in
  let new_journal =
    {
      redo = [];
      head = { command; state };
      undo = journal.head :: journal.undo;
    }
  in
  store_journal setup new_journal

let fetch_state setup =
  let* journal = fetch_journal setup in
  ok journal.head.state

let fetch_root_dir setup (state: state) =
  match state with
    | Empty ->
        ok Filename_map.empty
    | Non_empty state ->
        fetch_or_fail setup Dir state.root_dir

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
    let* hash_index = fetch_or_fail setup Hash_index hash_index_hash in
    let rec gather prefix_rev acc (hash_index: hash_index) =
      match hash_index with
        | Leaf bucket_hash ->
            let* bucket = fetch_or_fail setup Hash_index_bucket bucket_hash in
            list_fold_e acc (File_hash_map.bindings bucket) @@ fun acc (hash, paths) ->
            (* This helper is used by [check], so we can fail if we see something weird.
               Otherwise we could assume that hashes appear only in one bucket. *)
            let concrete_hash = Repository.concrete_file_hash hash in
            let* () = check_prefix (Hash.to_bin concrete_hash) (List.rev prefix_rev) in
            if File_hash_map.mem hash acc then
              failed [ sf "hash %s appears in two buckets" (Hash.to_hex concrete_hash) ]
            else
              ok (File_hash_map.add hash paths acc)
        | Node { zero; one } ->
            let* acc = gather (false :: prefix_rev) acc zero in
            gather (true :: prefix_rev) acc one
    in
    gather [] File_hash_map.empty hash_index

  let get setup (state: state) (hash: Repository.file_hash):
    (File_path_set.t, [> `failed ]) r =
    match state with
      | Empty ->
          ok File_path_set.empty
      | Non_empty state ->
          let* hash_index = fetch_or_fail setup Hash_index state.hash_index in
          let hash_bin = Hash.to_bin (Repository.concrete_file_hash hash) in
          let rec find bit (hash_index: hash_index) =
            match hash_index with
              | Leaf bucket_hash ->
                  let* bucket = fetch_or_fail setup Hash_index_bucket bucket_hash in
                  File_hash_map.find_opt hash bucket |> default File_path_set.empty |> ok
              | Node { zero; one } ->
                  find (bit + 1) (if get_bit hash_bin bit then one else zero)
          in
          find 0 hash_index

  (* Also returns the old list of paths that were associated to this hash. *)
  let add setup (state: state) (added_file_hash: Repository.file_hash)
      (path: Device.file_path): (hash_index hash * File_path_set.t, [> `failed ]) r =
    match state with
      | Empty ->
          let bucket =
            File_hash_map.singleton added_file_hash (File_path_set.singleton path)
          in
          let bucket_hash = Repo.hash bucket in
          let hash_index = Leaf bucket_hash in
          let hash_index_hash = Repo.hash hash_index in
          ok (hash_index_hash, File_path_set.empty)
      | Non_empty root ->
          let* hash_index = fetch_or_fail setup Hash_index root.hash_index in
          let hash_bin = Hash.to_bin (Repository.concrete_file_hash added_file_hash) in
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
                  let* bucket = fetch_or_fail setup Hash_index_bucket bucket_hash in
                  (* One could think that storing the size of each bucket in
                     the [hash_index] would allow to make the following test
                     without having to read the bucket, but we'll have to read it
                     anyway to add the path to it or to split it if it's too big. *)
                  if File_hash_map.cardinal bucket < max_bucket_size then
                    (* Bucket has room for more hashes. *)
                    let new_bucket, old_paths = add_to_bucket bucket in
                    let new_bucket_hash = Repo.hash new_bucket in
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
                      get_bit (Hash.to_bin (Repository.concrete_file_hash hash)) bit
                    in
                    let one, zero, old_paths =
                      if get_bit hash_bin bit then
                        let one, old_paths = add_to_bucket one in
                        one, zero, old_paths
                      else
                        let zero, old_paths = add_to_bucket zero in
                        one, zero, old_paths
                    in
                    let one_hash = Repo.hash one in
                    let zero_hash = Repo.hash zero in
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
          let new_hash_index_hash = Repo.hash new_hash_index in
          ok (new_hash_index_hash, old_paths)

  (* Does not actually store the result so that one can bulk remove. *)
  let remove setup (hash_index: hash_index) (hash: Repository.file_hash)
      (path: Device.file_path): (hash_index, [> `failed ]) r =
    let hash_bin = Hash.to_bin (Repository.concrete_file_hash hash) in
    let rec remove_from bit (hash_index: hash_index): (hash_index, _) r =
      match hash_index with
        | Leaf bucket_hash ->
            let* bucket = fetch_or_fail setup Hash_index_bucket bucket_hash in
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
            let new_bucket_hash = Repo.hash new_bucket in
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

  type found_file =
    {
      path: Device.file_path;
      hash: Repository.file_hash;
      size: int;
    }

  type found_dir =
    {
      dir_path_rev: Device.path;
      dir_hash: dir hash;
    }

  type found =
    | Found_file of found_file
    | Found_dir of found_dir

  let find setup (state: state) (path: Device.path): (found option, _) r =
    match state with
      | Empty ->
          ok None
      | Non_empty { root_dir; _ } ->
          let rec find_in_dir (dir_hash: dir hash) dir_path_rev (path: Device.path) =
            match path with
              | [] ->
                  ok (Some (Found_dir { dir_path_rev; dir_hash }))
              | head :: tail ->
                  let* dir = fetch_or_fail setup Dir dir_hash in
                  match Filename_map.find_opt head dir, tail with
                    | None, _
                    | Some (File _), _ :: _ ->
                        ok None
                    | Some (File { hash; size }), [] ->
                        ok (
                          Some (
                            Found_file {
                              path = (List.rev dir_path_rev, head);
                              hash;
                              size;
                            }
                          )
                        )
                    | Some (Dir { hash = head_dir_hash; _ }), _ ->
                        find_in_dir head_dir_hash (head :: dir_path_rev) tail
          in
          find_in_dir root_dir [] path

  (* Convert [paths] to a list of [found].
     Fail if one of the [paths] do not denote something that exists. *)
  let find_exact_list setup (state: state) (paths: Device.path list): (found list, _) r =
    let* list =
      list_fold_e [] paths @@ fun acc path ->
      let* found = find setup state path in
      match found with
        | None ->
            failed [ sf "no such file or directory: %s" (Device.show_path path) ]
        | Some found ->
            ok (found :: acc)
    in
    ok (List.rev list)

  let iter_dir_files_recursively setup (found_dir: found_dir)
      (f: Device.file_path -> found_file -> (unit, _) r) =
    let rec iter_dir_files_recursively ~subdir_path_rev (dir: dir) =
      list_iter_e (Filename_map.bindings dir) @@ fun (filename, dir_entry) ->
      match dir_entry with
        | File { hash; size } ->
            f (List.rev subdir_path_rev, filename) {
              path = (
                List.rev_append found_dir.dir_path_rev (List.rev subdir_path_rev),
                filename
              );
              hash;
              size;
            }
        | Dir { hash = subdir_hash; _ } ->
            let* subdir = fetch_or_fail setup Dir subdir_hash in
            iter_dir_files_recursively ~subdir_path_rev: (filename :: subdir_path_rev) subdir
    in
    let* dir = fetch_or_fail setup Dir found_dir.dir_hash in
    iter_dir_files_recursively ~subdir_path_rev: [] dir

  (* Same as [find_exact_list] but iterate instead of returning a list
     and support recursively listing directories (so it only returns files).
     Fails if a path denotes a directory. *)
  let iter_exact_list_files ~recursive setup (state: state) (paths: Device.path list)
      (f: found_file -> (unit, _) r) =
    (* First, find [paths]. If one doesn't exist we don't want to
       iterate nor recurse at all, we want to fail early. *)
    let* found = find_exact_list setup state paths in
    (* Then, actually iterate. *)
    list_iter_e found @@ fun found ->
    match found with
      | Found_file file ->
          f file
      | Found_dir dir ->
          if recursive then
            iter_dir_files_recursively setup dir @@ fun _ found_file -> f found_file
          else
            failed [
              sf "%s is a directory" (Device.show_path (List.rev dir.dir_path_rev));
            ]

  (* Same as [iter_exact_list_files] but return a list instead of iterating,
     and display progress. *)
  let find_exact_list_files ~recursive setup root paths =
    let count = ref 0 in
    let list = ref [] in
    let* () =
      iter_exact_list_files ~recursive setup root paths @@ fun found_file ->
      list := found_file :: !list;
      incr count;
      Prout.minor (fun () -> sf "Listing files... (%d)" !count);
      unit
    in
    ok (List.rev !list)

  type add_result =
    | Already_exists_same
    | Already_exists_different
    | Added of {
        new_dir_hash: dir hash;
        added_file_hash: Repository.file_hash;
        added_file_size: int;
      }

  let add setup (root_dir: dir) ((dir_path, filename): Device.file_path)
      (get_hash_and_size: unit -> (Repository.file_hash * int, _) r)
      (get_hash_just_checking: unit -> (Hash.t option, _) r):
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
                  let* dir = fetch_or_fail setup Dir head_dir_hash in
                  ok (dir, total_size, total_file_count)
          in
          let* update_result =
            update_dir (head :: dir_path_rev) head_dir tail
          in
          (
            match update_result with
              | Already_exists_same | Already_exists_different as x ->
                  ok x
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
                  let new_dir_hash = Repo.hash new_dir in
                  ok (Added { new_dir_hash; added_file_hash; added_file_size })
          )
      | [] ->
          match Filename_map.find_opt filename dir with
            | Some (Dir _) ->
                failed [ "file already exists as a directory" ]
            | Some (File { hash; _ }) ->
                let* new_hash = get_hash_just_checking () in
                (
                  match new_hash with
                    | None ->
                        ok Already_exists_same
                    | Some new_hash ->
                        if Hash.compare new_hash (Repository.concrete_file_hash hash) = 0 then
                          ok Already_exists_same
                        else
                          ok Already_exists_different
                )
            | None ->
                let* added_file_hash, added_file_size = get_hash_and_size () in
                let dir_entry =
                  File {
                    hash = added_file_hash;
                    size = added_file_size;
                  }
                in
                let new_dir = Filename_map.add filename dir_entry dir in
                let new_dir_hash = Repo.hash new_dir in
                ok (Added { new_dir_hash; added_file_hash; added_file_size })
    in
    update_dir [] root_dir dir_path

  type remove_result =
    | File_does_not_exist
    | Is_a_directory
    | Removed of {
        new_dir_hash: dir Repository.hash option; (* [None] if dir is now empty *)
        removed_file_hash: Repository.file_hash;
        removed_file_size: int;
      }

  let remove setup (root_dir: dir) (path: Device.file_path):
    (remove_result, [> `failed ]) r =
    let rec remove_from_dir (dir: dir) dir_path_rev (path: Device.path) =
      match path with
        | [] ->
            ok Is_a_directory
        | head :: tail ->
            match Filename_map.find_opt head dir with
              | None ->
                  ok File_does_not_exist
              | Some (Dir { hash; total_size; total_file_count }) ->
                  (* Requested to remove [head/tail] from [dir]. *)
                  (* Find [subdir] named [head] in [dir]. *)
                  let* subdir = fetch_or_fail setup Dir hash in
                  (* Remove [tail] from [subdir]. *)
                  let* remove_result =
                    remove_from_dir subdir (head :: dir_path_rev) tail
                  in
                  (* Update [dir]. *)
                  (
                    match remove_result with
                      | File_does_not_exist | Is_a_directory as x ->
                          ok x
                      | Removed { new_dir_hash; removed_file_hash; removed_file_size } ->
                          let new_dir =
                            match new_dir_hash with
                              | None ->
                                  (* Removed [subdir], i.e. [head], completely. *)
                                  Filename_map.remove head dir
                              | Some new_dir_hash ->
                                  (* Removed part of [head], which still contains files. *)
                                  Filename_map.add head
                                    (
                                      Dir {
                                        hash = new_dir_hash;
                                        total_size = total_size - removed_file_size;
                                        total_file_count = total_file_count - 1;
                                      }
                                    )
                                    dir
                          in
                          let* new_dir_hash =
                            if Filename_map.is_empty new_dir then
                              ok None
                            else
                              (* TODO: share with below? *)
                              let new_dir_hash = Repo.hash new_dir in
                              ok (Some new_dir_hash)
                          in
                          ok (
                            Removed {
                              new_dir_hash;
                              removed_file_hash;
                              removed_file_size;
                            }
                          )
                  )
              | Some (File { hash = removed_file_hash; size = removed_file_size }) ->
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
                        let* new_dir_hash =
                          if Filename_map.is_empty new_dir then
                            ok None
                          else
                            let new_dir_hash = Repo.hash new_dir in
                            ok (Some new_dir_hash)
                        in
                        ok (
                          Removed {
                            new_dir_hash;
                            removed_file_hash;
                            removed_file_size;
                          }
                        )
    in
    remove_from_dir root_dir [] (Device.path_of_file_path path)

end

(* Recursively check sizes, file counts, and whether reachable files are available. *)
let rec check_dir expected_hash_index setup
    (path: Device.path) (dir: dir hash) =
  let* dir = fetch_or_fail setup Dir dir in
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
            check_dir expected_hash_index setup entry_path hash
          in
          if size <> expected_size then
            failed [
              sf "wrong size for %s: expected %d, found %d"
                (Device.show_path entry_path)
                expected_size size;
            ]
          else if file_count <> expected_file_count then
            failed [
              sf "wrong file count for %s: expected %d, found %d"
                (Device.show_path entry_path)
                expected_file_count file_count;
            ]
          else (
            path_size := !path_size + size;
            path_file_count := !path_file_count + file_count;
            unit
          )
      | File { hash; size = expected_size } ->
          match
            if setup.main_dot_filou = None then
              (* Cannot get file sizes in clone-only mode. *)
              ok expected_size
            else
              get_file_size setup hash
          with
            | ERROR { code = `not_available; msg } ->
                failed (
                  sf "file %s (%s) is unvailable"
                    (Device.show_path entry_path)
                    (Hash.to_hex (Repository.concrete_file_hash hash)) ::
                  msg
                )
            | ERROR { code = `failed; _ } as x ->
                x
            | OK size ->
                if size <> expected_size then
                  failed [
                    sf "wrong size for %s (%s): expected %d, found %d"
                      (Device.show_path entry_path)
                      (Hash.to_hex (Repository.concrete_file_hash hash))
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

let check ~hash (setup: Setup.t) =
  let* () =
    let check_hashes description iter_hashes check_hash =
      Prout.major "Listing %ss..." description;
      let hashes = ref [] in
      let* () =
        let count = ref 0 in
        iter_hashes @@ fun hash ->
        Prout.minor (fun () -> sf "Listing %ss (%d)..." description !count);
        hashes := hash :: !hashes;
        unit
      in
      let hashes = !hashes in
      Prout.major "Checking %s hashes..." description;
      let count = List.length hashes in
      let index = ref 0 in
      let* () =
        list_iter_e hashes @@ fun hash ->
        incr index;
        let on_progress ~bytes ~size =
          Prout.minor @@ fun () ->
          sf "Checking %s hashes... (%d / %d) (%d%%) %s (%s / %s) (%d%%)"
            description
            !index count (!index * 100 / count)
            (Hash.to_hex hash)
            (show_size bytes) (show_size size)
            (bytes * 100 / size)
        in
        check_hash ~on_progress hash
      in
      if count = 1 then
        Prout.echo "Only 1 %s, which is not corrupted." description
      else
        Prout.echo "None of the %d %ss are corrupted." count description;
      unit
    in
    let check_objects () =
      let* () =
        match setup.main_dot_filou with
          | None ->
              unit
          | Some main_dot_filou ->
              check_hashes "main object" (Repo.iter_objects main_dot_filou)
                (Repo.check_object_hash main_dot_filou)
      in
      match setup.clone_dot_filou with
        | None ->
            unit
        | Some clone_dot_filou ->
            check_hashes "clone object" (Repo.iter_objects clone_dot_filou)
              (Repo.check_object_hash clone_dot_filou)
    in
    let check_files () =
      match setup.main_dot_filou with
        | None ->
            unit
        | Some main_dot_filou ->
            check_hashes "file" (Repo.iter_files main_dot_filou)
              (Repo.check_file_hash main_dot_filou)
    in
    match hash with
      | `no ->
          unit
      | `metadata ->
          check_objects ()
      | `all ->
          let* () = check_objects () in
          check_files ()
  in
  let* state = fetch_state setup in
  match state with
    | Empty ->
        Prout.echo_s "Repository is empty.";
        unit
    | Non_empty root ->
        let expected_hash_index = ref File_hash_map.empty in
        let* size, count = check_dir expected_hash_index setup [] root.root_dir in
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
                  (Hash.to_hex (Repository.concrete_file_hash hash))
              else if not (File_path_set.is_empty unexpected) then
                inconsistent "unexpected path(s) %s for %s"
                  (String.concat ", "
                     (List.map Device.show_file_path
                        (File_path_set.elements unexpected)))
                  (Hash.to_hex (Repository.concrete_file_hash hash))
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

type merged_dir_entry =
  | MDE_main of State.dir_entry
  | MDE_clone of Device.stat
  | MDE_both of State.dir_entry * Device.stat

type tree_entry_name =
  | TEN_path of Device.path
  | TEN_filename of Path.Filename.t

let tree ~color ~max_depth ~only_main ~only_dirs
    ~print_size ~print_file_count ~print_duplicates ~full_dir_paths
    (setup: Setup.t) (paths: Device.path list) =
  let* state = fetch_state setup in
  let* root_dir = fetch_root_dir setup state in
  let find_path path =
    let* dir_entry =
      match state with
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
                              let* subdir = fetch_or_fail setup Dir hash in
                              find subdir tail
            in
            find root_dir path
    in
    let* stat =
      match setup.workdir with
        | None ->
            ok None
        | Some workdir ->
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
              let* paths = Hash_index.get setup state hash in
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
              fetch_or_fail setup Dir hash
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
        | MDE_both (File { hash; size }, File _) ->
            let* status =
              let file_path =
                match entry_name with
                  | TEN_path path ->
                      (* [dir_path] is not correct in this case. *)
                      (
                        match Device.file_path_of_path path with
                          | None ->
                              (* This is the root. *)
                              None
                          | Some path ->
                              Some path
                      )
                  | TEN_filename filename ->
                      Some (dir_path, filename)
              in
              match file_path with
                | None ->
                    (* Could also be [Dir] but we'll do the same in both cases. *)
                    ok Cash.Does_not_exist
                | Some file_path ->
                    let on_progress ~bytes ~size =
                      Prout.minor @@ fun () ->
                      sf "Hashing... (%s / %s) (%d%%)"
                        (show_size bytes) (show_size size)
                        (bytes * 100 / size)
                    in
                    let result = Cash.get ~on_progress setup file_path in
                    Prout.clear_progress ();
                    result
            in
            if
              match status with
                | Does_not_exist | Dir ->
                    (* Inconsistent with what we observed before. *)
                    false
                | File workdir_hash ->
                    Hash.compare workdir_hash (Repository.concrete_file_hash hash) = 0
            then
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
        match setup.workdir with
          | None ->
              ok Filename_map.empty
          | Some workdir ->
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

let push_file ~verbose ~pushed_size ~total_size_to_push ~file_index ~file_count
    (setup: Setup.t) (state: state) (path: Device.file_path):
  (state, [> `failed ]) r =
  match setup.workdir with
    | None ->
        operation_not_available [ "cannot push with no work directory" ]
    | Some workdir ->
        trace ("failed to push file: " ^ Device.show_file_path path) @@
        let* root_dir = fetch_root_dir setup state in
        let* add_result =
          let on_hash_progress ~bytes ~size =
            Prout.minor @@ fun () ->
            sf "Pushing... (%s / %s) (%d%%) (file %d / %d) - hashing (%s / %s) (%d%%)"
              (show_size pushed_size) (show_size total_size_to_push)
              (pushed_size * 100 / total_size_to_push)
              file_index file_count
              (show_size bytes) (show_size size)
              (bytes * 100 / size)
          in
          let get_hash_and_size () =
            let* hash, size =
              let* main =
                match setup.main_dot_filou with
                  | None ->
                      failed [ "cannot store files in clone-only mode" ]
                  | Some main_dot_filou ->
                      ok main_dot_filou
              in
              Repo.store_file
                ~on_hash_progress
                ~on_copy_progress: (
                  fun ~bytes ~size: _ ->
                    let bytes = bytes + pushed_size in
                    let size = total_size_to_push in
                    Prout.minor @@ fun () ->
                    sf "Pushing... (%s / %s) (%d%%) (file %d / %d) %s"
                      (show_size bytes) (show_size size) (bytes * 100 / size)
                      file_index file_count (Device.show_file_path path)
                )
                ~source: workdir
                ~source_path: path
                ~target: main
            in
            Cash.set setup path (Repository.concrete_file_hash hash);
            ok (hash, size)
          in
          let get_hash_just_checking () =
            let* status = Cash.get ~on_progress: on_hash_progress setup path in
            ok (match status with Does_not_exist | Dir -> None | File hash -> Some hash)
          in
          Dir.add setup root_dir path get_hash_and_size get_hash_just_checking
        in
        match add_result with
          | Already_exists_same ->
              if verbose then Prout.echo "Already exists: %s" (Device.show_file_path path);
              ok state
          | Already_exists_different ->
              failed [
                sf "a different file with this name already exists: %s"
                  (Device.show_file_path path);
              ]
          | Added { new_dir_hash = new_root_dir_hash; added_file_hash; _ } ->
              let* new_hash_index_hash, old_paths =
                Hash_index.add setup state added_file_hash path
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

let push ~verbose ~yes (setup: Setup.t) (paths: Device.path list) =
  match setup.workdir with
    | None ->
        operation_not_available [ "cannot push with no work directory" ]
    | Some workdir ->
        let* journal = fetch_journal setup in
        let* checked_redo = Check_redo.check ~yes journal in
        Prout.major_s "Listing files...";
        let files = ref [] in
        let file_count = ref 0 in
        let rec add_path (path: Device.path) =
          if Device.same_paths path [ dot_filou ] then
            unit
          else
            let* stat = Device.stat workdir path in
            match stat with
              | Dir ->
                  Device.iter_read_dir workdir path @@ fun filename ->
                  add_path (path @ [ filename ])
              | File { size; _ } ->
                  match Device.file_path_of_path path with
                    | None ->
                        (* Should not happen, "." is a directory. *)
                        unit
                    | Some path ->
                        files := (path, size) :: !files;
                        incr file_count;
                        Prout.minor (fun () -> sf "Listing files... (%d)" !file_count);
                        unit
        in
        let* () = list_iter_e paths add_path in
        let files = List.rev !files in
        let file_count = !file_count in
        Prout.major_s "Listing files to push...";
        let state = journal.head.state in
        let* files_to_push =
          let file_index = ref 0 in
          list_filter_e files @@ fun ((file_path: Device.file_path), _) ->
          let* found = Dir.find setup state (Device.path_of_file_path file_path) in
          incr file_index;
          (
            Prout.minor @@ fun () ->
            sf "Listing files to push... (%d / %d) (%d%%)"
              !file_index file_count (!file_index * 100 / file_count)
          );
          match found with
            | None ->
                ok true
            | Some (Found_dir _) ->
                failed [
                  sf "%s already exists as a directory" (Device.show_file_path file_path);
                ]
            | Some (Found_file { path; hash; _ }) ->
                let* status =
                  let on_progress ~bytes ~size =
                    Prout.minor @@ fun () ->
                    sf "Listing files to push... (%d / %d) (%d%%) - hashing (%s / %s) (%d%%)"
                      !file_index file_count (!file_index * 100 / file_count)
                      (show_size size) (show_size bytes)
                      (size * 100 / bytes)
                  in
                  Cash.get ~on_progress setup path
                in
                match status with
                  | Does_not_exist | Dir ->
                      (* This is inconsistent with the list of files we found before. *)
                      failed [ sf "%s got removed" (Device.show_file_path path) ]
                  | File new_hash ->
                      if Hash.compare new_hash (Repository.concrete_file_hash hash) = 0 then
                        (
                          if verbose then
                            Prout.echo "Already exists: %s" (Device.show_file_path file_path);
                          ok false
                        )
                      else
                        failed [
                          sf "%s already exists with a different hash"
                            (Device.show_file_path file_path);
                        ]
        in
        let file_count = List.length files_to_push in
        Prout.major_s "Pushing files...";
        let total_size_to_push =
          List.fold_left' 0 files_to_push @@ fun acc (_, size) -> acc + size
        in
        let* state, _ =
          let file_index = ref 0 in
          list_fold_e (state, 0) files_to_push @@ fun (state, pushed_size) (path, size) ->
          incr file_index;
          let* state =
            push_file ~verbose ~pushed_size ~total_size_to_push ~file_index: !file_index
              ~file_count setup state path
          in
          ok (state, pushed_size + size)
        in
        let* () =
          store_state ~checked_redo setup state @@
          String.concat " " ("push" :: List.map Device.show_path paths)
        in
        Prout.echo "Pushed %d files (%s)." file_count (show_size total_size_to_push);
        unit

let pull ~verbose (setup: Setup.t) (paths: Device.path list) =
  match setup.workdir with
    | None ->
        operation_not_available [ "cannot pull with no work directory" ]
    | Some workdir ->
        let* state = fetch_state setup in
        Prout.major_s "Listing files...";
        let* files = Dir.find_exact_list_files ~recursive: true setup state paths in
        Prout.major_s "Listing files to pull...";
        let* files_to_pull =
          let file_count = List.length files in
          let file_index = ref 0 in
          list_filter_e files @@ fun file ->
          incr file_index;
          (
            Prout.minor @@ fun () ->
            sf "Listing files to pull... (%d / %d) (%d%%)"
              !file_index file_count (!file_index * 100 / file_count)
          );
          let* status =
            let on_progress ~bytes ~size =
              Prout.minor @@ fun () ->
              sf "Listing files to pull... (%d / %d) (%d%%) - hashing (%s / %s) (%d%%)"
                !file_index file_count (!file_index * 100 / file_count)
                (show_size size) (show_size bytes)
                (size * 100 / bytes)
            in
            Cash.get ~on_progress setup file.path
          in
          match status with
            | Does_not_exist ->
                ok true
            | Dir ->
                failed [
                  sf "%s already exists as a directory" (Device.show_file_path file.path);
                ]
            | File new_hash ->
                if Hash.compare new_hash (Repository.concrete_file_hash file.hash) = 0 then
                  (
                    if verbose then
                      Prout.echo "Already exists: %s"
                        (Device.show_file_path file.path);
                    ok false
                  )
                else
                  failed [
                    sf "%s already exists with a different hash"
                      (Device.show_file_path file.path);
                  ]
        in
        Prout.major_s "Pulling files...";
        let file_count = List.length files_to_pull in
        let* total_size_pulled =
          let file_index = ref 0 in
          let total_size_to_pull =
            List.fold_left' 0 files_to_pull (fun acc file -> acc + file.size)
          in
          list_fold_e 0 files_to_pull @@ fun pulled_size file ->
          incr file_index;
          let* () =
            let* main_dot_filou =
              match setup.main_dot_filou with
                | None ->
                    failed [ "cannot store files in clone-only mode" ]
                | Some main_dot_filou ->
                    ok main_dot_filou
            in
            Repo.fetch_file file.hash
              ~source: main_dot_filou
              ~target: workdir
              ~target_path: file.path
              ~on_progress: (
                fun ~bytes ~size: _ ->
                  let bytes = bytes + pulled_size in
                  let size = total_size_to_pull in
                  Prout.minor @@ fun () ->
                  sf "Pulling... (%d / %d) (%d%%) (file %d / %d) %s"
                    bytes size (bytes * 100 / size)
                    !file_index file_count (Device.show_file_path file.path)
              )
          in
          if verbose then
            Prout.echo "Pulled: %s" (Device.show_file_path file.path);
          ok (pulled_size + file.size)
        in
        Prout.echo "Pulled %d files (%s)." file_count (show_size total_size_pulled);
        unit

let remove_file setup (state: state) (path: Device.file_path): (state, _) r =
  match state with
    | Empty ->
        failed [ sf "no such file: %s" (Device.show_file_path path) ]
    | Non_empty root ->
        let* root_dir = fetch_or_fail setup Dir root.root_dir in
        let* remove_result = Dir.remove setup root_dir path in
        match remove_result with
          | File_does_not_exist ->
              failed [ sf "no such file: %s" (Device.show_file_path path) ]
          | Is_a_directory ->
              failed [ sf "%s is a directory" (Device.show_file_path path) ]
          | Removed { new_dir_hash = None; _ } ->
              ok Empty
          | Removed { new_dir_hash = Some new_dir_hash; removed_file_hash; _ } ->
              let* hash_index = fetch_or_fail setup Hash_index root.hash_index in
              let* new_hash_index =
                Hash_index.remove setup hash_index removed_file_hash path
              in
              let new_hash_index_hash = Repo.hash new_hash_index in
              let new_root = { root_dir = new_dir_hash; hash_index = new_hash_index_hash } in
              ok (Non_empty new_root)

let remove ~recursive ~yes setup (paths: Device.path list) =
  let* journal = fetch_journal setup in
  let* checked_redo = Check_redo.check ~yes journal in
  let state = journal.head.state in
  Prout.major "Listing files...";
  let* files_to_remove = Dir.find_exact_list_files ~recursive setup state paths in
  Prout.major "Removing files...";
  let count = List.length files_to_remove in
  let* state =
    let index = ref 0 in
    list_fold_e state files_to_remove @@ fun root { path; _ } ->
    incr index;
    (
      Prout.minor @@ fun () ->
      sf "Removing files... (%d / %d) (%d%%)" !index count (!index * 100 / count)
    );
    remove_file setup root path
  in
  Prout.major "Storing root...";
  let* () =
    store_state ~checked_redo setup state @@
    String.concat " " (
      "rm" :: (if recursive then [ "-r" ] else []) @ List.map Device.show_path paths
    )
  in
  Prout.echo "Removed %d files." count;
  unit

type copy_or_move = Copy | Move

let copy_or_move_file (action: copy_or_move) ~verbose setup root
    (source: Dir.found_file) (target_path: Device.file_path) =
  let* root =
    match action with
      | Copy -> ok root
      | Move -> remove_file setup root source.path
  in
  trace ("failed to add file: " ^ Device.show_file_path target_path) @@
  let* root_dir = fetch_root_dir setup root in
  let* add_result =
    Dir.add setup root_dir target_path
      (fun () -> ok (source.hash, source.size))
      (fun () -> ok (Some (Repository.concrete_file_hash source.hash)))
  in
  match add_result with
    | Already_exists_same ->
        if verbose then Prout.echo "Already exists: %s" (Device.show_file_path target_path);
        ok root
    | Already_exists_different ->
        failed [ sf "already exists: %s" (Device.show_file_path target_path) ]
    | Added { new_dir_hash = new_root_dir_hash; added_file_hash; _ } ->
        let* new_hash_index_hash, _ =
          Hash_index.add setup root added_file_hash target_path
        in
        if verbose then (
          Prout.echo "%s -> %s"
            (Device.show_file_path source.path)
            (Device.show_file_path target_path)
        );
        ok (
          Non_empty {
            root_dir = new_root_dir_hash;
            hash_index = new_hash_index_hash;
          }
        )

let copy_or_move_files (action: copy_or_move) ~verbose ~yes setup
    (source_paths: Device.path list) (target: Device.path_with_kind) =
  let* journal = fetch_journal setup in
  let* checked_redo = Check_redo.check ~yes journal in
  let state = journal.head.state in
  Prout.major "Checking paths...";
  let* (_: Filename_set.t) =
    list_fold_e Filename_set.empty source_paths @@ fun set source_path ->
    match Device.file_path_of_path source_path with
      | None ->
          (* It could make sense (moving ./x to target/x for all x).
             It makes checking for same filenames more complicated though.
             And it's also complicated in other places below. *)
          failed [
            sf "cannot %s the root directory"
              (match action with Copy -> "copy" | Move -> "move");
          ]
      | Some (_, filename) ->
          if Filename_set.mem filename set then
            failed [
              sf "two sources have the same name and would overwrite each other: %s"
                (Path.Filename.show filename)
            ]
          else
            ok (Filename_set.add filename set)
  in
  let* sources = Dir.find_exact_list setup state source_paths in
  let* mode =
    let target_path =
      match target with
        | File path -> Device.path_of_file_path path
        | Dir path -> path
    in
    let* found_target = Dir.find setup state target_path in
    match found_target with
      | Some (Found_dir _) ->
          ok (`into_dir (sources, target_path))
      | Some (Found_file _) ->
          failed [
            sf "target %s already exists and is a not a directory"
              (Device.show_path target_path);
          ]
      | None ->
          match sources, target with
            | [], _ ->
                (* Prevented by main.ml. *)
                failed [ "missing source" ]
            | [ Found_file source ], File target_path ->
                ok (`as_file (source, target_path))
            | [ Found_dir source ], File target_path ->
                ok (`as_dir (source, target_path))
            | _ :: _:: _, File _
            | _, Dir _ ->
                ok (`into_dir (sources, target_path))
  in
  Prout.major "Listing files...";
  let count = ref 0 in
  let incr_count () =
    incr count;
    Prout.minor (fun () -> sf "Listing files... (%d)" !count);
  in
  let* files =
    let list_for_as_dir acc (source: Dir.found_dir) (target_path: Device.file_path) =
      let acc = ref acc in
      let* () =
        Dir.iter_dir_files_recursively setup source @@
        fun (relative_dir, filename) found_file ->
        incr_count ();
        acc := (
          found_file,
          ((Device.path_of_file_path target_path @ relative_dir, filename)
           : Device.file_path)
        ) :: !acc;
        unit
      in
      ok !acc
    in
    match mode with
      | `as_file (source, target_path) ->
          incr_count ();
          ok [ source, target_path ]
      | `as_dir (source, target_path) ->
          list_for_as_dir [] source target_path
      | `into_dir (sources, target_path) ->
          list_fold_e [] sources @@ fun acc source ->
          match source with
            | Found_file ({ path = (_, filename); _ } as file) ->
                incr_count ();
                ok ((file, (target_path, filename)) :: acc)
            | Found_dir { dir_path_rev = []; _ } ->
                (* Cannot happen because we already filtered those out. *)
                failed [ "cannot move the root directory" ]
            | Found_dir ({ dir_path_rev = (filename :: _); _ } as dir) ->
                list_for_as_dir acc dir (target_path, filename)
  in
  let files = List.rev files in
  let count = !count in
  Prout.major "%s files..." (match action with Copy -> "Copying" | Move -> "Moving");
  let* state =
    let index = ref 0 in
    list_fold_e state files @@ fun state (source, target_path) ->
    Prout.major "%s files... (%d / %d) (%d%%)"
      (match action with Copy -> "Copying" | Move -> "Moving")
      !index count (!index * 100 / count);
    incr index;
    copy_or_move_file action ~verbose setup state source target_path
  in
  Prout.major "Storing root...";
  let* () =
    store_state ~checked_redo setup state @@
    String.concat " " (
      (
        match action with
          | Copy -> "cp"
          | Move -> "mv"
      )
      :: List.map Device.show_path source_paths
      @ [ Device.show_path_with_kind target ]
    )
  in
  (
    match action with
      | Copy -> Prout.echo "Copied %d files." count
      | Move -> Prout.echo "Moved %d files." count
  );
  unit

let update (setup: Setup.t) =
  match setup with
    | { clone_dot_filou = None; _ } | { main_dot_filou = None; _ }->
        operation_not_available [ "update needs both a main and a clone" ]
    | { clone_dot_filou = Some clone_dot_filou; main_dot_filou = Some main_dot_filou; _ } ->
        Prout.major_s "Transferring root...";
        let* root_hash = Repo.read_root main_dot_filou in
        let* () =
          match root_hash with
            | None ->
                failed [ "main repository is not initialized" ]
            | Some root_hash ->
                Repo.write_root [ clone_dot_filou ] root_hash
        in
        Prout.major_s "Listing objects...";
        let count = ref 0 in
        let* object_hashes =
          let result = ref [] in
          let* () =
            Repo.iter_objects main_dot_filou @@ fun hash ->
            Prout.minor (fun () -> sf "Listing objects... (%d)" !count);
            result := hash :: !result;
            unit
          in
          ok !result
        in
        let count = !count in
        Prout.major_s "Checking availability...";
        let* object_hashes_to_transfer =
          let index = ref 0 in
          list_filter_e object_hashes @@ fun hash ->
          let* available = Repo.object_is_available clone_dot_filou hash in
          incr index;
          (
            Prout.minor @@ fun () ->
            sf "Checking availability... (%d / %d) (%d%%)"
              !index count (!index * 100 / count)
          );
          ok (not available)
        in
        let to_transfer_count = List.length object_hashes_to_transfer in
        Prout.major "Transferring objects...";
        let* () =
          let index = ref 0 in
          list_iter_e object_hashes_to_transfer @@ fun hash ->
          let on_progress ~bytes =
            Prout.minor @@ fun () ->
            sf "Transferring objects... (%d / %d) (%d%%) %s (%s)"
              !index to_transfer_count (!index * 100 / to_transfer_count)
              (Hash.to_hex hash) (show_size bytes)
          in
          let* () =
            Repo.transfer_object
              ~source: main_dot_filou
              ~target: clone_dot_filou
              hash ~on_progress
          in
          incr index;
          unit
        in
        Prout.echo "Transferred %d objects." to_transfer_count;
        Prout.echo_s "Clone is up-to-date.";
        unit

let prune ~yes ~keep_undo ~keep_redo (setup: Setup.t) =
  match setup.main_dot_filou with
    | None ->
        (* TODO: we could garbage-collect the clone, without changing the journal? *)
        operation_not_available [ "main repository is not configured" ]
    | Some main_dot_filou ->
        let* journal = fetch_journal setup in
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
            Prout.echo "  Main repository: %s" (Device.show_location main_dot_filou);
            let clone =
              match setup.clone_dot_filou with
                | None -> "not configured"
                | Some clone_dot_filou -> Device.show_location clone_dot_filou
            in
            Prout.echo "            Clone: %s" clone;
            Prout.echo "";
            prompt_for_confirmation "Prune main repository and its clone?"
          )
        in
        if not yes then
          unit
        else
          let* () = store_journal setup new_journal in
          Prout.major_s "Computing reachable set...";
          let* reachable_objects_hashes, reachable_file_hashes =
            let on_progress ~object_count ~file_count =
              Prout.minor @@ fun () ->
              sf "Computing reachable set (%d objects, %d files)..."
                object_count file_count
            in
            Repo.get_reachable_hashes ~on_progress main_dot_filou
          in
          let reachable_hashes =
            Hash_set.union reachable_objects_hashes reachable_file_hashes
          in
          let list_unreachable_hashes description iter_hashes =
            Prout.major "Listing %ss..." description;
            let hashes = ref [] in
            let* () =
              let count = ref 0 in
              iter_hashes @@ fun hash ->
              Prout.minor (fun () -> sf "Listing %ss (%d)..." description !count);
              if not (Hash_set.mem hash reachable_hashes) then
                hashes := hash :: !hashes;
              unit
            in
            ok !hashes
          in
          let* unreachable_clone_object_hashes =
            match setup.clone_dot_filou with
              | None ->
                  ok None
              | Some clone_dot_filou ->
                  let* list =
                    list_unreachable_hashes "clone object"
                      (Repo.iter_objects clone_dot_filou)
                  in
                  ok (Some (clone_dot_filou, list))
          in
          let* unreachable_main_object_hashes =
            list_unreachable_hashes "main object"
              (Repo.iter_objects main_dot_filou)
          in
          let* unreachable_file_hashes =
            list_unreachable_hashes "file"
              (Repo.iter_files main_dot_filou)
          in
          let remove_list description from get_size remove_hash hashes =
            Prout.major "Removing unreachable %s from %s..." description from;
            let count = List.length hashes in
            let index = ref 0 in
            let total_size = ref 0 in
            let* () =
              list_iter_e hashes @@ fun hash ->
              let* size =
                match get_size hash with
                  | ERROR { code = `failed | `not_available; msg } ->
                      failed msg
                  | OK _ as x ->
                      x
              in
              let* () = remove_hash hash in
              (
                Prout.minor @@ fun () ->
                sf "Removing unreachable %s from %s... (%d / %d) (%d%%)"
                  description from !index count (!index * 100 / count)
              );
              total_size := !total_size + size;
              unit
            in
            Prout.echo "Removed %d %ss from %s totalling %s." count description from
              (show_size !total_size);
            unit
          in
          let* () =
            match unreachable_clone_object_hashes with
              | None ->
                  unit
              | Some (clone_dot_filou, unreachable_clone_object_hashes) ->
                  remove_list "object" "clone"
                    (Repo.get_object_size clone_dot_filou)
                    (Repo.remove_object clone_dot_filou)
                    unreachable_clone_object_hashes
          in
          let* () =
            remove_list "object" "main"
              (Repo.get_object_size main_dot_filou)
              (Repo.remove_object main_dot_filou)
              unreachable_main_object_hashes
          in
          let* () =
            remove_list "file" "main"
              (fun hash ->
                 Repo.get_file_size main_dot_filou (Repository.stored_file_hash hash))
              (Repo.remove_file main_dot_filou)
              unreachable_file_hashes
          in
          unit

let log setup =
  let* journal = fetch_journal setup in
  print_journal journal;
  unit

let undo setup ~count =
  let undo count =
    let* journal = fetch_journal setup in
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
      store_journal setup journal
  in
  let redo count =
    let* journal = fetch_journal setup in
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
      store_journal setup journal
  in
  if count > 0 then undo count else
  if count < 0 then redo (- count) else
    unit

let diff ~color setup ~before ~after =
  let* journal = fetch_journal setup in
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
  let* dir_before = fetch_root_dir setup root_before.state in
  let* dir_after = fetch_root_dir setup root_after.state in
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
          if
            Hash.compare
              (Repository.concrete_file_hash hash_before)
              (Repository.concrete_file_hash hash_after)
            = 0
          then
            unit
          else
            status `differs `file
      | Some (Dir { hash = hash_before; _ }), Some (Dir { hash = hash_after; _ }) ->
          let* concrete_hash_before = Repository.concrete_hash_or_fail hash_before in
          let* concrete_hash_after = Repository.concrete_hash_or_fail hash_after in
          if Hash.compare concrete_hash_before concrete_hash_after = 0 then
            unit
          else
            let* dir_before = fetch_or_fail setup Dir hash_before in
            let* dir_after = fetch_or_fail setup Dir hash_after in
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

let stats (setup: Setup.t) =
  let* reachable_count, total_bytes, total_disk_usage =
    Prout.major_s "Computing reachable set...";
    let* reachable_object_hashes, _ =
      let* location =
        match setup with
          | { clone_dot_filou = None; main_dot_filou = None; _ } ->
              operation_not_available [ "no repository is configured" ]
          | { main_dot_filou = Some repository_dot_filou; _ }
          | { clone_dot_filou = Some repository_dot_filou; main_dot_filou = None; _ } ->
              ok repository_dot_filou
      in
      let on_progress ~object_count ~file_count =
        Prout.minor @@ fun () ->
        sf "Computing reachable set (%d objects, %d files)..."
          object_count file_count
      in
      Repo.get_reachable_hashes ~on_progress location
    in
    let hashes = Hash_set.elements reachable_object_hashes in
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
      let* size = get_object_size setup hash in
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

let show setup obj =
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
  let show_hash hash =
    let* encoded = fetch_raw_or_fail setup hash in
    let* value = Object.decode_any encoded in
    Prout.echo "%s" (Pretty.show (Pretty.value_of_object value));
    unit
  in
  match obj with
    | `hash hash ->
        show_hash hash
    | `journal ->
        let* root_hash = read_root_hash setup in
        let* concrete_hash = Repository.concrete_hash_or_fail root_hash in
        show_hash concrete_hash
    | `root_dir | `hash_index as obj ->
        let* state = fetch_state setup in
        match state with
          | Empty ->
              Prout.echo_s "empty";
              unit
          | Non_empty non_empty_root ->
              let* hash =
                match obj with
                  | `root_dir -> Repository.concrete_hash_or_fail non_empty_root.root_dir
                  | `hash_index -> Repository.concrete_hash_or_fail non_empty_root.hash_index
              in
              show_hash hash

let show_config location: Config.t -> _ = function
  | Main ->
      Prout.echo "main repository: %s" (Device.show_location location)
  | Clone { main_location; no_cache } ->
      Prout.echo "clone repository: %s" (Device.show_location location);
      Prout.echo "main repository: %s" (Device.show_location main_location);
      Prout.echo "no cache: %b" no_cache

let config ~mode ~set_main ~set_no_cache =
  let* location, location_dot_filou, config = find_local_config mode in
  match set_main, set_no_cache with
    | None, None ->
        show_config location config;
        unit
    | _ ->
        match config with
          | Main ->
              failed [
                "cannot set main location";
                sf "%s is a main repository, not a clone repository"
                  (Device.show_location location)
              ]
          | Clone clone_config ->
              let clone_config =
                match set_main with
                  | None -> clone_config
                  | Some main_location -> { clone_config with main_location }
              in
              let clone_config =
                match set_no_cache with
                  | None -> clone_config
                  | Some no_cache -> { clone_config with no_cache }
              in
              write_config ~dot_filou: location_dot_filou (Clone clone_config)
