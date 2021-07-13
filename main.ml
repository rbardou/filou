open Misc

let error message =
  prerr_endline (String.concat ":\n  " ("Error" :: message));
  exit 1

let () =
  Clap.description "FILe Organizer Ultimate";
  let verbose =
    Clap.flag
      ~description: "Log more information."
      ~set_long: "verbose"
      ~set_short: 'v'
      false
  in
  let dry_run =
    Clap.flag
      ~description: "Read-only mode: do not write anything, only pretend."
      ~set_long: "dry-run"
      false
  in
  let color =
    Clap.flag
      ~description: "Use ANSI escape codes to colorize logs."
      ~set_long: "color"
      ~unset_long: "no-color"
      true
  in
  if not color then Progress_bar.disable ();
  let command =
    let tree_or_ls_args command =
      let max_depth =
        match command with
          | `tree ->
              Clap.optional_int
                ~long: "depth"
                ~short: 'D'
                ~description:
                  "Maximum depth to print. 0 prints only DIR itself and not its contents."
                ()
          | `ls ->
              Some (
                Clap.default_int
                  ~long: "depth"
                  ~short: 'D'
                  ~description:
                    "Maximum depth to print. 0 prints only DIR itself and not its contents."
                  1
              )
      in
      let only_main =
        Clap.flag
          ~set_long: "only-main"
          ~set_short: 'm'
          ~description: "Only print what is available in the main repository."
          false
      in
      let only_dirs =
        Clap.flag
          ~set_long: "only-dirs"
          ~set_short: 'd'
          ~description: "Only print directories, not files."
          false
      in
      let print_size =
        Clap.flag
          ~set_long: "size"
          ~set_short: 's'
          ~description: "Print file and directory sizes."
          false
      in
      let print_file_count =
        Clap.flag
          ~set_long: "count"
          ~set_short: 'c'
          ~description: "Print file count for directories."
          false
      in
      let print_duplicates =
        Clap.flag
          ~set_long: "duplicates"
          ~set_short: 'p'
          ~description:
            "For files that have copies in other directories, print \
             the total number of copies."
          false
      in
      let cache =
        Clap.flag
          ~set_long: "cache"
          ~set_short: 'C'
          ~description:
            "Only read from the clone repository cache, not the main \
             repository. Output may not be up-to-date."
          false
      in
      let path =
        Clap.default_string
          ~description: "Path to the directory to print."
          ~placeholder: "DIR"
          "."
      in
      (
        path, max_depth, only_main, only_dirs, print_size, print_file_count,
        print_duplicates, cache
      )
    in
    Clap.subcommand [
      (
        Clap.case
          ~description: "Initialize a main repository."
          "init"
        @@ fun () ->
        let location =
          Clap.default_string
            ~description:
              "Location of the repository to create. If it exists, it \
               must be empty."
            ~placeholder: "LOCATION"
            "."
        in
        `init location
      );
      (
        Clap.case
          ~description: "Create a clone repository from a main repository."
          "clone"
        @@ fun () ->
        let main_location =
          Clap.mandatory_string
            ~description: "Location of the main repository to clone."
            ~placeholder: "MAIN_LOCATION"
            ()
        in
        let clone_location =
          Clap.default_string
            ~description: "Location of the repository to create."
            ~placeholder: "CLONE_LOCATION"
            "."
        in
        `clone (main_location, clone_location)
      );
      (
        Clap.case
          ~description: "Print the directory tree."
          "tree"
        @@ fun () ->
        `tree (tree_or_ls_args `tree)
      );
      (
        Clap.case
          ~description:
            "Print the contents of a directory. Same as tree, but with \
             a default --depth of 1."
          "ls"
        @@ fun () ->
        `tree (tree_or_ls_args `ls)
      );
      (
        Clap.case
          ~description: "Copy files from a clone repository to a main repository."
          "push"
        @@ fun () ->
        let paths =
          Clap.list_string
            ~description:
              "Paths of the files or directories to push. Directories \
               are pushed recursively. If no PATH is specified, push \
               the current directory."
            ~placeholder: "PATH"
            ()
        in
        `push paths
      );
      (
        Clap.case
          ~description: "Copy files from a main repository to a clone repository."
          "pull"
        @@ fun () ->
        let paths =
          Clap.list_string
            ~description:
              "Paths of the files or directories to pull. Directories \
               are pushed recursively. If no PATH is specified, pull \
               the current directory."
            ~placeholder: "PATH"
            ()
        in
        `pull paths
      );
      (
        Clap.case
          ~description:
            "Remove files.\n\
             \n\
             Only metadata is removed. Files are still present in the \
             main repository but are unreachable and will be deleted \
             by 'prune'. In the clone, no file will be deleted outside \
             of the .filou directory."
          "rm"
        @@ fun () ->
        let recursive =
          Clap.flag
            ~description: "If a PATH is a directory, remove it recursively."
            ~set_long: "recursive"
            ~set_short: 'r'
            false
        in
        let paths =
          Clap.list_string
            ~description: "Paths of the files to remove."
            ~placeholder: "PATH"
            ()
        in
        `rm (paths, recursive)
      );
      (
        Clap.case
          ~description: "Check for potential corruptions."
          "check"
        @@ fun () ->
        let cache =
          Clap.flag
            ~set_long: "cache"
            ~set_short: 'C'
            ~description:
              "Only read from the clone repository cache, not the main \
               repository. If some objects are not available in the \
               cache, this will result in an error, but it does not \
               mean that the repository is corrupted, only that the \
               cache is partial. File sizes will not be checked."
            false
        in
        let hash_all =
          Clap.flag
            ~set_long: "hash"
            ~set_short: 'h'
            ~description:
              "Check hashes of all objects, including file \
               objects. Implies --metadata-hash."
            false
        in
        let hash_metadata =
          Clap.flag
            ~set_long: "metadata-hash"
            ~set_short: 'm'
            ~description: "Check hashes of metadata objects (but not file objects)."
            false
        in
        `check (cache, hash_all, hash_metadata)
      );
      (
        Clap.case
          ~description:
            "Update the clone cache.\n\
             \n\
             This copies all reachable non-file objects from the main \
             repository to the .filou directory of the clone. This \
             makes sure that read-only operations such as 'tree' have \
             up-to-date information when used with --cache."
          "update"
        @@ fun () ->
        `update
      );
      (
        Clap.case
          ~description:
            "Clear operation history, keeping only the current \
             state.\n\
             \n\
             THIS OPERATION CANNOT BE UNDONE. After 'prune', the undo \
             list and the redo list will be empty.\n\
             \n\
             Pruning deletes files deleted with 'rm' and can thus be \
             used to gain space. Pruning doesn't delete files in the \
             work directory, it only deletes objects in the main \
             repository and in the .filou directory of the clone."
          "prune"
        @@ fun () ->
        let yes =
          Clap.flag
            ~set_long: "yes"
            ~description: "Do not prompt for confirmation."
            false
        in
        let keep_undo =
          Clap.default_int
            ~long: "keep-undo"
            ~placeholder: "COUNT"
            ~description:
              "Keep COUNT items from the undo list. If the undo list \
               contains less than COUNT items, keep all of them."
            0
        in
        let keep_redo =
          Clap.default_int
            ~long: "keep-redo"
            ~placeholder: "COUNT"
            ~description:
              "Keep COUNT items from the redo list. If the redo list \
               contains less than COUNT items, keep all of them."
            0
        in
        let keep =
          Clap.default_int
            ~placeholder: "COUNT"
            ~description:
              "Keep COUNT items from the redo and the undo list. If \
               --keep-undo or --keep-redo are also specified, the \
               highest value is used."
            0
        in
        `prune (yes, keep_undo, keep_redo, keep)
      );
      (
        Clap.case
          ~description: "Show operation history."
          "log"
        @@ fun () ->
        `log
      );
      (
        Clap.case
          ~description:
            "Undo last operations.\n\
             \n\
             'undo' itself can be undone with 'redo', but only until a \
             new operation is done, at which point the redo list is \
             cleared."
          "undo"
        @@ fun () ->
        let count =
          Clap.default_int
            ~description:
              "How many operations to undo. If 0, do nothing. If \
               negative, redo instead.\n\
               \n\
               Use 'log' to see the redo-list and undo-list. It \
               displays the value to give to COUNT next to each entry \
               to restore them using 'undo'."
            ~placeholder: "COUNT"
            1
        in
        `undo count
      );
      (
        Clap.case
          ~description:
            "Redo operations that were undone with 'undo'.\n\
             \n\
             'redo' is equivalent to 'undo -1' and 'redo N' is \
             equivalent to 'undo -N'."
          "redo"
        @@ fun () ->
        let count =
          Clap.default_int
            ~description:
              "How many operations to redo. If 0, do nothing. If \
               negative, undo instead.\n\
               \n\
               Use 'log' to see the redo-list and undo-list. It \
               displays the value to give to COUNT next to each entry \
               to restore them using 'undo'. Negate this value to give \
               it to 'redo'."
            ~placeholder: "COUNT"
            1
        in
        `redo count
      );
    ]
  in
  Clap.close ();
  (* Belt and suspenders:
     - set device to RO even though [Repository] will not even try to write;
     - set both [Bare] and [Repository] to RO even though [Repository] calls [Bare]. *)
  if dry_run then (
    State.Bare.set_read_only ();
    State.Repository.set_read_only ();
  );
  let device_mode = if dry_run then Device.RO else RW in
  let parse_location = Device.parse_location device_mode in
  let find_local_clone () = Controller.find_local_clone device_mode in
  match
    match command with
      | `init location ->
          let* location = parse_location location in
          Controller.init location
      | `clone (main_location, clone_location) ->
          let* main_location = parse_location main_location in
          let* clone_location = parse_location clone_location in
          Controller.clone ~main_location ~clone_location
      | `tree (
          path, max_depth, only_main, only_dirs, print_size, print_file_count,
          print_duplicates, cache
        ) ->
          let* setup = find_local_clone ~clone_only: cache () in
          let* path = Device.parse_path (Clone.clone setup) path in
          Controller.tree ~color ~max_depth ~only_main ~only_dirs ~print_size
            ~print_file_count ~print_duplicates setup path
      | `push paths ->
          let* setup = find_local_clone ~clone_only: false () in
          let paths =
            match paths with
              | [] -> [ "." ]
              | _ -> paths
          in
          let* paths = list_map_e paths (Device.parse_path (Clone.clone setup)) in
          Controller.push ~verbose setup paths
      | `pull paths ->
          let* setup = find_local_clone ~clone_only: false () in
          let paths =
            match paths with
              | [] -> [ "." ]
              | _ -> paths
          in
          let* paths = list_map_e paths (Device.parse_path (Clone.clone setup)) in
          Controller.pull ~verbose setup paths
      | `rm (paths, recursive) ->
          (* TODO: show progress "Deleted X files." *)
          let* setup = find_local_clone ~clone_only: false () in
          let* paths = list_map_e paths (Device.parse_path (Clone.clone setup)) in
          Controller.remove ~recursive setup paths
      | `check (cache, hash_all, hash_metadata) ->
          let* setup = find_local_clone ~clone_only: cache () in
          let hash =
            if hash_all then `all else
            if hash_metadata then `metadata else
              `no
          in
          Controller.check ~clone_only: cache ~hash setup
      | `update ->
          let* setup = find_local_clone ~clone_only: false () in
          Controller.update setup
      | `prune (yes, keep_undo, keep_redo, keep) ->
          let keep_undo = max keep_undo keep in
          let keep_redo = max keep_redo keep in
          let* setup = find_local_clone ~clone_only: false () in
          Controller.prune ~yes ~keep_undo ~keep_redo setup
      | `log ->
          let* setup = find_local_clone ~clone_only: false () in
          Controller.log setup
      | `undo count ->
          let* setup = find_local_clone ~clone_only: false () in
          Controller.undo setup ~count
      | `redo count ->
          let* setup = find_local_clone ~clone_only: false () in
          Controller.undo setup ~count: (- count)
  with
    | OK () ->
        ()
    | ERROR { msg; _ } ->
        error msg
