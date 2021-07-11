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
        `check cache
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
            "Delete unreachable objects. This can take a while.\n\
             \n\
             Unreachable objects are no longer relevant for the \
             repository. They can be old metadata or actual files that \
             were removed using 'rm'. Unreachable objects can safely \
             be deleted if 'tree' shows you all the files that are \
             supposed to be here, even if you did not pull all of them."
          "prune"
        @@ fun () ->
        `prune
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
          (* TODO: modify Clap so that it parses negative numbers as numbers
             and not as option names? (then search for \\\\ here to fix the doc) *)
          Clap.default_int
            ~description:
              "How many operations to undo. If 0, do nothing. If \
               negative, redo instead.\n\
               \n\
               The minus sign in front of negative numbers must be \
               escaped using a backslack, which itself must be quoted \
               or escaped in most shells. For instance, write \\\\-1 \
               or '\\-1'. It may be easier to use 'redo' instead \
               though.\n\
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
             'redo' is equivalent to 'undo \\\\-1' and 'redo N' is \
             equivalent to 'undo \\\\-N'."
          "redo"
        @@ fun () ->
        let count =
          Clap.default_int
            ~description:
              "How many operations to redo. If 0, do nothing. If \
               negative, undo instead.\n\
               \n\
               The minus sign in front of negative numbers must be \
               escaped using a backslack, which itself must be quoted \
               or escaped in most shells. For instance, write \\\\-1 \
               or '\\-1'. It may be easier to use 'redo' instead \
               though.\n\
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
      | `check cache ->
          (* TODO: --metadata-hash for reachable non-file hashes and --hash for all hashes *)
          let* setup = find_local_clone ~clone_only: cache () in
          Controller.check ~clone_only: cache setup
      | `update ->
          let* setup = find_local_clone ~clone_only: false () in
          Controller.update setup
      | `prune ->
          (* TODO: show progress "Deleted X objects totalling X bytes." *)
          (* TODO: be able to prune the clone cache *)
          (* TODO: --cache could make sense too here *)
          let* setup = find_local_clone ~clone_only: false () in
          Controller.prune setup
      | `log ->
          assert false (* TODO *)
      | `undo _count ->
          assert false (* TODO *)
      | `redo _count ->
          assert false (* TODO *)
  with
    | OK () ->
        ()
    | ERROR { msg; _ } ->
        error msg
