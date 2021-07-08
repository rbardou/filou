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
      let path =
        Clap.default_string
          ~description: "Path to the directory to print."
          ~placeholder: "DIR"
          "."
      in
      (
        path, max_depth, only_main, only_dirs, print_size, print_file_count,
        print_duplicates
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
            "Remove files from the main repository. Files are not \
             removed from the clone repository if they have been \
             pulled. If other files exist with the same content, only \
             the specified paths are removed. The content is only \
             removed if no path lead to it."
          "rm"
        @@ fun () ->
        let paths =
          Clap.list_string
            ~description: "Paths of the files to remove."
            ~placeholder: "DIR"
            ()
        in
        `rm paths
      );
      (
        Clap.case
          ~description: "Check for potential corruptions."
          "check"
        @@ fun () ->
        let location =
          Clap.default_string
            ~description: "Location of the repository to check."
            ~placeholder: "LOCATION"
            "."
        in
        `check location
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
          print_duplicates
        ) ->
          let* setup = find_local_clone () in
          let* path = Device.parse_path (Clone.clone setup) path in
          Controller.tree ~color ~max_depth ~only_main ~only_dirs ~print_size
            ~print_file_count ~print_duplicates setup path
      | `push paths ->
          let* setup = find_local_clone () in
          let paths =
            match paths with
              | [] -> [ "." ]
              | _ -> paths
          in
          let* paths = list_map_e paths (Device.parse_path (Clone.clone setup)) in
          Controller.push ~verbose setup paths
      | `pull paths ->
          let* setup = find_local_clone () in
          let paths =
            match paths with
              | [] -> [ "." ]
              | _ -> paths
          in
          let* paths = list_map_e paths (Device.parse_path (Clone.clone setup)) in
          Controller.pull ~verbose setup paths
      | `rm _paths ->
          (*           let* (location, _) as clone = find_local_clone () in *)
          (*           let* paths = list_map_e paths (Device.parse_file_path location) in *)
          (*           Controller.remove ~verbose ~dry_run ~clone paths *)
          assert false (* TODO *)
      | `check location ->
          let* location = parse_location location in
          Controller.check location
      | `prune ->
          assert false (* TODO *)
      | `log ->
          assert false (* TODO *)
      | `undo ->
          assert false (* TODO *)
      | `redo ->
          assert false (* TODO *)
  with
    | OK () ->
        ()
    | ERROR { msg; _ } ->
        error msg
