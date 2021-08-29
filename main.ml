open Misc

let error message =
  prerr_endline (String.concat ":\n  " ("Error" :: message));
  exit 1

let clap_bool =
  let parse s =
    match String.lowercase_ascii s with
      | "true" | "yes" | "t" | "y" | "1" -> Some true
      | "false" | "no" | "f" | "n" | "0" -> Some false
      | _ -> None
  in
  let show = function true -> "true" | false -> "false" in
  Clap.typ ~name: "bool" ~dummy: false ~parse ~show

let main () =
  Clap.description "FILe Organizer Ultimate";
  let repository =
    Clap.optional_string
      ~description:
        "Location of the repository to operate on. By default, Filou \
         uses the first parent directory that contains a .filou \
         subdirectory."
      ~long: "repository"
      ~short: 'R'
      ()
  in
  let no_main =
    Clap.flag
      ~description:
        "Only operate on the clone, not on the main repository. Only \
         operations that do not change the state of the repository can \
         be performed in this mode. This flag is useful to ensure that \
         you do not modify the state of the repository, or if the main \
         repository is not available. If the cache is not up-to-date, \
         output may not be up-to-date. If the cache is incomplete, \
         some operations may fail. If --repository is a main \
         repository, fail."
      ~set_long: "no-main"
      ~set_short: 'C'
      false
  in
  let no_cache =
    Clap.flag
      ~description:
        "Do not read or store metadata in the clone cache, always read \
         directly from the main repository. This flag is useful if \
         both the main repository and the clone are on a local device \
         and if you do not need to store cache metadata for later in \
         case the main repository becomes unavailable. Ignored if \
         --repository is a main repository.\n\
         \n\
         When used with command 'clone', this parameter is saved in \
         the configuration file and all operations on the clone will \
         be performed as if --no-cache was specified."
      ~set_long: "no-cache"
      ~set_short: 'M'
      false
  in
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
      ~description: "Use ANSI escape codes to colorize logs and display progress."
      ~set_long: "color"
      ~unset_long: "no-color"
      true
  in
  let yes =
    Clap.flag
      ~set_long: "yes"
      ~description: "Do not prompt for confirmation."
      false
  in
  if not color then Prout.not_a_tty ();
  let command =
    let tree_or_ls_args command =
      let max_depth =
        match command with
          | `tree ->
              Clap.optional_int
                ~long: "depth"
                ~short: 'r'
                ~description:
                  "Maximum depth to print. 0 prints only DIR itself and not its contents."
                ()
          | `ls ->
              Some (
                Clap.default_int
                  ~long: "depth"
                  ~short: 'r'
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
          ~set_short: 'D'
          ~description:
            "For files that have copies in other directories, print \
             the path of those copies."
          false
      in
      let full_dir_paths =
        Clap.flag
          ~set_long: "full-dir-paths"
          ~set_short: 'p'
          ~description: "Show full directory paths. Useful for large trees."
          false
      in
      let paths =
        Clap.list_string
          ~description:
            "Paths of the directories and/or files to display. If no \
             PATH is specified, display the current directory."
          ~placeholder: "PATH"
          ()
      in
      (
        paths, max_depth, only_main, only_dirs, print_size, print_file_count,
        print_duplicates, full_dir_paths
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
            ~description:
              "Location of the main repository to clone.\n\
               \n\
               Locations can be of the form:\n\
               - file://<path> (or just <path>)\n\
               - ssh+filou://[username@]hostname[:port]/<path>\n\
               \n\
               file:// locations are local to the current host. Note \
               that file://x means ./x, not /x. If you actually mean \
               /x, specify file:///x (or just /x).\n\
               \n\
               ssh+filou:// locations are remote locations accessed \
               through SSH. Filou must be available in the PATH of the \
               remote host, as it will executed with 'listen' to \
               execute remote commands. Note that ssh+filou://host/x \
               means ./x (in the remote host), not /x. If you actually \
               mean /x, specify ssh+filou://host//x."
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
          ~description: "List new files in the work directory and files that differ."
          "status"
        @@ fun () ->
        let paths =
          Clap.list_string
            ~description:
              "Paths of the files or directories to list. Directories \
               are listed recursively. If no PATH is specified, list \
               the current directory."
            ~placeholder: "PATH"
            ()
        in
        `status paths
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
          ~description:
            "Move files.\n\
             \n\
             Only metadata is modified. In the clone, files outside of \
             the .filou directory are not moved."
          "mv"
        @@ fun () ->
        let paths =
          Clap.list_string
            ~description:
              "Paths of the files to move, and target path.\n\
               \n\
               The last path of the list is the target path. Other \
               paths are source paths.\n\
               \n\
               If the target is a directory, or if there are several \
               sources, sources are moved into this directory. If \
               there is only one source and the target does not exist, \
               the behavior depends on whether the target path denotes \
               a directory (i.e. is '.' or ends with a directory \
               separator): if it denotes a directory, the source is \
               moved into this directory. Else, the source is renamed."
            ~placeholder: "PATH"
            ()
        in
        `mv paths
      );
      (
        Clap.case
          ~description:
            "Copy files.\n\
             \n\
             Same as 'mv' but keep sources. Just like 'mv', this only \
             modifies metadata. Like all duplicate files, data itself \
             is not duplicated: all copies will simply point to the \
             same object hash."
          "cp"
        @@ fun () ->
        let paths =
          Clap.list_string
            ~description:
              "Paths of the files to copy, and target path.\n\
               \n\
               See 'mv' for a more detailed description (in particular \
               regarding the importance of the trailing directory \
               separator in the target path)."
            ~placeholder: "PATH"
            ()
        in
        `cp paths
      );
      (
        Clap.case
          ~description: "Check for potential corruptions."
          "check"
        @@ fun () ->
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
        `check (hash_all, hash_metadata)
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
      (
        Clap.case
          ~description: "Show the difference between two states in the operation history."
          "diff"
        @@ fun () ->
        let before =
          Clap.default_int
            ~description:
              "Index in the undo list of the old state to compare \
               with. This corresponds to the number given by 'log'. In \
               particular it can be 0 to refer to the current \
               state. Can be negative to refer to a redo item."
            ~placeholder: "BEFORE"
            1
        in
        let after =
          Clap.default_int
            ~description:
              "Index in the undo list of the new state to compare, \
               using the same convention as BEFORE."
            ~placeholder: "AFTER"
            0
        in
        `diff (before, after)
      );
      (
        Clap.case
          ~description:
            "Listen for remote queries. This command causes Filou to \
             read queries on its standard input and to respond to them \
             on its standard output. You should usually not run this \
             yourself: Filou runs this command itself through SSH when \
             it needs to."
          "listen"
        @@ fun () ->
        `listen
      );
      (
        Clap.case
          ~description:
            "Output some statistics about the disk usage of metadata.\n\
             \n\
             Disk usage assumes a block size of 4096."
          "stats"
        @@ fun () ->
        `stats
      );
      (
        Clap.case
          ~description: "Show an object. Mostly useful for debugging."
          "show"
        @@ fun () ->
        let obj =
          Clap.default_string
            ~description:
              "Object to show. Can be an object hash or an alias: \
               'journal', 'hash_index' or 'root_dir'."
            ~placeholder: "OBJECT"
            "journal"
        in
        `show obj
      );
      (
        Clap.case
          ~description:
            "With no arguments, show the configuration. With \
             arguments, update the configuration."
          "config"
        @@ fun () ->
        let config_command =
          Clap.subcommand [
            (
              Clap.case
                ~description: "Show the configuration."
                "show"
              @@ fun () ->
              `show
            );
            (
              Clap.case
                ~description: "Modify the configuration."
                "set"
              @@ fun () ->
              let value_to_set =
                Clap.subcommand [
                  (
                    Clap.case
                      ~description: "Set the location of the main repository."
                      "main"
                    @@ fun () ->
                    let value =
                      Clap.mandatory_string
                        ~description: "New location of the main repository."
                        ~placeholder: "MAIN"
                        ()
                    in
                    `main value
                  );
                  (
                    Clap.case
                      ~description: "Set no-cache mode (see --no-cache)."
                      "no-cache"
                    @@ fun () ->
                    let value =
                      Clap.mandatory clap_bool
                        ~description:
                          "New value for the no-cache option. 'true' \
                           sets no-cache mode, 'false' unsets it."
                        ~placeholder: "VALUE"
                        ()
                    in
                    `no_cache value
                  );
                ]
              in
              `set value_to_set
            );
            (
              Clap.case
                ~description: "Add filters to the ignore list."
                "ignore"
              @@ fun () ->
              let filters =
                Clap.list_string
                  ~description:
                    "Filter to add to the ignore list. Its format \
                     should be a Perl-compatible regular expression \
                     (PCRE). If the path of a file or a directory in \
                     the work directory matches a filter in the ignore \
                     list, this file or directory is ignored by \
                     'push'. For this purpose, paths always start with \
                     a '\\' (denoting the root of the work \
                     directory). Additionally, paths to directories \
                     end with a '\\' while file paths do not.\n\
                     \n\
                     Examples (regular expressions are quoted using \
                     shell syntax):\n\
                     - '\\.log$': ignore all files ending with \
                     .log;\n\
                     - '/_build/$': ignore all directories named \
                     '_build';\n\
                     - '^/.git/$': ignore a Git directory at the \
                     root;\n\
                     - '/test/?$': ignore all files and directories \
                     named 'test'."
                  ~placeholder: "FILTER"
                  ()
              in
              `set (`add_ignore filters)
            );
            (
              Clap.case
                ~description: "Remove filters to the ignore list."
                "unignore"
              @@ fun () ->
              let filters =
                Clap.list_string
                  ~description: "Filter to remove from the ignore list."
                  ~placeholder: "FILTER"
                  ()
              in
              `set (`remove_ignore filters)
            );
          ]
        in
        `config config_command
      );
    ]
  in
  Clap.close ();
  (* Belt and suspenders:
     - set device to RO even though [Repository] will not even try to write;
     - set both [Bare] and [Repository] to RO even though [Repository] calls [Bare]. *)
  if dry_run then (
    State.Repo.set_read_only ();
  );
  let device_mode = if dry_run then Device.RO else RW in
  let parse_location = Device.parse_location device_mode in
  let with_setup f =
    let* setup = Controller.find_setup ~repository ~no_main ~no_cache device_mode in
    let result = f setup in
    Cash.save setup;
    result
  in
  let parse_local_path_with_kind (setup: Setup.t) string =
    match setup.workdir with
      | None ->
          failed [ "cannot do that with no work directory" ]
      | Some (Local (_, root)) ->
          Device.parse_local_path root string
      | Some (SSH_filou _) ->
          failed [ "cannot operate on the work directory of remote clone repositories" ]
  in
  let parse_local_path setup string =
    (* TODO: some operations in Controller will use the resulting path
       as a file if the file exists; if the user explicitely added a /
       at the end, maybe we should fail instead? *)
    let* path = parse_local_path_with_kind setup string in
    match path with
      | Dir path -> ok path
      | File path -> ok (Device.path_of_file_path path)
  in
  match
    match command with
      | `init location ->
          let* location = parse_location location in
          Controller.init location
      | `clone (main_location, clone_location) ->
          let* main_location = parse_location main_location in
          let* clone_location = parse_location clone_location in
          Controller.clone ~no_cache ~main_location ~clone_location
      | `tree (
          paths, max_depth, only_main, only_dirs, print_size, print_file_count,
          print_duplicates, full_dir_paths
        ) ->
          with_setup @@ fun setup ->
          let paths =
            match paths with
              | [] -> [ "." ]
              | _ -> paths
          in
          let* paths = list_map_e paths (parse_local_path setup) in
          Controller.tree ~color ~max_depth ~only_main ~only_dirs ~print_size
            ~print_file_count ~print_duplicates ~full_dir_paths setup paths
      | `status paths ->
          with_setup @@ fun setup ->
          let paths =
            match paths with
              | [] -> [ "." ]
              | _ -> paths
          in
          let* paths = list_map_e paths (parse_local_path setup) in
          Controller.status ~color ~verbose setup paths
      | `push paths ->
          with_setup @@ fun setup ->
          let paths =
            match paths with
              | [] -> [ "." ]
              | _ -> paths
          in
          let* paths = list_map_e paths (parse_local_path setup) in
          Controller.push ~verbose ~yes setup paths
      | `pull paths ->
          with_setup @@ fun setup ->
          let paths =
            match paths with
              | [] -> [ "." ]
              | _ -> paths
          in
          let* paths = list_map_e paths (parse_local_path setup) in
          Controller.pull ~verbose setup paths
      | `rm (paths, recursive) ->
          with_setup @@ fun setup ->
          let* paths = list_map_e paths (parse_local_path setup) in
          Controller.remove ~recursive ~yes setup paths
      | `mv paths | `cp paths as command ->
          let* source_paths, target_path =
            match List.rev paths with
              | [] ->
                  failed [ "missing source and target paths" ]
              | [ _ ] ->
                  failed [ "missing target path" ]
              | head :: tail ->
                  ok (List.rev tail, head)
          in
          with_setup @@ fun setup ->
          let* source_paths = list_map_e source_paths (parse_local_path setup) in
          let* target_path = parse_local_path_with_kind setup target_path in
          let action =
            match command with
              | `mv _ -> Controller.Move
              | `cp _ -> Controller.Copy
          in
          Controller.copy_or_move_files action ~verbose ~yes setup source_paths target_path
      | `check (hash_all, hash_metadata) ->
          with_setup @@ fun setup ->
          let hash =
            if hash_all then `all else
            if hash_metadata then `metadata else
              `no
          in
          Controller.check ~hash setup
      | `update ->
          with_setup @@ fun setup ->
          Controller.update setup
      | `prune (yes, keep_undo, keep_redo, keep) ->
          let keep_undo = max keep_undo keep in
          let keep_redo = max keep_redo keep in
          with_setup @@ fun setup ->
          Controller.prune ~yes ~keep_undo ~keep_redo setup
      | `log ->
          with_setup @@ fun setup ->
          Controller.log setup
      | `undo count ->
          with_setup @@ fun setup ->
          Controller.undo setup ~count
      | `redo count ->
          with_setup @@ fun setup ->
          Controller.undo setup ~count: (- count)
      | `diff (before, after) ->
          with_setup @@ fun setup ->
          Controller.diff ~color setup ~before ~after
      | `listen ->
          Listen.run ()
      | `stats ->
          with_setup @@ fun setup ->
          Controller.stats setup
      | `show obj ->
          with_setup @@ fun setup ->
          Controller.show setup obj
      | `config `show ->
          Controller.config_show ~mode: device_mode
      | `config (`set value) ->
          Controller.config_set ~mode: device_mode value
  with
    | OK () ->
        Prout.clear_progress ()
    | ERROR { msg; _ } ->
        Prout.clear_progress ();
        error msg

let () =
  Printexc.record_backtrace true;
  try main () with exn ->
    Prout.clear_progress ();
    Printexc.print_backtrace stderr;
    error [ "uncaught exception"; Printexc.to_string exn ]
