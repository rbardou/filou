open Misc

let error message =
  prerr_endline (String.concat ":\n  " ("Error" :: message));
  exit 1

let () =
  Clap.description "FILe Organizer Ultimate";
(*   let verbose = *)
(*     Clap.flag *)
(*       ~description: "Log more information." *)
(*       ~set_long: "verbose" *)
(*       ~set_short: 'v' *)
(*       false *)
(*   in *)
(*   let dry_run = *)
(*     Clap.flag *)
(*       ~description: "Read-only mode: do not actually push or download files, only pretend." *)
(*       ~set_long: "dry-run" *)
(*       false *)
(*   in *)
  let color =
    Clap.flag
      ~description: "Use ANSI escape codes to colorize logs."
      ~set_long: "color"
      ~unset_long: "no-color"
      true
  in
  if not color then Progress_bar.disable ();
  let command =
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
            "List files that are currently available in a clone \
             repository and files that are also available in the main \
             repository."
          "ls"
        @@ fun () ->
        let path =
          Clap.default_string
            ~description: "Path to the directory to list."
            ~placeholder: "DIR"
            "."
        in
        `ls path
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
  match
    match command with
      | `init location ->
          let* location = Device.parse_location location in
          Controller.init location
      | `clone (main_location, clone_location) ->
          let* main_location = Device.parse_location main_location in
          let* clone_location = Device.parse_location clone_location in
          Controller.clone ~main_location ~clone_location
      | `push paths ->
          let* setup = Controller.find_local_clone () in
          let paths =
            match paths with
              | [] -> [ "." ]
              | _ -> paths
          in
          let* paths = list_map_e paths (Device.parse_path (Clone.clone setup)) in
          (* TODO: output stuff *)
          Controller.push setup paths
      | `pull _paths ->
(*           let* (location, _) as clone = Controller.find_local_clone () in *)
(*           let paths = *)
(*             match paths with *)
(*               | [] -> [ "." ] *)
(*               | _ -> paths *)
(*           in *)
(*           let* paths = list_map_e paths (Device.parse_path location) in *)
(*           Controller.pull ~verbose ~dry_run ~clone paths *)
          assert false (* TODO *)
      | `ls _path ->
(*           let* (location, _) as clone = Controller.find_local_clone () in *)
(*           let* path = Device.parse_path location path in *)
(*           Controller.list ~color ~clone path *)
          assert false (* TODO *)
      | `rm _paths ->
(*           let* (location, _) as clone = Controller.find_local_clone () in *)
(*           let* paths = list_map_e paths (Device.parse_file_path location) in *)
(*           Controller.remove ~verbose ~dry_run ~clone paths *)
          assert false (* TODO *)
      | `check location ->
          let* location = Device.parse_location location in
          Controller.check location
  with
    | OK () ->
        ()
    | ERROR { msg; _ } ->
        error msg
