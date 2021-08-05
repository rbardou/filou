open Mysc

let quiet = ref false

let with_quiet f =
  let old_quiet = !quiet in
  quiet := true;
  match f () with
    | exception exn ->
        quiet := old_quiet;
        raise exn
    | x ->
        quiet := old_quiet;
        x

let print_string s = if not !quiet then print_string s
let print_char c = if not !quiet then print_char c
let print_newline () = if not !quiet then print_newline ()
let print_endline s = if not !quiet then print_endline s
let echo x = Printf.ksprintf print_endline x

let () =
  Clap.description "Run Filou tests"

let filou_exe =
  Clap.default_string
    ~long: "filou"
    ~description: "Location of the Filou executable."
    ~placeholder: "PATH"
    "/tmp/filou-test/filou"

let main =
  Clap.default_string
    ~long: "main"
    ~description: "Where to put the main repository."
    ~placeholder: "PATH"
    "/tmp/filou-test/main"

let clone =
  Clap.default_string
    ~long: "clone"
    ~description: "Where to put the clone repository."
    ~placeholder: "PATH"
    "/tmp/filou-test/clone"

let clone2 =
  Clap.default_string
    ~long: "clone2"
    ~description: "Where to put the second clone repository."
    ~placeholder: "PATH"
    "/tmp/filou-test/clone2"

let () =
  Clap.close ()

let initial_current_directory = Sys.getcwd ()
let current_directory = ref initial_current_directory

let cd path =
  let path =
    if Filename.is_relative path then
      initial_current_directory // path
    else
      path
  in
  if !current_directory <> path then (
    echo "$ cd %s" path;
    Sys.chdir path
  )

let comment s =
  echo "# %s" s

let time name f =
  let start = Unix.gettimeofday () in
  f ();
  let time = Unix.gettimeofday () -. start in
  echo "time(%s) = %gs" name time

let cmd executable args =
  (* Print command. *)
  print_string "$ ";
  print_string (Filename.quote_if_needed executable);
  List.iter' args (fun arg -> print_char ' '; print_string (Filename.quote_if_needed arg));
  print_newline ();
  (* Execute command. Filou doesn't use stderr for now so we redirect stderr to stdout. *)
  match
    let pid =
      Unix.create_process executable (Array.of_list (executable :: args))
        Unix.stdin Unix.stdout Unix.stdout
    in
    snd (Unix.waitpid [] pid)
  with
    | exception Unix.Unix_error (error, _, _) ->
        echo "Failed to execute: %s" (Unix.error_message error)
    | WEXITED 0 -> ()
    | WEXITED n -> echo "Exit code: %d" n
    | WSIGNALED n -> echo "Killed by signal: %d" n
    | WSTOPPED n -> echo "Stopped by signal: %d" n

let create_file path contents =
  echo "$ echo %s > %s" (Filename.quote_if_needed contents) (Filename.quote_if_needed path);
  let ch = open_out path in
  output_string ch contents;
  close_out ch

let rm path =
  echo "$ rm %s" (Filename.quote_if_needed path);
  Unix.unlink path

let rm_rf path =
  if Filename.is_relative path then
    invalid_arg "rm_rf: path must be absolute to avoid mistakes";
  cmd "rm" [ "-r"; "-f"; path ]

let mv a b =
  echo "$ mv %s %s" (Filename.quote_if_needed a) (Filename.quote_if_needed b);
  Unix.rename a b

let mkdir path =
  echo "$ mkdir %s" (Filename.quote_if_needed path);
  Unix.mkdir path 0o750

let mkdir_p path =
  echo "$ mkdir -p %s" (Filename.quote_if_needed path);
  let rec create path =
    if not (Sys.file_exists path) then (
      create (Filename.dirname path);
      mkdir path;
    )
  in
  create path

let rmdir path =
  echo "$ rmdir %s" (Filename.quote_if_needed path);
  Unix.rmdir path

let cat path =
  echo "$ cat %s" (Filename.quote_if_needed path);
  let contents = read_file path in
  if String.length contents > 0 && contents.[String.length contents - 1] = '\n' then
    print_string contents
  else
    echo "%s%%" contents

let list_of_option ?name =
  function
    | None -> []
    | Some x ->
        match name with
          | None -> [ x ]
          | Some name -> [ name; x ]

let flag value string = if value then [ string ] else []

let v = true
let dry_run = true
let no_color = true

module Make_filou (P: sig val path: string option end) =
struct
  let run ?(v = false) ?(dry_run = false) ?(color = false) args =
    Option.iter cd P.path;
    let flags =
      flag v "-v" @
      flag dry_run "--dry-run" @
      flag (not color) "--no-color"
    in
    cmd filou_exe (flags @ args)

  let init ?v ?dry_run ?color ?main () =
    run ?v ?dry_run ?color ("init" :: list_of_option main)

  let clone ?v ?dry_run ?color ?main ?clone () =
    run ?v ?dry_run ?color ("clone" :: list_of_option main @ list_of_option clone)

  let push ?(v = true) ?dry_run ?color paths =
    run ~v ?dry_run ?color ("push" :: paths)

  let pull ?(v = true) ?dry_run ?color paths =
    run ~v ?dry_run ?color ("pull" :: paths)

  let ls ?v ?dry_run ?color ?path () =
    run ?v ?dry_run ?color ("ls" :: list_of_option path)

  let rm ?v ?dry_run ?color ?(r = false) paths =
    run ?v ?dry_run ?color ("rm" :: flag r "-r" @ paths)

  let check ?v ?dry_run ?color ?path ?(cache = false) () =
    run ?v ?dry_run ?color ("check" :: flag cache "--cache" @ list_of_option path)

  let tree ?v ?dry_run ?color ?depth ?(only_dirs = false) ?(size = false) ?(count = false)
      ?(duplicates = false) ?(cache = false) ?path () =
    run ?v ?dry_run ?color (
      "tree"
      :: list_of_option ~name: "--depth" (Option.map string_of_int depth)
      @ list_of_option path
      @ flag only_dirs "--only-dirs"
      @ flag size "--size"
      @ flag count "--count"
      @ flag duplicates "--duplicates"
      @ flag cache "--cache"
    )

  let prune ?v ?dry_run ?color () =
    run ?v ?dry_run ?color [ "prune"; "--yes" ]

  let update ?v ?dry_run ?color () =
    run ?v ?dry_run ?color [ "update" ]

  let log ?v ?dry_run ?color () =
    run ?v ?dry_run ?color [ "log" ]

  let diff ?v ?dry_run ?color ?before ?after () =
    if after <> None && before = None then invalid_arg "diff";
    run ?v ?dry_run ?color (
      "diff"
      :: list_of_option (Option.map string_of_int before)
      @ list_of_option (Option.map string_of_int after)
    )

  let undo ?v ?dry_run ?color ?count () =
    run ?v ?dry_run ?color ("undo" :: list_of_option (Option.map string_of_int count))

  let redo ?v ?dry_run ?color ?count () =
    run ?v ?dry_run ?color ("redo" :: list_of_option (Option.map string_of_int count))

  let stats ?v ?dry_run ?color () =
    run ?v ?dry_run ?color [ "stats" ]

  let config ?v ?dry_run ?color ?main () =
    run ?v ?dry_run ?color ("config" :: list_of_option ~name: "--main" main)

  let show ?v ?dry_run ?color ?what ?r () =
    run ?v ?dry_run ?color (
      "show" ::
      list_of_option what @
      list_of_option ~name: "-r" (Option.map string_of_int r)
    )
end

module Main = Make_filou (struct let path = Some main end)
module Clone = Make_filou (struct let path = Some clone end)
module Clone2 = Make_filou (struct let path = Some clone2 end)
module Filou = Make_filou (struct let path = None end)

let tree () = cmd "tree" [ "-a"; "-s"; "-F" ]

let main_tree () =
  cd main;
  tree ()

let clone_tree () =
  cd clone;
  tree ()

(* let trees () = *)
(*   main_tree (); *)
(*   clone_tree () *)

(* let hash path = cmd "sha256sum" [ path ] *)

let explore path =
  echo "$ explore %s" path;
  let contents = read_file path in
  Format.fprintf Format.str_formatter "%a@." Protype_robin.Explore.pp_string contents;
  let string = Format.flush_str_formatter () in
  print_string string;
  string

(* let explore_main path = *)
(*   cd main; *)
(*   explore path *)

let explore_clone path =
  cd clone;
  explore (".filou" // path)

(* let explore_hash hash = *)
(*   explore (String.sub hash 0 2 // String.sub hash 2 2 // hash) *)

(* let explore_main_hash hash = *)
(*   cd main; *)
(*   explore_hash hash *)

(* let explore_clone_hash hash = *)
(*   cd clone; *)
(*   explore_hash hash *)

let rec find_files ?(acc = Set.String.empty) path =
  if not (Sys.is_directory path) then
    Set.String.add path acc
  else
    let contents = Sys.readdir path |> Array.to_list in
    List.fold_left' acc contents @@ fun acc filename ->
    find_files ~acc (path // filename)

let diff_string_sets a b =
  Set.String.diff a b |> Set.String.iter (echo "- %s");
  Set.String.diff b a |> Set.String.iter (echo "+ %s")

let (=~*) str rex =
  match Rex.(str =~ perl _1 rex) with
    | None ->
        echo "%S does not match %S" str rex;
        exit 1
    | Some x ->
        x

let dot_filou_hash hash =
  ".filou" // String.sub hash 0 2 // String.sub hash 2 2 // hash

let small_repo () =
  comment "Initialize a main repository and a clone.";
  Filou.init ~main ();
  Filou.clone ~main ~clone ();
  let _ = explore_clone "config" in
  Clone.check ();
  Clone.tree ();
  Clone.log ();

  comment "Play with the configuration.";
  Filou.config ();
  Filou.config ~main: "/tmp" ();
  Filou.config ();
  Filou.config ~main ();
  Filou.config ();

  comment "Add a file.";
  cd clone;
  create_file "toto" "blablabla";
  Clone.push [ "toto" ];
  Clone.check ();
  Clone.tree ();
  Clone.log ();
  Clone.diff ();

  comment "Add the file again.";
  Clone.push [];
  Clone.check ();
  Clone.tree ();
  Clone.log ();
  Clone.diff ();

  comment "Add several files.";
  cd clone;
  create_file "tutu" "blu";
  create_file "titi" "blibli";
  Clone.push [ "tutu"; "titi" ];
  Clone.check ();
  Clone.tree ();
  Clone.log ();
  Clone.diff ();

  comment "Add files in subdirectories.";
  cd clone;
  mkdir "bla";
  mkdir "bla/bli";
  create_file "bla/bli/plouf" "plaf";
  mkdir "bla/blo";
  create_file "bla/blo/plouf" "plif";
  Clone.push ~dry_run: true [ "bla/bli/plouf"; "bla/blo/plouf" ];
  Clone.push [ "bla/bli/plouf"; "bla/blo/plouf" ];
  Clone.push [ "bla/bli/plouf"; "bla/blo/plouf" ];
  Clone.check ~cache: true ();
  Clone.log ();
  Clone.diff ~before: 3 ();

  comment "Test tree.";
  Clone.tree ();
  Clone.tree ~path: "bla" ();
  Clone.tree ~path: "bla/blo" ();
  cd (clone // "bla/bli");
  Filou.tree ();
  Clone.tree ~path: "bla/blo/blu/ble" ();
  Clone.tree ~path: "bla/blo/plouf/ble" ();
  Clone.tree ~path: "bla/blo/plouf" ();
  Clone.tree ~depth: 0 ();
  Clone.tree ~depth: 1 ();
  Clone.tree ~depth: 2 ();
  Clone.tree ~depth: 3 ();
  Clone.tree ~path: "bla" ~depth: 0 ();
  Clone.tree ~path: "bla" ~depth: 1 ();
  Clone.tree ~path: "bla" ~depth: 2 ();
  Clone.tree ~only_dirs: true ();
  Clone.tree ~only_dirs: true ~depth: 1 ();
  Clone.tree ~size: true ();
  Clone.tree ~count: true ();
  Clone.tree ~size: true ~count: true ();
  Clone.log ();

  comment "Test duplicates.";
  create_file "plif" "plif";
  Clone.push ~v: true ~dry_run: true [];
  Clone.tree ~duplicates: true ();
  Clone.push ~v: true [];
  Clone.tree ~duplicates: true ();
  Clone.log ();
  Clone.diff ();

  comment "Try to pull files.";
  rm "titi";
  rm "tutu";
  Clone.tree ();
  Clone.pull [ "titi" ];
  Clone.tree ();
  Clone.pull [ "tutu" ];
  Clone.tree ();
  rm "titi";
  rm "tutu";
  Clone.tree ();
  Clone.pull [ "titi"; "tutu" ];
  Clone.tree ();
  rm "tutu";
  Clone.tree ();
  Clone.pull [ "titi"; "tutu" ];
  Clone.tree ();
  rm "bla/bli/plouf";
  rmdir "bla/bli";
  rm "plif";
  rm "toto";
  Clone.tree ();
  Clone.pull ~v: true [ "bla/bli" ];
  Clone.tree ();
  rm "bla/bli/plouf";
  rmdir "bla/bli";
  Clone.tree ();
  Clone.pull ~v: true [ "bla" ];
  Clone.tree ();
  rm "bla/bli/plouf";
  rmdir "bla/bli";
  Clone.tree ();
  Clone.pull ~v: true [];
  Clone.tree ();
  Clone.check ();
  cat "bla/bli/plouf";
  cat "bla/blo/plouf";
  cat "plif";
  cat "titi";
  cat "toto";
  cat "tutu";
  Clone.log ();

  comment "Remove files.";
  Clone.tree ~size: true ~count: true ();
  Clone.rm [ "plif" ];
  Clone.check ();
  Clone.tree ~size: true ~count: true ();
  Clone.rm [ "titi"; "toto"; "tutu"; ];
  Clone.check ();
  Clone.tree ~size: true ~count: true ();
  Clone.rm [ "bla/bli/plouf" ];
  Clone.check ();
  Clone.tree ~size: true ~count: true ();
  Clone.rm [ "bla/blo/plouf" ];
  Clone.check ();
  Clone.tree ~size: true ~count: true ();
  Clone.push [];
  Clone.tree ~size: true ~count: true ();
  Clone.rm [ "plif"; "bla/bli/plouf"; "bla/blo/plouf" ];
  Clone.check ();
  Clone.tree ~size: true ~count: true ();
  Clone.push [];
  Clone.check ();
  Clone.tree ~size: true ~count: true ();
  Clone.rm [ "plif"; "bla/bli" ];
  Clone.check ();
  Clone.tree ~size: true ~count: true ();
  Clone.rm ~r: true [ "plif"; "bla/bli" ];
  Clone.check ();
  Clone.tree ~size: true ~count: true ();
  Clone.push [];
  Clone.tree ();
  Clone.rm ~r: true [ "bla" ];
  Clone.check ();
  Clone.tree ~size: true ~count: true ();
  Clone.push [];
  Clone.tree ();
  Clone.rm ~r: true [ "." ];
  Clone.check ();
  Clone.tree ~size: true ~count: true ();
  Clone.push [];
  Clone.tree ();
  Clone.log ();
  Clone.diff ~before: 13 ~after: 1 ();
  Clone.diff ();

  comment "Try to push a directory while a file already exists with the same name.";
  rm "toto";
  mkdir "toto";
  create_file "toto/wrong" "this shouldn't be here";
  Clone.push [];
  Clone.tree ~duplicates: true ();
  rm "toto/wrong";
  rmdir "toto";
  Clone.tree ~duplicates: true ();
  Clone.pull [];
  cat "toto";
  Clone.log ();

  comment "Same, but with a deeper directory.";
  rm "toto";
  mkdir "toto";
  mkdir "toto/tutu";
  create_file "toto/tutu/wrong" "this shouldn't be here";
  Clone.push [];
  rm "toto/tutu/wrong";
  rmdir "toto/tutu";
  rmdir "toto";
  Clone.pull [];
  cat "toto";
  Clone.log ();

  comment "Same, but with a deeper directory but the first directory exists.";
  rm "bla/bli/plouf";
  mkdir "bla/bli/plouf";
  mkdir "bla/bli/plouf/tutu";
  create_file "bla/bli/plouf/tutu/wrong" "this shouldn't be here";
  Clone.push [];
  rm "bla/bli/plouf/tutu/wrong";
  rmdir "bla/bli/plouf/tutu";
  rmdir "bla/bli/plouf";
  Clone.pull [];
  cat "bla/bli/plouf";
  Clone.tree ();
  Clone.log ();

  comment "Try to push a file while a directory already exists for one of its parent.";
  rm "bla/bli/plouf";
  rmdir "bla/bli";
  create_file "bla/bli" "I should be a directory";
  Clone.push [];
  Clone.tree ();
  rm "bla/bli";
  Clone.pull [];
  cat "bla/bli/plouf";
  Clone.log ();

  comment "Test update.";
  Clone.update ();
  cd clone;
  let root = explore ".filou/root" in
  let root_dir = root =~* "root_dir = ([0-9a-f]{64})" in
  let hash_index = root =~* "hash_index = ([0-9a-f]{64})" in
  rm (dot_filou_hash root_dir);
  Clone.update ();
  Clone.update ();
  rm (dot_filou_hash root_dir);
  rm (dot_filou_hash hash_index);
  Clone.update ();
  Clone.update ();
  rm_rf (clone // ".filou");
  Filou.clone ~main ~clone ();
  clone_tree ();
  Clone.update ();
  clone_tree ();
  Clone.check ~cache: true ();
  Clone.tree ~cache: true ();
  Clone.log ();

  comment "Play with undo / redo.";
  Clone.undo ();
  Clone.log ();
  Clone.tree ();
  Clone.undo ();
  Clone.log ();
  Clone.tree ();
  Clone.undo ~count: 3 ();
  Clone.log ();
  Clone.tree ();
  Clone.redo ~count: 4 ();
  Clone.log ();
  Clone.tree ();
  Clone.push [ "bla" ];
  Clone.log ();
  Clone.tree ();
  Clone.redo ();
  Clone.push [];
  Clone.log ();
  Clone.tree ();

  comment "Check what prune removes.";
  Clone.prune ();
  Clone.check ();
  Clone.tree ();
  Clone.log ();

  comment "Check that the main repository is not read with --cache.";
  mv main (main ^ ".backup");
  Clone.tree ();
  Clone.tree ~cache: true ();
  mv (main ^ ".backup") main;
  Clone.log ();

  comment "Check that needed objects are copied in the clone cache when fetching.";
  rm_rf (clone // ".filou");
  Filou.clone ~main ~clone ();
  Clone.tree ~path: "bla/bli" ();
  clone_tree ();
  Clone.update ();
  Clone.log ();

  comment "Test prune on the clone.";
  create_file "bla/bli/truc" "this is a truc";
  Clone.push [ "bla/bli" ];
  let clone_files_1 = find_files clone in
  let main_files_1 = find_files main in
  Clone.prune ();
  let clone_files_2 = find_files clone in
  let main_files_2 = find_files main in
  comment "Clone: some objects should have been removed, no files should have been added:";
  diff_string_sets clone_files_1 clone_files_2;
  comment "Main: should be the same diff:";
  diff_string_sets main_files_1 main_files_2;
  Clone.check ();
  Clone.tree ();
  comment "Check that if some files are missing from the cache, they are not added.";
  cd clone;
  let root = explore ".filou/root" in
  let root_dir = root =~* "root_dir = ([0-9a-f]{64})" in
  let hash_index = root =~* "hash_index = ([0-9a-f]{64})" in
  rm (dot_filou_hash root_dir);
  rm (dot_filou_hash hash_index);
  let clone_files_1 = find_files clone in
  let main_files_1 = find_files main in
  Clone.prune ();
  let clone_files_2 = find_files clone in
  let main_files_2 = find_files main in
  comment "Clone: diff should be empty:";
  diff_string_sets clone_files_1 clone_files_2;
  comment "Main: diff should also be empty:";
  diff_string_sets main_files_1 main_files_2;
  comment "Updating should restore 2 objects:";
  Clone.update ();
  comment "Check that if more objects exist in the clone than in the main, they are removed.";
  cd clone;
  mkdir ".filou/01";
  mkdir ".filou/01/23";
  create_file
    ".filou/01/23/01234567fb95b9dd5d20056bc24150b79354852e0f0a28ce578af2b3d2f4b859"
    "dummy object";
  let clone_files_1 = find_files clone in
  let main_files_1 = find_files main in
  Clone.prune ();
  let clone_files_2 = find_files clone in
  let main_files_2 = find_files main in
  comment "Clone: diff should show one removed file:";
  diff_string_sets clone_files_1 clone_files_2;
  comment "Main: diff should be empty:";
  diff_string_sets main_files_1 main_files_2;
  Clone.log ();

  comment "Push a file, remove it and re-push it from another clone.";
  rm_rf main;
  rm_rf clone;
  Filou.init ~main ();
  Filou.clone ~main ~clone ();
  cd clone;
  create_file "bla" "contents of bla first version";
  Clone.push [];
  Filou.clone ~main ~clone: clone2 ();
  cd clone2;
  Clone2.rm [ "bla" ];
  create_file "bla" "contents of bla second version";
  Clone2.push [];
  Clone.tree ();
  Clone.check ();

  comment "Test the show command with large recursivity.";
  Clone.show ~r: 1000 ();
  ()

let large_repo ?(seed = 0) ~files: file_count ~dirs: dir_count () =
  (* Create a random hierarchy of directories. *)
  Random.init seed;
  let dirs = Array.make dir_count "" in
  (* [dirs.(0)] is the root, so we keep it equal to [""]. *)
  for i = 1 to dir_count - 1 do
    let name = "dir" ^ string_of_int i in
    let parent = Random.int i in
    if parent = 0 then
      dirs.(i) <- name
    else
      dirs.(i) <- dirs.(parent) // name;
  done;

  (* Fill the hierarchy with files randomly. *)
  let files = Array.make file_count "" in
  for i = 0 to file_count - 1 do
    let name = "file" ^ string_of_int i in
    let parent = Random.int dir_count in
    files.(i) <- dirs.(parent) // name;
  done;

  comment (sf "Create clone with %d directories and %d files." dir_count file_count);
  (
    with_quiet @@ fun () ->
    mkdir_p clone;
    cd clone;
    for i = 0 to file_count - 1 do
      let path = files.(i) in
      mkdir_p (Filename.dirname path);
      create_file path ("this is " ^ path ^ ", a test file");
    done;
  );

  comment "Initialize main and clone.";
  Filou.init ~main ();
  Filou.clone ~main ~clone ();
  Clone.tree ();

  comment "Push.";
  (time "push" @@ fun () -> Clone.push ~v: false []);
  Clone.check ();
  Clone.stats ~v ();
  ()

let () =
  small_repo ();
(*   large_repo ~files: 1000 ~dirs: 200 (); *)
  ()
