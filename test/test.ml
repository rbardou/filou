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

(* let rec take ?(acc = []) n l = *)
(*   if n <= 0 then *)
(*     List.rev acc *)
(*   else *)
(*     match l with *)
(*       | [] -> *)
(*           List.rev acc *)
(*       | head :: tail -> *)
(*           take ~acc: (head :: acc) (n - 1) tail *)

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

let use_ssh =
  Clap.flag ~set_long: "ssh" ~description: "Configure clones to use SSH." false

let tests_to_run =
  Clap.list_string
    ~description: "Names of the tests to run."
    ~placeholder: "TEST_NAME"
    ()

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

let cmd_and_read executable args =
  (* Print command. *)
  print_string "$ ";
  print_string (Filename.quote_if_needed executable);
  List.iter' args (fun arg -> print_char ' '; print_string (Filename.quote_if_needed arg));
  print_newline ();
  (* Execute command. *)
  let stdout, stderr, status =
    try
      Run_and_read.run_and_read executable args
    with Unix.Unix_error (error, _, _) ->
      echo "Failed to execute: %s" (Unix.error_message error);
      exit 1
  in
  print_string stdout;
  print_string stderr;
  (
    match status with
      | WEXITED 0 -> ()
      | WEXITED n -> echo "Exit code: %d" n
      | WSIGNALED n -> echo "Killed by signal: %d" n
      | WSTOPPED n -> echo "Stopped by signal: %d" n
  );
  stdout, stderr

let create_file path contents =
  if String.length contents > 256 then
    echo "$ echo %s... > %s"
      (Filename.quote_if_needed (String.sub contents 0 256))
      (Filename.quote_if_needed path)
  else
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
  let run ?(v = false) ?(dry_run = false) ?(color = false)
      ?(no_main = false) ?(no_cache = false) args =
    Option.iter cd P.path;
    let flags =
      flag v "-v"
      @ flag dry_run "--dry-run"
      @ flag (not color) "--no-color"
      @ flag no_main "--no-main"
      @ flag no_cache "--no-cache"
    in
    cmd filou_exe (flags @ args)

  let run_and_read ?(v = false) ?(dry_run = false) ?(color = false) args =
    Option.iter cd P.path;
    let flags =
      flag v "-v"
      @ flag dry_run "--dry-run"
      @ flag (not color) "--no-color"
    in
    cmd_and_read filou_exe (flags @ args)

  let init ?v ?dry_run ?color ?main () =
    run ?v ?dry_run ?color ("init" :: list_of_option main)

  let clone ?v ?dry_run ?color ?no_cache ?main ?clone () =
    let main =
      if use_ssh then
        Option.map (fun main -> "filou+ssh://localhost/" ^ main) main
      else
        main
    in
    run ?v ?dry_run ?color ?no_cache ("clone" :: list_of_option main @ list_of_option clone)

  let push ?(v = true) ?dry_run ?color paths =
    run ~v ?dry_run ?color ("push" :: "--yes" :: paths)

  let pull ?(v = true) ?dry_run ?color paths =
    run ~v ?dry_run ?color ("pull" :: paths)

  let ls ?v ?dry_run ?color ?path () =
    run ?v ?dry_run ?color ("ls" :: list_of_option path)

  let rm ?v ?dry_run ?color ?(r = false) paths =
    run ?v ?dry_run ?color ("rm" :: "--yes" :: flag r "-r" @ paths)

  let mv ?(v = true) ?dry_run ?color paths =
    run ~v ?dry_run ?color ("mv" :: "--yes" :: paths)

  let cp ?(v = true) ?dry_run ?color paths =
    run ~v ?dry_run ?color ("cp" :: "--yes" :: paths)

  let check ?v ?dry_run ?color ?path ?no_main ?(h = false) () =
    run ?v ?dry_run ?color ?no_main (
      "check" :: list_of_option path @ flag h "-h"
    )

  let tree ?v ?dry_run ?color ?no_main
      ?depth ?(only_dirs = false) ?(size = false) ?(count = false)
      ?(duplicates = false) ?path () =
    run ?v ?dry_run ?color ?no_main (
      "tree"
      :: list_of_option ~name: "--depth" (Option.map string_of_int depth)
      @ list_of_option path
      @ flag only_dirs "--only-dirs"
      @ flag size "--size"
      @ flag count "--count"
      @ flag duplicates "--duplicates"
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

  let config_show () =
    run [ "config"; "show" ]

  let config_set_main value =
    let value =
      if use_ssh then
        "filou+ssh://localhost/" ^ value
      else
        value
    in
    run [ "config"; "set"; "main"; value ]

  let config_set_no_cache value =
    run [ "config"; "set"; "no-cache"; string_of_bool value ]

  let config_ignore filters =
    run ("config" :: "ignore" :: filters)

  let config_unignore filters =
    run ("config" :: "unignore" :: filters)

  let show ?v ?dry_run ?color ?what ?r () =
    run ?v ?dry_run ?color (
      "show" ::
      list_of_option what @
      list_of_option ~name: "-r" (Option.map string_of_int r)
    )

  let show_and_read ?v ?dry_run ?color ?what ?r () =
    run_and_read ?v ?dry_run ?color (
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

let dot_filou_meta_hash hash =
  ".filou" // "meta" // String.sub hash 0 2 // hash

let hexdump file =
  cmd "hexdump" [ "-C"; file ]

let explore _ = "" (* TODO *)

let small_repo () =
  comment "Initialize a main repository and a clone.";
  Filou.init ~main ();
  Filou.clone ~main ~clone ();
  hexdump (main // ".filou/config");
  hexdump (clone // ".filou/config");
  Clone.check ();
  Clone.tree ();
  Clone.log ();

  comment "Try to do something in the main repository directly.";
  Main.tree ();

  comment "Play with the configuration.";
  cd clone;
  Filou.config_show ();
  Filou.config_set_main "/tmp";
  Filou.config_show ();
  Filou.config_set_main main;
  Filou.config_show ();
  Filou.config_set_no_cache true;
  Filou.config_show ();
  Filou.config_set_no_cache false;
  Filou.config_show ();

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
  Clone.check ~no_main: true ();
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
  let stdout, _ = Clone.show_and_read () in
  let root_dir = stdout =~* "root_dir = ([0-9a-f]{64})" in
  let hash_index = stdout =~* "hash_index = ([0-9a-f]{64})" in
  rm (dot_filou_meta_hash root_dir);
  Clone.update ();
  Clone.update ();
  rm (dot_filou_meta_hash root_dir);
  rm (dot_filou_meta_hash hash_index);
  Clone.update ();
  Clone.update ();
  rm_rf (clone // ".filou");
  Filou.clone ~main ~clone ();
  clone_tree ();
  Clone.update ();
  clone_tree ();
  Clone.check ~no_main: true ();
  Clone.tree ~no_main: true ();
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
  Clone.tree ~no_main: true ();
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
  cat (main // ".filou/root");
  comment "Clone:";
  comment "- some meta files should have been removed";
  comment "- none should have been added, except the new journal (see above cat)";
  comment "- no data files should have been removed";
  diff_string_sets clone_files_1 clone_files_2;
  comment "Main: should be the same diff:";
  diff_string_sets main_files_1 main_files_2;
  Clone.check ();
  Clone.tree ();
  comment "Check that if some files are missing from the cache, they are not added.";
  cd clone;
  let stdout, _ = Clone.show_and_read () in
  let root_dir = stdout =~* "root_dir = ([0-9a-f]{64})" in
  let hash_index = stdout =~* "hash_index = ([0-9a-f]{64})" in
  rm (dot_filou_meta_hash root_dir);
  rm (dot_filou_meta_hash hash_index);
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
  mkdir ".filou/meta/01";
  create_file
    ".filou/meta/01/01234567fb95b9dd5d20056bc24150b79354852e0f0a28ce578af2b3d2f4b859"
    "dummy object";
  mkdir (main // ".filou/data/01");
  mkdir (main // ".filou/data/01/23");
  create_file
    (main //
     ".filou/data/01/23/01234567fb95b9dd5d20056bc24150b79354852e0f0a28ce578af2b3d2f4b85a")
    "dummy object";
  let clone_files_1 = find_files clone in
  let main_files_1 = find_files main in
  Clone.prune ();
  let clone_files_2 = find_files clone in
  let main_files_2 = find_files main in
  comment "Clone: diff should show one removed meta file:";
  diff_string_sets clone_files_1 clone_files_2;
  comment "Main: diff should show one removed data file:";
  diff_string_sets main_files_1 main_files_2;
  Clone.log ();

  comment "Test ignore filters.";
  Clone.config_ignore [ "/_build/$" ];
  Clone.config_show ();
  mkdir "_build";
  create_file "_build/ignored" "this is ignored";
  mkdir "bla/_build";
  create_file "bla/_build/ignored2" "this is also ignored";
  create_file "bla/_build/ignored3" "this is also also ignored";
  mkdir "bla/bli/_build";
  Clone.push ~v: true [];
  Clone.config_unignore [ "/_build/$" ];
  Clone.config_show ();
  Clone.config_ignore [ "/ignored$"; "/ignored2"; "gnored3" ];
  Clone.config_show ();
  Clone.push ~v: true [];
  Clone.config_unignore [ "gnored3"; "/ignored$" ];
  Clone.config_show ();
  Clone.push ~v: true [];
  Clone.config_unignore [ "/ignored2" ];
  Clone.config_show ();
  Clone.push ~v: true [];
  Clone.undo ~count: 4 ();
  rm "_build/ignored";
  rm "bla/_build/ignored2";
  rm "bla/_build/ignored3";
  rmdir "_build";
  rmdir "bla/_build";
  rmdir "bla/bli/_build";

  comment "mv: rename a file at the root";
  Clone.tree ();
  Clone.mv [ "plif"; "newplif" ];
  Clone.tree ();
  Clone.pull [ "newplif" ];
  cat "newplif";
  cat "plif";
  comment "mv: rename a file from root to another empty dir";
  Clone.mv [ "newplif"; "plop/newnewplif" ];
  Clone.tree ();
  comment "mv: rename a file from root to another non-empty dir";
  Clone.mv [ "titi"; "bla/newtiti" ];
  Clone.tree ();
  Clone.log ();
  comment "mv: rename a file to an existing file";
  Clone.mv [ "toto"; "tutu" ];
  Clone.tree ();
  comment "mv: check after the previous mv and undo";
  Clone.check ();
  Clone.log ();
  Clone.undo ~count: 3 ();
  rm "newplif";
  Clone.tree ();
  comment "mv: move several files to a new dir";
  Clone.mv [ "bla/bli/plouf"; "tutu"; "bidule" ];
  Clone.tree ();
  Clone.undo ();
  Clone.mv [ "bla/bli/plouf"; "tutu"; "bidule/" ];
  Clone.tree ();
  Clone.undo ();
  comment "mv: move several files to the root";
  Clone.mv [ "bla/bli/truc"; "bla/blo/plouf"; "." ];
  Clone.tree ();
  Clone.undo ();
  comment "mv: move a file to a directory";
  Clone.mv [ "toto"; "bla" ];
  Clone.tree ();
  Clone.mv [ "titi"; "bla/" ];
  Clone.tree ();
  Clone.undo ~count: 2 ();
  Clone.tree ();
  comment "mv: rename a directory";
  Clone.mv [ "bla"; "newbla" ];
  Clone.tree ();
  Clone.check ();
  Clone.undo ();
  comment "mv: move a directory";
  Clone.mv [ "bla"; "plop/plif/plouf/" ];
  Clone.tree ();
  Clone.check ();
  Clone.undo ();
  comment "mv: rename a directory into a deep directory";
  Clone.mv [ "bla"; "plop/plif/plouf" ];
  Clone.tree ();
  Clone.mv [ "plop/plif/plouf/bli"; "bidule/machin" ];
  Clone.tree ();
  Clone.check ();
  Clone.undo ~count: 2 ();
  comment "mv: move a directory into a directory that already exists";
  Clone.tree ();
  Clone.mv [ "bla/blo"; "bla/bli" ];
  Clone.tree ();
  Clone.undo ();
  Clone.mv [ "bla/blo"; "bla/bli/" ];
  Clone.tree ();
  Clone.undo ();
  comment "mv: move a file and a directory at the same time";
  Clone.tree ();
  Clone.mv [ "plif"; "bla/bli"; "some/new/directory" ];
  Clone.tree ();
  Clone.undo ();
  Clone.mv [ "plif"; "bla/bli"; "some/new/directory/" ];
  Clone.tree ();
  Clone.undo ();
  comment "mv: move a file onto itself";
  Clone.mv [ "toto"; "toto" ];
  Clone.tree ();
  comment "mv: move a file into a directory named after itself";
  Clone.tree ();
  Clone.mv [ "plif"; "plif/newplif" ];
  Clone.tree ();
  cat "plif";
  rm "plif";
  Clone.tree ();
  Clone.pull [];
  cat "plif/newplif";
  Clone.undo ();
  rm_rf (clone // "plif");
  Clone.pull [];
  Clone.tree ();
  (* TODO: It's a bit weird that this fails while the previous didn't. *)
  Clone.mv [ "plif"; "plif/" ];
  comment "mv: rename a directory into the root directory";
  Clone.mv [ "bla/bli"; "." ];
  Clone.tree ();
  Clone.undo ();
  comment "mv: rename a less deep directory into the root directory";
  Clone.mv [ "bla"; "." ];
  Clone.tree ();
  Clone.undo ();
  comment "mv: move the root directory";
  Clone.mv [ "."; "bla/bli" ];
  Clone.mv [ "."; "bla/bli/" ];
  Clone.check ();

  comment "cp: rename a file at the root";
  Clone.tree ();
  Clone.cp [ "plif"; "newplif" ];
  Clone.tree ~duplicates: true ();
  Clone.pull [ "newplif" ];
  cat "newplif";
  cat "plif";
  comment "cp: copy a file from root to another empty dir";
  Clone.cp [ "newplif"; "plop/newnewplif" ];
  Clone.tree ~duplicates: true ();
  comment "cp: copy a file from root to another non-empty dir";
  Clone.cp [ "titi"; "bla/newtiti" ];
  Clone.tree ~duplicates: true ();
  Clone.log ();
  comment "cp: copy a file to an existing file";
  Clone.cp [ "toto"; "tutu" ];
  Clone.tree ~duplicates: true ();
  comment "cp: check after the previous cp and undo";
  Clone.check ();
  Clone.log ();
  Clone.undo ~count: 3 ();
  rm "newplif";
  Clone.tree ~duplicates: true ();
  comment "cp: copy several files to a new dir";
  Clone.cp [ "bla/bli/plouf"; "tutu"; "bidule" ];
  Clone.tree ~duplicates: true ();
  Clone.undo ();
  Clone.cp [ "bla/bli/plouf"; "tutu"; "bidule/" ];
  Clone.tree ~duplicates: true ();
  Clone.undo ();
  comment "cp: copy several files to the root";
  Clone.cp [ "bla/bli/truc"; "bla/blo/plouf"; "." ];
  Clone.tree ~duplicates: true ();
  Clone.undo ();
  comment "cp: copy a file to a directory";
  Clone.cp [ "toto"; "bla" ];
  Clone.tree ~duplicates: true ();
  Clone.cp [ "titi"; "bla/" ];
  Clone.tree ~duplicates: true ();
  Clone.undo ~count: 2 ();
  Clone.tree ~duplicates: true ();
  (* TODO: should cp require a -r flag for the following? *)
  comment "cp: copy a directory";
  Clone.cp [ "bla"; "newbla" ];
  Clone.tree ~duplicates: true ();
  Clone.check ();
  Clone.undo ();
  comment "cp: copy a directory";
  Clone.cp [ "bla"; "plop/plif/plouf/" ];
  Clone.tree ~duplicates: true ();
  Clone.check ();
  Clone.undo ();
  comment "cp: copy a directory into a deep directory";
  Clone.cp [ "bla"; "plop/plif/plouf" ];
  Clone.tree ~duplicates: true ();
  Clone.cp [ "plop/plif/plouf/bli"; "bidule/machin" ];
  Clone.tree ~duplicates: true ();
  Clone.check ();
  Clone.undo ~count: 2 ();
  comment "cp: copy a directory into a directory that already exists";
  Clone.tree ~duplicates: true ();
  Clone.cp [ "bla/blo"; "bla/bli" ];
  Clone.tree ~duplicates: true ();
  Clone.undo ();
  Clone.cp [ "bla/blo"; "bla/bli/" ];
  Clone.tree ~duplicates: true ();
  Clone.undo ();
  comment "cp: copy a file and a directory at the same time";
  Clone.tree ~duplicates: true ();
  Clone.cp [ "plif"; "bla/bli"; "some/new/directory" ];
  Clone.tree ~duplicates: true ();
  Clone.undo ();
  Clone.cp [ "plif"; "bla/bli"; "some/new/directory/" ];
  Clone.tree ~duplicates: true ();
  Clone.undo ();
  comment "cp: copy a file onto itself";
  Clone.cp [ "toto"; "toto" ];
  Clone.tree ~duplicates: true ();
  comment "cp: copy a file into a directory named after itself";
  Clone.tree ~duplicates: true ();
  Clone.cp [ "plif"; "plif/newplif" ];
  Clone.tree ~duplicates: true ();
  cat "plif";
  Clone.cp [ "plif"; "plif/" ];
  comment "cp: copy a directory into the root directory";
  Clone.cp [ "bla/bli"; "." ];
  Clone.tree ~duplicates: true ();
  Clone.undo ();
  comment "cp: copy a less deep directory into the root directory";
  Clone.cp [ "bla"; "." ];
  Clone.tree ~duplicates: true ();
  Clone.undo ();
  comment "cp: copy the root directory";
  Clone.cp [ "."; "bla/bli" ];
  Clone.cp [ "."; "bla/bli/" ];
  Clone.check ();

  comment "cash: modify a file (keeping the same size) and try to push it again";
  Clone.tree ~duplicates: true ();
  cat "toto";
  rm "toto";
  create_file "toto" "TOTTOTTOT";
  Clone.tree ~duplicates: true ();
  Clone.push [ "toto" ];
  Clone.pull [ "toto" ];

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

  comment "Test the show command.";
  Clone.show ();
  let hash, _ = Clone.show_and_read ~what: "hash_index" () in
  Clone.show ~what: (String.trim hash) ();
  Clone.show ~what: "root_dir" ();

  comment "No-cache: check that cloning with --no-cache stores in config.";
  rm_rf main;
  rm_rf clone;
  Filou.init ~main ();
  Filou.clone ~main ~clone ~no_cache: true ();
  Clone.config_show ();
  cd clone;
  create_file "bla" "contents of bla";
  Clone.push [];
  cd clone;
  tree ();
  cd main;
  tree ();
  Clone.check ();
  cd clone;
  tree ();

  comment "--- End of small tests. ---"

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

  comment "Remove some files.";
  let to_remove = ref [] in
  for i = 0 to file_count / 20 - 1 do
    to_remove := files.(i) :: !to_remove;
  done;
  Clone.rm !to_remove;
  Clone.check ();

  comment "Push again.";
  Clone.push ~v: false [];
  Clone.check ();

  comment "Remove some from the workdir and pull.";
  for i = file_count / 2 to file_count / 2 + file_count / 20 - 1 do
    rm files.(i);
  done;
  Clone.pull ~v: false [];
  cat "dir1/dir12/dir78/file5499";

  comment "Create a rather large file.";
  create_file "large-file" (String.init 100_000_000 @@ fun i -> Char.chr (i mod 256));
  Clone.push [ "large-file" ];
  Clone.check ~h: true ();
  rm "large-file";
  Clone.pull [ "large-file" ];
  ()

let () =
  if List.mem "small" tests_to_run then
    small_repo ();
  if List.mem "large" tests_to_run then
    large_repo ~files: 10000 ~dirs: 500 ();
  ()
