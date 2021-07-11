open Mysc

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

  let push ?v ?dry_run ?color paths =
    run ?v ?dry_run ?color ("push" :: paths)

  let pull ?v ?dry_run ?color paths =
    run ?v ?dry_run ?color ("pull" :: paths)

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
    run ?v ?dry_run ?color [ "prune" ]

  let update ?v ?dry_run ?color () =
    run ?v ?dry_run ?color [ "update" ]
end

module Main = Make_filou (struct let path = Some main end)
module Clone = Make_filou (struct let path = Some clone end)
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
  Format.printf "%a@." Protype_robin.Explore.pp_string contents

let explore_main path =
  cd main;
  explore path

let explore_clone path =
  cd clone;
  explore (".filou" // path)

let explore_hash hash =
  explore (String.sub hash 0 2 // String.sub hash 2 2 // hash)

let explore_main_hash hash =
  cd main;
  explore_hash hash

(* let explore_clone_hash hash = *)
(*   cd clone; *)
(*   explore_hash hash *)

let () =
  comment "Initialize a main repository and a clone.";
  Filou.init ~main ();
  Filou.clone ~main ~clone ();
  explore_clone "config";
  Clone.check ();
  Clone.tree ();

  comment "Add a file.";
  cd clone;
  create_file "toto" "blablabla";
  Clone.push [ "toto" ];
  Clone.check ();
  Clone.tree ();

  comment "Add the file again.";
  Clone.push [];
  Clone.check ();
  Clone.tree ();

  comment "Add several files.";
  cd clone;
  create_file "tutu" "blu";
  create_file "titi" "blibli";
  Clone.push [ "tutu"; "titi" ];
  Clone.check ();
  Clone.tree ();

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

  comment "Test duplicates.";
  create_file "plif" "plif";
  Clone.push ~v: true ~dry_run: true [];
  Clone.tree ~duplicates: true ();
  Clone.push ~v: true [];
  Clone.tree ~duplicates: true ();

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

  comment "Try to push a file while a directory already exists for one of its parent.";
  rm "bla/bli/plouf";
  rmdir "bla/bli";
  create_file "bla/bli" "I should be a directory";
  Clone.push [];
  Clone.tree ();
  rm "bla/bli";
  Clone.pull [];
  cat "bla/bli/plouf";

  comment "Test update.";
  Clone.update ();
  cd clone;
  explore ".filou/root";
  rm ".filou/12/f4/12f41d5b6b1957d3b926019b508f08521e1614c9dff595500dc15b11cb13cd7b";
  Clone.update ();
  Clone.update ();
  rm ".filou/12/f4/12f41d5b6b1957d3b926019b508f08521e1614c9dff595500dc15b11cb13cd7b";
  rm ".filou/d1/0f/d10fb33ec821d25bc7ba85a23553a73e1f4d936a7eaf5632c559485c32f50512";
  Clone.update ();
  Clone.update ();
  rm_rf (clone // ".filou");
  Filou.clone ~main ~clone ();
  clone_tree ();
  Clone.update ();
  clone_tree ();
  Clone.check ~cache: true ();
  Clone.tree ~cache: true ();

  comment "Check what prune removes.";
  Clone.prune ();
  Clone.check ();
  Clone.tree ();

  comment "Check that the main repository is not read with --cache.";
  mv main (main ^ ".backup");
  Clone.tree ();
  Clone.tree ~cache: true ();
  mv (main ^ ".backup") main;

  comment "Check that needed objects are copied in the clone cache when fetching.";
  rm_rf (clone // ".filou");
  Filou.clone ~main ~clone ();
  Clone.tree ~path: "bla/bli" ();
  clone_tree ();
  Clone.update ();

  (* TODO: Check that with two clones A and B, if A pushes stuff, then if B reads
     this new stuff (e.g. with "tree"), the new objects are put in its cache,
     unless read-only mode (--dry-run) is active. *)

  ()
