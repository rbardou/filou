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

let list_of_option = function None -> [] | Some x -> [ x ]
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

  let rm ?v ?dry_run ?color paths =
    run ?v ?dry_run ?color ("rm" :: paths)
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

let trees () =
  main_tree ();
  clone_tree ()

let hash path = cmd "sha256sum" [ path ]

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

let () =
  comment "Initialize a main repository and a clone.";
  Filou.init ~main ();
  Filou.clone ~main ~clone ();
  trees ();
  explore_main "root";
  explore_main "30/b6/30b6b1e835d55d653feb9555039b083167d8a55a8879a9ae71fe029853c6a7d7";
  explore_clone "config";

  comment "Add a file.";
  cd clone;
  create_file "toto" "blablabla";
  Clone.push [ "toto" ];
  trees ();
  explore_main "root";
  explore_clone "root";
  explore_main "43/62/43628a90e30d5475fb81855cb7b994b364b181b1a538fdf88d5b782df542a1af";
  explore_clone "43/62/43628a90e30d5475fb81855cb7b994b364b181b1a538fdf88d5b782df542a1af";
  cd main;
  cat "49/2f/492f3f38d6b5d3ca859514e250e25ba65935bcdd9f4f40c124b773fe536fee7d";
  explore_main "67/17/6717e0988b22ed4459cd506aa2f47500f995bfd12b5162b0e2ef32d5e90755d3";
  explore_clone "67/17/6717e0988b22ed4459cd506aa2f47500f995bfd12b5162b0e2ef32d5e90755d3";
  explore_main "4a/e0/4ae0108366313268bd8cd1d15bb66c9f957ee59c6becee2c8d925e87be605df4";
  explore_clone "4a/e0/4ae0108366313268bd8cd1d15bb66c9f957ee59c6becee2c8d925e87be605df4";
  explore_main "d4/08/d40850c643ac4845ca143fd122616a1e4caa5b9c1ed7b031b90d4541b69a0f4a";
  explore_clone "d4/08/d40850c643ac4845ca143fd122616a1e4caa5b9c1ed7b031b90d4541b69a0f4a";

  comment "Add the file again.";
  Clone.push [];
  trees ();
(*   explore_main "root"; *)
(*   explore_main "97/f8/97f8bf2919daa18e16fc3f620986c72e48942f18753061c96257ddb837ef7484"; *)
(*   explore_main "43/2d/432d7d2d758f153c1da8b5c9df084ba6a07ff27d6ca1cf2878dee52cb7648875"; *)

(*   Filou.ls (); *)
(*   trees (); *)
(*   Filou.push []; *)
(*   Filou.ls (); *)
(*   rm "toto"; *)
(*   Filou.ls (); *)
(*   Filou.pull []; *)
(*   trees (); *)
(*   hash "toto"; *)
(*   Filou.push []; *)
(*   Filou.rm [ "toto" ]; *)
(*   trees (); *)
(*   Filou.ls (); *)

(*   comment "Play with two more files which are equal."; *)
(*   create_file "tatator" "anursetanruseitanrsiute"; *)
(*   create_file "tatator2" "anursetanruseitanrsiute"; *)
(*   Filou.push [ "toto" ]; *)
(*   Filou.push []; *)
(*   trees (); *)
(*   Filou.ls (); *)
(*   Filou.rm [ "toto" ]; *)
(*   trees (); *)
(*   Filou.rm [ "tatator2" ]; *)
(*   trees (); *)
(*   Filou.rm [ "tatator" ]; *)
(*   trees (); *)

(*   comment "Play with subdirectories."; *)
(*   mkdir "bla"; *)
(*   mkdir "bla/bli"; *)
(*   create_file "bla/bli/blo" "blabliblo"; *)
(*   create_file "bla/bli/blu" "bliblablo"; *)
(*   Filou.push [ "bla/bli" ]; *)
(*   trees (); *)
(*   Filou.ls (); *)
(*   Filou.push []; *)
(*   trees (); *)
(*   Filou.ls (); *)

(*   comment "Remove files from the clone and pull them again."; *)
(*   rm "bla/bli/blo"; *)
(*   rm "bla/bli/blu"; *)
(*   rm "tatator"; *)
(*   rm "tatator2"; *)
(*   rm "toto"; *)
(*   Filou.ls (); *)
(*   Filou.pull []; *)
(*   trees (); *)
(*   Filou.ls (); *)
(*   cat "bla/bli/blo"; *)
(*   cat "bla/bli/blu"; *)
(*   cat "tatator"; *)
(*   cat "tatator2"; *)
(*   cat "toto"; *)

(*   comment "Try to push a directory while a file already exists with the same name."; *)
(*   rm "toto"; *)
(*   mkdir "toto"; *)
(*   create_file "toto/wrong" "this shouldn't be here"; *)
(*   Filou.push []; *)
(*   trees (); *)
(*   Filou.ls (); *)
(*   rm "toto/wrong"; *)
(*   rmdir "toto"; *)
(*   Filou.pull []; *)
(*   cat "toto"; *)

(*   comment "Same, but with a deeper directory."; *)
(*   rm "toto"; *)
(*   mkdir "toto"; *)
(*   mkdir "toto/tutu"; *)
(*   create_file "toto/tutu/wrong" "this shouldn't be here"; *)
(*   Filou.push []; *)
(*   trees (); *)
(*   Filou.ls (); *)
(*   rm "toto/tutu/wrong"; *)
(*   rmdir "toto/tutu"; *)
(*   rmdir "toto"; *)
(*   Filou.pull []; *)
(*   cat "toto"; *)

(*   comment "Try to push a file while a directory already exists for one of its parent."; *)
(*   rm "bla/bli/blo"; *)
(*   rm "bla/bli/blu"; *)
(*   rmdir "bla/bli"; *)
(*   create_file "bla/bli" "I should be a directory"; *)
(*   Filou.push []; *)
(*   trees (); *)
(*   Filou.ls (); *)
(*   rm "bla/bli"; *)
(*   rmdir "bla"; *)
(*   create_file "bla" "I should be a directory"; *)
(*   Filou.push []; *)
(*   trees (); *)
(*   Filou.ls (); *)
(*   rm "bla"; *)
(*   Filou.pull []; *)

  ()
