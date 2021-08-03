let tty = ref true

let update_delay = ref 0.1

let not_a_tty () = tty := false; update_delay := 1.

let last_update = ref (Unix.gettimeofday ())

(* Whether we should restore cursor position before printing something new.
   Also used to know whether there is progress to clear. *)
let has_progress = ref false

let output_progress s =
  if !has_progress then
    (
      (* Restore saved cursor position. *)
      print_string "\027[u";
      print_string s;
      (* Erase after cursor. *)
      print_string "\027[J";
    )
  else if !tty then
    (
      (* Save cursor position. *)
      print_string "\027[s";
      print_string s;
      has_progress := true;
    )
  else
    (
      print_string s;
      (* Note: restoring cursor position doesn't seem to work if we print a \n,
         so we don't do it in the other branch. *)
      print_char '\n';
    );
  flush stdout

let minor f =
  let now = Unix.gettimeofday () in
  if now -. !last_update >= !update_delay then (
    last_update := now;
    output_progress (f ());
  )

let minor_s s =
  let now = Unix.gettimeofday () in
  if now -. !last_update >= !update_delay then (
    last_update := now;
    output_progress s;
  )

let major_s s =
  last_update := Unix.gettimeofday ();
  output_progress s

let major x = Printf.ksprintf major_s x

let print_s s =
  if !has_progress then
    (
      has_progress := false;
      (* Restore saved cursor position. *)
      print_string "\027[u";
      print_string s;
      (* Erase after cursor. *)
      print_string "\027[J";
    )
  else
    print_string s

let print x = Printf.ksprintf print_s x

let echo_s s =
  print_s s;
  print_char '\n';
  flush stdout

let echo x = Printf.ksprintf echo_s x

let clear_progress () =
  if !has_progress then (
    has_progress := false;
    (* Restore saved cursor position. *)
    print_string "\027[u";
    (* Erase after cursor. *)
    print_string "\027[J";
  )
