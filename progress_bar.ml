let tty = ref true

let not_a_tty () = tty := false

(* TODO: make it nestable? *)
let run ~quiet f =
  if quiet then
    f @@ fun ?major: _ _ -> ()
  else
    let updated_tty_once = ref false in
    let last_update = ref (Unix.gettimeofday ()) in
    let update_delay = if !tty then 0.1 else 1. in
    let result =
      f @@ fun ?(major = false) get_text ->
      let now = Unix.gettimeofday () in
      let should_update = major || (now -. !last_update >= update_delay) in
      if should_update then (
        last_update := now;
        if !tty then
          if !updated_tty_once then
            (
              (* Restore saved cursor position. *)
              print_string "\027[u";
              print_string (get_text ());
              (* Erase after cursor. *)
              print_string "\027[J";
            )
          else
            (
              (* Save cursor position. *)
              print_string "\027[s";
              print_string (get_text ());
              updated_tty_once := true;
            )
        else
          (
            print_string (get_text ());
            (* Note: restoring cursor position doesn't seem to work if we print a \n. *)
            print_char '\n';
          );
        flush stdout
      )
    in
    if !updated_tty_once then
      (
        (* Restore and erase. *)
        print_string "\027[u\027[J";
        flush stdout;
      );
    result
