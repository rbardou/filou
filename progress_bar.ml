let enabled = ref true

let disable () = enabled := false

let start () =
  if !enabled then
    print_string "\027[s"

let set x =
  Printf.ksprintf
    (
      fun s ->
        if !enabled then (
          print_string "\027[u";
          print_string s;
          print_string "\027[J";
          flush stdout
        )
    )
    x
