open Misc

type t =
  {
    main: Device.location option;
    workdir: Device.location;
    clone_dot_filou: Device.location;
  }

let make ~main ~workdir ?clone_dot_filou () =
  {
    main;
    workdir;
    clone_dot_filou = clone_dot_filou |> default (Device.sublocation workdir dot_filou);
  }

let main { main; _ } = main
let clone_dot_filou { clone_dot_filou; _ } = clone_dot_filou
let workdir { workdir; _ } = workdir
