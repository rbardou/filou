(* TODO: since we always have a workdir, make it non-optional?
   (or add an option --no-workdir, possibly useful in some cases?) *)
type t =
  {
    main_dot_filou: Device.location option;
    clone_dot_filou: Device.location option;
    workdir: Device.location option;
  }
