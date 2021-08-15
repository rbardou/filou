type t

val make:
  main: Device.location option ->
  workdir: Device.location ->
  ?clone_dot_filou: Device.location ->
  unit -> t

val main: t -> Device.location option
val clone_dot_filou: t -> Device.location
val workdir: t -> Device.location
