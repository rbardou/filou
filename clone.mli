type setup

val setup: main: Device.location -> clone: Device.location -> setup
val main: setup -> Device.location

(** Returns the location given to [setup], i.e. not [.filou]. *)
val clone: setup -> Device.location

module Make (R: Repository.S with type t = Device.location): Repository.S
  with type root = R.root and type t = setup
