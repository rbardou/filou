open Misc

type setup

val setup: main: Device.location option -> clone: Device.location -> setup
val main: setup -> Device.location option

(** Returns the location given to [setup], i.e. not [.filou]. *)
val clone: setup -> Device.location

module type S =
sig
  include Repository.S

  (** Transfer all reachable objects from main to clone, as well as the root.

      Return the number of objects that were not available in the clone
      and that were copied. *)
  val update:
    on_availability_check_progress: (checked: int -> count: int -> unit) ->
    on_copy_progress: (transferred: int -> count: int -> unit) ->
    t -> (int, [> `failed ]) r
end

module Make (R: Repository.S with type t = Device.location): S
  with type root = R.root and type t = setup
