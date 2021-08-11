(** Cache of file hashes for clone workdirs. *)

open Misc

type status =
  | Does_not_exist
  | Dir
  | File of Hash.t

val get: on_progress: (bytes: int -> size: int -> unit) ->
  Clone.setup -> Device.file_path -> (status, [> `failed ]) r

(** For when you compute a hash with other means but still want to cache it. *)
val set: Clone.setup -> Device.file_path -> Hash.t -> unit

val save: Clone.setup -> unit
