open Misc

type 'a hash

(* val hash_type: 'a Protype.t -> 'a hash Protype.t *)
(* val hex_of_hash: _ hash -> string *)
(* val bin_of_hash: _ hash -> string *)
(* val compare_hashes: _ hash -> _ hash -> int *)

val concrete_hash: _ hash -> Hash.t option
val concrete_hash_or_fail: _ hash -> (Hash.t, [> `failed ]) r
val stored_hash: Hash.t -> _ hash

(* module Hash_map: Map.S with type key = Hash.t *)

type file_hash

val concrete_file_hash: file_hash -> Hash.t
val stored_file_hash: Hash.t -> file_hash

val meta_filename: Path.Filename.t
val data_filename: Path.Filename.t

val hash_prefix_length: int

val data_path: Hash.t -> Path.Filename.t * Path.Filename.t * Path.Filename.t

module type OBJECT =
sig
  type 'a t
  type root
  val root: root t
  val encode: 'a t -> 'a -> string
  val decode: 'a t -> string -> ('a, [> `failed ]) r
  type dep =
    | Object: 'a t * 'a hash -> dep
    | File: file_hash -> dep
  val iter_deps: 'a t -> 'a -> (dep -> (unit, [> `failed ] as 'e) r) -> (unit, 'e) r
end

module type S =
sig
  type 'a object_type
  type root

  val set_read_only: unit -> unit

  val is_read_only: unit -> bool

  val hash: 'a -> 'a hash

  val store: Device.location list -> 'a object_type -> 'a hash -> (unit, [> `failed ]) r

  val fetch: Device.location -> 'a object_type -> 'a hash ->
    ('a, [> `failed | `not_available ]) r

  val fetch_raw: Device.location -> Hash.t -> (string, [> `failed | `not_available ]) r

  val store_file:
    on_hash_progress: (bytes: int -> size: int -> unit) ->
    on_copy_progress: (bytes: int -> size: int -> unit) ->
    source: Device.location ->
    source_path: Device.file_path ->
    target: Device.location ->
    (file_hash * int, [> `failed ]) r

  val fetch_file:
    on_progress: (bytes: int -> size: int -> unit) ->
    source: Device.location ->
    file_hash ->
    target: Device.location ->
    target_path: Device.file_path ->
    (unit, [> `already_exists | `failed | `not_available ]) r

  val get_object_size: Device.location -> Hash.t -> (int, [> `failed | `not_available ]) r

  val get_file_size: Device.location -> file_hash -> (int, [> `failed ]) r

  val write_root: Device.location list -> root hash -> (unit, [> `failed ]) r

  val read_root: Device.location -> (root hash option, [> `failed ]) r

  val object_is_available: Device.location -> Hash.t -> (bool, [> `failed ]) r

  val file_is_available: Device.location -> Hash.t -> (bool, [> `failed ]) r

  val transfer_object:
    on_progress: (bytes: int -> unit) ->
    source: Device.location ->
    target: Device.location ->
    Hash.t ->
    (unit, [> `failed | `not_available ]) r

  val iter_objects: Device.location -> (Hash.t -> (unit, [< `failed ]) r) ->
    (unit, [> `failed ]) r

  val iter_files: Device.location -> (Hash.t -> (unit, [< `failed ]) r) ->
    (unit, [> `failed ]) r

  val check_object_hash:
    Device.location ->
    on_progress: (bytes: int -> size: int -> unit) ->
    Hash.t -> (unit, [> `failed ]) r

  val check_file_hash:
    Device.location ->
    on_progress: (bytes: int -> size: int -> unit) ->
    Hash.t -> (unit, [> `failed ]) r

  val get_reachable_hashes:
    on_progress: (object_count: int -> file_count: int -> unit) ->
    Device.location ->
    (Hash_set.t * Hash_set.t, [> `failed | `no_root ]) r

  val remove_object: Device.location -> Hash.t -> (unit, [> `failed ]) r

  val remove_file: Device.location -> Hash.t -> (unit, [> `failed ]) r
end

module Make (Object: OBJECT): S
  with type 'a object_type = 'a Object.t
   and type root = Object.root
