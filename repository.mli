open Misc

type 'a hash

val hash_type: _ hash Protype.t
val hex_of_hash: _ hash -> string
val bin_of_hash: _ hash -> string
val compare_hashes: _ hash -> _ hash -> int

module Raw_hash_set:
sig
  type t
  val empty: t
  val add: _ hash -> t -> t
  val mem: _ hash -> t -> bool
end

type file

module type ROOT =
sig
  type t
  val version: int
  val typ: t Protype.t
end

module type S =
sig
  (** Result of [Repository.Make]. *)

  (** Configuration of a repository. *)
  type t

  (** Repository roots. *)
  type root

  (** Encode an object, store it, and return its hash. *)
  val store_now: t -> 'a Protype.t -> 'a ->
    ('a hash, [> `failed ]) r

  (** Same as [store_now], but only store if needed.

      More precisely, the object is only stored if it is referenced by another
      object which is stored using [store_now] or [store_root], or if you [fetch] it.

      If specified, [on_stored] is triggered after a successful store.

      If storing fails, or if [on_stored] fails, the error is caught by [store_now],
      [store_root] or [fetch]. *)
  val store_later: ?on_stored: (unit -> (unit, [ `failed ]) r) ->
    t -> 'a Protype.t -> 'a -> ('a hash, [> `failed ]) r

  (** Fetch an object from its hash and decode it. *)
  val fetch: t -> 'a Protype.t -> 'a hash ->
    ('a, [> `failed | `not_available ]) r

  (** Store a file as an object. *)
  val store_file: source: Device.location -> source_path: Device.file_path -> target: t ->
    on_progress: (bytes: int -> size: int -> unit) ->
    (file hash * int, [> `failed ]) r

  (** Fetch an object into a file.

      Return [false] if the file was not available. *)
  val fetch_file: source: t -> file hash ->
    target: Device.location -> target_path: Device.file_path ->
    on_progress: (bytes: int -> size: int -> unit) ->
    (unit, [> `failed | `not_available | `already_exists ]) r

  (** Get the size of a file, given its hash. *)
  val get_file_size: t -> file hash ->
    (int, [> `failed | `not_available ]) r

  (** Set the root. *)
  val store_root: t -> root ->
    (unit, [> `failed ]) r

  (** Read the root. *)
  val fetch_root: t ->
    (root, [> `failed ]) r

  (** Remove objects which are not in a given hash set. *)
  val garbage_collect: t -> everything_except: Raw_hash_set.t ->
    (unit, [> `failed ]) r
end

module Make (Root: ROOT): S with type root = Root.t and type t = Device.location
