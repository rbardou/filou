open Misc

type 'a hash

val hash_type: 'a Protype.t -> 'a hash Protype.t
val hex_of_hash: _ hash -> string
val bin_of_hash: _ hash -> string
val compare_hashes: _ hash -> _ hash -> int
val hash_of_hash: _ hash -> Hash.t

module Hash_set: Set.S with type elt = Hash.t

type file

val file_hash_type: file hash Protype.t

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

  (** Activate read-only mode.

      In read-only mode, nothing is written on the device, but
      non-file objects can still be fetched, including those that were
      "stored" during read-only mode, and file sizes can still be read
      with [get_file_size], even for files that were "stored" during
      read-only mode. Roots are not stored, and [fetch_root] returns
      the last root that was "stored" during read-only
      mode. [garbage_collect] does nothing.

      It is not possible to deactivate read-only mode because files of
      hashes that are obtained in this mode are not actually available,
      so it would be dangerous to store objects, as they could
      reference those files. You can actually still write objects if
      you re-apply the functor, but this is strongly discouraged.

      Read-only mode is meant to implement "dry runs". In particular,
      most read accesses that would occur when storing objects or files
      still occur even if they are not necessary. *)
  val set_read_only: unit -> unit

  (** Return whether [set_read_only] was called. *)
  val is_read_only: unit -> bool

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
  val fetch: t -> 'a hash ->
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

  (** Get the set of objects that are reachable from the root.

      If [files] is [false], do not return file hashes. Default is [true]. *)
  val reachable: ?files: bool -> t -> (Hash_set.t, [> `failed ]) r

  (** Remove unreachable objects.

      Return [(count, size)] where [count] is the number of objects that were
      removed and [size] is the sum of the size of those objects.

      If [reachable] is specified, assume that it is the set of reachable
      objects. This can be given to avoid recomputing it if you already
      computed it with [reachable] and did not modify anything in-between. *)
  val garbage_collect: ?reachable: Hash_set.t -> t -> (int * int, [> `failed ]) r

  (** Return whether an object is available. *)
  val available: t -> Hash.t -> (bool, [> `failed ]) r

  (** Copy an object from a repository to another.

      Does nothing if the object is already available in [target]. *)
  val transfer: ?on_progress: (int -> unit) -> source: t -> target: t -> Hash.t ->
    (unit, [> `failed | `not_available ]) r

  (** Copy the root from a repository to another. *)
  val transfer_root: ?on_progress: (int -> unit) -> source: t -> target: t -> unit ->
    (unit, [> `failed ]) r

  (** Check whether an object is corrupted. *)
  val check_hash: t -> Hash.t -> (unit, [> `failed | `corrupted | `not_available ]) r

  (** Get the size of an object on disk. *)
  val get_object_size: t -> Hash.t -> (int, [> `failed | `not_available ]) r
end

module Make (Root: ROOT): S with type root = Root.t and type t = Device.location
