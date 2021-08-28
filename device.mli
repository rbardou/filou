open Misc

type mode = RW | RO

type location =
  | Local of mode * Device_local.location
  | SSH_filou of mode * Device_ssh_filou.location

val mode: location -> mode

val parse_location: mode -> string -> (location, [> `failed ]) r

val show_location: location -> string

val sublocation: location -> Path.Filename.t -> location

(** Paths relative to a [location], with no [..].

    Can denote directories or files.

    Example: [["a"; "b"; "c"]] denotes ["a/b/c"]. *)
type path = Path.Filename.t list

(** File paths are never empty, as the empty path is [.], which is a directory. *)
type file_path = path * Path.Filename.t

val path_of_file_path: file_path -> path

val file_path_of_path: path -> file_path option

val same_paths: path -> path -> bool

val same_file_paths: file_path -> file_path -> bool

val compare_paths: path -> path -> int

val compare_file_paths: file_path -> file_path -> int

type path_with_kind =
  | Dir of path
  | File of file_path

val show_path_with_kind: path_with_kind -> string

(** Convert a string to a repository path.

    Suitable for command-line inputs.

    Also returns:
    - [`dir]: path ends with a directory separator;
    - [`file]: path doesn't end with a directory separator. *)
val parse_local_path: (Path.absolute, Path.dir) Path.t -> string ->
  (path_with_kind, [> `failed ]) r

(** Convert a path to a string. *)
val show_path: path -> string

(** Convert a path to a string for filtering.

    Paths always start with a [/].
    Paths end with a [/] if [is_dir] is [true]. *)
val show_path_for_filter: path -> is_dir: bool -> string

(** Convert a path to a string. *)
val show_file_path: file_path -> string

type lock

(** Create an empty file.

    If the file already exists, return an error. *)
val lock: location -> file_path -> (lock, [> `failed ]) r

(** Delete a file that was created with [lock]. *)
val unlock: location -> lock -> (unit, [> `failed ]) r

(** Call [lock], run a function, and ensure [unlock] is than called. *)
val with_lock: location -> file_path -> (unit -> ('a, [> `failed ] as 'e) r) -> ('a, 'e) r

(** Read a directory.

    Ignores [.] and [..]. *)
val read_dir: location -> path ->
  (Path.Filename.t list, [> `no_such_file | `failed ]) r

(** Read a directory and iterate on its entries.

    Ignores [.] and [..]. *)
val iter_read_dir: location -> path ->
  (Path.Filename.t -> (unit, [> `no_such_file | `failed ] as 'e) r) ->
  (unit, 'e) r

(** Write a file.

    Create parent directories first.

    If the file already exists, it is overwritten.
    This function guarantees that if the file is written, it is written in full.
    If this function is interrupted it may leave a temporary [.part] file though.
    In case of failures, created parent directories are not deleted. *)
val write_file: location -> file_path -> string -> (unit, [> `failed ]) r

(** Write a file incrementally.

    Usage: [write_file_incrementally location file_path write_contents]

    [write_contents] will be called with a function that it can call to write contents.
    If [write_contents] raises an exception, it is caught and [write_file_incrementally]
    returns [`failed]. *)
val write_file_incrementally: location -> file_path ->
  ((string -> int -> int -> (unit, [> `failed ]) r) -> (unit, [> `failed ] as 'e) r) ->
  (unit, 'e) r

(** Same as [write_file_incrementally] but for [bytes].

    This function should not modify your [bytes]. *)
val write_file_incrementally_bytes: location -> file_path ->
  ((bytes -> int -> int -> (unit, [> `failed ]) r) -> (unit, [> `failed ] as 'e) r) ->
  (unit, 'e) r

(** Read a file completely. *)
val read_file: location -> file_path -> (string, [> `no_such_file | `failed ]) r

(** Read a file incrementally.

    Usage: [read_file_incrementally location file_path read_contents]

    [read_contents] will be called with a function [read_bytes]
    that it can call to read contents. Function [read_bytes] returns [0] for end of file.
    If [read_contents] raises an exception, it is caught and [read_file_incrementally]
    returns [`failed]. *)
val read_file_incrementally: location -> file_path ->
  (
    (bytes -> int -> int -> (int, [> `failed ]) r) ->
    ('a, [> `no_such_file | `failed ] as 'e) r
  ) ->
  ('a, 'e) r

type stat = Device_common.stat =
  | File of { size: int; mtime: float }
  | Dir
  | Link_to_file of { path: Path.file Path.any_relativity; size: int; mtime: float }

val stat: location -> path -> (stat, [> `no_such_file | `failed ]) r

val exists: location -> path -> (bool, [> `failed ]) r

val file_exists: location -> file_path -> (bool, [> `failed ]) r

val dir_exists: location -> path -> (bool, [> `failed ]) r

(** Also returns the size. *)
val hash: on_progress: (bytes: int -> size: int -> unit) ->
  location -> file_path -> (Hash.t * int, [> `no_such_file | `failed ]) r

(** Copy a file.

    Equivalent to a combination of [read_file] and [write_file].
    In particular, this guarantees that if the file is written, it is written in full. *)
val copy_file: on_progress: (int -> unit) ->
  source: (location * file_path) -> target: (location * file_path) ->
  (unit, [> `no_such_file | `failed ]) r

val remove_file: location -> file_path -> (unit, [> `no_such_file | `failed ]) r
