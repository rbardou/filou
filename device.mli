open Misc

type mode = RW | RO

type location =
  | Local of mode * Path.absolute_dir

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

(** Convert a string to a repository path.

    Suitable for command-line inputs. *)
val parse_path: location -> string -> (path, [> `failed ]) r

val parse_file_path: location -> string -> (file_path, [> `failed ]) r

(** Convert a path to a string. *)
val show_path: path -> string

(** Convert a path to a string. *)
val show_file_path: file_path -> string

(** Create a file, do something and ensure the file is then removed.

    If the file already exists, return an error. *)
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

val check_directory_is_empty_or_inexistant: location -> path -> (unit, [> `failed ]) r

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

    [read_contents] will be called with a function that it can call to read contents.
    If [read_contents] raises an exception, it is caught and [read_file_incrementally]
    returns [`failed]. *)
val read_file_incrementally: location -> file_path ->
  (
    (bytes -> int -> int -> (int, [> `failed ]) r) ->
    ('a, [> `no_such_file | `failed ] as 'e) r
  ) ->
  ('a, 'e) r

type stat =
  | File of { path: file_path; size: int }
  | Dir

val stat: location -> path -> (stat, [> `no_such_file | `failed ]) r

val exists: location -> path -> (bool, [> `failed ]) r

val file_exists: location -> file_path -> (bool, [> `failed ]) r

val dir_exists: location -> path -> (bool, [> `failed ]) r

(** Also returns the size. *)
val hash: location -> file_path -> (Hash.t * int, [> `no_such_file | `failed ]) r

(** Copy a file.

    Equivalent to a combination of [read_file] and [write_file].
    In particular, this guarantees that if the file is written, it is written in full. *)
val copy_file: on_progress: (int -> unit) ->
  source: (location * file_path) -> target: (location * file_path) ->
  (unit, [> `no_such_file | `failed ]) r

val remove_file: location -> file_path -> (unit, [> `no_such_file | `failed ]) r
