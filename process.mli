(** Run external commands and read their outputs. *)

(** Run an external command and read its standard and error outputs.

    Usage: [let stdout, stderr = run_and_read executable arguments in ...]

    [executable] is the path to the executable to execute and [arguments]
    are the list of arguments to give it. Note that [arguments] shall not contain
    the executable name (contrary to [Unix.create_process] for instance).
    The executable is searched in the PATH.

    [buffer_size] is the initial size of the internal buffers.
    It is also how many bytes we try to read at a time.
    It does not have to be as big as the expected output.
    Default value is 1024.

    [argv0] is the value of the first item in the argument vector which is
    passed to the executable. Its default value is [executable].

    If [env] is not specified, the new process shares the environment variables
    of the current process. If [env] is specified, it is passed as the new value
    of the environment. It is a list of [(name, value)] pairs where [name]
    is the name of an environment variable and [value] is its value.
    All values that are not specified in [env] are not visible to the new process.

    If [stdin] is not specified, the new process shares the input of the current process.
    If [stdin] is specified, [stdin] is written on the standard input of the new process,
    and this standard input is then closed so that the new process knows that there
    is nothing left to read.

    If [timeout] is specified, kill the process if it does not finish after
    [timeout] seconds and stop reading its output even if there are remaining bytes.

    @raise [Unix.Unix_error] if the process cannot be executed, if its output cannot
    be read, if its input cannot be given, or if the process cannot be closed properly. *)
val run_and_read: ?buffer_size: int -> ?argv0: string -> ?env: (string * string) list ->
  ?stdin: string -> ?timeout: float -> string -> string list ->
  string * string * Unix.process_status
