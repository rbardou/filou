(** PROgress OUTput. *)

(** Declare that [stdout] is not a terminal. *)
val not_a_tty: unit -> unit

(** Output minor progress.

    Minor progress is only printed if nothing has been printed during the last
    [0.1] seconds when on a TTY, or during the last second when not on a TTY
    (i.e. if [not_a_tty] has been called).

    If actually printed, and if on a TTY, this replaces the last progress that was output.

    If not printed, the function given to [minor] is not called. *)
val minor: (unit -> string) -> unit

(** Same as [minor] but not lazy.

    You should only use this for constant strings. *)
val minor_s: string -> unit

(** Output major progress.

    Major progress is always printed.
    When on a TTY, it replaces the last progress that was output. *)
val major: ('a, unit, string, unit) format4 -> 'a

(** Same as [major] but without [printf]-like capability. *)
val major_s: string -> unit

(** Output text followed by a newline character.

    This replaces the last progress that was output, but this text itself
    will not be replaced, it will stay. *)
val echo: ('a, unit, string, unit) format4 -> 'a

(** Same as [echo] but without [printf]-like capability. *)
val echo_s: string -> unit

(** Same as [echo] but the text is not followed by a newline character.

    Also, the output is not flushed.

    Basically the same as [print_string] except that it clears progress. *)
val print: ('a, unit, string, unit) format4 -> 'a

(** Same as [print] but without [printf]-like capability. *)
val print_s: string -> unit

(** Remove the last progress that was output. *)
val clear_progress: unit -> unit
