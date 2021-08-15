let run_and_read ?(buffer_size = 1024) ?argv0 ?env ?stdin ?timeout executable arguments =
  let stdin_data =
    match stdin with
      | None -> ""
      | Some data -> data
  in
  (* Some precautions are needed to make sure everything is cleaned up even if an
     exception occurs. *)
  (* File descriptors are represented as [Unix.file_descr option ref]
     values that are set to [None] after they are closed so that:
     - we do not try to use them after they are closed;
     - we do not try to close them twice;
     - we know we still have to close them. *)
  let file_descriptors = ref [] in
  let close_fd fd =
    match !fd with
      | None ->
          ()
      | Some unix_fd ->
          (try Unix.close unix_fd with Unix.Unix_error _ -> ());
          fd := None
  in
  let pipe close_on_exec =
    let exit_fd, entrance_fd = Unix.pipe () in
    let pipe_exit = ref (Some exit_fd) in
    let pipe_entrance = ref (Some entrance_fd) in
    file_descriptors := pipe_exit :: pipe_entrance :: !file_descriptors;
    (
      match close_on_exec with
        | `cloexec_exit -> Unix.set_close_on_exec exit_fd
        | `cloexec_entrance -> Unix.set_close_on_exec entrance_fd
    );
    pipe_exit, pipe_entrance, exit_fd, entrance_fd
  in
  (* The running process is also stored as a ref to a state that can be:
     - [`not_running] (not started);
     - [`running] (SIGKILL was not sent and the pid was not freed);
     - [`killed] (SIGKILL was sent but the pid was not freed);
     - [`finished] (the pid was freed and we have an exit status);
     - [`waitpid_error] (failed to waitpid).
       This prevents killing the process twice or killing a process that does not exist,
       and ensures we know we still have to free the pid. *)
  let process = ref `not_running in
  let kill () =
    match !process with
      | `not_running | `killed _ | `finished _ | `waitpid_error _ ->
          ()
      | `running pid ->
          (* If [kill] fails, we still want to [waitpid] later to avoid a zombie process.
             So we act as if we did kill the process.
             We may block forever on [waitpid] because of this but it is very unlikely
             for [kill] to fail in the first place if the pid was not freed. *)
          (try Unix.kill pid Sys.sigkill with Unix.Unix_error _ -> ());
          process := `killed pid
  in
  let waitpid ~block =
    match !process with
      | `not_running | `finished _ | `waitpid_error _ ->
          ()
      | `killed pid | `running pid ->
          try
            if block then
              let _, status = Unix.waitpid [] pid in
              process := `finished status
            else
              let waitpid_pid, status = Unix.waitpid [ WNOHANG ] pid in
              if waitpid_pid = pid then process := `finished status
          with Unix.Unix_error _ as exn ->
            process := `waitpid_error exn
  in
  let finally () =
    List.iter close_fd !file_descriptors;
    kill ();
    waitpid ~block: true;
  in
  Fun.protect ~finally @@ fun () ->
  (* Start the process. *)
  let stdin, stdout, stderr =
    let argv0 = match argv0 with None -> executable | Some x -> x in
    let argv = Array.of_list (argv0 :: arguments) in
    (* Create pipes.
       The part that will not be used by the child process must be closed on exec
       since the child process will not use it. *)
    let stdin_entrance, stdin_exit_fd, close_stdin_exit =
      match stdin with
        | None ->
            ref None, Unix.stdin, fun () -> ()
        | Some _ ->
            let stdin_exit, stdin_entrance, stdin_exit_fd, _ = pipe `cloexec_entrance in
            stdin_entrance,
            stdin_exit_fd,
            fun () -> close_fd stdin_exit
    in
    let stdout_exit, stdout_entrance, _, stdout_entrance_fd = pipe `cloexec_exit in
    let stderr_exit, stderr_entrance, _, stderr_entrance_fd = pipe `cloexec_exit in
    let pid =
      match env with
        | None ->
            Unix.create_process executable argv
              stdin_exit_fd stdout_entrance_fd stderr_entrance_fd
        | Some env ->
            let env = Array.of_list (List.map (fun (n, v) -> n ^ "=" ^ v) env) in
            Unix.create_process_env executable argv env
              stdin_exit_fd stdout_entrance_fd stderr_entrance_fd
    in
    process := `running pid;
    (* For each pipe, close the part that was given to the child process since
       we will not use it. *)
    close_stdin_exit ();
    close_fd stdout_entrance;
    close_fd stderr_entrance;
    (* Only return what will actually be used by this process. *)
    stdin_entrance, stdout_exit, stderr_exit
  in
  (* The process is running and pipes are set up.
     Now we wait for the process to exit, feeding it input when its stdin is ready,
     and reading its output when its stdout / stderr are ready.
     This needs to be done concurrently, otherwise those pipes could get full. *)
  let end_time =
    match timeout with
      | None ->
          None
      | Some timeout ->
          Some (Unix.gettimeofday () +. timeout)
  in
  let stdin_offset = ref 0 in
  let stdout_buffer = Buffer.create buffer_size in
  let stderr_buffer = Buffer.create buffer_size in
  let bytes = Bytes.create buffer_size in
  while
    (
      match !process with
        | `not_running | `finished _ | `waitpid_error _ ->
            false
        | `running _ | `killed _ ->
            true
    ) ||
    (match !stdin with None -> false | Some _ -> true) ||
    (match !stdout with None -> false | Some _ -> true) ||
    (match !stderr with None -> false | Some _ -> true)
  do
    (* Check the timeout. *)
    let timeout, too_late =
      match end_time with
        | None ->
            -1., false
        | Some end_time ->
            let now = Unix.gettimeofday () in
            if now >= end_time then
              0., true
            else
              end_time -. now, false
    in
    if too_late then
      (* Timeout: close everything and kill the process. *)
      (
        close_fd stdin;
        close_fd stdout;
        close_fd stderr;
        kill ();
        (* This should not block for long since we just sent SIGKILL
           (unless [kill] failed of course - see the comment in [kill]). *)
        waitpid ~block: true;
      )
    else match !stdout, !stderr, !stdin with
      | None, None, None ->
          (* If we are no longer reading the process outputs or writing to its
             standard input, it's time to start checking whether the process
             is still running so that we can eventually stop the loop.
             But if the caller requested a timeout, do not block:
             we may want to send SIGKILL. *)
          waitpid ~block: (match end_time with None -> false | Some _ -> true);
          (
            match !process with
              | `not_running | `finished _ | `waitpid_error _ ->
                  ()
              | `running _ | `killed _ ->
                  (* Wait (so that we don't use all the CPU) and try again.
                     On some operating systems we may be interrupted (with EINTR)
                     when the child terminates to gain time, but:
                     - maybe the child already stopped since we called [waitpid];
                     - some operating systems may not do that;
                     so we cannot count on it. Hence the low delay for the wait. *)
                  try
                    Unix.sleepf 0.001
                  with Unix.Unix_error _ ->
                    (* If sleeping fails for any reason other than EINTR, we will
                       just use all the CPU. *)
                    ()
          )
      | _ ->
          (* At least one file descriptor is still open, so we can call [Unix.select].
             Read what can be read, write if we can. *)
          match
            let select_for_reading =
              List.flatten [
                (match !stdout with None -> [] | Some fd -> [ fd ]);
                (match !stderr with None -> [] | Some fd -> [ fd ]);
              ]
            in
            let select_for_writing = match !stdin with None -> [] | Some fd -> [ fd ] in
            Unix.select select_for_reading select_for_writing [] timeout
          with
            | exception Unix.Unix_error (EINTR, _, _) ->
                (* EINTR can happen in particular when we receive SIGCHLD. *)
                ()
            | ready_to_read, ready_to_write, _ ->
                (
                  match !stdin with
                    | None ->
                        ()
                    | Some fd ->
                        if List.mem fd ready_to_write then
                          let data_len = String.length stdin_data in
                          let remaining_len = data_len - !stdin_offset in
                          if remaining_len > 0 then (
                            let written_len =
                              try
                                Unix.write_substring fd
                                  stdin_data !stdin_offset remaining_len
                              with Unix.Unix_error (EINTR, _, _) ->
                                0
                            in
                            if written_len > 0 then
                              stdin_offset := !stdin_offset + written_len;
                          );
                          if !stdin_offset >= data_len then close_fd stdin
                );
                let read_into output buffer =
                  match !output with
                    | None ->
                        ()
                    | Some fd ->
                        if List.mem fd ready_to_read then
                          match Unix.read fd bytes 0 buffer_size with
                            | exception Unix.Unix_error (EINTR, _, _) ->
                                ()
                            | len ->
                                if len > 0 then
                                  Buffer.add_subbytes buffer bytes 0 len
                                else
                                  close_fd output
                in
                read_into stdout stdout_buffer;
                read_into stderr stderr_buffer
  done;
  let exit_status =
    match !process with
      | `not_running ->
          (* Apparently we didn't even start the process.
             This can only happen if an exception is raised before or by [create_process].
             But if an exception was raised, we do not reach this point. *)
          assert false
      | `running _ | `killed _ ->
          (* We cannot reach this point because the while loop does not stop if the process
             has one of those states. *)
          assert false
      | `waitpid_error exn ->
          raise exn
      | `finished status ->
          status
  in
  Buffer.contents stdout_buffer, Buffer.contents stderr_buffer, exit_status
