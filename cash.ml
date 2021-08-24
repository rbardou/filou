open Misc

type status =
  | Does_not_exist
  | Dir
  | File of Hash.t

(* On-disk representation: each directory x/y/z becomes a file .filou/x-/y-/z-/self,
   where self is a list of (filename * hash * size * mtime) entries. *)

type dir_entry =
  {
    hash: Hash.t;
    size: int;
    mtime: float;
  }

module Filename_map = Map.Make (Path.Filename)

type dir = dir_entry Filename_map.t

let write_dir buffer (value: dir) =
  W.(string @@ fun _ _ -> ()) buffer "Fcashdi1";
  W.int_32 buffer (Filename_map.cardinal value);
  Filename_map.iter' value @@ fun filename { hash; size; mtime } ->
  State.Object.write_filename buffer filename;
  State.Object.write_concrete_hash buffer hash;
  W.int_64 buffer size;
  W.float_64 buffer mtime

let read_dir buffer: dir =
  let typ = R.(string @@ fun _ -> 8) buffer in
  if typ <> "Fcashdi1" then
    raise (Failed [ sf "unknown cash dir file type: %S" typ ]);
  let count = R.int_32 buffer in
  let result = ref Filename_map.empty in
  for _ = 1 to count do
    let filename = State.Object.read_filename buffer in
    let hash = State.Object.read_concrete_hash buffer in
    let size = R.int_64 buffer in
    let mtime = R.float_64 buffer in
    result := Filename_map.add filename { hash; size; mtime } !result
  done;
  !result

module Device_path =
struct
  type t = Device.path
  let compare = Device.compare_paths
end

module Path_map = Map.Make (Device_path)

(* In-memory cache: files that we don't need to save. *)
let up_to_date: dir Path_map.t ref = ref Path_map.empty

(* In-memory cache: files that we modified and that we should eventually save
   (we don't want to write to disk every time we update an entry). *)
let modified: dir Path_map.t ref = ref Path_map.empty

let next_save = ref None

let delay_between_saves = 1.

let add_dash filename = Path.Filename.parse_exn (Path.Filename.show filename ^ "-")

let cash_filename = Path.Filename.parse_exn "cash"
let self_filename = Path.Filename.parse_exn "self"

let self (dir_path: Device.path): Device.file_path =
  cash_filename :: List.map add_dash dir_path, self_filename

let save (setup: Setup.t) =
  match setup.clone_dot_filou with
    | None ->
        ()
    | Some clone_dot_filou ->
        match Device.mode clone_dot_filou with
          | RO ->
              ()
          | RW ->
              (
                Path_map.iter' !modified @@ fun dir_path dir ->
                if Filename_map.is_empty dir then
                  match Device.remove_file clone_dot_filou (self dir_path) with
                    | ERROR { code = `failed; msg } ->
                        warn_msg msg "failed to remove hash cash"
                    | ERROR { code = `no_such_file; _ } | OK () ->
                        ()
                else
                  let contents =
                    W.to_string @@ fun buffer ->
                    write_dir buffer dir
                  in
                  match
                    Device.write_file clone_dot_filou (self dir_path) contents
                  with
                    | ERROR { code = `failed; msg } ->
                        warn_msg msg "failed to write hash cash"
                    | OK () ->
                        ()
              );
              up_to_date := (
                Path_map.merge' !up_to_date !modified @@ fun _ up_to_date modified ->
                match up_to_date, modified with x, None | _, (Some _ as x) -> x
              );
              modified := Path_map.empty

let maybe_save setup =
  let now = Unix.gettimeofday () in
  match !next_save with
    | None ->
        next_save := Some (now +. delay_between_saves)
    | Some next_save ->
        if now >= next_save then
          save setup

let read_cache_for_dir (setup: Setup.t) (dir_path: Device.path) =
  match Path_map.find_opt dir_path !modified with
    | Some dir ->
        dir
    | None ->
        match Path_map.find_opt dir_path !up_to_date with
          | Some dir ->
              dir
          | None ->
              match setup.clone_dot_filou with
                | None ->
                    Filename_map.empty
                | Some clone_dot_filou ->
                    match Device.read_file clone_dot_filou (self dir_path) with
                      | ERROR { code = `failed; msg } ->
                          warn_msg msg "failed to read hash cash";
                          Filename_map.empty
                      | ERROR { code = `no_such_file; _ } ->
                          Filename_map.empty
                      | OK contents ->
                          match decode_rawbin_string read_dir contents with
                            | ERROR { code = `failed; msg } ->
                                warn_msg msg "failed to decode hash cash";
                                Filename_map.empty
                            | OK dir ->
                                up_to_date := Path_map.add dir_path dir !up_to_date;
                                dir

let set_cache setup ((dir_path, filename): Device.file_path) (value: dir_entry option) =
  let old_dir = read_cache_for_dir setup dir_path in
  let new_dir =
    match value with
      | None ->
          Filename_map.remove filename old_dir
      | Some value ->
          Filename_map.add filename value old_dir
  in
  up_to_date := Path_map.remove dir_path !up_to_date;
  (* We add the map to [modified] even if it's empty, so that once we save,
     we notice that we can now remove the file. *)
  modified := Path_map.add dir_path new_dir !modified;
  maybe_save setup

let get ~on_progress (setup: Setup.t) ((dir_path, filename) as file_path: Device.file_path) =
  match setup.workdir with
    | None ->
        ok Does_not_exist
    | Some workdir ->
        match Device.stat workdir (Device.path_of_file_path file_path) with
          | ERROR { code = `failed; _ } as x ->
              x
          | ERROR { code = `no_such_file; _ } ->
              ok Does_not_exist
          | OK Dir ->
              set_cache setup file_path None;
              ok Dir
          | OK (File { size; mtime }) ->
              let dir = read_cache_for_dir setup dir_path in
              let from_cache =
                match Filename_map.find_opt filename dir with
                  | Some dir_entry ->
                      if dir_entry.size = size && dir_entry.mtime = mtime then
                        Some dir_entry
                      else
                        None
                  | None ->
                      None
              in
              match from_cache with
                | Some dir_entry ->
                    ok (File dir_entry.hash)
                | None ->
                    (* TODO: progress *)
                    match
                      Device.hash ~on_progress workdir file_path
                    with
                      | ERROR { code = `failed; _ } as x ->
                          x
                      | ERROR { code = `no_such_file; _ } ->
                          ok Does_not_exist
                      | OK (hash, _) ->
                          let dir_entry = { hash; size; mtime } in
                          set_cache setup file_path (Some dir_entry);
                          ok (File hash)

let set (setup: Setup.t) file_path hash =
  match setup.workdir with
    | None ->
        ()
    | Some workdir ->
        match Device.stat workdir (Device.path_of_file_path file_path) with
          | ERROR { code = (`failed | `no_such_file); _ } | OK Dir ->
              ()
          | OK (File { size; mtime }) ->
              set_cache setup file_path (Some { hash; size; mtime })
