open Misc

type path = Path.Filename.t list

type file_path = Path.Filename.t list * Path.Filename.t

let path_of_device_path rpath =
  match List.rev rpath with
    | [] ->
        Path.(D Current)
    | head :: tail ->
        let rec dir = function
          | [] ->
              Path.Current
          | head :: tail ->
              Path.Dir (head, dir tail)
        in
        Path.F (Path.File (head, dir tail))

let device_path_of_path full_path =
  let rec gather: 'a. _ -> (Path.relative, 'a) Path.t -> _ =
    fun (type a) acc (path: (Path.relative, a) Path.t) ->
      match path with
        | Current ->
            OK acc
        | Parent _ ->
            failed [ "path contains parent directories (..): " ^ Path.show full_path ]
        | File (filename, parent) ->
            gather (filename :: acc) parent
        | Dir (filename, parent) ->
            gather (filename :: acc) parent
  in
  gather [] full_path

let path_of_file_path ((path, file): file_path) =
  path @ [ file ]

let file_path_of_path (path: path) =
  match List.rev path with
    | [] ->
        None
    | head :: tail ->
        Some (List.rev tail, head)

type path_with_kind =
  | Dir of path
  | File of file_path

let parse_local_path repository_root string =
  match Path.parse string with
    | None ->
        failed [ "invalid path: \"" ^ String.escaped string ^ "\"" ]
    | Some path ->
        match
          Path.Any.to_absolute path
          |> Path.Any_kind.to_relative ~from: repository_root
        with
          | R_empty ->
              OK (Dir [])
          | R_none ->
              failed [
                "path denotes a file that is outside of the repository directory: "
                ^ string
              ]
          | R_some (D path) ->
              let* path = device_path_of_path path in
              ok (Dir path)
          | R_some (F (File (filename, parent))) ->
              let* parent = device_path_of_path parent in
              ok (File (parent, filename))

let show_path (path: path) =
  match path with
    | [] -> "."
    | _ -> String.concat "/" (List.map Path.Filename.show path)

let show_file_path path =
  show_path (path_of_file_path path)

let show_path_with_kind = function
  | Dir path -> show_path path ^ "/"
  | File path -> show_file_path path

let compare_paths a b = String.compare (show_path a) (show_path b)
let compare_file_paths a b = compare_paths (path_of_file_path a) (path_of_file_path b)
let same_paths a b = compare_paths a b = 0
let same_file_paths a b = same_paths (path_of_file_path a) (path_of_file_path b)

type mode = RW | RO

type stat =
  | File of { size: int; mtime: float }
  | Dir
  | Link_to_file of { path: Path.file Path.any_relativity; size: int; mtime: float }

let failed_to_read_directory path msg =
  failed (("failed to read directory: " ^ (Path.show path)) :: msg)

let failed_to_write_file path msg =
  failed (("failed to write file: " ^ path) :: msg)

let failed_to_read_file path msg =
  failed (("failed to read file: " ^ path) :: msg)

let read_only () =
  failed [ "read-only mode is active" ]
