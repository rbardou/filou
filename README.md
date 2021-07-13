# Filou

Warning: this is experimental and may very well cause significant data loss for you!

Filou is similar to Git, but targets the use case of sharing a large amount of large files
among devices. You create a main repository and clone it.
Main repositories are repositories without work directories, i.e. all files are
stored as hashed objects. Clones do not contain your files as hashed objects though,
only metadata: when you pull and push files, they are pulled to and pushed from
a work directory. So files are not present both as objects and as actual files in clones,
which is important for large files.

You don't have to pull all files in clones. You can pull only the files you need.
A typical use case is: need to fetch a particular directory on a new computer?
Clone the main repository locally and just pull this directory. Similarly, you don't
have to pull anything if you just want to push new files.

Contrary to Git this is not a versioning system though. There is an undo history
and you can in particular undo file removals, but once you prune, this undo history
is cleared and files are actually removed forever.

Filou maintains the total size and file count of directories.
This makes it easy to see at a glance what takes space.

Filou also tells you if you add several files with different names but same contents.
The contents of such duplicates are stored only once in the main repository,
but all paths are stored.

## Modules

### Device

Devices abstract file systems, providing read and write access to files and directories.
They only give access from a specific root directory.
Currently the only implementation is a local one, i.e. direct access to the local disk.
But in theory the same interface could be provided for remote devices accessed through,
say, SSH, a custom Filou server, a Filou binary started with SSH,
or HTTP servers (in read-only mode).

### Repository

Repositories abstract maps from hashes to objects.
They use the Device module to store and read files.
Each file represents an object, and the name of the file is the object's hash.
Files are split into subdirectories using a prefix of the hash
to avoid very large directories.

### Clone

Clone caches provide a way to synchronize two repositories:
- a main repository;
- a clone repository.

Clone provides a similar interface to Repository, except that the clone repository
also contains metadata objects (not file objects) so that it can be read when
the main repository is not available.

File objects represent files that the user wants to manage.
They can be very large. For this reason, they are not stored in tho clone.
Instead, the user can request to pull a copy in the work directory.

### State

Module State defines data to be stored in a repository.
It provides the types and encodings, and applies the repository functor.

### Controller

Module Controller implements the various operations that one can do on a repository.
