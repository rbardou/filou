# Filou

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

### Clone_cache

Clone caches provide a way to synchronize two repositories:
- a main repository;
- a clone repository.

Clone_cache provides a similar interface to Repository, except that:
- objects are written in both repositories (with the exception of file objects, see below);
- to read an object, it first tries to find the object in the cache.

Since objects are identified by their hash, if an object exists in the cache,
it is necessarily equal to the corresponding object in the main repository.

File objects represent files that the user wants to manage.
They can be very large. For this reason, they are not cached.
They are stored as objects in the main repository, but never in the clone.
Instead, the user can request to put a copy in the work directory.
But it does not have to. Since the user may edit this copy, it cannot be trusted
to not have been modified and its hash must be recomputed when it is needed
(although one can use size and modification date to approximate whether this is needed).

### Memory_cache

Memory caches store objects in memory to avoid having to read them from a Device.
To avoid consuming too much memory, file objects are not cached in memory,
and there is a limit to the total size to keep in memory.

Memory_cache provides a similar interface to Repository and Clone_cache.

### State

Module State defines data to be stored in a repository.
It provides the types and encodings, and applies the repository functors.

### Controller

Module Controller implements the various operations that one can do on a repository.
