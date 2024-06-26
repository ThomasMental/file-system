
//
// Starter code for CS 454/654
// You SHOULD change this file
//

#include "rpc.h"
#include "debug.h"
INIT_LOG

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>
#include <cstdlib>
#include <fuse.h>
#include <map>
#include <string>
#include "rw_lock.h"

// Global state server_persist_dir.
char *server_persist_dir = nullptr;

// Important: the server needs to handle multiple concurrent client requests.
// You have to be careful in handling global variables, especially for updating them.
// Hint: use locks before you update any global variable.

// store the status of the file
// 0 for read
// 1 for write
struct status {
    int rw;
    rw_lock_t *lock = new rw_lock_t; 
    uint64_t fd;

    // Constructor that initializes member variables
    status(int rw) : rw(rw) {
        rw_lock_init(lock);
    }
};

std::map<std::string, status*> file_status;

// We need to operate on the path relative to the server_persist_dir.
// This function returns a path that appends the given short path to the
// server_persist_dir. The character array is allocated on the heap, therefore
// it should be freed after use.
// Tip: update this function to return a unique_ptr for automatic memory management.
char *get_full_path(char *short_path) {
    int short_path_len = strlen(short_path);
    int dir_len = strlen(server_persist_dir);
    int full_len = dir_len + short_path_len + 1;

    char *full_path = (char *)malloc(full_len);

    // First fill in the directory.
    strcpy(full_path, server_persist_dir);
    // Then append the path.
    strcat(full_path, short_path);
    DLOG("Full path: %s\n", full_path);

    return full_path;
}

// The server implementation of getattr.
int watdfs_getattr(int *argTypes, void **args) {
    // Get the arguments.
    // The first argument is the path relative to the mountpoint.
    char *short_path = (char *)args[0];
    // The second argument is the stat structure, which should be filled in
    // by this function.
    struct stat *statbuf = (struct stat *)args[1];
    // The third argument is the return code, which should be set be 0 or -errno.
    int *ret = (int *)args[2];

    // Get the local file name, so we call our helper function which appends
    // the server_persist_dir to the given path.
    char *full_path = get_full_path(short_path);

    // Initially we set the return code to be 0.
    *ret = 0;

    // TODO: Make the stat system call, which is the corresponding system call needed
    // to support getattr. You should use the statbuf as an argument to the stat system call.
    // Let sys_ret be the return code from the stat system call.
    int sys_ret = stat(full_path, statbuf);

    if (sys_ret < 0) {
        *ret = -errno;
    } else {
        *ret = sys_ret;
    }

    // Clean up the full path, it was allocated on the heap.
    free(full_path);
    DLOG("Returning code: %d", *ret);
    // The RPC call succeeded, so return 0.
    return 0;
}

// The server implementation of mknod.
int watdfs_mknod(int *argTypes, void **args) {
    // path, mode, dev, retcode
    char *short_path = (char *)args[0];
    mode_t *mode = (mode_t *)args[1];
    dev_t *dev = (dev_t *)args[2];
    int *ret = (int *)args[3];
    char *full_path = get_full_path(short_path);

    // mknod system call
    *ret = 0;
    int sys_ret = mknod(full_path, *mode, *dev);
    if (sys_ret < 0) {
        *ret = -errno;
    } else {
        *ret = sys_ret;
    }

    // Clean up and return
    free(full_path);
    DLOG("Returning code: %d", *ret);
    return 0;
}

// The server implementation of open.
int watdfs_open(int *argTypes, void **args) {
    // path, fi, retcode
    char *short_path = (char *)args[0];
    struct fuse_file_info *fi = (struct fuse_file_info *)args[1];
    int *ret = (int *)args[2];
    char *full_path = get_full_path(short_path);

    // check if the file is initialized in the status
    if(file_status.find(std::string(short_path)) == file_status.end()) {
        // the file is opened for the first time
        int rw;
        if((fi->flags & O_ACCMODE) != O_RDONLY) {
            // write mode
            rw = 1;
        }
        else {
            // read mode 
            rw = 0;
        }
        file_status[std::string(short_path)] = new status(rw);
        DLOG("The file status is initialized.");
    }
    else {
        // the file is not opened for the first time
        // check file read/write status
        if(file_status[std::string(short_path)]->rw == 1) {
            // the file is in the writing mode
            DLOG("The file mode is write.");
            if((fi->flags & O_ACCMODE)  != O_RDONLY) {
                // the client also try to write the file, return error
                return -EACCES;
            }
        }
        else {
            // the file is in the reading mode
            DLOG("The file mode is read.");
            if((fi->flags & O_ACCMODE)  != O_RDONLY) {
                // the client try to write the file, record write mode
                file_status[std::string(short_path)]->rw = 1;
            }
        }
    }
    
    // open system call
    *ret = 0;
    int sys_ret = open(full_path, O_RDWR);

    if (sys_ret < 0) {
        *ret = -errno;
    }
    else {
        fi->fh = sys_ret;
    }

    if(file_status[std::string(short_path)]->rw == 1 && (fi->flags & O_ACCMODE) != O_RDONLY) {
        file_status[std::string(short_path)]->fd = fi->fh;
    }
    
    // Clean up and return
    free(full_path);
    DLOG("Returning code: %d", *ret);
    return 0;
}

// The server implementation of release.
int watdfs_release(int *argTypes, void **args) {
    // path, fi, retcode
    char *short_path = (char *)args[0];
    struct fuse_file_info *fi = (struct fuse_file_info *)args[1];
    int *ret = (int *)args[2];
    char *full_path = get_full_path(short_path);

    // release system call
    *ret = 0;
    int sys_ret = close(fi->fh);

    // close the file and clean the status
    if(file_status[std::string(short_path)]->fd == fi->fh) {
        file_status[std::string(short_path)]->rw = 0;
    }


    if (sys_ret < 0) {
        *ret = -errno;
    } else {
        *ret = sys_ret;
    }

    // Clean up and return
    free(full_path);
    DLOG("Returning code: %d", *ret);
    return 0;
}

// The server implementation of read.
int watdfs_read(int *argTypes, void **args) {
    // path, buf, size, offset, fi, retcode
    char *short_path = (char *)args[0];
    char *buf = (char *)args[1];
    size_t *size = (size_t *)args[2];
    off_t *offset = (off_t *)args[3];
    struct fuse_file_info *fi = (struct fuse_file_info *)args[4];
    int *ret = (int *)args[5];
    char *full_path = get_full_path(short_path);

    // read system call
    *ret = 0;
    int sys_ret = pread(fi->fh, buf, *size, *offset);
    DLOG("Buffer: %s", buf);
    if (sys_ret < 0) {
        *ret = -errno;
    } else {
        *ret = sys_ret;
    }

    // Clean up and return
    free(full_path);
    DLOG("Number Read: %d", *ret);
    return 0;
}

// The server implementation of write.
int watdfs_write(int *argTypes, void **args) {
    // path, buf, size, offset, fi, retcode
    char *short_path = (char *)args[0];
    char *buf = (char *)args[1];
    size_t *size = (size_t *)args[2];
    off_t *offset = (off_t *)args[3];
    struct fuse_file_info *fi = (struct fuse_file_info *)args[4];
    int *ret = (int *)args[5];
    char *full_path = get_full_path(short_path);

    // write system call
    *ret = 0;
    DLOG("Size is %ld", *size);
    int sys_ret = pwrite(fi->fh, buf, *size, *offset);
    if (sys_ret < 0) {
        *ret = -errno;
    } else {
        *ret = sys_ret;
    }

    // Clean up and return
    free(full_path);
    DLOG("Number Write: %d", *ret);
    return 0;
}

// The server implementation of truncate.
int watdfs_truncate(int *argTypes, void **args) {
    // path, newsize, retcode
    char *short_path = (char *)args[0];
    off_t *newsize = (off_t *)args[1];
    int *ret = (int *)args[2];
    char *full_path = get_full_path(short_path);

    // truncate system call
    *ret = 0;
    int sys_ret = truncate(full_path, *newsize);
    if (sys_ret < 0) {
        *ret = -errno;
    } else {
        *ret = sys_ret;
    }

    // Clean up and return
    free(full_path);
    DLOG("Returning code: %d", *ret);
    return 0;
}

// The server implementation of fsync.
int watdfs_fsync(int *argTypes, void **args) {
    // path, fi, retcode
    char *short_path = (char *)args[0];
    struct fuse_file_info *fi = (struct fuse_file_info *)args[1];
    int *ret = (int *)args[2];
    char *full_path = get_full_path(short_path);

    // fsync system call
    *ret = 0;
    int sys_ret = fsync(fi->fh);
    if (sys_ret < 0) {
        *ret = -errno;
    } else {
        *ret = sys_ret;
    }

    // Clean up and return
    free(full_path);
    DLOG("Returning code: %d", *ret);
    return 0;
}

// The server implementation of utimensat.
int watdfs_utimensat(int *argTypes, void **args) {
    // path, ts, retcode
    char *short_path = (char *)args[0];
    struct timespec *ts = (struct timespec *)args[1];
    int *ret = (int *)args[2];
    char *full_path = get_full_path(short_path);
    DLOG("Atime: %ld %ld", ts[0].tv_sec, ts[0].tv_nsec);
    DLOG("Mtime: %ld %ld", ts[1].tv_sec, ts[1].tv_nsec);

    // utimensat system call
    *ret = 0;
    int sys_ret = utimensat(0, full_path, ts, 0);
    if (sys_ret < 0) {
        *ret = -errno;
    } else {
        *ret = sys_ret;
    }

    // Clean up and return
    free(full_path);
    DLOG("Returning code: %d", *ret);
    return 0;
}

// The server implementation of lock.
int watdfs_lock(int *argTypes, void **args) {
    // path, rw_lock_mode_t, retcode
    char *short_path = (char *)args[0];
    char *full_path = get_full_path(short_path);
    rw_lock_mode_t *mode = (rw_lock_mode_t *)args[1];
    int *ret = (int *)args[2];

    rw_lock_t *lock = file_status[std::string(short_path)]->lock;

    *ret = 0;
    int sys_ret = rw_lock_lock(lock, *mode);
    if (sys_ret < 0) {
        *ret = -errno;
        DLOG("Fail to lock");
    } else {
        *ret = sys_ret;
    }

    // Clean up and return
    free(full_path);
    DLOG("Returning code: %d", *ret);
    return 0;
}

// The server implementation of lock.
int watdfs_unlock(int *argTypes, void **args) {
    // path, rw_lock_mode_t, retcode
    char *short_path = (char *)args[0];
    char *full_path = get_full_path(short_path);
    rw_lock_mode_t *mode = (rw_lock_mode_t *)args[1];
    int *ret = (int *)args[2];

    rw_lock_t *lock = file_status[std::string(short_path)]->lock;

    *ret = 0;
    int sys_ret = rw_lock_unlock(lock, *mode);
    if (sys_ret < 0) {
        *ret = -errno;
    } else {
        *ret = sys_ret;
    }

    // Clean up and return
    free(full_path);
    DLOG("Returning code: %d", *ret);
    return 0;
}

// The main function of the server.
int main(int argc, char *argv[]) {
    // argv[1] should contain the directory where you should store data on the
    // server. If it is not present it is an error, that we cannot recover from.
    if (argc != 2) {
        // In general, you shouldn't print to stderr or stdout, but it may be
        // helpful here for debugging. Important: Make sure you turn off logging
        // prior to submission!
        // See watdfs_client.cpp for more details
        // # ifdef PRINT_ERR
        // std::cerr << "Usage:" << argv[0] << " server_persist_dir";
        // #endif
        return -1;
    }
    // Store the directory in a global variable.
    server_persist_dir = argv[1];

    // TODO: Initialize the rpc library by calling `rpcServerInit`.
    // Important: `rpcServerInit` prints the 'export SERVER_ADDRESS' and
    // 'export SERVER_PORT' lines. Make sure you *do not* print anything
    // to *stdout* before calling `rpcServerInit`.
    DLOG("Initializing server...");

    int ret = rpcServerInit();
    // TODO: If there is an error with `rpcServerInit`, it maybe useful to have
    // debug-printing here, and then you should return.
    if(ret < 0) { 
        DLOG("Failed to Initialize RPC Server.");
        return ret;
    }

    // TODO: Register your functions with the RPC library.
    // Note: The braces are used to limit the scope of `argTypes`, so that you can
    // reuse the variable for multiple registrations. Another way could be to
    // remove the braces and use `argTypes0`, `argTypes1`, etc.

    // getattr
    {
        // There are 3 args for the function (see watdfs_client.cpp for more
        // detail).
        int argTypes[4];
        // First is the path.
        argTypes[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // The second argument is the statbuf.
        argTypes[1] =
            (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // The third argument is the retcode.
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // Finally we fill in the null terminator.
        argTypes[3] = 0;

        // We need to register the function with the types and the name.
        ret = rpcRegister((char *)"getattr", argTypes, watdfs_getattr);
        if (ret < 0) {
            DLOG("Failed to register the RPC: getattr");
            return ret;
        }
    }

    // mknod
    {
        int argTypes[5];
        // path: input, type: ARG_CHAR
        argTypes[0] = 
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // mode: input, type: ARG_INT
        argTypes[1] = (1u << ARG_INPUT) | (ARG_INT << 16u);
        // dev: input, type: ARG_LONG
        argTypes[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
        // retcode: output, type: ARG_INT
        argTypes[3] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // null terminator.
        argTypes[4] = 0;

        ret = rpcRegister((char *)"mknod", argTypes, watdfs_mknod);
        if (ret < 0) {
            DLOG("Failed to register the RPC: mknod");
            return ret;
        }
    }

    // open
    {
        int argTypes[4];
        // path: input, type: ARG_CHAR
        argTypes[0] = 
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // fi: input, output, type: ARG_char
        argTypes[1] = (1u << ARG_INPUT) | (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | 
                      (ARG_CHAR << 16u) | 1u; 
        // retcode: output, type: ARG_INT
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // null terminator.
        argTypes[3] = 0;

        ret = rpcRegister((char *)"open", argTypes, watdfs_open);
        if (ret < 0) {
            DLOG("Failed to register the RPC: open");
            return ret;
        }
    }

    // release
    {
        int argTypes[4];
        // path: input, type: ARG_CHAR
        argTypes[0] = 
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // fi: input, output, type: ARG_char
        argTypes[1] = 
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u; 
        // retcode: output, type: ARG_INT
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // null terminator.
        argTypes[3] = 0;

        ret = rpcRegister((char *)"release", argTypes, watdfs_release);
        if (ret < 0) {
            DLOG("Failed to register the RPC: release");
            return ret;
        }
    }

    // read
    {
        int argTypes[7];
        // path: input, type: ARG_CHAR
        argTypes[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // buf: output, type: ARG_CHAR
        argTypes[1] =
            (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // size：input, type: ARG_LONG
        argTypes[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
        // offset: input, type: ARG_LONG
        argTypes[3] = (1u << ARG_INPUT) | (ARG_LONG << 16u);   
        // fi: input, type: ARG_CHAR
        argTypes[4] = 
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // retcode: output, type: ARG_INT
        argTypes[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // null terminator.
        argTypes[6] = 0;

        ret = rpcRegister((char *)"read", argTypes, watdfs_read);
        if (ret < 0) {
            DLOG("Failed to register the RPC: read");
            return ret;
        }
    }

    // write
    {
        int argTypes[7];
        // path: input, type: ARG_CHAR
        argTypes[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // buf: output, type: ARG_CHAR
        argTypes[1] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // size：input, type: ARG_LONG
        argTypes[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
        // offset: input, type: ARG_LONG
        argTypes[3] = (1u << ARG_INPUT) | (ARG_LONG << 16u);   
        // fi: input, type: ARG_CHAR
        argTypes[4] = 
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // retcode: output, type: ARG_INT
        argTypes[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // null terminator.
        argTypes[6] = 0;

        ret = rpcRegister((char *)"write", argTypes, watdfs_write);
        if (ret < 0) {
            DLOG("Failed to register the RPC: write");
            return ret;
        }
    }

    // truncate
    {
        int argTypes[4];
        // path: input, type: ARG_CHAR
        argTypes[0] = 
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // fi: input, type: ARG_LONG
        argTypes[1] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
        // retcode: output, type: ARG_INT
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // null terminator.
        argTypes[3] = 0;

        ret = rpcRegister((char *)"truncate", argTypes, watdfs_truncate);
        if (ret < 0) {
            DLOG("Failed to register the RPC: truncate");
            return ret;
        }
    }

    // fsync
    {
        int argTypes[4];
        // path: input, type: ARG_CHAR
        argTypes[0] = 
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // fi: input, type: ARG_char
        argTypes[1] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // retcode: output, type: ARG_INT
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // null terminator.
        argTypes[3] = 0;

        ret = rpcRegister((char *)"fsync", argTypes, watdfs_fsync);
        if (ret < 0) {
            DLOG("Failed to register the RPC: fsync");
            return ret;
        }
    }

    // utimensat
    {
        int argTypes[4];
        // path: input, type: ARG_CHAR
        argTypes[0] = 
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // ts: input, type: ARG_char
        argTypes[1] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // retcode: output, type: ARG_INT
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // null terminator.
        argTypes[3] = 0;

        ret = rpcRegister((char *)"utimensat", argTypes, watdfs_utimensat);
        if (ret < 0) {
            DLOG("Failed to register the RPC: utimensat");
            return ret;
        }
    }

    // lock
    {
        int argTypes[3];
        // path: input, type: ARG_CHAR
        argTypes[0] = 
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // rw_lock_mode_t: input, type: ARG_int
        argTypes[1] = (1u << ARG_INPUT) |  (ARG_INT << 16u);
        // retcode: output, type: ARG_INT
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // null terminator.
        argTypes[3] = 0;

        ret = rpcRegister((char *)"lock", argTypes, watdfs_lock);
        if (ret < 0) {
            DLOG("Failed to register the RPC: lock");
            return ret;
        }
    }

    // unlock
    {
        int argTypes[3];
        // path: input, type: ARG_CHAR
        argTypes[0] = 
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // rw_lock_mode_t: input, type: ARG_int
        argTypes[1] = (1u << ARG_INPUT) |  (ARG_INT << 16u);
        // retcode: output, type: ARG_INT
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // null terminator.
        argTypes[3] = 0;

        ret = rpcRegister((char *)"unlock", argTypes, watdfs_unlock);
        if (ret < 0) {
            DLOG("Failed to register the RPC: unlock");
            return ret;
        }
    }

    // Hand over control to the RPC library by calling `rpcExecute`.
    ret = rpcExecute();
    if(ret < 0) { 
        DLOG("Failed to Execute the RPC.");
        return ret;
    }

    return ret;
}
