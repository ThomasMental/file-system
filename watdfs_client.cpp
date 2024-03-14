//
// Starter code for CS 454/654
// You SHOULD change this file
//

#include "watdfs_client.h"
#include "debug.h"
INIT_LOG

#include "rpc.h"

// SETUP AND TEARDOWN
void *watdfs_cli_init(struct fuse_conn_info *conn, const char *path_to_cache,
                      time_t cache_interval, int *ret_code) {
    // TODO: set up the RPC library by calling `rpcClientInit`.
    int ret = rpcClientInit();

    // TODO: check the return code of the `rpcClientInit` call
    // `rpcClientInit` may fail, for example, if an incorrect port was exported.
    if(ret < 0) { 
        DLOG("Failed to initialize RPC Client.");
    }
    else {
        DLOG("Success to initialize RPC Client.");
    }

    // It may be useful to print to stderr or stdout during debugging.
    // Important: Make sure you turn off logging prior to submission!
    // One useful technique is to use pre-processor flags like:
    // # ifdef PRINT_ERR
    // std::cerr << "Failed to initialize RPC Client" << std::endl;
    // #endif
    // Tip: Try using a macro for the above to minimize the debugging code.

    // TODO Initialize any global state that you require for the assignment and return it.
    // The value that you return here will be passed as userdata in other functions.
    // In A1, you might not need it, so you can return `nullptr`.
    void *userdata = nullptr;

    // TODO: save `path_to_cache` and `cache_interval` (for A3).

    // TODO: set `ret_code` to 0 if everything above succeeded else some appropriate
    // non-zero value.
    *ret_code = ret;

    // Return pointer to global state data.
    return userdata;
}

void watdfs_cli_destroy(void *userdata) {
    // TODO: clean up your userdata state.

    // TODO: tear down the RPC library by calling `rpcClientDestroy`.
    int ret = rpcClientDestroy();
    if(ret < 0) { 
        DLOG("Failed to Destroy RPC Client.");
    }
    else {
        DLOG("Success to Destroy RPC Client.");
    }
}

// GET FILE ATTRIBUTES
int watdfs_cli_getattr(void *userdata, const char *path, struct stat *statbuf) {
    // SET UP THE RPC CALL
    DLOG("watdfs_cli_getattr called for '%s'", path);
    
    // set up the args
    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    // path: input, type: ARG_CHAR
    int pathlen = strlen(path) + 1;
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    args[0] = (void *)path;

    // statbuf: output, type: ARG_CHAR
    arg_types[1] = (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint) sizeof(struct stat); // statbuf
    args[1] = (void *)statbuf;

    // retcode: output, type: ARG_INT
    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int ret = 0;
    args[2] = (void *)&ret;

    // no corresponding args
    arg_types[3] = 0;

    // make RPC call
    int rpc_ret = rpcCall((char *)"getattr", arg_types, args);

    // handle return
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("getattr rpc failed with error '%d'", rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = ret;
    }

    if (fxn_ret < 0) {
        // If the return code of watdfs_cli_getattr is negative (an error), then 
        // we need to make sure that the stat structure is filled with 0s. Otherwise,
        // FUSE will be confused by the contradicting return values.
        memset(statbuf, 0, sizeof(struct stat));
    }

    delete []args;
    return fxn_ret;
}

// CREATE, OPEN AND CLOSE
int watdfs_cli_mknod(void *userdata, const char *path, mode_t mode, dev_t dev) {
    // Called to create a file.
    DLOG("watdfs_cli_mknod create file for '%s'", path);
    
    // set up the args
    int ARG_COUNT = 4;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    // path: input, type: ARG_CHAR
    int pathlen = strlen(path) + 1;
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    args[0] = (void *)path;

    // mode: input, type: ARG_INT
    arg_types[1] = (1u << ARG_INPUT) | (ARG_INT << 16u);
    args[1] = (void *)&mode;

    // dev: input, type: ARG_LONG
    arg_types[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[2] = (void *)&dev;

    // retcode: output, type: ARG_INT
    arg_types[3] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int ret = 0;
    args[3] = (void *)&ret;

    // no corresponding args
    arg_types[4] = 0;

    // make RPC call
    int rpc_ret = rpcCall((char *)"mknod", arg_types, args);

    // handle return
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("mknod rpc failed with error '%d'", rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = ret;
    }

    delete []args;
    return fxn_ret;
}


int watdfs_cli_open(void *userdata, const char *path,
                    struct fuse_file_info *fi) {
    // Called during open.
    // You should fill in fi->fh.
    DLOG("watdfs_cli_open called for '%s'", path);
    
    // set up the args
    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    // path: input, type: ARG_CHAR
    int pathlen = strlen(path) + 1;
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    args[0] = (void *)path;

    // fi: input, output, type: ARG_CHAR
    arg_types[1] = (1u << ARG_INPUT) | (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | 
                   (ARG_CHAR << 16u) | (uint) sizeof(struct fuse_file_info); 
    args[1] = (void *)fi;

    // retcode: output, type: ARG_INT
    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int ret = 0;
    args[2] = (void *)&ret;

    // no corresponding args
    arg_types[3] = 0;

    // make RPC call
    int rpc_ret = rpcCall((char *)"open", arg_types, args);

    // handle return
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("open rpc failed with error '%d'", rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = ret;
    }

    delete []args;
    return fxn_ret;
}

int watdfs_cli_release(void *userdata, const char *path,
                       struct fuse_file_info *fi) {
     // Called during open.
    // You should fill in fi->fh.
    DLOG("watdfs_cli_close called for '%s'", path);
    
    // set up the args
    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    // path: input, type: ARG_CHAR
    int pathlen = strlen(path) + 1;
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    args[0] = (void *)path;

    // fi: input, type: ARG_CHAR
    arg_types[1] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 
                   (uint) sizeof(struct fuse_file_info); 
    args[1] = (void *)fi;

    // retcode: output, type: ARG_INT
    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int ret = 0;
    args[2] = (void *)&ret;

    // no corresponding args
    arg_types[3] = 0;

    // make RPC call
    int rpc_ret = rpcCall((char *)"release", arg_types, args);

    // handle return
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("release rpc failed with error '%d'", rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = ret;
    }

    delete []args;
    return fxn_ret;
}

// READ AND WRITE DATA
int watdfs_cli_read(void *userdata, const char *path, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi) {
    // Read size amount of data at offset of file into buf.

    DLOG("watdfs_cli_read called for '%s'", path);
    DLOG("Read %ld amount of data", size);
    // set up the args
    int ARG_COUNT = 6;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    // path: input, type: ARG_CHAR
    int pathlen = strlen(path) + 1;
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    args[0] = (void *)path;

    // buf: output, type: ARG_CHAR
    // handle it in the loop

    // size：input, type: ARG_LONG
    arg_types[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[2] = (void *)&size;

    // offset: input, type: ARG_LONG
    arg_types[3] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[3] = (void *)&offset;
    
    // fi: input, type: ARG_CHAR
    arg_types[4] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 
                   (uint) sizeof(struct fuse_file_info); 
    args[4] = (void *)fi;

    // retcode: output, type: ARG_INT
    arg_types[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int ret = 0;
    args[5] = (void *)&ret;

    // no corresponding args
    arg_types[6] = 0;
    
    // make RPC call
    // Handle size greater than the maximum array size of the RPC.
    int fxn_ret = 0;
    size_t read_size;
    int remain_size = size;
    while(remain_size > 0) {
        // handle the large size 
        if(remain_size >= MAX_ARRAY_LEN) {
            read_size = MAX_ARRAY_LEN;
            DLOG("Large read");
        }
        else {
            read_size = remain_size;
            DLOG("Last read");
        }
        arg_types[1] =
            (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) read_size;
        args[1] = (void *)buf;
        args[2] = (void *)&read_size;
        args[3] = (void *)&offset;
        int rpc_ret = rpcCall((char *)"read", arg_types, args);
        if (rpc_ret < 0) {
            DLOG("read rpc failed with error '%d'", rpc_ret);
            fxn_ret = -EINVAL;
            break;
        } else {
            fxn_ret += ret;
        }

        offset += read_size;
        buf += read_size;
        remain_size -= read_size;
    }

    delete []args;
    DLOG("Read bytes: %d", fxn_ret);
    return fxn_ret;
}

int watdfs_cli_write(void *userdata, const char *path, const char *buf,
                     size_t size, off_t offset, struct fuse_file_info *fi) {
    // Write size amount of data at offset of file from buf.
    DLOG("watdfs_cli_write called for '%s'", path);
    DLOG("Write %ld amount of data", size);
    // set up the args
    int ARG_COUNT = 6;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    // path: input, type: ARG_CHAR
    int pathlen = strlen(path) + 1;
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    args[0] = (void *)path;

    // buf: input, type: ARG_CHAR
    // handle it in the loop

    // size：input, type: ARG_LONG
    arg_types[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[2] = (void *)&size;

    // offset: input, type: ARG_LONG
    arg_types[3] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[3] = (void *)&offset;
    
    // fi: input, type: ARG_CHAR
    arg_types[4] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 
                   (uint) sizeof(struct fuse_file_info); 
    args[4] = (void *)fi;

    // retcode: output, type: ARG_INT
    arg_types[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int ret = 0;
    args[5] = (void *)&ret;

    // no corresponding args
    arg_types[6] = 0;
    
    // make RPC call
    // Handle size greater than the maximum array size of the RPC.
    int fxn_ret = 0;
    size_t write_size;
    int remain_size = size;
    while(remain_size > 0) {
        // handle the large size 
        if(remain_size >= MAX_ARRAY_LEN) {
            write_size = MAX_ARRAY_LEN;
            DLOG("Large write");
        }
        else {
            write_size = remain_size;
            DLOG("Last write");
        }
        arg_types[1] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) write_size;
        args[1] = (void *)buf;
        args[2] = (void *)&write_size;
        args[3] = (void *)&offset;
        int rpc_ret = rpcCall((char *)"write", arg_types, args);
        if (rpc_ret < 0) {
            DLOG("write rpc failed with error '%d'", rpc_ret);
            fxn_ret = -EINVAL;
            break;
        } else {
            fxn_ret += ret;
            DLOG("Finish: %d", fxn_ret);
        }

        offset += write_size;
        buf += write_size;
        remain_size -= write_size;
    }

    delete []args;
    DLOG("Write bytes: %d", fxn_ret);
    return fxn_ret;
}

int watdfs_cli_truncate(void *userdata, const char *path, off_t newsize) {
    // Change the file size to newsize.
    DLOG("watdfs_cli_truncate called for '%s'", path);
    
    // set up the args
    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    // path: input, type: ARG_CHAR
    int pathlen = strlen(path) + 1;
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    args[0] = (void *)path;

    // newsize: input, type: ARG_LONG
    arg_types[1] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[1] = (void *)&newsize;

    // retcode: output, type: ARG_INT
    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int ret = 0;
    args[2] = (void *)&ret;

    // no corresponding args
    arg_types[3] = 0;

    // make RPC call
    int rpc_ret = rpcCall((char *)"truncate", arg_types, args);

    // handle return
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("truncate rpc failed with error '%d'", rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = ret;
    }

    delete []args;
    return fxn_ret;
}

int watdfs_cli_fsync(void *userdata, const char *path,
                     struct fuse_file_info *fi) {
    // Force a flush of file data.
    DLOG("watdfs_cli_fsync called for '%s'", path);
    
    // set up the args
    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    // path: input, type: ARG_CHAR
    int pathlen = strlen(path) + 1;
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    args[0] = (void *)path;

    // fi: input, type: ARG_CHAR
    arg_types[1] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 
                   (uint) sizeof(struct fuse_file_info); 
    args[1] = (void *)fi;

    // retcode: output, type: ARG_INT
    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int ret = 0;
    args[2] = (void *)&ret;

    // no corresponding args
    arg_types[3] = 0;

    // make RPC call
    int rpc_ret = rpcCall((char *)"fsync", arg_types, args);

    // handle return
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("fsync rpc failed with error '%d'", rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = ret;
    }

    delete []args;
    return fxn_ret;
}

// CHANGE METADATA
int watdfs_cli_utimensat(void *userdata, const char *path,
                       const struct timespec ts[2]) {
    // Change file access and modification times.
    DLOG("watdfs_cli_utimensat called for '%s'", path);
    DLOG("Atime: %ld %ld", ts[0].tv_sec, ts[0].tv_nsec);
    DLOG("Mtime: %ld %ld", ts[1].tv_sec, ts[1].tv_nsec);
    
    // set up the args
    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    // path: input, type: ARG_CHAR
    int pathlen = strlen(path) + 1;
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    args[0] = (void *)path;

    // ts: input, type: ARG_CHAR
    arg_types[1] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 
                   (uint) 2 *sizeof(struct timespec); 
    args[1] = (void *)ts;

    // retcode: output, type: ARG_INT
    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int ret = 0;
    args[2] = (void *)&ret;

    // no corresponding args
    arg_types[3] = 0;

    // make RPC call
    int rpc_ret = rpcCall((char *)"utimensat", arg_types, args);

    // handle return
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("utimensat rpc failed with error '%d'", rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = ret;
    }

    delete []args;
    return fxn_ret;
}
