//
// Starter code for CS 454/654
// You SHOULD change this file
//

#include "watdfs_client.h"
#include "debug.h"
INIT_LOG

#include "rpc.h"
#include "rw_lock.h"
#include <map>
#include <string>

// store the status of the file
// 0 for read
// 1 for write
struct status {
    int rw;
    time_t tc;
    int client_fd;
    struct fuse_file_info *server_fi = new struct fuse_file_info;

    // Constructor that initializes member variables
    status(int rw, time_t tc, int client_fd, struct fuse_file_info *fi) : 
        rw(rw), tc(tc), client_fd(client_fd) {
            server_fi->flags = fi->flags;
            server_fi->fh = fi->fh;
        }

    ~status() {
        delete server_fi;
    }
};

struct user_data {
    char *path_to_cache;
    time_t cache_interval;

    // store the status of the file
    // 0 for read
    // 1 for write
    std::map<std::string, status*> file_status;

    // Constructor that initializes member variables
    user_data(const char* path, time_t interval) : path_to_cache(nullptr), cache_interval(interval) {
        if (path != nullptr) {
            // Allocate memory for the path_to_cache and copy the string
            path_to_cache = (char *)path;
        }
    }
};


/*******
 * RPC Function 
 *******/

// GET FILE ATTRIBUTES
int rpc_getattr(void *userdata, const char *path, struct stat *statbuf) {
    // SET UP THE RPC CALL
    DLOG("rpc_getattr called for '%s'", path);
    
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
int rpc_mknod(void *userdata, const char *path, mode_t mode, dev_t dev) {
    // Called to create a file.
    DLOG("rpc_mknod create file for '%s'", path);
    
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


int rpc_open(void *userdata, const char *path,
             struct fuse_file_info *fi) {
    // Called during open.
    // You should fill in fi->fh.
    DLOG("rpc_open called for '%s'", path);
    
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
    } 
    fxn_ret = rpc_ret;

    delete []args;
    return fxn_ret;
}

int rpc_release(void *userdata, const char *path,
                struct fuse_file_info *fi) {
     // Called during open.
    // You should fill in fi->fh.
    DLOG("rpc_close called for '%s'", path);
    
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
int rpc_read(void *userdata, const char *path, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi) {
    // Read size amount of data at offset of file into buf.

    DLOG("rpc_read called for '%s'", path);
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

int rpc_write(void *userdata, const char *path, const char *buf,
                     size_t size, off_t offset, struct fuse_file_info *fi) {
    // Write size amount of data at offset of file from buf.
    DLOG("rpc_write called for '%s'", path);
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

int rpc_truncate(void *userdata, const char *path, off_t newsize) {
    // Change the file size to newsize.
    DLOG("rpc_truncate called for '%s'", path);
    
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

int rpc_fsync(void *userdata, const char *path,
                     struct fuse_file_info *fi) {
    // Force a flush of file data.
    DLOG("rpc_fsync called for '%s'", path);
    
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
int rpc_utimensat(void *userdata, const char *path,
                 const struct timespec ts[2]) {
    // Change file access and modification times.
    DLOG("rpc_utimensat called for '%s'", path);
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

int rpc_lock(void *userdata, const char *path, rw_lock_mode_t mode) {
    DLOG("rpc_lock called.");
    // set up the args
    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    // path: input, type: ARG_CHAR
    int pathlen = strlen(path) + 1;
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    args[0] = (void *)path;

    // rw_lock_mode_t: input, type: ARG_int
    arg_types[1] = (1u << ARG_INPUT) |  (ARG_INT << 16u);
    args[1] = (void *)&mode;

    // retcode: output, type: ARG_INT
    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int ret = 0;
    args[2] = (void *)&ret;

    // no corresponding args
    arg_types[3] = 0;

    // make RPC call
    int rpc_ret = rpcCall((char *)"lock", arg_types, args);

    // handle return
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("lock rpc failed with error '%d'", rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = ret;
    }

    delete []args;
    return fxn_ret;
}

int rpc_unlock(void *userdata, const char *path, rw_lock_mode_t mode) {
    DLOG("rpc_unlock called.");
    // set up the args
    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    // path: input, type: ARG_CHAR
    int pathlen = strlen(path) + 1;
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    args[0] = (void *)path;

    // rw_lock_mode_t: input, type: ARG_int
    arg_types[1] = (1u << ARG_INPUT) |  (ARG_INT << 16u);
    args[1] = (void *)&mode;

    // retcode: output, type: ARG_INT
    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int ret = 0;
    args[2] = (void *)&ret;

    // no corresponding args
    arg_types[3] = 0;
    
    // make RPC call
    int rpc_ret = rpcCall((char *)"unlock", arg_types, args);

    // handle return
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("unlock rpc failed with error '%d'", rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = ret;
    }

    delete []args;
    return fxn_ret;
}

/*******
 * Upload/Download functions
 *******/

// find the local full file path to cache directory
char *get_full_path(struct user_data *userdata, const char *short_path) {
    int short_path_len = strlen(short_path);
    int dir_len = strlen(userdata->path_to_cache);
    int full_len = dir_len + short_path_len + 1;

    char *full_path = (char *)malloc(full_len);

    // First fill in the directory.
    strcpy(full_path, userdata->path_to_cache);
    // Then append the path.
    strcat(full_path, short_path);
    DLOG("Full path: %s\n", full_path);

    return full_path;
}

// check the freshness using time 
int freshness_check(struct user_data *userdata, const char *path) {
    // 1 means read local file
    // 2 means update Tc
    // 0 mains file is not fresh
    DLOG("Start freshness check.");

    char *full_path = get_full_path(userdata, path);

    // get the client file metadata
    struct stat *statbuf_client = new struct stat;
    int ret;
    ret = stat(full_path, statbuf_client);
    if(ret < 0) {
        return ret;
    }
    time_t t_client = statbuf_client->st_mtim.tv_nsec;

    // get the server file metadata
    struct stat *statbuf_server = new struct stat;
    ret = rpc_getattr((void *)userdata, path, statbuf_server);
    if(ret < 0) {
        return ret;
    }
    time_t t_server = statbuf_server->st_mtim.tv_nsec;

    // get T and T_c
    time_t t_now = time(nullptr);
    time_t t_c = userdata->file_status[std::string(path)]->tc;
    if((t_now - t_c) < userdata->cache_interval) {
        return 1;
    }

    // compare the time and check freshness
    if((t_client == t_server)) 
    {
        return 2;
    }
    else {
        return 0;
    }

    return 0;
}

// transfer the file from the server to the client
int watdfs_download(struct user_data *userdata, const char *path) {
    // truncate the file at the client, get file attributes from the server,
    // read the file from the server, write the file to the client, and
    // then update the file metadata at the client.
    int fxn_ret = 0;
    char *full_path = get_full_path(userdata, path);

    DLOG("Download Path: %s\n", full_path);

    int ret = truncate(full_path, 0);
    if(ret < 0) {
        DLOG("Error: Failed to reset the file.");
        free(full_path);
        return ret;
    }

    // lock at the start of the download
    ret = rpc_lock((void *)userdata, path, RW_READ_LOCK);
    DLOG("Lock read");
    
    struct stat *statbuf = new struct stat;
    ret = rpc_getattr(userdata, path, statbuf);
    if(ret < 0) {
        DLOG("Error: Failed to get attributes of the file.");
        delete statbuf;
        free(full_path);
        return ret;
    }
    
    // read the server file to local buf
    size_t size = statbuf->st_size; 
    char *buf = (char *)malloc(size);
    struct fuse_file_info *fi = new struct fuse_file_info;
    fi->fh = userdata->file_status[std::string(path)]->server_fi->fh;
    fi->flags = userdata->file_status[std::string(path)]->server_fi->flags;

    ret = rpc_read((void *)userdata, path, buf, size, 0, fi);
    if(ret < 0) {
        DLOG("Error: Failed to read the file.");
        free(full_path);
        free(buf);
        return ret;
    }

    // unlock after finish the download
    ret = rpc_unlock((void *)userdata, path, RW_READ_LOCK);
    DLOG("Unlock read");

    DLOG("Buffer: %s", buf);

    // get the cli_fd from the file status
    int client_fd = userdata->file_status[std::string(path)]->client_fd;
    userdata->file_status[std::string(path)]->tc = time(nullptr);

    // write the data from buf to local file
    ret = pwrite(client_fd, buf, size, 0);
    if(ret < 0) {
        DLOG("Error: Failed to write file.");
        return ret;
    }

    // update the metatdata
    struct timespec ts[2];
    ts[0] = (struct timespec)(statbuf->st_atim);
    ts[1] = (struct timespec)(statbuf->st_mtim);
    ret = utimensat(0, full_path, ts, 0);
    if(ret < 0) {
        DLOG("Error: Failed to change the meta data.");
        free(full_path);
        free(buf);
        return -errno;
    }

    free(full_path);
    free(buf);
    return fxn_ret;
}


// transfer the file from the client to server
int watdfs_upload(struct user_data *userdata, const char *path) {
    // read the file from local cache, truncate and write the file to server,
    // update the metadata on the server
    int fxn_ret = 0;
    int ret;
    char *full_path = get_full_path(userdata, path);

    DLOG("Cache file path: %s\n", full_path);


    struct stat *statbuf = new struct stat;
    ret = stat(full_path, statbuf);
    if(ret < 0) {
        DLOG("Error: Failed to get attributes of the file.");
        delete statbuf;
        free(full_path);
        return ret;
    }

    // read the cache file to local buf
    size_t size = statbuf->st_size; 
    char *buf = (char *)malloc(size);
    int client_fd = userdata->file_status[std::string(path)]->client_fd;
    ret = pread(client_fd, buf, size, 0);
    if(ret < 0) {
        DLOG("Error: Failed to read the file.");
        free(full_path);
        free(buf);
        return ret;
    }

    DLOG("Buffer: %s", buf);
    DLOG("Size: %ld", size);

    // lock at the start of the download
    ret = rpc_lock((void *)userdata, path, RW_WRITE_LOCK);
    DLOG("Lock write");
    
    // write the data from cache to remote file
    ret = rpc_truncate((void *)userdata, path, size);
    if(ret < 0) {
        DLOG("Error: Failed to truncate server file.");
        return ret;
    }

    // write the data from cache to remote file
    ret = rpc_write((void *)userdata, path, buf, size, 0, userdata->file_status[std::string(path)]->server_fi);
    if(ret < 0) {
        DLOG("Error: Failed to write server file.");
        return ret;
    }

    // update the metadata on the server
    struct timespec ts[2];
    ts[0] = (struct timespec)(statbuf->st_atim);
    ts[1] = (struct timespec)(statbuf->st_mtim);
    ret = rpc_utimensat((struct user_data *)userdata, path, ts);
    if(ret < 0) {
        DLOG("Error: Failed to set metadata of server file.");
        return ret;
    }

    // unlock after finish the download
    ret = rpc_unlock((void *)userdata, path, RW_WRITE_LOCK);
    DLOG("Unlock write");

    free(full_path);
    free(buf);
    return fxn_ret;
}

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
    // TODO: save `path_to_cache` and `cache_interval` (for A3).
    struct user_data *userdata = new user_data(path_to_cache, cache_interval);
    DLOG("Success to initialize userdata.");

    // TODO: set `ret_code` to 0 if everything above succeeded else some appropriate
    // non-zero value.
    *ret_code = ret;

    // Return pointer to global state data.
    return (void *)userdata;
}

void watdfs_cli_destroy(void *userdata) {
    // TODO: clean up your userdata state.
    free(((struct user_data *)userdata)->path_to_cache);
    
    delete (struct user_data *)userdata;

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
    int fxn_ret = 0;
    int ret;
    char *full_path = get_full_path((struct user_data*)userdata, path);

    // check whether the server has the file
    ret = rpc_getattr(userdata, path, statbuf);
    if (ret < 0) {
        // The file does not exit. Create a new file with O_CREAT
        DLOG("File does not exist.");
        free(full_path);
        return ret;
    }

    // check open state of the file at the client
    if(((struct user_data *)userdata)->file_status.find(std::string(path)) == ((struct user_data *)userdata)->file_status.end()) {
        // file has not been opened

        // open the file on the client
        int client_fd;
        ret = open(full_path, O_RDWR);
        if(ret < 0) { 
            DLOG("Failed to open the local file.");
            ret = mknod(full_path, statbuf->st_mode, statbuf->st_dev);
            ret = open(full_path, O_RDWR);
        }
        client_fd = ret;

        // open the file on the server
        struct fuse_file_info *fi = new struct fuse_file_info;
        fi->flags = O_RDONLY;
        ret = rpc_open(userdata, path, fi);
        if(ret < 0) {
            DLOG("Error: Fail to open the server file.");
            return ret;
        }

        DLOG("Open the local file.");

        // temp record for download function
        ((struct user_data *)userdata)->file_status[std::string(path)] = new status(0, time(nullptr), client_fd, fi);

        // download the file from the server
        ret = watdfs_download((struct user_data *)userdata, path);
        if(ret < 0) {
            DLOG("Error: Fail to download the file.");
            fxn_ret = ret;
        }

        // close the file on server
        ret = rpc_release(userdata, path, fi);
        if(ret < 0) {
            DLOG("Error: Fail to close the server file.");
            fxn_ret = ret;
        }

        ret = close(client_fd);
        if(ret < 0) {
            DLOG("Error: Fail to close the file.");
            fxn_ret = ret;
        }

        // remove the record 
        ((struct user_data *)userdata)->file_status.erase(std::string(path));

        DLOG("Close the local file.");

        delete fi;
    }
    else {
        // file is already opened
        if(((struct user_data *)userdata)->file_status[std::string(path)]->rw == 0) {
            // read mode -> freshness check for getattr
            int check_ret = freshness_check((struct user_data *)userdata, path);
            if(check_ret) {
                if(check_ret == 2) {
                    // update the tc to current time
                    ((struct user_data *)userdata)->file_status[std::string(path)]->tc = time(nullptr);
                }
            }
            else {
                // download the remote file 
                struct fuse_file_info *fi = new struct fuse_file_info;
                fi->fh = ((struct user_data *)userdata)->file_status[std::string(path)]->server_fi->fh;
                ret = watdfs_download((struct user_data *)userdata, path);
                if(ret < 0) {
                    DLOG("Error: Fail to download the server file.");
                    return ret;
                }
            }
        }
    }

    // get attributes from the local file
    ret = stat(full_path, statbuf);
    if(ret < 0) {
        DLOG("Error: Fail to get attributes of the file.");
        return ret;
    }

    return fxn_ret;
}

// CREATE, OPEN AND CLOSE
int watdfs_cli_mknod(void *userdata, const char *path, mode_t mode, dev_t dev){
    int fxn_ret = 0;
    int ret;
    char *full_path = get_full_path((struct user_data *)userdata, path);

    // create client and server files 
    ret = mknod(full_path, mode, dev);
    if(ret < 0) { 
        DLOG("Failed to create the local file.");
        return ret;
    }

    ret = rpc_mknod(userdata, path, mode, dev);
    if(ret < 0) { 
        DLOG("Failed to create the local file.");
        return ret;
    }

    // open the file on the server
    int client_fd;
    ret = open(full_path, O_RDWR);
    if(ret < 0) {
        DLOG("Error: Fail to open the server file.");
        return ret;
    }
    client_fd = ret;

    struct fuse_file_info *fi = new struct fuse_file_info;
    fi->flags = O_RDWR;
    ret = rpc_open(userdata, path, fi);
    if(ret < 0) {
        DLOG("Error: Fail to open the server file.");
        return ret;
    }

    DLOG("Open the local file.");
    // temp record for download function
    ((struct user_data *)userdata)->file_status[std::string(path)] = new status(0, time(nullptr), client_fd, fi);

    // upload the file to the server
    ret = watdfs_upload((struct user_data *)userdata, path);
    if(ret < 0) {
        DLOG("Error: Fail to upload the file.");
        fxn_ret = ret;
    }
        
    // close the file on server
    ret = rpc_release(userdata, path, fi);
    if(ret < 0) {
        DLOG("Error: Fail to close the server file.");
        fxn_ret = ret;
    }

    ret = close(client_fd);
    if(ret < 0) {
        DLOG("Error: Fail to close the file.");
        fxn_ret = ret;
    }

    // remove the record 
    ((struct user_data *)userdata)->file_status.erase(std::string(path));
    DLOG("Close the local file.");

    return fxn_ret;
}

int watdfs_cli_open(void *userdata, const char *path,
                    struct fuse_file_info *fi) {
    char *full_path = get_full_path((struct user_data *)userdata, path);
    struct stat *statbuf = new struct stat;
    int ret = rpc_getattr(userdata, path, statbuf);
    if(ret < 0) {
        DLOG("Error: Failed to open the file.");
        free(full_path);
        return ret;
    }

    int fxn_ret = 0;

    int client_fd = 0;
    if(((struct user_data *)userdata)->file_status.find(std::string(path)) != ((struct user_data *)userdata)->file_status.end()) {
        // the file is already opened in the client
        DLOG("Error: The file is opened.");
        return -EMFILE;
    }
    else {
        // open the file on client
        ret = open(full_path, O_RDWR);
        if(ret < 0) {
            DLOG("Error: Failed to open the file.");
            ret = mknod(full_path, statbuf->st_mode, statbuf->st_dev);
            ret = open(full_path, O_RDWR);
        }
        // store the client file descriptor
        client_fd = ret;
    }

    // open the file on server
    ret = rpc_open(userdata, path, fi);
    if(ret < 0) {
        DLOG("Error: Fail to open the file.");
        return ret;
    }
    
    // store the open mode
    // 0 for read, 1 for write
    int rw = 0;
    if((fi->flags & O_ACCMODE) != O_RDONLY) {
        rw = 1;
        DLOG("File is opened in read mode.");
    }
    else {
        rw = 0;
        DLOG("File is opened in write mode.");
    }

    ((struct user_data *)userdata)->file_status[std::string(path)] = new status(rw, time(nullptr), client_fd, fi);
    DLOG("Record the status. Start downloading.");

    // File exists on the server 
    ret = watdfs_download((struct user_data *)userdata, path);
    if(ret < 0) {
        DLOG("Error: Fail to download the file.");
        return ret;
    }

    free(full_path);
    return fxn_ret;
}

int watdfs_cli_release(void *userdata, const char *path,
                       struct fuse_file_info *fi) {
    int ret;

    int fxn_ret = 0;

    // check if the file is opened in write mode
    if(((struct user_data *)userdata)->file_status[std::string(path)]->rw == 1) {
        // upload the file
        DLOG("Upload the file.");
        int ret = watdfs_upload((struct user_data *)userdata, path);
        if(ret < 0) {
            DLOG("Error: Fail to upload the file.");
            return ret;
        }

    }

    // if the file is opened in the client
    if(((struct user_data *)userdata)->file_status.find(std::string(path)) != ((struct user_data *)userdata)->file_status.end()) {
        // close the file and erase the state
        ret = close(((struct user_data *)userdata)->file_status[std::string(path)]->client_fd);
        ((struct user_data *)userdata)->file_status.erase(std::string(path));
        DLOG("Close the file.");
    }
   
    if(ret < 0) {
        DLOG("Error: Fail to close the file.");
        return ret;
    }

    // release the server file
    ret = rpc_release(userdata, path, fi);
    if(ret < 0) {
        DLOG("Error: Fail to close the file.");
        return ret;
    }
    return fxn_ret;
}

// READ AND WRITE DATA
int watdfs_cli_read(void *userdata, const char *path, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi) {
    int fxn_ret = 0;

    // check freshness condition
    int ret;
    int check_ret = freshness_check((struct user_data *)userdata, path);
    if(check_ret) {
        if(check_ret == 2) {
            // update the tc to current time
            ((struct user_data *)userdata)->file_status[std::string(path)]->tc = time(nullptr);
        }
    }
    else {
        // download the remote file 
        ret = watdfs_download((struct user_data *)userdata, path);
        if(ret < 0) {
            DLOG("Error: Fail to download the server file.");
            return ret;
        }
    }

    // read from the local file
    int client_fd = ((struct user_data *)userdata)->file_status[std::string(path)]->client_fd;
    ret = pread(client_fd, buf, size, offset);
    if(ret < 0) {
        DLOG("Error: Fail to read the local file.");
        return ret;
    }
    else {
        fxn_ret = ret;
    }
    
    return fxn_ret;
}

int watdfs_cli_write(void *userdata, const char *path, const char *buf,
                     size_t size, off_t offset, struct fuse_file_info *fi) {
    int fxn_ret = 0;

    int ret;

    // write to the local copy of the file
    int client_fd = ((struct user_data *)userdata)->file_status[std::string(path)]->client_fd;
    ret = pwrite(client_fd, buf, size, offset);
    fxn_ret = ret;
    if(ret < 0) {
        DLOG("Error: Fail to write the local file.");
        return ret;
    }

    // check freshness condition
    int check_ret = freshness_check((struct user_data *)userdata, path);
    if(check_ret) {
        return fxn_ret;
    }
    else {
        // upload the local file 
        ret = watdfs_upload((struct user_data *)userdata, path);
        if(ret < 0) {
            DLOG("Error: Fail to write the file.");
            return ret;
        } 
    }

    return fxn_ret;
}

int watdfs_cli_truncate(void *userdata, const char *path, off_t newsize)  {
    int fxn_ret = 0;
    int ret;
    char *full_path = get_full_path((struct user_data*)userdata, path);

    // check whether the server has the file
    struct stat *statbuf = new struct stat;
    ret = rpc_getattr(userdata, path, statbuf);
    if (ret < 0) {
        // The file does not exit. Create a new file with O_CREAT
        DLOG("File does not exist.");
        free(full_path);
        return ret;
    }

    // check open state of the file at the client
    if(((struct user_data *)userdata)->file_status.find(std::string(path)) == ((struct user_data *)userdata)->file_status.end()) {
        // file has not been opened

        // open the file on the client
        int client_fd;
        ret = open(full_path, O_RDWR);
        if(ret < 0) { 
            DLOG("Failed to open the local file.");
            ret = mknod(full_path, statbuf->st_mode, statbuf->st_dev);
            ret = open(full_path, O_RDWR);
        }
        client_fd = ret;

        // open the file on the server
        struct fuse_file_info *fi = new struct fuse_file_info;
        fi->flags = O_RDWR;
        ret = rpc_open(userdata, path, fi);
        if(ret < 0) {
            DLOG("Error: Fail to open the server file.");
            return ret;
        }

        DLOG("Open the local file.");

        // temp record for download function
        ((struct user_data *)userdata)->file_status[std::string(path)] = new status(0, time(nullptr), client_fd, fi);

        // download the file from the server
        ret = watdfs_download((struct user_data *)userdata, path);
        if(ret < 0) {
            DLOG("Error: Fail to download the file.");
            fxn_ret = ret;
        }

        // perform truncate operation in cached file
        ret = truncate(full_path, newsize);
        if(ret < 0) {
            DLOG("Error: Fail to truncate the local file.");
            fxn_ret = ret;
        }

        // upload the file to the server
        ret = watdfs_upload((struct user_data *)userdata, path);
        if(ret < 0) {
            DLOG("Error: Fail to upload the file.");
            fxn_ret = ret;
        }
        

        // close the file on server
        ret = rpc_release(userdata, path, fi);
        if(ret < 0) {
            DLOG("Error: Fail to close the server file.");
            fxn_ret = ret;
        }

        ret = close(client_fd);
        if(ret < 0) {
            DLOG("Error: Fail to close the file.");
            fxn_ret = ret;
        }

        // remove the record 
        ((struct user_data *)userdata)->file_status.erase(std::string(path));
        DLOG("Close the local file.");

        delete fi;
    }
    else {
        // file is already opened
        if(((struct user_data *)userdata)->file_status[std::string(path)]->rw == 0) {
            // read mode -> write call fails
            return -EMFILE;
        }
        else {
            // perform truncate operation in cached file
            ret = truncate(full_path, newsize);
            if(ret < 0) {
                DLOG("Error: Fail to truncate the local file.");
                fxn_ret = ret;
            }

            // write mode -> freshness check before write
            int check_ret = freshness_check((struct user_data *)userdata, path);
            if(check_ret) {
                return fxn_ret;
            }
            else {
                
                // upload the local file 
                ret = watdfs_upload((struct user_data *)userdata, path);
                if(ret < 0) {
                    DLOG("Error: Fail to write the file.");
                    return ret;
                }
            }
        }
    }

    return fxn_ret;
}

int watdfs_cli_fsync(void *userdata, const char *path,
                     struct fuse_file_info *fi)  {
    // push the local file to the server
    int fxn_ret = 0;
    int ret;
    char *full_path = get_full_path((struct user_data*)userdata, path);

    // check open state of the file at the client
    if(((struct user_data *)userdata)->file_status[std::string(path)]->rw == 0) {
        // read only mode -> error
        return -1;
    }
    else {
        struct stat *statbuf = new struct stat;
        ret = stat(full_path, statbuf);
        if(ret < 0) {
            DLOG("Error: Fail to get attributes of the local file.");
            return ret;
        }

        int client_fd = ((struct user_data *)userdata)->file_status[std::string(path)]->client_fd;
        ret = fsync(client_fd);
        if(ret < 0) {
            DLOG("Error: Fail to fsync the local file.");
            return ret;
        }

        int ret = watdfs_upload((struct user_data *)userdata, path);
        if(ret < 0) {
            DLOG("Error: Fail to upload the file.");
            return ret;
        }

        // update tc
        ((struct user_data *)userdata)->file_status[std::string(path)]->tc = time(nullptr);
    }

    
    return fxn_ret;
}

// CHANGE METADATA
int watdfs_cli_utimensat(void *userdata, const char *path,
                       const struct timespec ts[2]) {
    int fxn_ret = 0;
    int ret;
    char *full_path = get_full_path((struct user_data*)userdata, path);

    // check whether the server has the file
    struct stat *statbuf = new struct stat;
    ret = rpc_getattr(userdata, path, statbuf);
    if (ret < 0) {
        // The file does not exit. Create a new file with O_CREAT
        DLOG("File does not exist.");
        free(full_path);
        return ret;
    }

    // check open state of the file at the client
    if(((struct user_data *)userdata)->file_status.find(std::string(path)) == ((struct user_data *)userdata)->file_status.end()) {
        // file has not been opened

        // open the file on the client
        int client_fd;
        ret = open(full_path, O_RDWR);
        if(ret < 0) { 
            DLOG("Failed to open the local file.");
            ret = mknod(full_path, statbuf->st_mode, statbuf->st_dev);
            ret = open(full_path, O_RDWR);
        }
        client_fd = ret;

        // open the file on the server
        struct fuse_file_info *fi = new struct fuse_file_info;
        fi->flags = O_RDWR;
        ret = rpc_open(userdata, path, fi);
        if(ret < 0) {
            DLOG("Error: Fail to open the server file.");
            return ret;
        }

        DLOG("Open the local file.");

        // temp record for download function
        ((struct user_data *)userdata)->file_status[std::string(path)] = new status(0, time(nullptr), client_fd, fi);

        // download the file from the server
        ret = watdfs_download((struct user_data *)userdata, path);
        if(ret < 0) {
            DLOG("Error: Fail to download the file.");
            fxn_ret = ret;
        }

        // perform utimensat operation in cached file
        ret = utimensat(0, full_path, ts, 0);
        if(ret < 0) {
            DLOG("Error: Fail to utimensat the local file.");
            fxn_ret = ret;
        }

        // upload the file to the server
        ret = watdfs_upload((struct user_data *)userdata, path);
        if(ret < 0) {
            DLOG("Error: Fail to upload the file.");
            fxn_ret = ret;
        }
        

        // close the file on server
        ret = rpc_release(userdata, path, fi);
        if(ret < 0) {
            DLOG("Error: Fail to close the server file.");
            fxn_ret = ret;
        }

        ret = close(client_fd);
        if(ret < 0) {
            DLOG("Error: Fail to close the file.");
            fxn_ret = ret;
        }

        // remove the record 
        ((struct user_data *)userdata)->file_status.erase(std::string(path));
        DLOG("Close the local file.");

        delete fi;
    }
    else {
        // file is already opened
        if(((struct user_data *)userdata)->file_status[std::string(path)]->rw == 0) {
            // read mode -> write call fails
            return -EMFILE;
        }
        else {
            // write mode -> freshness check before upload

            // perform utimensat operation in cached file
            ret = utimensat(0, full_path, ts, 0);
            if(ret < 0) {
                DLOG("Error: Fail to utimensat the local file.");
                fxn_ret = ret;
            }

            int check_ret = freshness_check((struct user_data *)userdata, path);
            if(check_ret) {
                return fxn_ret;
            }
            else {
                // upload the local file 
                ret = watdfs_upload((struct user_data *)userdata, path);
                if(ret < 0) {
                    DLOG("Error: Fail to write the file.");
                    return ret;
                }
            }
        }
    }

    return fxn_ret;
}
