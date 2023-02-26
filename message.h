#ifndef __message_h__
#define __message_h__
#include "mfs.h"

# define DIR_ENTRIES_IN_BLOCK (64) // MFS_BLOCK_SIZE / sizeof(MFS_DirEnt_t)

// An Inode-table half-block
typedef struct N_Trace{
        int inodes[16];    // Addresses of inodes
} N_Trace;

enum MFS_OPS {
  MFS_INIT,
  MFS_LOOKUP,
  MFS_STAT,
  MFS_WRITE,
  MFS_READ,
  MFS_CREAT,
  MFS_UNLINK,
  MFS_SHUTDOWN,
  MFS_FEEDBACK
};

typedef struct Block_t {
  MFS_DirEnt_t data_blocks[DIR_ENTRIES_IN_BLOCK];
} Block_t;

// An Inode-table
typedef struct track_t{
        int tfinal;
        int node_array[256]; // Addresses of half-blocks (N_Traces)
        int inode_count;
} track_t;

typedef struct message_t {
        char buf[4096];  // buffer
        char name[28];  // name being passed
        int offset;  // offset
        int nbytes;  // number of bytes
        int mtype;   // fiile type
        int node_num;   // inode number
        enum MFS_OPS msg;  // operation (MFS)
        MFS_Stat_t st;   // Stat struct 
} message_t;

#endif // __message_h__
