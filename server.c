#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

#include "mfs.h"
#include "udp.h"
#include "message.h"
#include "ufs.h"
#include "debug.h"

int fd = -1;
super_t super;
unsigned int highest_inode = 0;
unsigned int hghst_alloc_dblk = 0;

// set up the needed functions
int read_inode(unsigned int, inode_t *);
dir_ent_t* lookup_file(int, char*, unsigned int*);
int write_file(int inum, void *buf, unsigned int offset, int nbytes, int type);
int alloc_dblk(void);
int fsread(int addr, void *ptr, size_t nbytes);
int fswrite(unsigned int addr, void *ptr, size_t nbytes);
int new_inode(int);

int initialize_serv(char* );
int run_udp(int);
int end_serv();

int fsread(int addr, void *ptr, size_t nbytes) {
  lseek(fd, addr, SEEK_SET);
  return read(fd, ptr, nbytes);
}

int fswrite(unsigned int addr, void *ptr, size_t nbytes) {
  debug("In fswrite: writing at addr %u.%d bytes %lu\n", 
    addr / UFS_BLOCK_SIZE, addr % UFS_BLOCK_SIZE, nbytes);
  lseek(fd, addr, SEEK_SET);
  int rc = write(fd, ptr, nbytes);
  return rc;
}

void inode_dbg(int inum) {
  inode_t * ind = (inode_t *) malloc(sizeof(inode_t));
  read_inode(inum, ind);
  if (ind == NULL) return;
  debug("inum %d type: %d size: %d ", inum, ind->type, ind->size);
  debug("direct: ");
  for(int i = 0; i < DIRECT_PTRS; i++) {
    debug("%d ", ind->direct[i]);
  }
  debug("\n");
}

void dir_dbg(int inum) {
  debug("inum %d dir entries: ", inum);
  inode_t *ind = (inode_t *) malloc(sizeof(inode_t));
  read_inode(inum, ind);
  if (ind == NULL) return;
  for(int i = 0; i < (ind->size / sizeof(dir_ent_t)); i++) {
    dir_ent_t de;
    fsread(ind->direct[0] * UFS_BLOCK_SIZE + i * sizeof(dir_ent_t), 
      &de, sizeof(dir_ent_t)); 
    debug("%s %d, ", de.name, de.inum);
  }
  debug("\n");
}

/* a 32-bit mask for a number starting from leftmost bit*/
unsigned int mask(unsigned int num) {
  return 0x1 << (8 * sizeof(unsigned int) - (num % (8 * sizeof(unsigned int))) - 1);
}

unsigned int bmaddr(unsigned int start, unsigned int inum) {
  return start * UFS_BLOCK_SIZE 
    + floor(inum / (8 * sizeof(unsigned int))) * sizeof(unsigned int);  
}

int read_inode(unsigned int inum, inode_t * ind) {
  unsigned int ibm; 
  fsread(bmaddr(super.inode_bitmap_addr, inum), 
    &ibm, sizeof(unsigned int));
  int valid = ibm && mask(inum);
  if (!valid) {
    ind = NULL;
    return -1;
  };

  fsread(super.inode_region_addr * UFS_BLOCK_SIZE + inum * sizeof(inode_t), 
    ind, sizeof(inode_t));
  return 0;
}

void write_inode(int inum, inode_t *inode) {
  fswrite(super.inode_region_addr * UFS_BLOCK_SIZE + inum * sizeof(inode_t),
    inode, sizeof(inode_t));
}

/*
lookup_file: Find a file in a parent directory
params: parent-inum, file-name, 
returns: dir_ent and addr if found, 
    NULL if par inode not found, par inode not dir or file not found

Iterates over directory entries in data block of parent to find a file. 
*/
dir_ent_t* lookup_file(int pinum, char* name, unsigned int *addr){
  debug("In lookup_file: pinum %d name %s. entering ...\n", pinum, name);
  inode_t *nd = (inode_t *) malloc(sizeof(inode_t));
  read_inode(pinum, nd);
  
  if(nd == NULL || nd->type != UFS_DIRECTORY) return NULL;
    
  unsigned int mxb = ceil(1.0 * nd->size / UFS_BLOCK_SIZE); 

  for (int i = 0; i < mxb; i++) {
    debug("In lookup_file: reading direct block %d (addr %d)\n", i, nd->direct[i]);

    dir_ent_t * de = (dir_ent_t *) malloc(sizeof(dir_ent_t));
    for (int j = 0; j < UFS_BLOCK_SIZE / sizeof(dir_ent_t); j++) {
      unsigned int deaddr = nd->direct[i] * UFS_BLOCK_SIZE + j * sizeof(dir_ent_t);
      fsread(deaddr, de, sizeof(dir_ent_t));
      if(strcmp(de->name, name) == 0 && de->inum != -1) {
        debug("In lookup_file: file found. inum %d addr %u. returning ...\n", de->inum, deaddr);
        *addr = deaddr;
        return de;
      }
    }
  }
  debug("In lookup_file: file not found. returning NULL\n");
  return NULL;
}

/*
creat_file: Create a new regular file or directory in a parent dir
params: parent inum, new-file type, new-file name
return: 0 on success, -1 on failure
*/
int creat_file(int pinum, int type, char *name) {
  debug("In creat_file: to create file %s. entering ...\n", name);
  /* Check if par is dir*/
  inode_t *pnd = (inode_t *) malloc(sizeof(inode_t));
  read_inode(pinum, pnd);
  if(pnd == NULL || pnd ->type != UFS_DIRECTORY) return -1;

  /* Check if name already exists */
  unsigned int addr;
  dir_ent_t* lde = lookup_file(pinum, name, &addr);
  if (lde != NULL) return 0;

  int ninum = new_inode(type);
  if (ninum == -1) return -1;

  /* if new dir, add . and .. */
  if (type == UFS_DIRECTORY) {
    inode_t *nnd = (inode_t *) malloc(sizeof(inode_t));
    read_inode(ninum, nnd);

    int ndb = alloc_dblk();
    if (ndb == -1) return -1;
    nnd->direct[0] = ndb;
    
    dir_block_t db;
    strcpy(db.entries[0].name, "."); 
    db.entries[0].inum = ninum;
    strcpy(db.entries[1].name, "..");
    db.entries[1].inum = pinum; 
    for (int i = 2; i < UFS_BLOCK_SIZE / sizeof(dir_ent_t); i++)
      db.entries[i].inum = -1;
    fswrite(ndb * UFS_BLOCK_SIZE, &db, sizeof(dir_block_t));
    nnd->size = 2 * sizeof(dir_ent_t);
    write_inode(ninum, nnd);
  }

  /* write in parent data*/
  inode_t *pind = (inode_t *) malloc(sizeof(inode_t));
  read_inode(pinum, pind);
  dir_ent_t de;
  de.inum = ninum;
  strcpy(de.name, name);
  write_file(pinum, &de, pind->size, sizeof(dir_ent_t), UFS_DIRECTORY);
  debug("In creat_file: file created. returning ...\n");
  return 0; 
}

/*
write_file
param: inode-num, offset (0-indexed), data, nbytes
returns: nbytes written

Writes to one block or two blocks. 
Adds a data block if not enough space in current block.
Updates size and direct fields of a inode.
*/

int write_file(int inum, void *buf, unsigned int offset, int nbytes, int type) {
  debug("In write_file: write inode %d at addr %u. entering ...\n", inum, offset);
  if (type == UFS_REGULAR_FILE) {
    buf = (char *) buf;
  } else {
    buf = (dir_ent_t *) buf;
  }

  inode_t *fnd = (inode_t *) malloc(sizeof(inode_t));
  read_inode(inum, fnd);
  if (fnd == NULL || fnd->type != type) return -1;
  inode_dbg(inum);

  int ofd = floor(1.0 * offset / UFS_BLOCK_SIZE);
  if (ofd > (DIRECT_PTRS - 1)) return -1;

  unsigned int ofr = offset % UFS_BLOCK_SIZE;
  unsigned int offree = UFS_BLOCK_SIZE - ofr;
  int d = ofd;
  while(d >= 0 && fnd->direct[d] == -1) {
    unsigned int ndb = alloc_dblk();
    if (ndb == -1) return -1;
    debug("In write_file: adding new dblk %u to inum %d direct[%u].\n", ndb, inum, d);
    fnd->direct[d] = ndb;
    d--;
  } 
  write_inode(inum, fnd);
  if(nbytes <= offree) {
    fswrite(fnd->direct[ofd] * UFS_BLOCK_SIZE + ofr, buf, nbytes);
  } else {
    if (ofd == (DIRECT_PTRS - 1)) return -1;
    int ndb = alloc_dblk();
    if (ndb == -1) return -1;
    fnd->direct[ofd + 1] = ndb;
    fswrite(fnd->direct[ofd] * UFS_BLOCK_SIZE + ofr, buf, offree);
    fswrite(fnd->direct[ofd + 1] * UFS_BLOCK_SIZE, 
      buf + sizeof(char) * offree, nbytes - offree);
  }
  fnd->size = (offset + nbytes) > fnd->size ? offset + nbytes: fnd->size;
  write_inode(inum, fnd);
  inode_dbg(inum);
  if(type == UFS_DIRECTORY) dir_dbg(inum);
  debug("In write_file. returning ...\n");
  return 0;
}

/*
newDataBlock function: returns block address.
    - Find a free data block from data bitmap
    - Set bit to 1 for this data block in data bitmap
    - data block address = (super.data_region_addr + bit index in bitmap) * block_size
    
*/
int alloc_dblk() {
  debug("In alloc_dblk: entering ...\n");
  /* set in d-bitmap */
  if (hghst_alloc_dblk == (super.data_region_len - 1)) return -1;

  unsigned int bits;
  hghst_alloc_dblk += 1;
  unsigned int dbtaddr = bmaddr(super.data_bitmap_addr, hghst_alloc_dblk);
  fsread(dbtaddr, &bits, sizeof(unsigned int)); 
  bits |= mask(hghst_alloc_dblk);
  fswrite(dbtaddr, &bits, sizeof(unsigned int));
  debug("In alloc_dblk: allocated dblk addr %d. returning ...\n", 
    super.data_region_addr + hghst_alloc_dblk);
  return super.data_region_addr + hghst_alloc_dblk;
}

/*
read_file
param: inode-num, buf, offset, nbytes
returns: nbytes read

Read from one block or two blocks as nbytes <= 4096.
*/
int read_file(int inum, char* buf, int offset, int nbytes) {
  debug("In read_file: read inum %d at offset %u entering ...\n", inum, offset);
  inode_t *fnd = (inode_t *) malloc(sizeof(inode_t));
  read_inode(inum, fnd);
  if (fnd == NULL) return -1;
  inode_dbg(inum);

  unsigned int rdb = floor(1.0 * offset / UFS_BLOCK_SIZE);
  if (rdb > (DIRECT_PTRS - 1)) return -1;

  unsigned int rds = offset % UFS_BLOCK_SIZE;
  unsigned int rdf = UFS_BLOCK_SIZE - rds;
  if (nbytes <= rdf) {
    if(fnd->direct[rdb] == -1) return -1;
    fsread(fnd->direct[rdb] * UFS_BLOCK_SIZE + rds, buf, nbytes);
  } else {
    if (rdb == (DIRECT_PTRS - 1)) return -1;
    if(fnd->direct[rdb] == -1 || fnd->direct[rdb + 1] == -1) return -1;
    fsread(fnd->direct[rdb] * UFS_BLOCK_SIZE + rds, buf, rdf);
    fsread(fnd->direct[rdb + 1] * UFS_BLOCK_SIZE, buf + sizeof(char) * rdf, nbytes - rdf);
  }
  debug("In read_file: file read. returning ...\n");
  return 0;
}

/*
params: parent inum, file-name

Remove a regular file or directory name from the parent dir.

- Get parent-inode from parent inum (call getInode)
- Lookup file and get entry address of file (call lookupFile)
- Cast entry to dir_ent_t and get inum.
- Get inode from file-inum (call getInode)
- If file type is dir and size > 0, throw err
- Mark sizeof(dir_ent_t) bytes as invalid at entry address.
- Return success
*/
int unlink_file(int pinum, char *name) {
  debug("In unlink_file: entering ...\n");
  unsigned int addr;
  dir_ent_t *de = lookup_file(pinum, name, &addr);
  if (de == NULL) return -1;

  if (de->inum != -1) {
    inode_t *ind = (inode_t *) malloc(sizeof(inode_t));  
    read_inode(de->inum, ind);
    debug("In unlink_file: to delete ");
    inode_dbg(de->inum);
    if (ind->type == UFS_DIRECTORY && ind->size > 2 * sizeof(dir_ent_t)) {
      debug("In unlink_file. dir nonempty. returning ...\n");
      return -1;
    }
    strcpy(de->name, "");
    de->inum = -1;
    fswrite(addr, de, sizeof(dir_ent_t)); 
  }

  /* update size */
  inode_t * pnd = (inode_t *) malloc(sizeof(inode_t));
  read_inode(pinum, pnd);
  int i;
  for(i = 0; pnd->direct[i] != -1; i++) {
    if (pnd->direct[i] = (addr / UFS_BLOCK_SIZE)) 
      break;
  }
  unsigned int offset = i * UFS_BLOCK_SIZE + addr % UFS_BLOCK_SIZE;
  pnd->size = (offset < pnd->size)? offset: pnd->size;
  write_inode(pinum, pnd);
  debug("In unlink_file: parent ");
  inode_dbg(pinum);

  debug("In unlink_file: unlinked. returning ...\n");
  return 0;
}


/*
newInode function: create a new inode
params: type
return: inum on success, -1 on failure
*/
int new_inode(int type) {
  debug("In new_inode: to create type %d. entering ...\n", type);
  /* set in i-bitmap*/
  if (highest_inode == (super.num_inodes - 1)) return -1;

  highest_inode += 1;
  unsigned int bits;
  fsread(bmaddr(super.inode_bitmap_addr, highest_inode), 
    &bits, sizeof(unsigned int)); 
  bits |= mask(highest_inode);
  fswrite(bmaddr(super.inode_bitmap_addr, highest_inode), 
    &bits, sizeof(unsigned int));

  /* write in inode table */
  inode_t newnd;
  newnd.type = type;
  newnd.size = 0;
  for (int i = 0; i < DIRECT_PTRS; i++) {
    newnd.direct[i] = -1;
  }

  fswrite(super.inode_region_addr * UFS_BLOCK_SIZE + highest_inode * sizeof(inode_t), 
          &newnd, sizeof(inode_t));
  
  debug("In new_inode: inode created of inum %d. returning ...", highest_inode);
  return highest_inode;
}

int end_serv() {
  fsync(fd);
  exit(0);
}

int initialize_serv(char* image_path) {
  fd = open(image_path, O_RDWR | O_CREAT, S_IRWXU);

  struct stat fs;
  if(fstat(fd, &fs) < 0) {
    perror("initialize_serv: Cannot open file");
  }

  fsread(0, &super, sizeof(super_t));
  debug("Read super block. Inode rgn addr: %d, #inodes: %d\n", 
    super.inode_region_addr, super.num_inodes);
  inode_dbg(0);
	
  return 0;
}

int run_udp(int port) { 
  int sd=-1;
  if((sd =   UDP_Open(port))< 0){
    perror("initialize_serv: port open fail");
    return -1;
  }

  struct sockaddr_in s;
  message_t buf_pk,  rx_pk;

  while (1) {
    if( UDP_Read(sd, &s, (char *)&buf_pk, sizeof(message_t)) < 1)
      continue;


    if(buf_pk.msg == MFS_LOOKUP){
      /*
        - Get parent inum, file name from message.
        - Lookup file and get entry address (call lookupFile)
        - If found: 
            - Read entry address into dir_ent_t struct
            - Return inum
        - Else throw err
        */
      unsigned int addr;
      dir_ent_t *de = lookup_file(buf_pk.node_num, buf_pk.name, &addr);
      if (de != NULL) {
        rx_pk.node_num = de->inum;
      } else {
        rx_pk.node_num = -1;
      }
      rx_pk.msg = MFS_FEEDBACK;
      UDP_Write(sd, &s, (char*)&rx_pk, sizeof(message_t));
    }
    else if(buf_pk.msg == MFS_STAT){
        /*
        - Get inum from message
        - Get inode from inum (call getInode)
        - Return MFS-Stat struct with type and size of inode
        */
      inode_t *ind = (inode_t *) malloc(sizeof(inode_t));
      read_inode(buf_pk.node_num, ind);
      if (ind != NULL) {
        rx_pk.node_num = 0;
        rx_pk.st.size = ind->size;
        rx_pk.st.type = ind->type;
      } 
      else rx_pk.node_num = -1;
      rx_pk.msg = MFS_FEEDBACK;
      UDP_Write(sd, &s, (char*)&rx_pk, sizeof(message_t));

    }
    else if(buf_pk.msg == MFS_WRITE){
      rx_pk.node_num = write_file(buf_pk.node_num, buf_pk.buf, 
        buf_pk.offset, buf_pk.nbytes, UFS_REGULAR_FILE);
      rx_pk.msg = MFS_FEEDBACK;
      UDP_Write(sd, &s, (char*)&rx_pk, sizeof(message_t));

    }
    else if(buf_pk.msg == MFS_READ){
      rx_pk.node_num = read_file(buf_pk.node_num, rx_pk.buf, buf_pk.offset, buf_pk.nbytes);
      rx_pk.msg = MFS_FEEDBACK;
      UDP_Write(sd, &s, (char*)&rx_pk, sizeof(message_t));

    }
    else if(buf_pk.msg == MFS_CREAT){
      rx_pk.node_num = creat_file(buf_pk.node_num, buf_pk.mtype, buf_pk.name);
      rx_pk.msg = MFS_FEEDBACK;
      UDP_Write(sd, &s, (char*)&rx_pk, sizeof(message_t));

    }
    else if(buf_pk.msg == MFS_UNLINK){
      rx_pk.node_num = unlink_file(buf_pk.node_num, buf_pk.name);
      rx_pk.msg = MFS_FEEDBACK;
      UDP_Write(sd, &s, (char*)&rx_pk, sizeof(message_t));

    }
    else if(buf_pk.msg == MFS_SHUTDOWN) {
     /*
      - Write any remaining data to image
      - Break from loop
      */
      rx_pk.msg = MFS_FEEDBACK;
      UDP_Write(sd, &s, (char*)&rx_pk, sizeof(message_t));
      end_serv();
    }
    else if(buf_pk.msg == MFS_FEEDBACK) {
      rx_pk.msg = MFS_FEEDBACK;
      UDP_Write(sd, &s, (char*)&rx_pk, sizeof(message_t));
    }
    else {
      perror("invalid MFS function");
      return -1;
    }
  }

  return 0;
}

int main(int argc, char *argv[]) {
	if(argc != 3) {
		perror("Usage: server <portnum> <image>\n");
		exit(1);
	}

	initialize_serv(argv[2]);
  run_udp(atoi(argv[1]));

	return 0;
}
