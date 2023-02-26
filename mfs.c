#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <assert.h>
#include <string.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>

#include "mfs.h"
#include "udp.h"
#include "message.h"
#include "debug.h"

/* Server_To_Client: Send file operation message to server and receive feedback.

Use message_t struct for messages. 
*/
int Server_To_Client(message_t *send, message_t *receive, char *server, int pnum)
{
	int sd = UDP_Open(0);
	if(sd < -1){
		// open failure
		perror("udp_send: failed to open socket.");
		return -1;
	}

	struct sockaddr_in sock;
	struct sockaddr_in sock1;
	int rc = UDP_FillSockAddr(&sock, server, pnum);

	struct timeval tv;
	tv.tv_usec=0; 
	tv.tv_sec=3; 

	if(rc < 0){
                perror("upd_send: failed to find host");
                return -1;
        }

	int timeout = 5;
	fd_set set;
	while(1){
		// usf FDZERO on set
		FD_ZERO(&set);
		
		// set set
		FD_SET(sd,&set);

		// Write using the udp_write funcction
		UDP_Write(sd, &sock, (char*)send, sizeof(message_t));

		// make sure this was successful
		if(select(sd+1, &set, NULL, NULL, &tv)){

			// read using udp_read
			rc = UDP_Read(sd, &sock1, (char*)receive, sizeof(message_t));

			// check to make sure read was successful
			if(rc > 0){

				// close open port
				UDP_Close(sd);
				return 0;
			}
		}else{

			// wait for one less second now
			timeout --;
		}
	}
}

// important global variables
char* my_serv = NULL; // server being used
int working = 0; // make sure the server is currently working
int prt = 10000; // base port


/* MFS_Init: set up server and port */
int MFS_Init(char *hostname, int port) {
	prt = port;
	working = 1;
	my_serv = strdup(hostname); 
	return 0;
}

/* MFS_Lookup: looks up name based on given pinum and name 
returns: inum on success, -1 on failure
*/
int MFS_Lookup(int pinum, char *name){
	debug("In MFS_Lookup: entering ...\n");

	if(name == NULL || strlen(name) > 28){
		return -1;
	}

	message_t send;

	send.node_num = pinum;
	strcpy((char*)&(send.name), name);
	send.msg = MFS_LOOKUP;

	
	if(!working){
        return -1;
    }

	message_t receive;

	int ret = Server_To_Client( &send, &receive, my_serv, prt);
	debug("In MFS_Lookup: server retcode %d, received inum %d\n", ret, receive.node_num);
		
	if(ret <= -1){
		return -1;
	}
	else{
		// return the inum of the received message
		return receive.node_num;
	}
}

/* MFS_Stat: takes inum 
returns: 0 on success, 1 on failure

Fills up stat of a file
*/
int MFS_Stat(int inum, MFS_Stat_t *m) {
	debug("In MFS_Stat: entering ...\n");
	message_t send;
	send.msg = MFS_STAT;
	send.node_num = inum;
	
	if(!working){
                return -1;
	}

	message_t receive;

	if(Server_To_Client(&send, &receive, my_serv, prt) <= -1){
		return -1;
	}
	if (receive.node_num == -1) return -1;
	
	m->type = receive.st.type;
	m->size = receive.st.size;

	debug("In MFS_Stat: success. returning ...\n");
	return 0;
}

int MFS_Write(int inum, char *buffer, int offset, int nbytes){
	debug("In MFS_Write. entering ...\n");
	if (offset < 0 || nbytes > 4096) {
		return -1;
	}

	message_t send;

  	for(int i = 0; i<4096; i++){
        send.buf[i] = buffer[i];
    }

	send.nbytes = nbytes;
	send.msg = MFS_WRITE;
	send.offset = offset;
	send.node_num = inum;

	if(!working){
        return -1;
    }
	
	message_t receive;

	if(Server_To_Client(&send, &receive, my_serv, prt) <= -1){
		return -1;
	}

	debug("In MFS_Write: ret %d. returning ...\n", receive.node_num);
	return receive.node_num;
}


int MFS_Read(int inum, char *buffer, int offset, int nbytes){	
	debug("In MFS_Read: entering ...\n");
	if (offset < 0 || nbytes > 4096)
		return -1;
		
	message_t send;

	send.nbytes = nbytes;
	send.node_num = inum;
	send.msg = MFS_READ;
	send.offset = offset;

	if(!working){
        return -1;
    }

	message_t receive;

	if(Server_To_Client(&send, &receive, my_serv, prt) <= -1){
		return -1;
	}

	if(receive.node_num == 0) {
		for(int i = 0; i < nbytes; i++){
			buffer[i] = receive.buf[i];
		}
	}

	debug("In MFS_Read: ret %d. returning ... \n", receive.node_num);
	return receive.node_num;
}

/* MFS_Creat: Create a dir/file based on type with a name and pinum */
int MFS_Creat(int pinum, int type, char *name){
	debug("In MFS_Creat: entering ...\n");
	if(name == NULL || strlen(name) > 28){
		return -1;
	}

	message_t send;

	send.mtype = type;
	send.msg = MFS_CREAT;
	send.node_num = pinum;
	strcpy(send.name, name);
	
	
	if(!working){
                return -1;
        }

	
	message_t receive;

	if(Server_To_Client(&send, &receive, my_serv, prt) <= -1){
		return -1;
	}

	debug("In MFS_Creat. ret %d returning ...\n", receive.node_num);
	return receive.node_num;
}

// MFS_Unlilnk method, unlinks based on pinum and name parameters
int MFS_Unlink(int pinum, char *name){
	debug("In MFS_Unlink: entering ...");

	if(name == NULL || strlen(name) > 28){
		return -1;
	}

	// sending message
	message_t send;

	// fill that message
	send.msg = MFS_UNLINK;
	strcpy(send.name, name);
	send.node_num = pinum;

	// obviously this has to be done yet again
	if(!working){
                return -1;
        }

	// receive!
	message_t receive;

	// actually send!	
	if(Server_To_Client(&send, &receive, my_serv, prt) <= -1){
		return -1;
	}

	debug("In MFS_Unlink: ret %d returning ...\n", receive.node_num);
	return receive.node_num;
}

int MFS_Shutdown(){
	debug("In MFS_Shutdown. entering ... \n");
	message_t send;
	send.msg = MFS_SHUTDOWN;
	message_t receive;

	if(Server_To_Client(&send, &receive, my_serv, prt) <= -1){
		return -1;
	}

	debug("In MFS_Shutdown. returning ...\n");
	return 0;
}
