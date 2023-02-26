#include <stdio.h>
#include <assert.h>
#include "udp.h"
#include "mfs.c"
#include "message.h"
#include "ufs.h"

void test_lookup() {
	assert(MFS_Lookup(0, "..") == 0);
	assert(MFS_Lookup(0, "a.txt") == -1); 
}

void test_create() {
	assert(MFS_Creat(0, MFS_REGULAR_FILE, "a.txt") == 0);
	assert(MFS_Lookup(0, "a.txt") == 1);
}

void test_write() {
	MFS_Creat(0, MFS_REGULAR_FILE, "a.txt");
	int inum = MFS_Lookup(0, "a.txt");
	char buf[30] = "abcde";
	assert(MFS_Write(inum, buf, 0, 6) == 0);
	assert(MFS_Read(inum, buf, 2, 4) == 0);
	assert(strcmp(buf, "cde") == 0); 
}

void test_stat() {
	MFS_Creat(0, MFS_REGULAR_FILE, "a.txt");
	int inum = MFS_Lookup(0, "a.txt");
	MFS_Stat_t st;
	assert(MFS_Stat(inum, &st) == 0);
	assert(st.size == 0);
	assert(st.type == 1);
}

void test_create_dir() {
	MFS_Creat(0, MFS_DIRECTORY, "test");
	int inum = MFS_Lookup(0, "test");
	assert(MFS_Lookup(inum, ".") == inum);
	assert(MFS_Lookup(inum, "..") == 0);
}

void test_unlink() {
	MFS_Creat(0, MFS_DIRECTORY, "testdir");
	int inum = MFS_Lookup(0, "testdir");
	MFS_Creat(inum, MFS_REGULAR_FILE, "testfile");

	assert(MFS_Unlink(0, "testdir") == -1);
	assert(MFS_Unlink(inum, "testfile") == 0);
	assert(MFS_Unlink(0, "testdir") == 0);
}

int main(int argc, char *argv[]) {
	MFS_Init("localhost", 3004);
	
	test_unlink();
	return 0;

	int rc = -1;
	// loop through
	for(int i = 0; i < 1; i++) {
		int index = i;

		// create a string
		char str[15];

		sprintf(str, "%d", index);
		
		// creat file;
		rc = MFS_Creat(0, MFS_DIRECTORY, str);
		if(rc < 0){
			exit(0);
		}

		// test for lookup
		rc = MFS_Lookup(1, ".");
		rc = MFS_Lookup(1, "..");


		// full creat test
		rc = MFS_Creat(0, MFS_REGULAR_FILE, "test");
		if(rc < 0){
			exit(0);
		}

		rc = MFS_Lookup(1, ".");
		rc = MFS_Lookup(1, "..");

	}

	int inum2 = MFS_Lookup(0, "test_dir");
	if(inum2 < 0){
		exit(0);
	}


	rc = MFS_Creat(inum2, MFS_REGULAR_FILE, "test");
	if(rc < 0){
		exit(0);
	}

 	inum2 = MFS_Lookup(inum2, "test");
	if(inum2 < 0){
		exit(0);
	}

	// create reply an send
	char* reply = malloc(4096);
	char* send = malloc(4096);
	char* attempt2 = malloc(4096);

	// set up some tests
	strcpy(send, "Testing functionality!");
	strcpy(attempt2, "Second test");
	
	rc = MFS_Write(inum2, send, 1, 1);
	if(rc == -1){
		exit(0);
	}
	MFS_Read(inum2, reply, 1, 1);
	if(rc == -1){
		exit(0);
	}

	if(strcmp(reply, send) != 0){
		exit(0);
	}
	rc = MFS_Write(inum2, attempt2, 2, 1);
	if(rc == -1){
		exit(0);
	}
	MFS_Read(inum2, reply, 2, 1);
	if(rc == -1){
		exit(0);
	}
	if(strcmp(reply, attempt2) != 0){
		exit(0);
	}

	rc = MFS_Unlink(0, "test");
	if(rc == -1){
		exit(0);
	}

	inum2 = MFS_Lookup(0, "test");
	if(inum2 >= 0){
		exit(0);
	}
	
	rc = MFS_Unlink(0, "test");
	if(rc == -1){
		exit(0);
	}

	rc = MFS_Unlink(0, "test_dir");
	if(rc != -1){
		exit(0);
	}

	inum2 = MFS_Lookup(0, "test_dir");
	if(inum2 < 0){
		exit(0);
	}

	rc = MFS_Unlink(inum2, "test");
	if(rc == -1){
		exit(0);
	}
	
	inum2 = MFS_Lookup(inum2, "test");
	if(inum2 >= 0){
		exit(0);
	}

	rc = MFS_Unlink(0, "test_dir");
	if(rc == -1){
		exit(0);
	}

	return 0;
}
