#ifndef __DEBUG_H__
#define __DEBUG_H__

#ifdef DEBUG 
#define debug(fmt, ...) fprintf(stderr, fmt, ##__VA_ARGS__)
#else 
#define debug(fmt, ...) ((void) 0)
#endif 

#endif 
