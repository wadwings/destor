/* chunking.h
 the main fuction is to chunking the file!
 */

#ifndef CHUNK_H_
#define CHUNK_H_

void chunkAlg_init();
int rabin_chunk_data(unsigned char *p, int n);
int normalized_rabin_chunk_data(unsigned char *p, int n);

void ae_init();
int ae_chunk_data(unsigned char *p, int n);

#endif