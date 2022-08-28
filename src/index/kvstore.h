#ifndef KVSTORE_H_
#define KVSTORE_H_

#include "../destor.h"

void init_kvstore();
extern struct post_compress_entry;

extern void (*close_kvstore)();
extern int64_t* (*kvstore_lookup)(char *key);
extern void (*kvstore_update)(char *key, int64_t id);
extern void (*kvstore_delete)(char* key, int64_t id);

extern void (*close_kvstore_post_compress)();
extern struct post_compress_entry* (*kvstore_lookup_post_compress)(char *key);
extern void (*kvstore_update_post_compress)(char *key, int64_t id, fingerprint fp);
extern void (*kvstore_delete_post_compress)(char* key, int64_t id);

extern void (*close_kvstore_fp_to_fp)();
extern char* (*kvstore_lookup_fp_to_fp)(fingerprint fp);
extern void (*kvstore_update_fp_to_fp)(fingerprint key, fingerprint value);
extern void (*kvstore_delete_fp_to_fp)(fingerprint key);


#endif
