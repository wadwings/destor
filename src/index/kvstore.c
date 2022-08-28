#include "kvstore.h"

extern void init_kvstore_htable();
extern void close_kvstore_htable();
extern int64_t* kvstore_htable_lookup(char* key);
extern void kvstore_htable_update(char* key, int64_t id);
extern void kvstore_htable_delete(char* key, int64_t id);

extern void init_kvstore_htable_post_compress();
extern void close_kvstore_htable_post_compress();
extern struct post_compress_entry* kvstore_htable_lookup_post_compress(char* key);
extern void kvstore_htable_update_post_compress(char* key, int64_t id, fingerprint fp);
extern void kvstore_htable_delete_post_compress(char* key, int64_t id);

extern void init_kvstore_htable_fp_to_fp();
extern void close_kvstore_htable_fp_to_fp();
extern char* kvstore_htable_lookup_fp_to_fp(char* key);
extern void kvstore_htable_update_fp_to_fp(char* key, fingerprint value);
extern void kvstore_htable_delete_fp_to_fp(char* key);

/*
 * Mapping a fingerprint (or feature) to the prefetching unit.
 */

void (*close_kvstore)();
int64_t* (*kvstore_lookup)(char *key);
void (*kvstore_update)(char* key, int64_t id);
void (*kvstore_delete)(char* key, int64_t id);

void (*close_kvstore_post_compress)();
struct post_compress_entry* (*kvstore_lookup_post_compress)(char *key);
void (*kvstore_update_post_compress)(char* key, int64_t id, fingerprint fp);
void (*kvstore_delete_post_compress)(char* key, int64_t id);

void (*close_kvstore_fp_to_fp)();
char* (*kvstore_lookup_fp_to_fp)(fingerprint fp);
void (*kvstore_update_fp_to_fp)(fingerprint key, fingerprint value);
void (*kvstore_delete_fp_to_fp)(fingerprint key);

void init_kvstore() {

    switch(destor.index_key_value_store){
    	case INDEX_KEY_VALUE_HTABLE:
    		init_kvstore_htable();
        init_kvstore_htable_post_compress();
        init_kvstore_htable_fp_to_fp();

    		close_kvstore = close_kvstore_htable;
    		kvstore_lookup = kvstore_htable_lookup;
    		kvstore_update = kvstore_htable_update;
    		kvstore_delete = kvstore_htable_delete;

        close_kvstore_post_compress = close_kvstore_htable_post_compress;
    		kvstore_lookup_post_compress = kvstore_htable_lookup_post_compress;
    		kvstore_update_post_compress = kvstore_htable_update_post_compress;
    		kvstore_delete_post_compress = kvstore_htable_delete_post_compress;

        close_kvstore_fp_to_fp = close_kvstore_htable_fp_to_fp;
        kvstore_lookup_fp_to_fp = kvstore_htable_lookup_fp_to_fp;
        kvstore_update_fp_to_fp = kvstore_htable_update_fp_to_fp;
        kvstore_delete_fp_to_fp = kvstore_htable_delete_fp_to_fp;

    		break;
    	default:
    		WARNING("Invalid key-value store!");
    		exit(1);
    }
}
