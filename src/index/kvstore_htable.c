/*
 * kvstore_htable.c
 *
 *  Created on: Mar 23, 2014
 *      Author: fumin
 */

#include "../destor.h"
#include "index.h"

typedef char* kvpair;

#define get_key(kv) (kv)
#define get_value(kv) ((int64_t*)(kv+destor.index_key_size))
#define get_value_post_compress(kv) ((post_compress_entry*)(kv+destor.index_key_size))
#define get_value_fp_to_fp(kv) (kv+destor.index_key_size)

static GHashTable *htable, *htable_post_compress, *htable_fp_to_fp;

static int32_t kvpair_size, kvpair_size_post_compress, kvpair_size_fp_to_fp;

/*
 * Create a new kv pair.
 */
static kvpair new_kvpair_full(char* key){
    kvpair kvp = malloc(kvpair_size);
    memcpy(get_key(kvp), key, destor.index_key_size);
    int64_t* values = get_value(kvp);
    int i;
    for(i = 0; i<destor.index_value_length; i++){
    	values[i] = TEMPORARY_ID;
    }
    return kvp;
}

static kvpair new_kvpair_full_post_compress(char* key){
    kvpair kvp = malloc(kvpair_size_post_compress);
    memcpy(get_key(kvp), key, destor.index_key_size);
    post_compress_entry* values = get_value(kvp);
    for(int i = 0; i<destor.index_value_length; i++){
    	values[i].id = TEMPORARY_ID;
    }
    return kvp;
}

static kvpair new_kvpair_full_fp_to_fp(char* key){
    kvpair kvp = malloc(kvpair_size_fp_to_fp);
    memcpy(get_key(kvp), key, destor.index_key_size);
    return kvp;
}

static kvpair new_kvpair(){
	 kvpair kvp = malloc(kvpair_size);
	 int64_t* values = get_value(kvp);
	 int i;
	 for(i = 0; i<destor.index_value_length; i++){
		 values[i] = TEMPORARY_ID;
	 }
	 return kvp;
}

static kvpair new_kvpair_post_compress(){
	 kvpair kvp = malloc(kvpair_size_post_compress);
	 post_compress_entry* values = get_value_post_compress(kvp);
	 for(int i = 0; i<destor.index_value_length; i++){
		 values[i].id = TEMPORARY_ID;
	 }
	 return kvp;
}

static kvpair new_kvpair_fp_to_fp(){
  kvpair kvp = malloc(kvpair_size_fp_to_fp);
  return kvp;
}

/*
 * IDs in value are in FIFO order.
 * value[0] keeps the latest ID.
 */
static void kv_update(kvpair kv, int64_t id){
    int64_t* value = get_value(kv);
	memmove(&value[1], value,
			(destor.index_value_length - 1) * sizeof(int64_t));
	value[0] = id;
}

static void kv_update_post_compress(kvpair kv, int64_t id, fingerprint fp){
  post_compress_entry* value = get_value(kv);
	memmove(&value[1], value,
			(destor.index_value_length - 1) * sizeof(struct post_compress_entry));
	value[0].id = id;
  memcpy(value[0].fp, fp, sizeof(fingerprint));
}

static void kv_update_fp_to_fp(kvpair kv, fingerprint fp){
  fingerprint value = get_value(kv);
  memcpy(value, fp, sizeof(fingerprint));
}

static inline void free_kvpair(kvpair kvp){
	free(kvp);
}

void init_kvstore_htable(){
    kvpair_size = destor.index_key_size + destor.index_value_length * 8;

    if(destor.index_key_size >=4)
    	htable = g_hash_table_new_full(g_int_hash, g_feature_equal,
			free_kvpair, NULL);
    else
    	htable = g_hash_table_new_full(g_feature_hash, g_feature_equal,
			free_kvpair, NULL);

	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/htable");

	/* Initialize the feature index from the dump file. */
	FILE *fp;
	if ((fp = fopen(indexpath, "r"))) {
		/* The number of features */
		int key_num;
		fread(&key_num, sizeof(int), 1, fp);
		for (; key_num > 0; key_num--) {
			/* Read a feature */
			kvpair kv = new_kvpair();
			fread(get_key(kv), destor.index_key_size, 1, fp);

			/* The number of segments/containers the feature refers to. */
			int id_num, i;
			fread(&id_num, sizeof(int), 1, fp);
			assert(id_num <= destor.index_value_length);

			for (i = 0; i < id_num; i++)
				/* Read an ID */
				fread(&get_value(kv)[i], sizeof(int64_t), 1, fp);

			g_hash_table_insert(htable, get_key(kv), kv);
		}
		fclose(fp);
	}

	sdsfree(indexpath);
}

void init_kvstore_htable_post_compress(){
    kvpair_size_post_compress = destor.index_key_size + sizeof(post_compress_entry) * 8;

    if(destor.index_key_size >=4)
    	htable_post_compress = g_hash_table_new_full(g_int_hash, g_feature_equal,
			free_kvpair, NULL);
    else
    	htable_post_compress = g_hash_table_new_full(g_feature_hash, g_feature_equal,
			free_kvpair, NULL);

	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/htable_post_compress");

	/* Initialize the feature index from the dump file. */
	FILE *fp;
	if ((fp = fopen(indexpath, "r"))) {
		/* The number of features */
		int key_num;
		fread(&key_num, sizeof(int), 1, fp);
		for (; key_num > 0; key_num--) {
			/* Read a feature */
			kvpair kv = new_kvpair_post_compress();
			fread(get_key(kv), destor.index_key_size, 1, fp);

			/* The number of segments/containers the feature refers to. */
			int id_num;
			fread(&id_num, sizeof(int), 1, fp);
			assert(id_num <= destor.index_value_length);

			for (int i = 0; i < id_num; i++)
				/* Read an ID */
				fread(&get_value_post_compress(kv)[i], sizeof(post_compress_entry), 1, fp);
			g_hash_table_insert(htable_post_compress, get_key(kv), kv);
		}
		fclose(fp);
	}
	sdsfree(indexpath);
}

void init_kvstore_htable_fp_to_fp(){
    kvpair_size_fp_to_fp = destor.index_key_size + sizeof(fingerprint);

    if(destor.index_key_size >=4)
    	htable_fp_to_fp = g_hash_table_new_full(g_int_hash, g_feature_equal,
			free_kvpair, NULL);
    else
    	htable_fp_to_fp = g_hash_table_new_full(g_feature_hash, g_feature_equal,
			free_kvpair, NULL);

	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/htable_fp_to_fp");

	/* Initialize the feature index from the dump file. */
	FILE *fp;
	if ((fp = fopen(indexpath, "r"))) {
		/* The number of features */
		int key_num;
		fread(&key_num, sizeof(int), 1, fp);
		for (; key_num > 0; key_num--) {
			/* Read a feature */
			kvpair kv = new_kvpair_fp_to_fp();
			fread(get_key(kv), destor.index_key_size, 1, fp);
      fread(get_value_fp_to_fp(kv), sizeof(fingerprint), 1, fp);
			/* The number of segments/containers the feature refers to. */
			g_hash_table_insert(htable_post_compress, get_key(kv), kv);
		}
		fclose(fp);
	}
	sdsfree(indexpath);
}

void close_kvstore_htable() {
	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/htable");

	FILE *fp;
	if ((fp = fopen(indexpath, "w")) == NULL) {
		perror("Can not open index/htable for write because:");
		exit(1);
	}

	NOTICE("flushing hash table!");
	int key_num = g_hash_table_size(htable);
	fwrite(&key_num, sizeof(int), 1, fp);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, htable);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		/* Write a feature. */
		kvpair kv = value;
		if(fwrite(get_key(kv), destor.index_key_size, 1, fp) != 1){
			perror("Fail to write a key!");
			exit(1);
		}

		/* Write the number of segments/containers */
		if(fwrite(&destor.index_value_length, sizeof(int), 1, fp) != 1){
			perror("Fail to write a length!");
			exit(1);
		}
		int i;
		for (i = 0; i < destor.index_value_length; i++)
			if(fwrite(&get_value(kv)[i], sizeof(int64_t), 1, fp) != 1){
				perror("Fail to write a value!");
				exit(1);
			}

	}

	/* It is a rough estimation */
	destor.index_memory_footprint = g_hash_table_size(htable)
			* (destor.index_key_size + sizeof(int64_t) * destor.index_value_length + 4);

	fclose(fp);

	NOTICE("flushing hash table successfully!");

	sdsfree(indexpath);

	g_hash_table_destroy(htable);
}

void close_kvstore_htable_post_compress() {
	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/htable_post_compress");

	FILE *fp;
	if ((fp = fopen(indexpath, "w")) == NULL) {
		perror("Can not open index/htable_post_compress for write because:");
		exit(1);
	}

	NOTICE("flushing hash table!");
	int key_num = g_hash_table_size(htable_post_compress);
	fwrite(&key_num, sizeof(int), 1, fp);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, htable_post_compress);
	while (g_hash_table_iter_next(&iter, &key, &value)) {

		/* Write a feature. */
		kvpair kv = value;
		if(fwrite(get_key(kv), destor.index_key_size, 1, fp) != 1){
			perror("Fail to write a key!");
			exit(1);
		}

		/* Write the number of segments/containers */
		if(fwrite(&destor.index_value_length, sizeof(int), 1, fp) != 1){
			perror("Fail to write a length!");
			exit(1);
		}
		int i;
		for (i = 0; i < destor.index_value_length; i++)
			if(fwrite(&get_value_post_compress(kv)[i], sizeof(post_compress_entry), 1, fp) != 1){
				perror("Fail to write a value!");
				exit(1);
			}

	}

	/* It is a rough estimation */
	destor.index_memory_footprint = g_hash_table_size(htable_post_compress)
			* (destor.index_key_size + sizeof(int64_t) * destor.index_value_length + 4);

	fclose(fp);

	NOTICE("flushing hash table successfully!");

	sdsfree(indexpath);

	g_hash_table_destroy(htable_post_compress);
}

void close_kvstore_htable_fp_to_fp() {
	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/htable_fp_to_fp");

	FILE *fp;
	if ((fp = fopen(indexpath, "w")) == NULL) {
		perror("Can not open index/htable_post_compress for write because:");
		exit(1);
	}

	NOTICE("flushing hash table!");
	int key_num = g_hash_table_size(htable_fp_to_fp);
	fwrite(&key_num, sizeof(int), 1, fp);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, htable_fp_to_fp);
	while (g_hash_table_iter_next(&iter, &key, &value)) {

		/* Write a feature. */
		kvpair kv = value;
		if(fwrite(get_key(kv), destor.index_key_size, 1, fp) != 1){
			perror("Fail to write a key!");
			exit(1);
		}

		/* Write the number of segments/containers */
    if(fwrite(get_value_fp_to_fp(kv), sizeof(fingerprint), 1, fp) != 1){
      perror("Fail to write a value!");
      exit(1);
    }
	}

	/* It is a rough estimation */
	destor.index_memory_footprint = g_hash_table_size(htable_fp_to_fp)
			* (destor.index_key_size + sizeof(fingerprint) + 4);

	fclose(fp);

	NOTICE("flushing hash table successfully!");

	sdsfree(indexpath);

	g_hash_table_destroy(htable_fp_to_fp);
}
/*
 * For top-k selection method.
 */

int64_t* kvstore_htable_lookup(char* key) {
	kvpair kv = g_hash_table_lookup(htable, key);
	return kv ? get_value(kv) : NULL;
}

post_compress_entry* kvstore_htable_lookup_post_compress(char* key) {
	kvpair kv = g_hash_table_lookup(htable_post_compress, key);
	return kv ? get_value_post_compress(kv) : NULL;
}

fingerprint* kvstore_htable_lookup_fp_to_fp(char* key) {
	kvpair kv = g_hash_table_lookup(htable_fp_to_fp, key);
	return kv ? get_value_fp_to_fp(kv) : NULL;
}

void kvstore_htable_update(char* key, int64_t id) {
	kvpair kv = g_hash_table_lookup(htable, key);
	if (!kv) {
		kv = new_kvpair_full(key);
		g_hash_table_replace(htable, get_key(kv), kv);
	}
	kv_update(kv, id);
}

void kvstore_htable_update_post_compress(char* key, int64_t id, fingerprint fp) {
	kvpair kv = g_hash_table_lookup(htable_post_compress, key);
	if (!kv) {
		kv = new_kvpair_full_post_compress(key);
		g_hash_table_replace(htable_post_compress, get_key(kv), kv);
	}
	kv_update_post_compress(kv, id, fp);
}

void kvstore_htable_update_fp_to_fp(char* key, fingerprint fp) {
	kvpair kv = g_hash_table_lookup(htable_post_compress, key);
	if (!kv) {
		kv = new_kvpair_full_post_compress(key);
		g_hash_table_replace(htable_post_compress, get_key(kv), kv);
	}
	kv_update_fp_to_fp(kv, fp);
}

/* Remove the 'id' from the kvpair identified by 'key' */
void kvstore_htable_delete(char* key, int64_t id){
	kvpair kv = g_hash_table_lookup(htable, key);
	if(!kv)
		return;

	int64_t *value = get_value(kv);
	int i;
	for(i=0; i<destor.index_value_length; i++){
		if(value[i] == id){
			value[i] = TEMPORARY_ID;
			/*
			 * If index exploits physical locality,
			 * the value length is 1. (correct)
			 * If index exploits logical locality,
			 * the deleted one should be in the end. (correct)
			 */
			/* NOTICE: If the backups are not deleted in FIFO order, this assert should be commented */
			assert((i == destor.index_value_length - 1)
					|| value[i+1] == TEMPORARY_ID);
			if(i < destor.index_value_length - 1 && value[i+1] != TEMPORARY_ID){
				/* If the next ID is not TEMPORARY_ID */
				memmove(&value[i], &value[i+1], (destor.index_value_length - i - 1) * sizeof(int64_t));
			}
			break;
		}
	}

	/*
	 * If all IDs are deleted, the kvpair is removed.
	 */
	if(value[0] == TEMPORARY_ID){
		/* This kvpair can be removed. */
		g_hash_table_remove(htable, key);
	}
}

void kvstore_htable_delete_post_compress(char* key, int64_t id){
	kvpair kv = g_hash_table_lookup(htable_post_compress, key);
	if(!kv)
		return;

	post_compress_entry *value = get_value_post_compress(kv);
	for(int i=0; i<destor.index_value_length; i++){
		if(value[i].id == id){
			value[i].id = TEMPORARY_ID;
			/*
			 * If index exploits physical locality,
			 * the value length is 1. (correct)
			 * If index exploits logical locality,
			 * the deleted one should be in the end. (correct)
			 */
			/* NOTICE: If the backups are not deleted in FIFO order, this assert should be commented */
			assert((i == destor.index_value_length - 1)
					|| value[i+1].id == TEMPORARY_ID);
			if(i < destor.index_value_length - 1 && value[i+1].id != TEMPORARY_ID){
				/* If the next ID is not TEMPORARY_ID */
				memmove(&value[i], &value[i+1], (destor.index_value_length - i - 1) * sizeof(post_compress_entry));
			}
			break;
		}
	}

	/*
	 * If all IDs are deleted, the kvpair is removed.
	 */
	if(value[0].id == TEMPORARY_ID){
		/* This kvpair can be removed. */
		g_hash_table_remove(htable_post_compress, key);
	}
}


void kvstore_htable_delete_fp_to_fp(char* key){
	kvpair kv = g_hash_table_lookup(htable_fp_to_fp, key);
	if(!kv)
		return;
  g_hash_table_remove(htable_fp_to_fp, key);
}
