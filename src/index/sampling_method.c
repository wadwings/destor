#include "../destor.h"
#include "index.h"
#include <math.h>

int rabin_rolling = 0;
/*
 * Sampling features for a chunk sequence.
 */
GHashTable *(*sampling)(GSequence *chunks, int32_t chunk_num);

/*
 * Used by Extreme Binning and Silo.
 */
static GHashTable *index_sampling_min(GSequence *chunks, int32_t chunk_num)
{

  chunk_num = (chunk_num == 0) ? g_sequence_get_length(chunks) : chunk_num;
  int feature_num = 1;
  if (destor.index_sampling_method[1] != 0 && chunk_num > destor.index_sampling_method[1])
  {
    /* Calculate the number of features we need */
    int remain = chunk_num % destor.index_sampling_method[1];
    feature_num = chunk_num / destor.index_sampling_method[1];
    feature_num = (remain * 2 > destor.index_sampling_method[1]) ? feature_num + 1 : feature_num;
  }

  GSequence *candidates = g_sequence_new(free);
  GSequenceIter *iter = g_sequence_get_begin_iter(chunks);
  GSequenceIter *end = g_sequence_get_end_iter(chunks);
  for (; iter != end; iter = g_sequence_iter_next(iter))
  {
    /* iterate the queue */
    struct chunk *c = g_sequence_get(iter);

    if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
      continue;

    if (g_sequence_get_length(candidates) < feature_num || memcmp(&c->fp, g_sequence_get(g_sequence_iter_prev(g_sequence_get_end_iter(candidates))),
                                                                  sizeof(fingerprint)) < 0)
    {
      /* insufficient candidates or new candidate */
      fingerprint *new_candidate = (fingerprint *)malloc(
          sizeof(fingerprint));
      memcpy(new_candidate, &c->fp, sizeof(fingerprint));
      g_sequence_insert_sorted(candidates, new_candidate,
                               g_fingerprint_cmp, NULL);
      if (g_sequence_get_length(candidates) > feature_num)
      {
        g_sequence_remove(
            g_sequence_iter_prev(
                g_sequence_get_end_iter(candidates)));
      }
    }
  }

  GHashTable *features = g_hash_table_new_full(g_feature_hash,
                                               g_feature_equal, free, NULL);

  while (g_sequence_get_length(candidates) > 0)
  {
    fingerprint *candidate = g_sequence_get(
        g_sequence_get_begin_iter(candidates));
    char *feature = malloc(destor.index_key_size);
    memcpy(feature, candidate, destor.index_key_size);
    g_hash_table_insert(features, feature, NULL);
    g_sequence_remove(g_sequence_get_begin_iter(candidates));
  }
  g_sequence_free(candidates);

  if (g_hash_table_size(features) == 0)
  {
    WARNING("Dedup phase: An empty segment and thus no min-feature is selected!");
    char *feature = malloc(destor.index_key_size);
    memset(feature, 0xff, destor.index_key_size);
    g_hash_table_insert(features, feature, NULL);
  }

  return features;
}

/*
 * Used by Extreme Binning and Silo.
 */
static GHashTable *index_sampling_optimized_min(GSequence *chunks,
                                                int32_t chunk_num)
{

  chunk_num = (chunk_num == 0) ? g_sequence_get_length(chunks) : chunk_num;
  int feature_num = 1;
  if (destor.index_sampling_method[1] != 0 && chunk_num > destor.index_sampling_method[1])
  {
    /* Calculate the number of features we need */
    int remain = chunk_num % destor.index_sampling_method[1];
    feature_num = chunk_num / destor.index_sampling_method[1];
    feature_num =
        (remain * 2 > destor.index_sampling_method[1]) ? feature_num + 1 : feature_num;
  }

  struct anchor
  {
    fingerprint anchor;
    fingerprint candidate;
  };

  int off = 8;
  fingerprint prefix[off + 1];
  int count = 0;
  memset(prefix, 0xff, sizeof(fingerprint) * (off + 1));

  /* Select anchors */
  GSequence *anchors = g_sequence_new(free);

  GSequenceIter *iter = g_sequence_get_begin_iter(chunks);
  GSequenceIter *end = g_sequence_get_end_iter(chunks);
  for (; iter != end; iter = g_sequence_iter_next(iter))
  {
    /* iterate the queue */
    struct chunk *c = g_sequence_get(iter);

    if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
      continue;

    memmove(&prefix[1], prefix, sizeof(fingerprint) * (off));
    memcpy(&prefix[0], &c->fp, sizeof(fingerprint));
    if (g_sequence_get_length(anchors) < feature_num || memcmp(&c->fp,
                                                               g_sequence_get(
                                                                   g_sequence_iter_prev(
                                                                       g_sequence_get_end_iter(anchors))),
                                                               sizeof(fingerprint)) < 0)
    {
      /* insufficient candidates or new candidate */
      struct anchor *new_anchor = (struct anchor *)malloc(
          sizeof(struct anchor));
      memcpy(new_anchor->anchor, &c->fp, sizeof(fingerprint));
      if (count >= off)
      {
        memcpy(&new_anchor->candidate, &prefix[off],
               sizeof(fingerprint));
      }
      else
      {
        memcpy(&new_anchor->candidate, &prefix[count],
               sizeof(fingerprint));
      }

      g_sequence_insert_sorted(anchors, new_anchor, g_fingerprint_cmp,
                               NULL);
      if (g_sequence_get_length(anchors) > feature_num)
        g_sequence_remove(
            g_sequence_iter_prev(g_sequence_get_end_iter(anchors)));
    }
    count++;
  }

  GHashTable *features = g_hash_table_new_full(g_feature_hash,
                                               g_feature_equal, free, NULL);

  while (g_sequence_get_length(anchors) > 0)
  {
    struct anchor *a = g_sequence_get(g_sequence_get_begin_iter(anchors));

    char *feature = malloc(destor.index_key_size);
    memcpy(feature, &a->candidate, destor.index_key_size);

    g_hash_table_insert(features, feature, NULL);
    g_sequence_remove(g_sequence_get_begin_iter(anchors));
  }
  g_sequence_free(anchors);

  if (g_hash_table_size(features) == 0)
  {
    WARNING("Dedup phase: An empty segment and thus no min-feature is selected!");
    char *feature = malloc(destor.index_key_size);
    memset(feature, 0xff, destor.index_key_size);
    g_hash_table_insert(features, feature, NULL);
  }

  return features;
}

/*
 * Used by Sparse Indexing.
 */
static GHashTable *index_sampling_random(GSequence *chunks, int32_t chunk_num)
{
  assert(destor.index_sampling_method[1] != 0);
  GHashTable *features = g_hash_table_new_full(g_feature_hash,
                                               g_feature_equal, free, NULL);

  GSequenceIter *iter = g_sequence_get_begin_iter(chunks);
  GSequenceIter *end = g_sequence_get_end_iter(chunks);
  for (; iter != end; iter = g_sequence_iter_next(iter))
  {
    /* iterate the queue */
    struct chunk *c = g_sequence_get(iter);

    if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
      continue;

    int *head = (int *)&c->fp[16];
    if ((*head) % destor.index_sampling_method[1] == 0)
    {
      if (!g_hash_table_contains(features, &c->fp))
      {
        char *new_feature = malloc(destor.index_key_size);
        memcpy(new_feature, &c->fp, destor.index_key_size);
        g_hash_table_insert(features, new_feature, NULL);
      }
    }
  }

  if (g_hash_table_size(features) == 0)
  {
    /* No feature? */
    WARNING("Dedup phase: no features are sampled");
    char *new_feature = malloc(destor.index_key_size);
    memset(new_feature, 0x00, destor.index_key_size);
    g_hash_table_insert(features, new_feature, NULL);
  }
  return features;
}

static GHashTable *index_sampling_uniform(GSequence *chunks, int32_t chunk_num)
{
  assert(destor.index_sampling_method[1] != 0);
  GHashTable *features = g_hash_table_new_full(g_feature_hash,
                                               g_feature_equal, free, NULL);
  int count = 0;

  GSequenceIter *iter = g_sequence_get_begin_iter(chunks);
  GSequenceIter *end = g_sequence_get_end_iter(chunks);
  for (; iter != end; iter = g_sequence_iter_next(iter))
  {
    struct chunk *c = g_sequence_get(iter);
    /* Examine whether fp is a feature */
    if (count % destor.index_sampling_method[1] == 0)
    {
      if (!g_hash_table_contains(features, &c->fp))
      {
        char *new_feature = malloc(destor.index_key_size);
        memcpy(new_feature, &c->fp, destor.index_key_size);
        g_hash_table_insert(features, new_feature, NULL);
      }
    }
    count++;
  }

  if (g_hash_table_size(features) == 0)
  {
    /* No feature? Empty segment.*/
    assert(chunk_num == 0);
    WARNING("Dedup phase: An empty segment and thus no uniform-feature is selected!");
    char *new_feature = malloc(destor.index_key_size);
    memset(new_feature, 0x00, destor.index_key_size);
    g_hash_table_insert(features, new_feature, NULL);
  }
  return features;
}
const int m[12] = {1058066430, 1103835961, 2116768769, 188918377, 1826857779, 189005785, 1379540075, 1276361770, 139716908, 1747801688, 168629972, 1817848860};
const int a[12] = {1343283062, 2059780743, 1884597173, 1079384469, 1776513971, 139568451, 1580284543, 1736553059, 1251340262, 1560972512, 98742820, 388126111};

subchunks *index_sampling_super_features(struct chunk *c)
{
  subchunks *res = (subchunks *)malloc(sizeof(struct subchunks));
  const int features_number = SUPER_FINGERPRINT_SIZE * FEATURES_PER_SFS;
  unsigned int features[features_number];
  unsigned int t;
  for(int i = 0; i < features_number; i++)
  {
    features[i] = 0;
  }
  for (int i = 0; i < c->size; i++)
  {
    unsigned int fingerprint = rabin_function(c->data, c->size, i);
    for (int j = 0; j < features_number; j++)
    {
      t = m[j] * fingerprint + a[j];
      if (features[j] < t)
      {
        features[j] = t;
      }
    }
  }
  rabin_rolling = 0;
  res->super_features = (fingerprint *)malloc(SUPER_FINGERPRINT_SIZE * sizeof(fingerprint));
  char * features_char = (char *)malloc(sizeof(int) * FEATURES_PER_SFS);
  MD5_CTX ctx;
  for (int i = 0; i < SUPER_FINGERPRINT_SIZE; i++)
  {
    for (int j = 0; j < FEATURES_PER_SFS; j++)
    {
      memcpy(features_char + j * sizeof(int), features + i * FEATURES_PER_SFS + j, sizeof(int)); 
    }
    memset(res->super_features[i], 0x00, sizeof(fingerprint));
    MD5_Init(&ctx);
    MD5_Update(&ctx, features_char, FEATURES_PER_SFS * sizeof(int));
    MD5_Final(res->super_features[i], &ctx);
  }
	free(features_char);
  return res;
}

subchunks *index_sampling_finesse(struct chunk *c)
{
  subchunks *res = (subchunks *)malloc(sizeof(struct subchunks));
  const int features_number = SUPER_FINGERPRINT_SIZE * FEATURES_PER_SFS;
  unsigned int features[features_number];
  unsigned int t;
  const unsigned int sub_chunksize = c->size / features_number;
  for (int i = 0; i < features_number; i++)
  {
    for (int j = 0; j < sub_chunksize; j++)
    {
      t = rabin_function(c->data, c->size, i * sub_chunksize + j);
      if (features[i] < t)
      {
        features[i] = t;
      }
    }
  }
  rabin_rolling = 0;
  unsigned int features_group[SUPER_FINGERPRINT_SIZE][FEATURES_PER_SFS];
  for(int i = 0; i < FEATURES_PER_SFS; i++)
  {
    for(int j = 0; j < SUPER_FINGERPRINT_SIZE; j++){
      for(int k = j; k < SUPER_FINGERPRINT_SIZE; k++){
        if(features[j * FEATURES_PER_SFS + i] < features[k * FEATURES_PER_SFS + i]){
          unsigned int temp = features[j * FEATURES_PER_SFS + i];
          features[j * FEATURES_PER_SFS + i] = features[k * FEATURES_PER_SFS + i];
          features[k * FEATURES_PER_SFS + i] = temp;
        }
      }
      features_group[j][i] = features[i * FEATURES_PER_SFS + j];
    }
  }
  MD5_CTX ctx;
  res->super_features = (fingerprint *)malloc(SUPER_FINGERPRINT_SIZE * sizeof(fingerprint));
  char * features_char = (char *)malloc(sizeof(int) * FEATURES_PER_SFS);
  for(int i = 0; i < SUPER_FINGERPRINT_SIZE; i++){
    for(int j = 0; j < FEATURES_PER_SFS; j++){
      memcpy(features_char + j * sizeof(int), features_group[i][j], sizeof(int));
    }
    memset(res->super_features[i], 0x00, sizeof(fingerprint));
    MD5_Init(&ctx);
    MD5_Update(&ctx, features_char, FEATURES_PER_SFS * sizeof(int));
    MD5_Final(res->super_features[i], &ctx);
  }
  return res;
}

unsigned int rabin_function(unsigned char *data, unsigned int size, int index)
{
  static int rabin_fingerprint = 0;
  if (rabin_rolling)
  {
    rabin_fingerprint = (rabin_fingerprint * 256 - (unsigned int)pow(256, SLIDING_WINDOW_SIZE - 1) * data[(unsigned int)(index - 1) % size] + data[(index + SLIDING_WINDOW_SIZE - 1) % size]);
  }
  else
  {
    rabin_fingerprint = 0;
    for (int i = 0; i < SLIDING_WINDOW_SIZE; i++)
    {
      rabin_fingerprint = (rabin_fingerprint * 256 + data[(i + index) % size]);
    }
    rabin_rolling = 1;
  }    
  return rabin_fingerprint;
}

void init_sampling_method()
{
  switch (destor.index_sampling_method[0])
  {
  case INDEX_SAMPLING_RANDOM:
    sampling = index_sampling_random;
    break;
  case INDEX_SAMPLING_OPTIMIZED_MIN:
    sampling = index_sampling_optimized_min;
    break;
  case INDEX_SAMPLING_MIN:
    sampling = index_sampling_min;
    break;
  case INDEX_SAMPLING_UNIFORM:
    sampling = index_sampling_uniform;
    break;
  default:
    fprintf(stderr, "Invalid sampling method!\n");
    exit(1);
  }
}
