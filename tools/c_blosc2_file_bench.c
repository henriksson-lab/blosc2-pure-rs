/*
 * Small C-Blosc2 file benchmark helper used by tools/bench_against_c_blosc2.py.
 *
 * This intentionally mirrors the Rust CLI surface: compress/decompress raw files
 * to contiguous Blosc2 frames with explicit codec, filter, chunk, and thread
 * settings.
 */

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <blosc2.h>

static void usage(void) {
  fprintf(stderr,
          "Usage:\n"
          "  c_blosc2_file_bench compress INPUT OUTPUT [options]\n"
          "  c_blosc2_file_bench decompress INPUT OUTPUT [options]\n"
          "\n"
          "Options:\n"
          "  --codec NAME         blosclz, lz4, lz4hc, zlib, zstd\n"
          "  --clevel N           compression level 0..9\n"
          "  --typesize N         logical type size\n"
          "  --blocksize N        explicit block size, 0 = automatic\n"
          "  --chunksize N        input bytes per frame chunk\n"
          "  --splitmode NAME     always, never, auto, forward\n"
          "  --nthreads N         thread count\n"
          "  --filter NAME        nofilter, shuffle, bitshuffle, delta, truncprec\n"
          "  --filter-meta N      metadata byte for final filter slot\n"
          "  --use-dict           enable codec dictionary training\n");
}

static int parse_i32(const char *value, int32_t *out) {
  char *end = NULL;
  errno = 0;
  long parsed = strtol(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' || parsed < INT32_MIN || parsed > INT32_MAX) {
    return -1;
  }
  *out = (int32_t)parsed;
  return 0;
}

static int parse_u8(const char *value, uint8_t *out) {
  int32_t parsed = 0;
  if (parse_i32(value, &parsed) != 0 || parsed < 0 || parsed > 255) {
    return -1;
  }
  *out = (uint8_t)parsed;
  return 0;
}

static int parse_size(const char *value, size_t *out) {
  char *end = NULL;
  errno = 0;
  unsigned long long parsed = strtoull(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' || parsed == 0) {
    return -1;
  }
  *out = (size_t)parsed;
  return 0;
}

static int parse_codec(const char *name) {
  if (strcmp(name, "blosclz") == 0) return BLOSC_BLOSCLZ;
  if (strcmp(name, "lz4") == 0) return BLOSC_LZ4;
  if (strcmp(name, "lz4hc") == 0) return BLOSC_LZ4HC;
  if (strcmp(name, "zlib") == 0) return BLOSC_ZLIB;
  if (strcmp(name, "zstd") == 0) return BLOSC_ZSTD;
  return -1;
}

static int parse_filter(const char *name) {
  if (strcmp(name, "nofilter") == 0 || strcmp(name, "none") == 0) return BLOSC_NOFILTER;
  if (strcmp(name, "shuffle") == 0) return BLOSC_SHUFFLE;
  if (strcmp(name, "bitshuffle") == 0) return BLOSC_BITSHUFFLE;
  if (strcmp(name, "delta") == 0) return BLOSC_DELTA;
  if (strcmp(name, "truncprec") == 0) return BLOSC_TRUNC_PREC;
  return -1;
}

static int parse_splitmode(const char *name) {
  if (strcmp(name, "always") == 0 || strcmp(name, "always_split") == 0) return BLOSC_ALWAYS_SPLIT;
  if (strcmp(name, "never") == 0 || strcmp(name, "never_split") == 0) return BLOSC_NEVER_SPLIT;
  if (strcmp(name, "auto") == 0 || strcmp(name, "auto_split") == 0) return BLOSC_AUTO_SPLIT;
  if (strcmp(name, "forward") == 0 || strcmp(name, "forward_compat") == 0 ||
      strcmp(name, "forward_compat_split") == 0) {
    return BLOSC_FORWARD_COMPAT_SPLIT;
  }
  return -1;
}

typedef struct {
  int codec;
  uint8_t clevel;
  int32_t typesize;
  int32_t blocksize;
  size_t chunksize;
  int splitmode;
  int16_t nthreads;
  int filter;
  uint8_t filter_meta;
  int use_dict;
} bench_options;

static int parse_options(int argc, char **argv, int start, bench_options *opts) {
  opts->codec = BLOSC_BLOSCLZ;
  opts->clevel = 9;
  opts->typesize = 1;
  opts->blocksize = 0;
  opts->chunksize = 1000000;
  opts->splitmode = BLOSC_FORWARD_COMPAT_SPLIT;
  opts->nthreads = 4;
  opts->filter = BLOSC_SHUFFLE;
  opts->filter_meta = 0;
  opts->use_dict = 0;

  for (int i = start; i < argc;) {
    const char *key = argv[i];
    if (strcmp(key, "--use-dict") == 0) {
      opts->use_dict = 1;
      i += 1;
      continue;
    }
    if (i + 1 >= argc) {
      fprintf(stderr, "missing value for %s\n", argv[i]);
      return -1;
    }
    const char *value = argv[i + 1];
    if (strcmp(key, "--codec") == 0) {
      opts->codec = parse_codec(value);
      if (opts->codec < 0) return -1;
    } else if (strcmp(key, "--clevel") == 0) {
      if (parse_u8(value, &opts->clevel) != 0 || opts->clevel > 9) return -1;
    } else if (strcmp(key, "--typesize") == 0) {
      if (parse_i32(value, &opts->typesize) != 0 || opts->typesize <= 0) return -1;
    } else if (strcmp(key, "--blocksize") == 0) {
      if (parse_i32(value, &opts->blocksize) != 0 || opts->blocksize < 0) return -1;
    } else if (strcmp(key, "--chunksize") == 0) {
      if (parse_size(value, &opts->chunksize) != 0) return -1;
    } else if (strcmp(key, "--splitmode") == 0) {
      opts->splitmode = parse_splitmode(value);
      if (opts->splitmode < 0) return -1;
    } else if (strcmp(key, "--nthreads") == 0) {
      int32_t parsed = 0;
      if (parse_i32(value, &parsed) != 0 || parsed <= 0 || parsed > INT16_MAX) return -1;
      opts->nthreads = (int16_t)parsed;
    } else if (strcmp(key, "--filter") == 0) {
      opts->filter = parse_filter(value);
      if (opts->filter < 0) return -1;
    } else if (strcmp(key, "--filter-meta") == 0) {
      if (parse_u8(value, &opts->filter_meta) != 0) return -1;
    } else {
      fprintf(stderr, "unknown option: %s\n", key);
      return -1;
    }
    i += 2;
  }
  return 0;
}

static int compress_file(const char *input_path, const char *output_path, bench_options opts) {
  int rc = 1;
  FILE *input = NULL;
  void *buffer = NULL;
  blosc2_schunk *schunk = NULL;

  blosc2_cparams cparams = BLOSC2_CPARAMS_DEFAULTS;
  cparams.compcode = (uint8_t)opts.codec;
  cparams.clevel = opts.clevel;
  cparams.typesize = opts.typesize;
  cparams.blocksize = opts.blocksize;
  cparams.splitmode = (uint8_t)opts.splitmode;
  cparams.nthreads = opts.nthreads;
  cparams.use_dict = opts.use_dict;
  cparams.filters[BLOSC2_MAX_FILTERS - 1] = (uint8_t)opts.filter;
  cparams.filters_meta[BLOSC2_MAX_FILTERS - 1] = opts.filter_meta;
  if ((opts.typesize == 1 || opts.typesize == 2 || opts.typesize == 4 || opts.typesize == 8) &&
      opts.filter == BLOSC_DELTA) {
    cparams.filters[BLOSC2_MAX_FILTERS - 1] = opts.typesize > 1 ? BLOSC_SHUFFLE : BLOSC_NOFILTER;
    cparams.filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_DELTA;
    cparams.filters_meta[BLOSC2_MAX_FILTERS - 1] = 0;
  }

  blosc2_dparams dparams = BLOSC2_DPARAMS_DEFAULTS;
  dparams.nthreads = opts.nthreads;

  remove(output_path);
  blosc2_storage storage = {
      .cparams = &cparams,
      .dparams = &dparams,
      .contiguous = true,
      .urlpath = (char *)output_path,
  };
  schunk = blosc2_schunk_new(&storage);
  if (schunk == NULL) {
    fprintf(stderr, "failed to create C-Blosc2 schunk\n");
    goto out;
  }

  input = fopen(input_path, "rb");
  if (input == NULL) {
    perror("failed to open input");
    goto out;
  }
  buffer = malloc(opts.chunksize);
  if (buffer == NULL) {
    perror("malloc");
    goto out;
  }

  for (;;) {
    size_t nread = fread(buffer, 1, opts.chunksize, input);
    if (nread > INT32_MAX) {
      fprintf(stderr, "chunk too large for C-Blosc2 API\n");
      goto out;
    }
    if (blosc2_schunk_append_buffer(schunk, buffer, (int32_t)nread) < 0) {
      fprintf(stderr, "failed to append C-Blosc2 chunk\n");
      goto out;
    }
    if (nread < opts.chunksize) {
      if (ferror(input)) {
        perror("failed to read input");
        goto out;
      }
      break;
    }
  }

  rc = 0;

out:
  free(buffer);
  if (input != NULL) fclose(input);
  if (schunk != NULL) blosc2_schunk_free(schunk);
  return rc;
}

static int decompress_file(const char *input_path, const char *output_path, bench_options opts) {
  int rc = 1;
  FILE *output = NULL;
  void *buffer = NULL;
  blosc2_set_nthreads(opts.nthreads);
  blosc2_schunk *schunk = blosc2_schunk_open(input_path);
  if (schunk == NULL) {
    fprintf(stderr, "failed to open C-Blosc2 frame\n");
    return 1;
  }

  int32_t chunksize = schunk->chunksize > 0 ? schunk->chunksize : INT32_MAX;
  buffer = malloc((size_t)chunksize);
  if (buffer == NULL) {
    perror("malloc");
    goto out;
  }
  output = fopen(output_path, "wb");
  if (output == NULL) {
    perror("failed to open output");
    goto out;
  }

  for (int64_t nchunk = 0; nchunk < schunk->nchunks; nchunk++) {
    int32_t dsize = blosc2_schunk_decompress_chunk(schunk, nchunk, buffer, chunksize);
    if (dsize < 0) {
      fprintf(stderr, "C-Blosc2 decompression error: %d\n", dsize);
      goto out;
    }
    if (fwrite(buffer, 1, (size_t)dsize, output) != (size_t)dsize) {
      perror("failed to write output");
      goto out;
    }
  }

  rc = 0;

out:
  free(buffer);
  if (output != NULL) fclose(output);
  blosc2_schunk_free(schunk);
  return rc;
}

int main(int argc, char **argv) {
  if (argc < 4) {
    usage();
    return 2;
  }

  bench_options opts;
  if (parse_options(argc, argv, 4, &opts) != 0) {
    usage();
    return 2;
  }

  blosc2_init();
  int rc;
  if (strcmp(argv[1], "compress") == 0) {
    rc = compress_file(argv[2], argv[3], opts);
  } else if (strcmp(argv[1], "decompress") == 0) {
    rc = decompress_file(argv[2], argv[3], opts);
  } else {
    usage();
    rc = 2;
  }
  blosc2_destroy();
  return rc;
}
