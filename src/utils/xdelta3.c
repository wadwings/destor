//
// Permission to distribute this example by
// Copyright (C) 2007 Ralf Junker
// Ralf Junker <delphi@yunqa.de>
// http://www.yunqa.de/delphi/

//---------------------------------------------------------------------------

#include <stdio.h>
#include <sys/stat.h>
#include "xdelta3.h"
#include "../xdelta3/xdelta3.h"
#include "../xdelta3/xdelta3.c"
#include "../destor.h"
//---------------------------------------------------------------------------

#define MAX(x, y) ((x) > (y) ? (x) : (y))
#define MIN(x, y) ((x) < (y) ? (x) : (y))

int code(
    int encode,
    char *src,
    char *in,
    int src_len,
    int in_len,
    char *out)
{
  int r, ret, in_read = 0, outbuflen = 0;
	xd3_stream stream;
  xd3_config config;
  xd3_source source;
  void *Input_Buf;
  int Input_Buf_Read;
  const int BufSize = XD3_ALLOCSIZE;
  memset(&stream, 0, sizeof(stream));
  memset(&source, 0, sizeof(source));
  xd3_init_config(&config, XD3_ADLER32);
  config.winsize = BufSize;
  xd3_config_stream(&stream, &config);

  if (src_len)
  {
    // r = fstat(fileno(SrcFile), &statbuf);
    // if (r)
    //   return r;
    
    source.blksize = BufSize;
    int src_buffer_size = MIN(src_len, source.blksize);
    source.curblk = malloc(source.blksize);
    memcpy(source.curblk, src, src_buffer_size);
    /* Load 1st block of stream. */
    // r = fseek(SrcFile, 0, SEEK_SET);
    // if (r)
    //   return r;
    // source.onblk = fread((void*)source.curblk, 1, source.blksize, SrcFile);
    source.onblk = src_buffer_size;
    source.curblkno = 0;
    /* Set the stream. */
    src_len -= src_buffer_size;
    xd3_set_source(&stream, &source);
  }

  Input_Buf = malloc(BufSize);

  // fseek(InFile, 0, SEEK_SET);
  do
  {
    Input_Buf_Read = MIN(BufSize, in_len);
    memcpy(Input_Buf, in + in_read, Input_Buf_Read);
    in_len -= Input_Buf_Read;
    in_read += Input_Buf_Read;
    if (Input_Buf_Read <= BufSize)
    {
      xd3_set_flags(&stream, XD3_FLUSH | stream.flags);
    }
    xd3_avail_input(&stream, Input_Buf, Input_Buf_Read);
    int while_n_break = 1;
    while (while_n_break)
    {

      if (encode)
        ret = xd3_encode_input(&stream);
      else
        ret = xd3_decode_input(&stream);

      switch (ret)
      {
        case XD3_INPUT:
        {
//          fprintf(stderr, "XD3_INPUT\n");
          while_n_break = 0;
          break;
        }

        case XD3_OUTPUT:
        {
//          fprintf(stderr, "XD3_OUTPUT\n");

          memcpy(out + outbuflen, stream.next_out, stream.avail_out);
          outbuflen += stream.avail_out;
          // r = fwrite(stream.next_out, 1, stream.avail_out, OutFile);

          // if (r != (int)stream.avail_out)
          //   return r;

          xd3_consume_output(&stream);
          break;
        }

        case XD3_GETSRCBLK:
        {
//          fprintf(stderr, "XD3_GETSRCBLK %qd\n", source.getblkno);
          if (src_len)
          {
            int src_buffer_size = MIN(src_len, source.blksize);
            memcpy(source.curblk, src + source.blksize * source.getblkno, src_buffer_size);

            // r = fseek(SrcFile, source.blksize * source.getblkno, SEEK_SET);
            // if (r)
            //   return r;
            source.onblk = src_buffer_size;
            source.curblkno = source.getblkno;
            src_len -= src_buffer_size;
          }
          break;
        }

        case XD3_GOTHEADER:
        {
//          fprintf(stderr, "XD3_GOTHEADER\n");
          break;
        }

        case XD3_WINSTART:
        {
//          fprintf(stderr, "XD3_WINSTART\n");
          break;
        }

        case XD3_WINFINISH:
        {
//          fprintf(stderr, "XD3_WINFINISH\n");
          break;
        }

        default:
        {
          fprintf(stderr, "!!! INVALID %s %d !!!\n",
                  stream.msg, ret);
          *out = NULL;
          return -1;
        }
      }
    }
  } while (Input_Buf_Read == BufSize);

  free(Input_Buf);

  free((void *)source.curblk);
  xd3_close_stream(&stream);
  xd3_free_stream(&stream);

  return outbuflen;
};

int xdelta3_encode(char *src, char *in, int srclen, int inlen, char *out){

  // if (argc != 4)
  // {
  //   fprintf(stderr, "usage: %s source input\n", argv[0]);
  //   return 1;
  // }
  // char *src = (char*)malloc(50000 * sizeof(char));
  // char *in = (char*)malloc(50000 * sizeof(char));
  // char *out = (char*)malloc(50000 * sizeof(char));

  // FILE *src_file = fopen(argv[1], "r");
  // FILE *in_file = fopen(argv[2], "r");
  // FILE *out_file = fopen(argv[3], "w");
  
  // int outlen, srclen, inlen;
  int * outlen = malloc(sizeof(int));
  int r;
  r = xd3_encode_memory(in, inlen, src, srclen, out, outlen, inlen, 0);

  if (r == -1)
  {
    fprintf(stderr, "Decode error: %d\n", r);
    return r;
  }

  return *outlen;

  // fwrite(out, sizeof(char), outlen, out_file);

  /* Decode */

  // r = code(0, InFile, SrcFile, OutFile, 0x1000);

  // fclose(src_file);
  // fclose(in_file);
  // fclose(out_file);

  // if (r)
  // {
  //   fprintf(stderr, "Decode error: %d\n", r);
  //   return r;
  // }

  return outlen;
}

int xdelta3_decode(char *src, char *in, int srclen, int inlen, char *out){

  // if (argc != 4)
  // {
  //   fprintf(stderr, "usage: %s source input\n", argv[0]);
  //   return 1;
  // }
  // char *src = (char*)malloc(50000 * sizeof(char));
  // char *in = (char*)malloc(50000 * sizeof(char));
  // char *out = (char*)malloc(50000 * sizeof(char));

  // FILE *src_file = fopen(argv[1], "r");
  // FILE *in_file = fopen(argv[2], "r");
  // FILE *out_file = fopen(argv[3], "w");
  
  // int outlen, srclen, inlen;

  // srclen = fread(src, 1, 50000, src_file);
  // inlen = fread(in, 1, 50000, in_file);
  /* Encode */

  int * outlen = malloc(sizeof(int));
  int r;
  r = xd3_decode_memory(in, inlen, src, srclen, out, outlen, destor.chunk_max_size, 0);

  if (r == -1)
  {
    fprintf(stderr, "Decode error: %d\n", r);
    return r;
  }

  return *outlen;

  // fwrite(out, sizeof(char), outlen, out_file);

  // /* Decode */

  // // r = code(0, InFile, SrcFile, OutFile, 0x1000);

  // fclose(src_file);
  // fclose(in_file);
  // fclose(out_file);

  // if (r)
  // {
  //   fprintf(stderr, "Decode error: %d\n", r);
  //   return r;
  // }

}