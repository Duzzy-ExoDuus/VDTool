#include <stdio.h>
#include <string>
#include <zlib.h>

void compress_raw_data(void *in_data, size_t in_data_size, void *out_data, size_t &out_data_size)
{
	z_stream zs;
	zs.zalloc = 0;
	zs.zfree = 0;
	zs.next_in = (Bytef*)in_data;
	zs.avail_in = (uInt)in_data_size;
	zs.next_out = (Bytef*)out_data;
	zs.avail_out = (uInt)in_data_size;

	deflateInit(&zs, Z_DEFAULT_COMPRESSION);

	deflate(&zs, Z_FINISH);

	deflateEnd(&zs);

	out_data_size = zs.total_out;
}

long decompress_raw_data(void *in_data, size_t in_data_size, void *out_data, size_t out_data_size)
{
	z_stream zs;
	memset(&zs, 0, sizeof(zs));

	zs.next_in = (Bytef*)in_data;
	zs.avail_in = (uInt)in_data_size;
	zs.next_out = (Bytef*)(out_data);
	zs.avail_out = (uInt)out_data_size;

	inflateInit(&zs);

	inflate(&zs, Z_NO_FLUSH);

	inflateEnd(&zs);

	return zs.total_out;
}
