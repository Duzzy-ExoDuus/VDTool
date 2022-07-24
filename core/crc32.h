#include <stdint.h>

#if defined(__GNUC__)
	#include <x86intrin.h>
#else
	#include <intrin.h>
#endif

uint64_t sse42_crc32(const uint64_t *buffer, size_t len)
{
	uint64_t hash = 0;

	for (int i = 0; i < len / 8; i++)
	{
		hash = _mm_crc32_u64(hash, buffer[i]);
	}

	return hash;
}
