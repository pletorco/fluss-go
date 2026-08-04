package fgo

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"sort"
)

func sortedBuckets(buckets map[int32]ServerNode) ([]int32, error) {
	if len(buckets) == 0 {
		return nil, fmt.Errorf("%w: table has no buckets", ErrMetadata)
	}
	ids := make([]int32, 0, len(buckets))
	for id := range buckets {
		if id < 0 {
			return nil, fmt.Errorf("%w: invalid bucket %d", ErrMetadata, id)
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// flussBucket implements FlussBucketingFunction from Fluss 0.9.1. Its first hash is the
// little-endian, seed-42 Murmur3 variant used by MurmurHashUtils.hashUnsafeBytes.
func flussBucket(key []byte, bucketCount int) (int32, error) {
	if len(key) == 0 || bucketCount <= 0 {
		return 0, fmt.Errorf("%w: bucket key and positive bucket count are required", ErrInvalidConfig)
	}
	hash := murmurBytes(key, 42)
	hash = murmurInt(hash)
	return int32(hash % uint32(bucketCount)), nil
}

func murmurBytes(value []byte, seed uint32) uint32 {
	hash := seed
	aligned := len(value) - len(value)%4
	for offset := 0; offset < aligned; offset += 4 {
		hash = murmurMixHash(hash, binary.LittleEndian.Uint32(value[offset:]))
	}
	for _, tail := range value[aligned:] {
		hash = murmurMixHash(hash, uint32(int32(int8(tail))))
	}
	hash ^= uint32(len(value))
	return murmurFinal(hash)
}

func murmurInt(value uint32) uint32 {
	hash := murmurMixHash(0, value)
	hash ^= 4
	hash = murmurFinal(hash)
	if int32(hash) >= 0 {
		return hash
	}
	if hash == 1<<31 {
		return 0
	}
	return uint32(-int32(hash))
}

func murmurMixHash(hash, value uint32) uint32 {
	value *= 0xcc9e2d51
	value = bits.RotateLeft32(value, 15)
	value *= 0x1b873593
	hash ^= value
	hash = bits.RotateLeft32(hash, 13)
	return hash*5 + 0xe6546b64
}

func murmurFinal(hash uint32) uint32 {
	hash ^= hash >> 16
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	return hash ^ hash>>16
}
