// Package storageadapter contains protocol-neutral validation shared by
// optional remote-object adapters.
package storageadapter

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// ParseObjectURI parses an object-store URI without accepting credentials,
// query parameters, or fragments.
func ParseObjectURI(raw, scheme string) (string, string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != scheme || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", fmt.Errorf("invalid %s URI", scheme)
	}
	escapedKey := strings.TrimPrefix(parsed.EscapedPath(), "/")
	key, err := url.PathUnescape(escapedKey)
	if err != nil || key == "" {
		return "", "", fmt.Errorf("invalid %s object key", scheme)
	}
	return parsed.Host, key, nil
}

// ValidateRange validates and normalizes an exact object byte range.
func ValidateRange(offset, length, expectedSize, maxBytes int64) (int64, int64, error) {
	if offset < 0 || expectedSize <= 0 || offset >= expectedSize {
		return 0, 0, fmt.Errorf("offset or expected size is outside the object")
	}
	if length == 0 {
		length = expectedSize - offset
	}
	if maxBytes == 0 {
		maxBytes = expectedSize
	}
	if length <= 0 || maxBytes <= 0 || length > maxBytes ||
		length > expectedSize-offset {
		return 0, 0, fmt.Errorf("length exceeds the object or configured limit")
	}
	return offset, length, nil
}

// ParseContentRange parses an HTTP bytes Content-Range value.
func ParseContentRange(value string) (int64, int64, int64, error) {
	unit, rangeAndSize, ok := strings.Cut(value, " ")
	if !ok || unit != "bytes" {
		return 0, 0, 0, fmt.Errorf("invalid content range")
	}
	byteRange, size, ok := strings.Cut(rangeAndSize, "/")
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid content range")
	}
	start, end, ok := strings.Cut(byteRange, "-")
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid content range")
	}
	startValue, startErr := strconv.ParseInt(start, 10, 64)
	endValue, endErr := strconv.ParseInt(end, 10, 64)
	sizeValue, sizeErr := strconv.ParseInt(size, 10, 64)
	if startErr != nil || endErr != nil || sizeErr != nil ||
		startValue < 0 || endValue < startValue || sizeValue <= endValue {
		return 0, 0, 0, fmt.Errorf("invalid content range")
	}
	return startValue, endValue, sizeValue, nil
}
