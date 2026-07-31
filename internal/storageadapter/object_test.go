package storageadapter

import "testing"

func TestParseObjectURI(t *testing.T) {
	host, key, err := ParseObjectURI("oss://bucket/a%20folder/object", "oss")
	if err != nil || host != "bucket" || key != "a folder/object" {
		t.Fatalf("ParseObjectURI() = %q, %q, %v", host, key, err)
	}
	for _, value := range []string{
		"", "s3://bucket/key", "oss://bucket", "oss://user@bucket/key",
		"oss://bucket/key?version=1", "oss://bucket/key#fragment", "oss://bucket/%zz",
	} {
		if _, _, err := ParseObjectURI(value, "oss"); err == nil {
			t.Errorf("ParseObjectURI(%q) succeeded", value)
		}
	}
}

func TestValidateRange(t *testing.T) {
	offset, length, err := ValidateRange(2, 0, 8, 0)
	if err != nil || offset != 2 || length != 6 {
		t.Fatalf("ValidateRange() = %d, %d, %v", offset, length, err)
	}
	for _, values := range [][4]int64{
		{-1, 1, 2, 2}, {0, 1, 0, 1}, {2, 1, 2, 2}, {0, -1, 2, 2},
		{0, 2, 2, 1}, {1, 2, 2, 2}, {0, 1, 2, -1},
	} {
		if _, _, err := ValidateRange(values[0], values[1], values[2], values[3]); err == nil {
			t.Errorf("ValidateRange(%v) succeeded", values)
		}
	}
}

func TestParseContentRange(t *testing.T) {
	start, end, total, err := ParseContentRange("bytes 10-19/20")
	if err != nil || start != 10 || end != 19 || total != 20 {
		t.Fatalf("ParseContentRange() = %d, %d, %d, %v", start, end, total, err)
	}
	for _, value := range []string{
		"", "items 0-1/2", "bytes 0/2", "bytes a-1/2", "bytes 1-0/2",
		"bytes 0-2/2", "bytes 0-1/*",
	} {
		if _, _, _, err := ParseContentRange(value); err == nil {
			t.Errorf("ParseContentRange(%q) succeeded", value)
		}
	}
}
