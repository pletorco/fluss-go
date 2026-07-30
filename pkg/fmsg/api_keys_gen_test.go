package fmsg

import "testing"

func TestGeneratedAPIKeys(t *testing.T) {
	apis := APIKeys()
	if got, want := len(apis), 60; got != want {
		t.Fatalf("len(APIKeys()) = %d, want %d", got, want)
	}
	for index, api := range apis {
		if got, want := api.Key, APIKey(1000+index); got != want {
			t.Fatalf("APIKeys()[%d].Key = %d, want %d", index, got, want)
		}
	}
	for _, key := range []APIKey{APIKeyPutKv, APIKeyLookup, APIKeyPrefixLookup} {
		api, ok := LookupAPIKey(key)
		if !ok {
			t.Fatalf("LookupAPIKey(%d) returned no metadata", key)
		}
		if got, want := api.MaxVersion, int16(1); got != want {
			t.Fatalf("%s max version = %d, want %d", api.Name, got, want)
		}
	}
}
