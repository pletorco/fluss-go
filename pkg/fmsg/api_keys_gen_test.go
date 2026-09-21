package fmsg

import "testing"

func TestGeneratedAPIKeys(t *testing.T) {
	apis := APIKeys()
	if got, want := len(apis), 67; got != want {
		t.Fatalf("len(APIKeys()) = %d, want %d", got, want)
	}
	for index, api := range apis {
		if got, want := api.Key, APIKey(1000+index); got != want {
			t.Fatalf("APIKeys()[%d].Key = %d, want %d", index, got, want)
		}
	}
	if got, want := apis[len(apis)-1].Key, APIKeyRemoveServerTagByRack; got != want {
		t.Fatalf("last API key = %d, want %d", got, want)
	}
	for key, want := range map[APIKey]int16{
		APIKeyProduceLog:            1,
		APIKeyPutKv:                 3,
		APIKeyLookup:                1,
		APIKeyGetKvSnapshotMetadata: 1,
		APIKeyPrefixLookup:          1,
		APIKeyAlterTable:            1,
	} {
		api, ok := LookupAPIKey(key)
		if !ok {
			t.Fatalf("LookupAPIKey(%d) returned no metadata", key)
		}
		if got := api.MaxVersion; got != want {
			t.Fatalf("%s max version = %d, want %d", api.Name, got, want)
		}
	}
}
