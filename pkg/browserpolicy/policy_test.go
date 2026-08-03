package browserpolicy

import (
	"reflect"
	"strings"
	"testing"
)

func TestCanonicalizeIsExactDeterministicAndPolicyBound(t *testing.T) {
	canonical, digest, err := Canonicalize(Full, []string{
		"https://xn--bcher-kva.example:8443",
		"https://example.com",
		"https://example.com",
	})
	if err != nil {
		t.Fatalf("Canonicalize: %v", err)
	}
	want := []string{"https://example.com", "https://xn--bcher-kva.example:8443"}
	if !reflect.DeepEqual(canonical, want) || len(digest) != 64 {
		t.Fatalf("canonical = %#v, digest = %q", canonical, digest)
	}
	if validated, err := ValidateCanonical(Full, want); err != nil || validated != digest {
		t.Fatalf("ValidateCanonical = %q, %v", validated, err)
	}
	if _, _, err := Canonicalize(Restricted, want); err == nil {
		t.Fatal("restricted accepted mutation origins")
	}
	if _, _, err := Canonicalize(Full, []string{}); err == nil {
		t.Fatal("full accepted an empty mutation scope")
	}
	if _, err := ValidateCanonical(Full, []string{
		"https://xn--bcher-kva.example:8443",
		"https://example.com",
	}); err == nil {
		t.Fatal("unsorted wire scope was accepted")
	}
}

func TestCanonicalizeRejectsAmbiguousOrBroadOrigins(t *testing.T) {
	for _, origin := range []string{
		"http://example.com",
		"https://user@example.com",
		"https://example.com/",
		"https://example.com/path",
		"https://example.com?query=1",
		"https://example.com#fragment",
		"https://*.example.com",
		"https://example.com.",
		"https://example.com%2fcollector",
		"https://127.1",
		"https://2130706433",
		"https://0x7f.1",
		"https://0177.1",
		"https://09.1",
		"https://1.2.3.999",
		"https://１２７。１",
		"https://[::ffff:127.0.0.1]",
		"https://[::ffff:7f00:1]",
		" https://example.com",
	} {
		if _, _, err := Canonicalize(Full, []string{origin}); err == nil {
			t.Fatalf("invalid origin %q was accepted", origin)
		}
	}
	if canonical, _, err := Canonicalize(Full, []string{"https://123.example"}); err != nil || !reflect.DeepEqual(canonical, []string{"https://123.example"}) {
		t.Fatalf("ordinary numeric DNS label was rejected: %#v, %v", canonical, err)
	}
	if canonical, _, err := Canonicalize(Full, []string{
		"HTTPS://BÜCHER.example:0443",
		"https://example.com:08443",
	}); err != nil || !reflect.DeepEqual(canonical, []string{
		"https://example.com:8443",
		"https://xn--bcher-kva.example",
	}) {
		t.Fatalf("IDNA/default-port origin was not canonicalized: %#v, %v", canonical, err)
	}
	tooMany := make([]string, MaxOrigins+1)
	for index := range tooMany {
		tooMany[index] = "https://" + strings.Repeat("a", index/26+1) + string(rune('a'+index%26)) + ".example"
	}
	if _, _, err := Canonicalize(Full, tooMany); err == nil {
		t.Fatal("more than 32 origins were accepted")
	}
}
