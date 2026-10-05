package resourcemetadata

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"
)

func TestMetadataNormalizationAndBoundaries(t *testing.T) {
	m, err := Parse([]byte(`{"publisher_name":"  Example  ","license":" ","repository_url":"https://example.com/team/repo","release_notes":"  line one\nline two  "}`))
	if err != nil || m.PublisherName != "Example" || m.License != "" || m.ReleaseNotes != "line one\nline two" {
		t.Fatalf("%+v %v", m, err)
	}
	raw, _ := json.Marshal(m)
	if strings.Contains(string(raw), `"license"`) {
		t.Fatal("empty fields retained")
	}
	for _, raw := range []string{`{"repository_url":"https://example.com/a b"}`, `{"repository_url":"https://example.com/a\u00a0b"}`, `{"publisher_name":"team\u202e"}`, `{"license":"MI\u200bT"}`, `{"repository_url":"https://example.com/a\ufeffb"}`, `{"repository_url":"https://example.com/a\u200bb"}`, `null`, `[]`, `{} {}`, `{"unknown":"x"}`, `{"license":null}`, `{"license":true}`, `{"publisher_name":"bad\u0001"}`, `{"repository_url":"javascript:alert(1)"}`, `{"repository_url":"https://user:pass@example.com/repo"}`, `{"repository_url":"https://example.com/repo?token=secret"}`, `{"repository_url":"https://example.com/repo#"}`, `{"repository_url":"https://example.com/repo?"}`} {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, n := range []int{4000, 4001} {
		raw, _ := json.Marshal(Metadata{ReleaseNotes: strings.Repeat("😀", n)})
		_, err := Parse(raw)
		if (err == nil) != (n == 4000) {
			t.Fatalf("unicode limit %d: %v", n, err)
		}
	}
}

func TestFiltersRejectDuplicatesAndKeepExactValues(t *testing.T) {
	for _, query := range []string{"provider=codex&provider=claude", "sort=popular", "capability=bad%20id", "page=1&page=2", "provider=local"} {
		q, _ := url.ParseQuery(query)
		if _, err := ParseFilters(q, false); err == nil {
			t.Fatalf("accepted %s", query)
		}
	}
	q, _ := url.ParseQuery("capability=unknown/valid-id&provider=claude&sort=name")
	f, err := ParseFilters(q, false)
	if err != nil || f.Capability != "unknown/valid-id" || f.Provider != "claude" || f.Sort != "name" {
		t.Fatalf("%+v %v", f, err)
	}
	q, _ = url.ParseQuery("tag=Data&capability=data/analysis")
	f, err = ParseFilters(q, true)
	if err != nil || f.Tag != "Data" {
		t.Fatalf("%+v %v", f, err)
	}
}
