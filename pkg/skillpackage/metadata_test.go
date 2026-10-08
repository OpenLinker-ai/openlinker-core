package skillpackage

import (
	"encoding/json"
	"github.com/google/uuid"
	"strings"
	"testing"
)

func TestDisplayMetadataExplicitContractAndCompatibility(t *testing.T) {
	v := publicVersion{ID: uuid.New(), PackageID: uuid.New(), PublicationMetadata: json.RawMessage(`{"release_notes":"Public"}`)}
	for _, tc := range []struct {
		name, description string
		compatible        bool
	}{{"report", "short", true}, {"Report", "short", false}, {"claude-helper", "short", false}, {strings.Repeat("a", 65), "short", false}, {"report", strings.Repeat("字", 1025), false}, {"report", strings.Repeat("字", 1024), true}} {
		response, err := displayMetadata(v, Bundle{Name: tc.name, Description: tc.description, Files: map[string]string{"SKILL.md": "secret"}})
		if err != nil || response.LocalInstallCompatible != tc.compatible {
			t.Fatalf("compatibility %q: %v", tc.name, err)
		}
		raw, _ := json.Marshal(response)
		if strings.Contains(string(raw), "secret") || strings.Contains(string(raw), "files") {
			t.Fatal("files leaked")
		}
	}
	for _, bad := range []string{`{"owner_id":"private"}`, `{"license":42}`, `{"repository_url":"https://user:secret@example.test"}`, `null`} {
		v.PublicationMetadata = json.RawMessage(bad)
		if _, err := displayMetadata(v, Bundle{}); err == nil {
			t.Fatalf("invalid stored metadata accepted: %s", bad)
		}
	}
}
