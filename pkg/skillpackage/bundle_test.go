package skillpackage

import (
	"strings"
	"testing"
)

func validImport() ImportRequest {
	return ImportRequest{Version: "1.0.0", Providers: []string{"codex"}, Files: map[string]string{"SKILL.md": "---\nname: report\ndescription: Write reports\n---\nUse references/example.txt.\n", "references/example.txt": "Example"}}
}

func TestValidateImportBoundaries(t *testing.T) {
	for _, name := range []string{"/absolute", "../escape", "a/../../escape", "a/./b", ".env", "refs/.secret", "C:\\secret", "a\x00b"} {
		t.Run(name, func(t *testing.T) {
			req := validImport()
			req.Files[name] = "data"
			if _, _, _, err := ValidateImport(req); err == nil {
				t.Fatal("accepted unsafe file")
			}
		})
	}
	for _, tc := range []struct {
		name   string
		modify func(*ImportRequest)
	}{
		{"frontmatter", func(r *ImportRequest) { r.Files["SKILL.md"] = "no frontmatter" }},
		{"duplicate yaml", func(r *ImportRequest) {
			r.Files["SKILL.md"] = "---\nname: first\nname: second\ndescription: test\n---\nbody"
		}},
		{"binary", func(r *ImportRequest) { r.Files["data"] = "\xff" }},
		{"oversize", func(r *ImportRequest) { r.Files["data"] = strings.Repeat("x", 65536) }},
		{"file directory collision", func(r *ImportRequest) { r.Files["references"] = "not a directory" }},
		{"command path", func(r *ImportRequest) { r.RequiredCommands = []string{"/bin/sh"} }},
		{"unsupported provider", func(r *ImportRequest) { r.Providers = []string{"other"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := validImport()
			tc.modify(&req)
			if _, _, _, err := ValidateImport(req); err == nil {
				t.Fatal("accepted invalid package")
			}
		})
	}
	first := validImport()
	bundle, raw, digest, err := ValidateImport(first)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Name != "report" || len(digest) != 64 || !strings.Contains(raw, "references/example.txt") {
		t.Fatal("missing package metadata")
	}
	first.Files["references/example.txt"] = "Changed"
	_, _, updated, err := ValidateImport(first)
	if err != nil || updated == digest {
		t.Fatal("content change did not change digest")
	}
}

func TestUnicodeManifestLimitsAndEncodedSize(t *testing.T) {
	req := validImport()
	req.Files["SKILL.md"] = "---\nname: " + strings.Repeat("中", 120) + "\ndescription: " + strings.Repeat("文", 2000) + "\n---\nInstructions"
	if _, _, _, err := ValidateImport(req); err != nil {
		t.Fatal(err)
	}
	req.Files["SKILL.md"] = strings.Replace(req.Files["SKILL.md"], "name: ", "name: 中", 1)
	if _, _, _, err := ValidateImport(req); err == nil {
		t.Fatal("accepted 121-character name")
	}
	req = validImport()
	req.Files["escaped.txt"] = strings.Repeat("<", 12000)
	_, _, _, err := ValidateImport(req)
	if v, ok := err.(*ValidationError); !ok || v.Code != "SKILL_PACKAGE_PAYLOAD_TOO_LARGE" {
		t.Fatalf("expected encoded size error, got %v", err)
	}
}
