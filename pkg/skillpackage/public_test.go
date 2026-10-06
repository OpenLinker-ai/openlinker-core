package skillpackage

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestArchiveDirectoryUsesClientSafeNameOrIdentifier(t *testing.T) {
	id := uuid.MustParse("6f1c2b8a-1d2e-4f5a-9b0c-123456789abc")
	for name, want := range map[string]string{
		"release-notes":         "release-notes",
		"a1":                    "a1",
		"Release Notes":         "skill-" + id.String(),
		"release--notes":        "skill-" + id.String(),
		"-release":              "skill-" + id.String(),
		"发布说明":                  "skill-" + id.String(),
		"claude-helper":         "skill-" + id.String(),
		"anthropic-tools":       "skill-" + id.String(),
		"../escape":             "skill-" + id.String(),
		strings.Repeat("a", 65): "skill-" + id.String(),
		strings.Repeat("a", 64): strings.Repeat("a", 64),
	} {
		if got := ArchiveDirectory(name, id); got != want {
			t.Fatalf("ArchiveDirectory(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestBuildArchiveIsDeterministicAndContainsOnlyPackageFiles(t *testing.T) {
	files := map[string]string{"SKILL.md": "---\nname: x\ndescription: y\n---\nbody\n", "references/b.md": "b", "a.txt": "a"}
	first, err := BuildArchive("x", files)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		again, err := BuildArchive("x", map[string]string{"a.txt": "a", "references/b.md": "b", "SKILL.md": files["SKILL.md"]})
		if err != nil || !bytes.Equal(first, again) {
			t.Fatalf("archive bytes changed: %v", err)
		}
	}
	reader, err := zip.NewReader(bytes.NewReader(first), int64(len(first)))
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, file := range reader.File {
		if file.Method != zip.Store || !file.Modified.Equal(archiveEpoch) {
			t.Fatalf("entry %s has non-canonical metadata", file.Name)
		}
		names = append(names, file.Name)
	}
	if strings.Join(names, ",") != "x/SKILL.md,x/a.txt,x/references/b.md" {
		t.Fatalf("unexpected entries %v", names)
	}
}

func TestVerifyStoredVersionRejectsTamperedBytes(t *testing.T) {
	_, payload, digest, err := ValidateImport(validImport())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifyStoredVersion("1.0.0", payload, digest); err != nil {
		t.Fatalf("valid payload rejected: %v", err)
	}
	tampered := strings.Replace(payload, "Example", "Changed", 1)
	for name, tc := range map[string][2]string{
		"digest mismatch":       {tampered, digest},
		"wrong version label":   {payload, digest},
		"non-canonical spacing": {strings.Replace(payload, `{"required_commands"`, `{ "required_commands"`, 1), sha256Hex(strings.Replace(payload, `{"required_commands"`, `{ "required_commands"`, 1))},
	} {
		version := "1.0.0"
		if name == "wrong version label" {
			version = "bad version"
		}
		if _, err := verifyStoredVersion(version, tc[0], tc[1]); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	// Re-hashing tampered bytes still fails when they no longer re-encode identically.
	unsafe := strings.Replace(payload, `"references/example.txt"`, `"../example.txt"`, 1)
	if _, err := verifyStoredVersion("1.0.0", unsafe, sha256Hex(unsafe)); err == nil {
		t.Fatal("unsafe stored path accepted")
	}
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
