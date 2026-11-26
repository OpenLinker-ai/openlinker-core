package catalogregistry_test

import (
	"archive/tar"
	"bytes"
	"strings"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/catalogregistry"
	"github.com/stretchr/testify/require"
)

type tarEntry struct {
	name   string
	body   string
	flag   byte
	pax    map[string]string
	format tar.Format
}

func buildTar(t *testing.T, entries []tarEntry, trailing []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		flag := e.flag
		if flag == 0 {
			flag = tar.TypeReg
		}
		h := &tar.Header{Name: e.name, Typeflag: flag, Mode: 0o644, Size: int64(len(e.body)), PAXRecords: e.pax, Format: e.format}
		if flag == tar.TypeDir {
			h.Mode, h.Size = 0o755, 0
		}
		if flag == tar.TypeSymlink {
			h.Size, h.Linkname = 0, "SKILL.md"
		}
		require.NoError(t, tw.WriteHeader(h))
		if flag == tar.TypeReg {
			_, err := tw.Write([]byte(e.body))
			require.NoError(t, err)
		}
	}
	require.NoError(t, tw.Close())
	return append(buf.Bytes(), trailing...)
}

func TestDecodeArtifactTar(t *testing.T) {
	skill := "---\nname: demo\ndescription: Demo\n---\nDo it.\n"
	good := buildTar(t, []tarEntry{{name: "SKILL.md", body: skill}, {name: "references/", flag: tar.TypeDir}, {name: "references/a.md", body: "a"}}, nil)
	a, err := catalogregistry.DecodeArtifact("tar", good)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"SKILL.md": skill, "references/a.md": "a"}, a.Files)

	bad := map[string][]byte{
		"duplicate":      buildTar(t, []tarEntry{{name: "SKILL.md", body: skill}, {name: "SKILL.md", body: "other"}}, nil),
		"file dir clash": buildTar(t, []tarEntry{{name: "SKILL.md", body: skill}, {name: "a", body: "x"}, {name: "a/b", body: "y"}}, nil),
		"dir then file":  buildTar(t, []tarEntry{{name: "a/", flag: tar.TypeDir}, {name: "a", body: "x"}}, nil),
		"symlink":        buildTar(t, []tarEntry{{name: "SKILL.md", body: skill}, {name: "link", flag: tar.TypeSymlink}}, nil),
		"pax records":    buildTar(t, []tarEntry{{name: "SKILL.md", body: skill, pax: map[string]string{"comment": "x"}, format: tar.FormatPAX}}, nil),
		"gnu format":     buildTar(t, []tarEntry{{name: "SKILL.md", body: skill, format: tar.FormatGNU}}, nil),
		"traversal":      buildTar(t, []tarEntry{{name: "../SKILL.md", body: skill}}, nil),
		"absolute":       buildTar(t, []tarEntry{{name: "/SKILL.md", body: skill}}, nil),
		"trailing data":  buildTar(t, []tarEntry{{name: "SKILL.md", body: skill}}, []byte("garbage")),
		"too large":      buildTar(t, []tarEntry{{name: "SKILL.md", body: strings.Repeat("a", 65<<10)}}, nil),
		"truncated":      good[:600],
		"empty":          buildTar(t, nil, nil),
	}
	for name, data := range bad {
		_, err := catalogregistry.DecodeArtifact("tar", data)
		require.ErrorIs(t, err, catalogregistry.ErrArtifactInvalid, name)
	}
	many := []tarEntry{{name: "SKILL.md", body: skill}}
	for i := 0; i < 32; i++ {
		many = append(many, tarEntry{name: "f" + strings.Repeat("x", i), body: "x"})
	}
	_, err = catalogregistry.DecodeArtifact("tar", buildTar(t, many, nil))
	require.ErrorIs(t, err, catalogregistry.ErrArtifactInvalid)
}

func TestDecodeArtifactBundleAndMarkdown(t *testing.T) {
	a, err := catalogregistry.DecodeArtifact("openlinker-bundle-v1", []byte(`{"required_commands":["git"],"name":"x","description":"y","files":{"SKILL.md":"s"},"capability_ids":[],"providers":["codex"]}`))
	require.NoError(t, err)
	require.Equal(t, []string{"codex"}, a.Providers)
	require.Equal(t, []string{"git"}, a.RequiredCommands)
	_, err = catalogregistry.DecodeArtifact("openlinker-bundle-v1", []byte(`{"files":{"SKILL.md":"s"},"extra":1}`))
	require.ErrorIs(t, err, catalogregistry.ErrArtifactInvalid)
	_, err = catalogregistry.DecodeArtifact("openlinker-bundle-v1", []byte(`{"files":{}}`))
	require.ErrorIs(t, err, catalogregistry.ErrArtifactInvalid)
	a, err = catalogregistry.DecodeArtifact("skill-md", []byte("body"))
	require.NoError(t, err)
	require.Equal(t, map[string]string{"SKILL.md": "body"}, a.Files)
	_, err = catalogregistry.DecodeArtifact("zip", nil)
	require.ErrorIs(t, err, catalogregistry.ErrArtifactInvalid)
}
