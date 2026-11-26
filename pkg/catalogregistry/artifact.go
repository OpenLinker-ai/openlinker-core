package catalogregistry

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"path"
	"strings"

	"github.com/OpenLinker-ai/openlinker-core/pkg/skillpackage"
)

var ErrArtifactInvalid = errors.New("registry artifact is not a valid Core skill package")

const (
	maxArtifactFiles    = 32
	maxArtifactFileSize = 64 << 10
)

type Artifact struct {
	Files            map[string]string
	Providers        []string
	CapabilityIDs    []string
	RequiredCommands []string
}

func DecodeArtifact(kind string, data []byte) (Artifact, error) {
	switch kind {
	case "skill-md":
		return Artifact{Files: map[string]string{"SKILL.md": string(data)}}, nil
	case "openlinker-bundle-v1":
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		var b skillpackage.Bundle
		if err := dec.Decode(&b); err != nil || dec.More() || len(b.Files) == 0 {
			return Artifact{}, ErrArtifactInvalid
		}
		return Artifact{Files: b.Files, Providers: b.Providers, CapabilityIDs: b.CapabilityIDs, RequiredCommands: b.RequiredCommands}, nil
	case "tar":
		files, err := readCanonicalTar(data)
		if err != nil {
			return Artifact{}, err
		}
		return Artifact{Files: files}, nil
	}
	return Artifact{}, ErrArtifactInvalid
}

func readCanonicalTar(data []byte) (map[string]string, error) {
	if len(data) < 1024 || len(data)%512 != 0 || bytes.ContainsFunc(data[len(data)-1024:], func(r rune) bool { return r != 0 }) {
		return nil, ErrArtifactInvalid
	}
	r := bytes.NewReader(data)
	tr := tar.NewReader(r)
	files := map[string]string{}
	dirs := map[string]bool{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, ErrArtifactInvalid
		}
		if h.Format != tar.FormatUSTAR && h.Format != tar.FormatUnknown || len(h.PAXRecords) > 0 || h.Xattrs != nil {
			return nil, ErrArtifactInvalid
		}
		name := h.Name
		switch h.Typeflag {
		case tar.TypeDir:
			name = strings.TrimSuffix(name, "/")
			if !safePath(name) || dirs[name] {
				return nil, ErrArtifactInvalid
			}
			if _, clash := files[name]; clash {
				return nil, ErrArtifactInvalid
			}
			dirs[name] = true
		case tar.TypeReg:
			if !safePath(name) || h.Size < 0 || h.Size > maxArtifactFileSize {
				return nil, ErrArtifactInvalid
			}
			if _, dup := files[name]; dup || dirs[name] {
				return nil, ErrArtifactInvalid
			}
			if len(files) >= maxArtifactFiles {
				return nil, ErrArtifactInvalid
			}
			body, err := io.ReadAll(io.LimitReader(tr, maxArtifactFileSize+1))
			if err != nil || int64(len(body)) != h.Size {
				return nil, ErrArtifactInvalid
			}
			files[name] = string(body)
		default:
			return nil, ErrArtifactInvalid
		}
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		return nil, ErrArtifactInvalid
	}
	for _, b := range rest {
		if b != 0 {
			return nil, ErrArtifactInvalid
		}
	}
	for name := range files {
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if _, clash := files[parent]; clash {
				return nil, ErrArtifactInvalid
			}
		}
	}
	if len(files) == 0 {
		return nil, ErrArtifactInvalid
	}
	return files, nil
}

func safePath(name string) bool {
	return name != "" && name != "." && !path.IsAbs(name) && path.Clean(name) == name && !strings.HasPrefix(name, "../") && name != ".." && !strings.Contains(name, "\\")
}
