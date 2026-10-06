// Package resourcemetadata validates publisher-supplied display information.
// It is never used to construct execution payloads or establish identity.
package resourcemetadata

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
)

const MaxBytes = 64 * 1024

type Metadata struct {
	PublisherName string `json:"publisher_name,omitempty"`
	RepositoryURL string `json:"repository_url,omitempty"`
	License       string `json:"license,omitempty"`
	ReleaseNotes  string `json:"release_notes,omitempty"`
}

// DecodeObject rejects null, arrays, unknown fields, trailing JSON and oversized
// requests. Empty publication bodies are handled explicitly by the caller.
func DecodeObject(raw []byte, out any) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || len(raw) > MaxBytes || raw[0] != '{' || !utf8.Valid(raw) {
		return errors.New("expected a bounded JSON object")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON object")
	}
	return nil
}

func Parse(raw []byte) (Metadata, error) {
	var fields map[string]json.RawMessage
	if err := DecodeObject(raw, &fields); err != nil {
		return Metadata{}, err
	}
	var m Metadata
	for k, v := range fields {
		var target *string
		var limit int
		switch k {
		case "publisher_name":
			target, limit = &m.PublisherName, 80
		case "repository_url":
			target, limit = &m.RepositoryURL, 2048
		case "license":
			target, limit = &m.License, 128
		case "release_notes":
			target, limit = &m.ReleaseNotes, 4000
		default:
			return Metadata{}, errors.New("unknown metadata field")
		}
		if len(v) == 0 || v[0] != '"' || json.Unmarshal(v, target) != nil {
			return Metadata{}, errors.New("metadata fields must be strings")
		}
		*target = strings.TrimSpace(*target)
		if utf8.RuneCountInString(*target) > limit {
			return Metadata{}, errors.New("metadata field exceeds limit")
		}
		for _, r := range *target {
			if (k == "publisher_name" || k == "license" || k == "repository_url") && unicode.Is(unicode.Cf, r) {
				return Metadata{}, errors.New("format characters are not allowed in publisher, license or repository URL")
			}
			if unicode.IsControl(r) && !(k == "release_notes" && (r == '\n' || r == '\t' || r == '\r')) {
				return Metadata{}, errors.New("control characters are not allowed")
			}
		}
	}
	if m.RepositoryURL != "" {
		u, err := url.Parse(m.RepositoryURL)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.ContainsAny(m.RepositoryURL, "\\#") || strings.IndexFunc(m.RepositoryURL, unicode.IsSpace) >= 0 {
			return Metadata{}, errors.New("repository_url must be an HTTPS URL without credentials, query or fragment")
		}
	}
	raw, _ = json.Marshal(m)
	if len(raw) > MaxBytes {
		return Metadata{}, errors.New("metadata exceeds limit")
	}
	return m, nil
}
