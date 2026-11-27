package catalogconfig

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

type Audience string

const (
	AudiencePublic  Audience = "public"
	AudienceMembers Audience = "members"
	AudienceAdmins  Audience = "admins"
)

type Registry struct {
	Name                string
	URL                 string
	Consumer            string
	Token               string
	Audience            Audience
	AssertionAudience   string
	SigningKeyID        string
	SigningKey          ed25519.PrivateKey
	AllowHTTP           bool
	AllowPrivateNetwork bool
	Writes              bool
	PublicURL           string
	base                *url.URL
}

func (r Registry) PublicBase() string {
	for _, candidate := range []string{r.PublicURL, r.AssertionAudience, r.URL} {
		u, err := url.Parse(candidate)
		if candidate == "" || err != nil || u.Host == "" {
			continue
		}
		u.User, u.RawQuery, u.ForceQuery, u.Fragment, u.RawFragment = nil, "", false, "", ""
		return strings.TrimRight(u.String(), "/")
	}
	return ""
}

func (r Registry) CanSign() bool { return r.SigningKey != nil }

func (r Registry) Base() *url.URL {
	if r.base == nil {
		return nil
	}
	u := *r.base
	return &u
}

var (
	namePattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
	consumerPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	kidPattern      = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

type rawRegistry struct {
	Name                string `json:"name"`
	URL                 string `json:"url"`
	Consumer            string `json:"consumer"`
	Token               string `json:"token"`
	Audience            string `json:"audience"`
	AssertionAudience   string `json:"assertion_audience"`
	SigningKeyID        string `json:"signing_key_id"`
	SigningKey          string `json:"signing_key"`
	AllowHTTP           bool   `json:"allow_http"`
	AllowPrivateNetwork bool   `json:"allow_private_network"`
	Writes              bool   `json:"writes"`
	PublicURL           string `json:"public_url"`
}

func Parse(raw string) ([]Registry, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.DisallowUnknownFields()
	var items []rawRegistry
	if err := dec.Decode(&items); err != nil {
		return nil, errors.New("CATALOG_REGISTRIES must be a JSON array of registry objects with known fields")
	}
	out := make([]Registry, 0, len(items))
	seen := map[string]bool{}
	for i, item := range items {
		reg, err := parseOne(item)
		if err != nil {
			return nil, fmt.Errorf("CATALOG_REGISTRIES[%d]: %w", i, err)
		}
		if seen[reg.Name] {
			return nil, fmt.Errorf("CATALOG_REGISTRIES[%d]: duplicate name %q", i, reg.Name)
		}
		seen[reg.Name] = true
		out = append(out, reg)
	}
	return out, nil
}

func parseOne(item rawRegistry) (Registry, error) {
	if !namePattern.MatchString(item.Name) {
		return Registry{}, errors.New("name must be 1-32 lowercase letters, digits or hyphens")
	}
	u, err := url.Parse(item.URL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || strings.HasSuffix(item.URL, "?") || strings.HasSuffix(item.URL, "#") {
		return Registry{}, errors.New("url must be an absolute URL without credentials, query or fragment")
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !item.AllowHTTP {
			return Registry{}, errors.New("url must use https unless allow_http is set")
		}
	default:
		return Registry{}, errors.New("url must use https or http")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	if len(item.Consumer) > 40 || !consumerPattern.MatchString(item.Consumer) {
		return Registry{}, errors.New("consumer must be the gateway name registered at the registry")
	}
	if !strings.HasPrefix(item.Token, "olr_") || len(item.Token) < 40 || strings.ContainsAny(item.Token, " \t\r\n") {
		return Registry{}, errors.New("token must be a registry API token")
	}
	audience := Audience(item.Audience)
	if audience != AudiencePublic && audience != AudienceMembers && audience != AudienceAdmins {
		return Registry{}, errors.New("audience must be public, members or admins")
	}
	reg := Registry{Name: item.Name, URL: u.String(), Consumer: item.Consumer, Token: item.Token, Audience: audience,
		AllowHTTP: item.AllowHTTP, AllowPrivateNetwork: item.AllowPrivateNetwork, Writes: item.Writes, base: u}
	if (item.SigningKey == "") != (item.SigningKeyID == "") {
		return Registry{}, errors.New("signing_key and signing_key_id must be set together")
	}
	if item.SigningKey != "" {
		seed, err := base64.StdEncoding.DecodeString(item.SigningKey)
		if err != nil || len(seed) != ed25519.SeedSize {
			return Registry{}, errors.New("signing_key must be a base64 Ed25519 seed of 32 bytes")
		}
		if !kidPattern.MatchString(item.SigningKeyID) {
			return Registry{}, errors.New("signing_key_id must be 1-64 of A-Z a-z 0-9 . _ -")
		}
		aud, err := url.Parse(item.AssertionAudience)
		if err != nil || item.AssertionAudience == "" || aud.Host == "" || (aud.Scheme != "https" && aud.Scheme != "http") {
			return Registry{}, errors.New("assertion_audience must be the absolute URL the registry expects in assertions")
		}
		reg.SigningKey = ed25519.NewKeyFromSeed(seed)
		reg.SigningKeyID = item.SigningKeyID
		reg.AssertionAudience = item.AssertionAudience
	}
	if item.PublicURL != "" {
		pub, err := url.Parse(item.PublicURL)
		if err != nil || pub.Host == "" || (pub.Scheme != "https" && pub.Scheme != "http") || pub.User != nil || strings.ContainsAny(item.PublicURL, "?#") {
			return Registry{}, errors.New("public_url must be an absolute http(s) URL without credentials, query or fragment")
		}
		reg.PublicURL = item.PublicURL
	}
	if reg.Writes && reg.SigningKey == nil {
		return Registry{}, errors.New("writes need signing_key, signing_key_id and assertion_audience")
	}
	return reg, nil
}

func Projection(regs []Registry, name, publicURL string) (Registry, error) {
	if name == "" {
		if publicURL != "" {
			return Registry{}, errors.New("CATALOG_PROJECTION_PUBLIC_URL needs CATALOG_PROJECTION_REGISTRY")
		}
		return Registry{}, nil
	}
	var reg Registry
	found := false
	for _, r := range regs {
		if r.Name == name {
			reg, found = r, true
		}
	}
	if !found {
		return Registry{}, errors.New("CATALOG_PROJECTION_REGISTRY must name one of CATALOG_REGISTRIES")
	}
	if !reg.CanSign() {
		return Registry{}, errors.New("the projection registry needs signing_key, signing_key_id and assertion_audience")
	}
	u, err := url.Parse(publicURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return Registry{}, errors.New("CATALOG_PROJECTION_PUBLIC_URL must be an https URL without credentials, query or fragment")
	}
	return reg, nil
}
