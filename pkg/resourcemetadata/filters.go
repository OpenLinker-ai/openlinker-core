package resourcemetadata

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Filters struct{ Provider, Capability, Tag, Sort string }

var capabilityID = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[/_-][a-z0-9]+)*$`)

func ParseFilters(q url.Values, mcp bool) (Filters, error) {
	f := Filters{Provider: strings.TrimSpace(q.Get("provider")), Capability: strings.TrimSpace(q.Get("capability")), Tag: strings.TrimSpace(q.Get("tag")), Sort: strings.TrimSpace(q.Get("sort"))}
	for _, key := range []string{"q", "page", "size", "provider", "capability", "tag", "sort"} {
		if len(q[key]) > 1 {
			return f, errors.New("duplicate query parameter")
		}
	}
	if (f.Sort != "" && f.Sort != "newest" && f.Sort != "name") || (f.Provider != "" && f.Provider != "claude" && f.Provider != "codex") || (mcp && f.Provider != "") || (!mcp && f.Tag != "") {
		return f, errors.New("invalid directory filter")
	}
	if f.Capability != "" && (len(f.Capability) > 200 || !capabilityID.MatchString(f.Capability)) {
		return f, errors.New("invalid capability ID")
	}
	if utf8.RuneCountInString(f.Tag) > 100 || strings.IndexFunc(f.Tag, unicode.IsControl) >= 0 {
		return f, errors.New("invalid tag")
	}
	return f, nil
}
