package attachment

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// RoutePath is the API path of uploads and capability links.
const RoutePath = "/api/v1/attachments"

func LogPath(p string) string {
	if strings.HasPrefix(path.Clean("/"+p), RoutePath+"/") {
		return RoutePath + "/:token"
	}
	return p
}

// Field naming convention shared with the trial consoles, Cloud service
// forms and Agent Node: single values end in _image_url / _file_url, arrays
// in _image_urls / _file_urls (or are exactly those names).
var fieldSuffixes = []string{"image_url", "image_urls", "file_url", "file_urls"}

// IsAttachmentField reports whether a top-level input key carries attachments.
func IsAttachmentField(key string) bool {
	for _, suffix := range fieldSuffixes {
		if key == suffix || (strings.HasSuffix(key, "_"+suffix) && len(key) > len(suffix)+1) {
			return true
		}
	}
	return false
}

// Links builds and parses capability links under one public origin.
type Links struct{ base string }

// NewLinks accepts an absolute http(s) origin, for example https://example.com.
func NewLinks(origin string) (Links, error) {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(origin), "/"))
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return Links{}, errors.New("attachment public origin must be an absolute http(s) origin")
	}
	return Links{base: parsed.Scheme + "://" + parsed.Host}, nil
}

func (l Links) Link(token string) string { return l.base + RoutePath + "/" + token }

// Token returns the token of a link issued under this origin, or "".
func (l Links) Token(link string) string {
	prefix := l.base + RoutePath + "/"
	if l.base == "" || !strings.HasPrefix(link, prefix) {
		return ""
	}
	token := strings.TrimPrefix(link, prefix)
	if !ValidToken(token) {
		return ""
	}
	return token
}

// FieldError is a field-level rejection of a Run input.
type FieldError struct {
	Code  string
	Field string
	Msg   string
}

func (e *FieldError) Error() string { return fmt.Sprintf("%s: %s", e.Field, e.Msg) }

const (
	CodeNotOwned = "ATTACHMENT_NOT_OWNED"
	CodeExpired  = "ATTACHMENT_EXPIRED"
	CodeLimit    = "ATTACHMENT_LIMIT"
	CodeInvalid  = "ATTACHMENT_INVALID"
)

// CheckInput validates the attachment fields of a Run input for caller and
// returns the attachment IDs to bind. Links issued by another origin are left
// to the Agent; links issued here must belong to caller and still be live.
func (s *Store) CheckInput(caller uuid.UUID, links Links, input any) ([]string, error) {
	object, ok := input.(map[string]any)
	if !ok {
		return nil, nil
	}
	keys := make([]string, 0, len(object))
	for key := range object {
		if IsAttachmentField(key) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var ids []string
	seen := map[string]bool{}
	count := 0
	for _, key := range keys {
		var values []string
		switch value := object[key].(type) {
		case nil:
			continue
		case string:
			values = []string{value}
		case []any:
			for _, item := range value {
				text, ok := item.(string)
				if !ok {
					return nil, &FieldError{Code: CodeInvalid, Field: key, Msg: "must contain only links"}
				}
				values = append(values, text)
			}
		default:
			continue
		}
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			count++
			if count > MaxPerRun {
				return nil, &FieldError{Code: CodeLimit, Field: key, Msg: fmt.Sprintf("at most %d attachments per run", MaxPerRun)}
			}
			token := links.Token(value)
			if token == "" {
				if links.base != "" && strings.HasPrefix(value, links.base+RoutePath) {
					return nil, &FieldError{Code: CodeExpired, Field: key, Msg: "attachment link is invalid or expired"}
				}
				// Any other link is an ordinary public link the Agent downloads.
				continue
			}
			meta, err := s.Lookup(token)
			if err != nil {
				return nil, &FieldError{Code: CodeExpired, Field: key, Msg: "attachment expired or was released; upload it again"}
			}
			if meta.Owner != caller.String() {
				return nil, &FieldError{Code: CodeNotOwned, Field: key, Msg: "attachment belongs to another account"}
			}
			if meta.RunID != "" {
				return nil, &FieldError{Code: CodeExpired, Field: key, Msg: "attachment was already used by another run; upload it again"}
			}
			if !seen[meta.ID] {
				seen[meta.ID] = true
				ids = append(ids, meta.ID)
			}
		}
	}
	return ids, nil
}

// RunBinder adapts a Store to the runtime's Run creation hooks.
type RunBinder struct {
	Store *Store
	Links Links
}

func (b RunBinder) CheckRunInput(caller uuid.UUID, input any) ([]string, error) {
	return b.Store.CheckInput(caller, b.Links, input)
}

func (b RunBinder) BindRun(run uuid.UUID, ids []string) error { return b.Store.Bind(run, ids) }
