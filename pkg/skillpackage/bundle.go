// Package skillpackage owns the private package registry and its Core contracts.
// It never executes package contents or changes Runtime Worker delivery semantics.
package skillpackage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const MetadataKey = "_openlinker_skill_packages"
const Feature = "skill_packages.v1"
const MaxBindings = 5
const MaxPayloadBytes = 65536

type ImportRequest struct {
	RequiredCommands []string          `json:"required_commands"`
	Version          string            `json:"version"`
	Files            map[string]string `json:"files"`
	CapabilityIDs    []string          `json:"capability_ids"`
	Providers        []string          `json:"providers"`
}

type Bundle struct {
	RequiredCommands []string          `json:"required_commands"`
	Name             string            `json:"name"`
	Description      string            `json:"description"`
	Files            map[string]string `json:"files"`
	CapabilityIDs    []string          `json:"capability_ids"`
	Providers        []string          `json:"providers"`
}

var versionPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)
var commandPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._+-]{0,63}$`)
var filePattern = regexp.MustCompile(`^[a-zA-Z0-9._/-]+$`)

type ValidationError struct{ Code, Message string }

func (e *ValidationError) Error() string { return e.Message }

func ValidateImport(req ImportRequest) (Bundle, string, string, error) {
	fail := func(code, message string) (Bundle, string, string, error) {
		return Bundle{}, "", "", &ValidationError{Code: code, Message: message}
	}
	if !versionPattern.MatchString(req.Version) {
		return fail("SKILL_PACKAGE_VERSION_INVALID", "version must contain 1–64 letters, numbers, dots, hyphens or underscores")
	}
	if len(req.Files) < 1 || len(req.Files) > 32 {
		return fail("SKILL_PACKAGE_FILE_COUNT", "include SKILL.md and at most 31 supporting UTF-8 files")
	}
	for name, content := range req.Files {
		if !utf8.ValidString(content) || strings.ContainsRune(content, 0) {
			return fail("SKILL_PACKAGE_ENCODING_INVALID", "files must contain UTF-8 text without NUL bytes")
		}
		if name == "." || len(name) > 180 || !filePattern.MatchString(name) || path.IsAbs(name) || path.Clean(name) != name || strings.HasPrefix(name, "../") || strings.Contains(name, "/.") || strings.HasPrefix(name, ".") {
			return fail("SKILL_PACKAGE_PATH_UNSAFE", "files must have safe relative paths and UTF-8 text contents")
		}
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if _, exists := req.Files[parent]; exists {
				return fail("SKILL_PACKAGE_PATH_UNSAFE", "a file cannot also be a parent directory")
			}
		}
	}
	markdown := strings.ReplaceAll(req.Files["SKILL.md"], "\r\n", "\n")
	if !strings.HasPrefix(markdown, "---\n") {
		return fail("SKILL_PACKAGE_FRONTMATTER_REQUIRED", "SKILL.md must start with YAML frontmatter containing name and description")
	}
	end := strings.Index(markdown[4:], "\n---\n")
	if end < 0 {
		return fail("SKILL_PACKAGE_FRONTMATTER_REQUIRED", "SKILL.md frontmatter must end with a --- line followed by instructions")
	}
	var front struct {
		Name        string `yaml:"name"`
		Description string `yaml:"description"`
	}
	if err := yaml.Unmarshal([]byte(markdown[4:4+end]), &front); err != nil {
		return fail("SKILL_PACKAGE_FRONTMATTER_INVALID", "SKILL.md frontmatter is invalid")
	}
	front.Name, front.Description = strings.TrimSpace(front.Name), strings.TrimSpace(front.Description)
	if utf8.RuneCountInString(front.Name) < 1 || utf8.RuneCountInString(front.Name) > 120 || utf8.RuneCountInString(front.Description) < 1 || utf8.RuneCountInString(front.Description) > 2000 || strings.TrimSpace(markdown[4+end+5:]) == "" {
		return fail("SKILL_PACKAGE_MANIFEST_INVALID", "SKILL.md needs a name, description and instructions")
	}
	if len(req.CapabilityIDs) > 5 {
		return fail("SKILL_PACKAGE_CAPABILITY_LIMIT", "map at most 5 standard capabilities")
	}
	capabilities := unique(req.CapabilityIDs)
	providers := unique(req.Providers)
	if len(providers) < 1 || len(providers) > 2 {
		return fail("SKILL_PACKAGE_PROVIDER_REQUIRED", "choose Codex, Claude, or both")
	}
	for _, provider := range providers {
		if provider != "codex" && provider != "claude" {
			return fail("SKILL_PACKAGE_PROVIDER_UNSUPPORTED", "unsupported provider")
		}
	}
	commands := unique(req.RequiredCommands)
	if len(commands) > 16 {
		return fail("SKILL_PACKAGE_COMMAND_LIMIT", "at most 16 required commands are allowed")
	}
	for _, command := range commands {
		if !commandPattern.MatchString(command) {
			return fail("SKILL_PACKAGE_COMMAND_INVALID", "required commands must be plain executable names")
		}
	}
	bundle := Bundle{RequiredCommands: commands, Name: front.Name, Description: front.Description, Files: req.Files, CapabilityIDs: capabilities, Providers: providers}
	raw, err := json.Marshal(bundle)
	if err != nil {
		return fail("SKILL_PACKAGE_ENCODING_INVALID", "package could not be encoded")
	}
	if len(raw) > MaxPayloadBytes {
		return fail("SKILL_PACKAGE_PAYLOAD_TOO_LARGE", "encoded package exceeds 64 KiB")
	}
	digest := sha256.Sum256(raw)
	return bundle, string(raw), hex.EncodeToString(digest[:]), nil
}

func unique(values []string) []string {
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !slices.Contains(out, value) {
			out = append(out, value)
		}
	}
	slices.Sort(out)
	return out
}
