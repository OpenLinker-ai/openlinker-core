package catalogregistry

import "github.com/OpenLinker-ai/openlinker-core/pkg/catalogconfig"

type (
	Registry = catalogconfig.Registry
	Audience = catalogconfig.Audience
)

const (
	AudiencePublic  = catalogconfig.AudiencePublic
	AudienceMembers = catalogconfig.AudienceMembers
	AudienceAdmins  = catalogconfig.AudienceAdmins
)

func Parse(raw string) ([]Registry, error) { return catalogconfig.Parse(raw) }
