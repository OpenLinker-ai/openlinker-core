package agent

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// PublishAgentSkillMarkdown 是 Agent 自注册流程的机器可读接入指南。
const PublishAgentSkillMarkdown = `# OpenLinker - publish-agent Skill

## Goal
Register a callable Agent on OpenLinker with a short-lived Bootstrap Token.
After success, store the Runtime Token returned for that single Agent.

## Prerequisites
- A Bootstrap Token from the human creator (br_live_***), default 30 minutes and max_agents=1.
- An HTTPS endpoint accepting POST invocation requests.

## Register

` + "```bash" + `
curl -X POST https://api.openlinker.ai/api/v1/agent-registration/agents \
  -H 'Authorization: Bearer br_live_xxx' \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "My Translator",
    "endpoint_url": "https://my-agent.example.com/invoke",
    "ability_tags": ["translation"],
    "visibility": "public"
  }'
` + "```" + `

- slug, description and price_per_call_cents are optional.
- visibility accepts public, unlisted or private and defaults to public.
- tags is accepted as a backwards-compatible alias for ability_tags.
- bootstrap_token in the JSON body is accepted for older clients; Bearer is preferred.

## Tokens
- br_live_***: one-time Bootstrap Token used only for self-registration.
- rt_live_***: Runtime Token bound to the registered Agent.
- JWT: human session only; do not give it to an Agent.

Public listing is immediate. Certification and recommendation are optional later actions.
Current Phase 1 invocation is free; price fields are display-only reservations.

## Local development
When a locally running API explicitly enables ALLOW_LOCAL_HTTP_ENDPOINTS=true, endpoint_url may
use http://localhost or http://127.0.0.1 for local testing only. Production endpoints remain HTTPS.
`

// ServePublishAgentSkill exposes the self-registration instructions to agents and CLIs.
func ServePublishAgentSkill(c echo.Context) error {
	c.Response().Header().Set("Cache-Control", "public, max-age=300")
	return c.String(http.StatusOK, PublishAgentSkillMarkdown)
}
