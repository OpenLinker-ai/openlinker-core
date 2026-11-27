package catalogregistry_test

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/catalogregistry"
	"github.com/stretchr/testify/require"
)

func seed() string {
	return base64.StdEncoding.EncodeToString(make([]byte, ed25519.SeedSize))
}

func TestParseRegistries(t *testing.T) {
	regs, err := catalogregistry.Parse("")
	require.NoError(t, err)
	require.Empty(t, regs)
	regs, err = catalogregistry.Parse("  []  ")
	require.NoError(t, err)
	require.Empty(t, regs)

	ok := `[{"name":"openlinker","url":"https://registry.example/registry/","consumer":"core-a","token":"olr_abcdefghijklmnop_` + strings.Repeat("s", 40) + `","audience":"public","assertion_audience":"https://openlinker.ai/registry","signing_key_id":"k1","signing_key":"` + seed() + `"}]`
	regs, err = catalogregistry.Parse(ok)
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, "https://registry.example/registry", regs[0].URL)
	require.True(t, regs[0].CanSign())
	require.Equal(t, catalogregistry.AudiencePublic, regs[0].Audience)
	require.Equal(t, "https://openlinker.ai/registry", regs[0].PublicBase())

	public := strings.Replace(ok, `"audience":"public"`, `"audience":"public","public_url":"https://catalog.example/registry/"`, 1)
	regs, err = catalogregistry.Parse(public)
	require.NoError(t, err)
	require.Equal(t, "https://catalog.example/registry", regs[0].PublicBase())

	withCredentials := strings.Replace(ok, `"assertion_audience":"https://openlinker.ai/registry"`, `"assertion_audience":"https://user:secret@openlinker.ai/registry?x=1#y"`, 1)
	regs, err = catalogregistry.Parse(withCredentials)
	require.NoError(t, err)
	require.Equal(t, "https://openlinker.ai/registry", regs[0].PublicBase())

	unsigned := `[{"name":"mem","url":"http://registry:8080/registry","consumer":"core-a","token":"olr_abcdefghijklmnop_` + strings.Repeat("s", 40) + `","audience":"members","allow_http":true,"allow_private_network":true}]`
	regs, err = catalogregistry.Parse(unsigned)
	require.NoError(t, err)
	require.False(t, regs[0].CanSign())
	require.False(t, regs[0].Writes)
	require.Equal(t, "http://registry:8080/registry", regs[0].PublicBase())

	writes := strings.Replace(ok, `"audience":"public"`, `"audience":"public","writes":true`, 1)
	regs, err = catalogregistry.Parse(writes)
	require.NoError(t, err)
	require.True(t, regs[0].Writes)

	token := `"token":"olr_abcdefghijklmnop_` + strings.Repeat("s", 40) + `"`
	bad := map[string]string{
		"not json":           `{`,
		"unknown field":      `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"public","extra":1}]`,
		"bad name":           `[{"name":"A B","url":"https://r.example","consumer":"c",` + token + `,"audience":"public"}]`,
		"duplicate name":     `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"public"},{"name":"a","url":"https://s.example","consumer":"c",` + token + `,"audience":"public"}]`,
		"http without allow": `[{"name":"a","url":"http://r.example","consumer":"c",` + token + `,"audience":"public"}]`,
		"userinfo":           `[{"name":"a","url":"https://u:p@r.example","consumer":"c",` + token + `,"audience":"public"}]`,
		"query":              `[{"name":"a","url":"https://r.example/?x=1","consumer":"c",` + token + `,"audience":"public"}]`,
		"fragment":           `[{"name":"a","url":"https://r.example/#x","consumer":"c",` + token + `,"audience":"public"}]`,
		"scheme":             `[{"name":"a","url":"ftp://r.example","consumer":"c",` + token + `,"audience":"public"}]`,
		"audience":           `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"everyone"}]`,
		"token prefix":       `[{"name":"a","url":"https://r.example","consumer":"c","token":"secret","audience":"public"}]`,
		"no consumer":        `[{"name":"a","url":"https://r.example",` + token + `,"audience":"public"}]`,
		"key without id":     `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"public","signing_key":"` + seed() + `"}]`,
		"id without key":     `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"public","signing_key_id":"k1"}]`,
		"short key":          `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"public","signing_key_id":"k1","signing_key":"AAAA"}]`,
		"key needs audience": `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"public","signing_key_id":"k1","signing_key":"` + seed() + `"}]`,
		"writes need a key":  `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"public","writes":true}]`,
		"public url query":   `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"public","public_url":"https://p.example/?x=1"}]`,
		"public url scheme":  `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"public","public_url":"ftp://p.example"}]`,
		"public url empty q": `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"public","public_url":"https://p.example/?"}]`,
		"public url empty #": `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"public","public_url":"https://p.example/#"}]`,
		"public url creds":   `[{"name":"a","url":"https://r.example","consumer":"c",` + token + `,"audience":"public","public_url":"https://u:p@p.example/"}]`,
	}
	for name, raw := range bad {
		_, err := catalogregistry.Parse(raw)
		require.Error(t, err, name)
		require.NotContains(t, err.Error(), "olr_", name)
		require.NotContains(t, err.Error(), seed(), name)
	}
}
