package catalogregistry

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestPathParamsAreDecodedExactlyOnce(t *testing.T) {
	e := echo.New()
	var got []string
	e.GET("/m/:ns/:name/:version", func(c echo.Context) error {
		_, _, base, err := entryPath(c, "mcp/servers")
		if err != nil {
			return c.String(http.StatusBadRequest, "bad")
		}
		v, err := version(c, "mcp/servers")
		if err != nil {
			return c.String(http.StatusBadRequest, "bad")
		}
		got = append(got, base+"|"+v)
		return c.String(http.StatusOK, "ok")
	})
	long := strings.Repeat("n", 150)
	cases := map[string]int{
		"/m/io.github.a/w/v%2541":                                       200,
		"/m/io.github.a/w/100%25":                                       200,
		"/m/io.github.a/w/release%20candidate":                          200,
		"/m/io.github.a/" + long + "/1":                                 200,
		"/m/io.github.a/" + long + "x" + strings.Repeat("y", 50) + "/1": 400,
		"/m/io.github.a/w/release%2F2026":                               200,
		"/m/io.github.a/w/%0A":                                          400,
	}
	for path, want := range cases {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != want {
			t.Fatalf("%s: status %d, want %d", path, rec.Code, want)
		}
	}
	joined := strings.Join(got, "\n")
	for _, want := range []string{"|v%41", "|100%", "|release candidate", "|release/2026"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing decoded value %q in %s", want, joined)
		}
	}
}
