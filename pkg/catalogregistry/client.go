package catalogregistry

import (
	"bytes"
	"container/list"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/endpointurl"
	"github.com/google/uuid"
)

var (
	ErrUnavailable     = errors.New("catalog registry unavailable")
	ErrUnknownRegistry = errors.New("unknown catalog registry")
	ErrInvalidTarget   = errors.New("invalid catalog target")
)

const (
	MaxJSONBytes     int64 = 1 << 20
	MaxArtifactBytes int64 = 256 << 10
	cacheTTL               = 5 * time.Minute
	cacheEntries           = 512
)

type Resolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

type Options struct {
	Resolver Resolver
	Now      func() time.Time
}

type User struct {
	ID       uuid.UUID
	GitHubID string
}

type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

type Client struct {
	regs    map[string]*Registry
	order   []string
	clients map[string]*http.Client
	now     func() time.Time
	cache   *responseCache
}

func NewClient(regs []Registry, opts Options) *Client {
	resolver := opts.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	c := &Client{regs: map[string]*Registry{}, clients: map[string]*http.Client{}, now: now, cache: newResponseCache(cacheEntries)}
	for i := range regs {
		reg := regs[i]
		c.regs[reg.Name] = &reg
		c.order = append(c.order, reg.Name)
		c.clients[reg.Name] = newHTTPClient(reg, resolver)
	}
	return c
}

func (c *Client) Registries() []Registry {
	out := make([]Registry, 0, len(c.order))
	for _, name := range c.order {
		out = append(out, *c.regs[name])
	}
	return out
}

func (c *Client) Registry(name string) (Registry, bool) {
	reg, ok := c.regs[name]
	if !ok {
		return Registry{}, false
	}
	return *reg, true
}

func allowedAddr(reg Registry, addr netip.Addr) bool {
	addr = addr.Unmap()
	if addr.Zone() != "" {
		return false
	}
	if !reg.AllowPrivateNetwork {
		return endpointurl.IsPublicAddr(addr)
	}
	if addr.IsUnspecified() || addr.IsMulticast() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsInterfaceLocalMulticast() {
		return false
	}
	return true
}

func newHTTPClient(reg Registry, resolver Resolver) *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                 nil,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			var addrs []netip.Addr
			if ip, err := netip.ParseAddr(host); err == nil {
				addrs = []netip.Addr{ip}
			} else {
				found, err := resolver.LookupIPAddr(ctx, host)
				if err != nil {
					return nil, err
				}
				for _, a := range found {
					if ip, ok := netip.AddrFromSlice(a.IP); ok {
						addrs = append(addrs, ip)
					}
				}
			}
			if len(addrs) == 0 {
				return nil, ErrUnavailable
			}
			for _, addr := range addrs {
				if !allowedAddr(reg, addr) {
					return nil, ErrUnavailable
				}
			}
			var lastErr error
			for _, addr := range addrs {
				conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(addr.Unmap().String(), port))
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			return nil, lastErr
		},
	}
	return &http.Client{
		Timeout:   15 * time.Second,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func canonicalQuery(query url.Values) string {
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(query.Get(k)))
	}
	return strings.Join(parts, "&")
}

func (c *Client) resolve(reg *Registry, target string) (*url.URL, error) {
	path, rawQuery, _ := strings.Cut(target, "?")
	if !strings.HasPrefix(path, "/v1/") || strings.Contains(path, "//") || strings.Contains(path, "\\") {
		return nil, ErrInvalidTarget
	}
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return nil, ErrInvalidTarget
	}
	for _, values := range query {
		if len(values) != 1 {
			return nil, ErrInvalidTarget
		}
	}
	base := reg.Base()
	u := *base
	u.RawPath = ""
	u.Path = base.Path + path
	u.RawQuery = canonicalQuery(query)
	if !endpointurl.SameOrigin(&u, base) {
		return nil, ErrInvalidTarget
	}
	return &u, nil
}

func randomJTI() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func signAssertion(reg *Registry, user *User, method string, u *url.URL, body []byte, acts []string, now time.Time) (string, error) {
	header, err := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "openlinker-user-assertion+jwt", "kid": reg.SigningKeyID})
	if err != nil {
		return "", err
	}
	target := u.EscapedPath()
	if u.RawQuery != "" {
		target += "?" + u.RawQuery
	}
	bh := sha256.Sum256(body)
	claims := map[string]any{"aud": reg.AssertionAudience, "cid": reg.Consumer, "sub": user.ID.String(), "act": acts,
		"req": map[string]string{"m": method, "t": target, "bh": hex.EncodeToString(bh[:])},
		"iat": now.Unix(), "exp": now.Add(60 * time.Second).Unix(), "jti": randomJTI()}
	if user.GitHubID != "" {
		claims["gh"] = user.GitHubID
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signing := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	sig := ed25519.Sign(reg.SigningKey, []byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (c *Client) Get(ctx context.Context, name, target string, user *User, acts []string) (Response, error) {
	return c.get(ctx, name, target, user, acts, MaxJSONBytes, false)
}

func (c *Client) GetCached(ctx context.Context, name, target string, user *User) (Response, error) {
	return c.get(ctx, name, target, user, []string{"read"}, MaxJSONBytes, true)
}

func (c *Client) GetArtifact(ctx context.Context, name, target string, user *User, acts []string) (Response, error) {
	return c.get(ctx, name, target, user, acts, MaxArtifactBytes, false)
}

func (c *Client) get(ctx context.Context, name, target string, user *User, acts []string, limit int64, useCache bool) (Response, error) {
	reg, ok := c.regs[name]
	if !ok {
		return Response{}, ErrUnknownRegistry
	}
	u, err := c.resolve(reg, target)
	if err != nil {
		return Response{}, err
	}
	signed := user != nil && reg.CanSign() && len(acts) > 0
	key := ""
	if useCache && !signed {
		key = reg.Name + "\x00" + string(reg.Audience) + "\x00GET\x00" + u.RequestURI()
		if res, ok := c.cache.get(key, c.now()); ok {
			return res, nil
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Response{}, ErrInvalidTarget
	}
	req.Header.Set("Authorization", "Bearer "+reg.Token)
	req.Header.Set("Accept", "application/json")
	if signed {
		assertion, err := signAssertion(reg, user, http.MethodGet, u, nil, acts, c.now())
		if err != nil {
			return Response{}, err
		}
		req.Header.Set("OpenLinker-User-Assertion", assertion)
	}
	res, err := c.clients[reg.Name].Do(req)
	if err != nil {
		return Response{}, ErrUnavailable
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 && res.StatusCode < 400 {
		return Response{}, ErrUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil || int64(len(body)) > limit {
		return Response{}, ErrUnavailable
	}
	out := Response{Status: res.StatusCode, Header: res.Header.Clone(), Body: body}
	if key != "" && res.StatusCode == http.StatusOK {
		c.cache.put(key, out, c.now().Add(cacheTTL))
	}
	return out, nil
}

type cacheEntry struct {
	key     string
	res     Response
	expires time.Time
}

type responseCache struct {
	mu    sync.Mutex
	max   int
	items map[string]*list.Element
	lru   *list.List
}

func newResponseCache(max int) *responseCache {
	return &responseCache{max: max, items: map[string]*list.Element{}, lru: list.New()}
}

func (c *responseCache) get(key string, now time.Time) (Response, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return Response{}, false
	}
	entry := el.Value.(*cacheEntry)
	if !now.Before(entry.expires) {
		c.lru.Remove(el)
		delete(c.items, key)
		return Response{}, false
	}
	c.lru.MoveToFront(el)
	return Response{Status: entry.res.Status, Header: entry.res.Header.Clone(), Body: bytes.Clone(entry.res.Body)}, true
}

func (c *responseCache) put(key string, res Response, expires time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.lru.Remove(el)
	}
	c.items[key] = c.lru.PushFront(&cacheEntry{key: key, res: Response{Status: res.Status, Header: res.Header.Clone(), Body: bytes.Clone(res.Body)}, expires: expires})
	for c.lru.Len() > c.max {
		last := c.lru.Back()
		c.lru.Remove(last)
		delete(c.items, last.Value.(*cacheEntry).key)
	}
}
