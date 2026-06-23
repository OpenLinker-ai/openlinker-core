package cloudbridge

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestNormalizePage(t *testing.T) {
	tests := []struct {
		name     string
		page     int32
		size     int32
		wantPage int32
		wantSize int32
	}{
		{name: "defaults", page: 0, size: 0, wantPage: defaultPage, wantSize: defaultSize},
		{name: "negative", page: -4, size: -1, wantPage: defaultPage, wantSize: defaultSize},
		{name: "keeps valid", page: 3, size: 50, wantPage: 3, wantSize: 50},
		{name: "caps size", page: 2, size: maxSize + 1, wantPage: 2, wantSize: maxSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPage, gotSize := normalizePage(tt.page, tt.size)
			if gotPage != tt.wantPage || gotSize != tt.wantSize {
				t.Fatalf("normalizePage = (%d,%d), want (%d,%d)", gotPage, gotSize, tt.wantPage, tt.wantSize)
			}
		})
	}
}

func TestParseInt32Query(t *testing.T) {
	e := echo.New()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/?page=12&bad=nope&negative=-5", nil), httptest.NewRecorder())
	if got := parseInt32Query(c, "page", 1); got != 12 {
		t.Fatalf("page = %d", got)
	}
	if got := parseInt32Query(c, "bad", 7); got != 7 {
		t.Fatalf("bad = %d", got)
	}
	if got := parseInt32Query(c, "missing", 8); got != 8 {
		t.Fatalf("missing = %d", got)
	}
	if got := parseInt32Query(c, "negative", 9); got != -5 {
		t.Fatalf("negative = %d", got)
	}
}

func TestToRunListItem(t *testing.T) {
	runID := uuid.New()
	agentID := uuid.New()
	duration := int32(1234)
	started := time.Date(2026, 6, 20, 20, 30, 0, 0, time.FixedZone("CST", 8*60*60))

	item := toRunListItem(db.Run{
		ID:         runID,
		AgentID:    agentID,
		Status:     "success",
		CostCents:  42,
		DurationMs: &duration,
		StartedAt:  started,
		Source:     "mcp",
	}, "agent-slug", "Agent Name")

	if item.ID != runID.String() || item.AgentID != agentID.String() || item.AgentSlug != "agent-slug" || item.AgentName != "Agent Name" {
		t.Fatalf("identity fields = %#v", item)
	}
	if item.Status != "success" || item.CostCents != 42 || item.DurationMs == nil || *item.DurationMs != duration || item.Source != "mcp" {
		t.Fatalf("run fields = %#v", item)
	}
	if item.StartedAt != "2026-06-20T12:30:00Z" {
		t.Fatalf("started_at = %q", item.StartedAt)
	}
}

func TestToAgentStatsItem(t *testing.T) {
	id := uuid.New()
	item := toAgentStatsItem(&db.ListAgentStatsForCreatorRow{
		ID:                id,
		Slug:              "summarizer",
		Name:              "Summarizer",
		Status:            "approved",
		PricePerCallCents: 15,
		LifetimeCalls:     99,
		LifetimeRevenue:   1485,
		CallsThisMonth:    7,
		RevenueThisMonth:  105,
	})

	if item.ID != id.String() || item.Slug != "summarizer" || item.Name != "Summarizer" || item.Status != "approved" {
		t.Fatalf("identity fields = %#v", item)
	}
	if item.PriceCents != 15 || item.LifetimeCalls != 99 || item.LifetimeRevenue != 1485 || item.CallsThisMonth != 7 || item.RevenueThisMonth != 105 {
		t.Fatalf("stats fields = %#v", item)
	}
}
