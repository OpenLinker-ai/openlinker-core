package cloudbridge

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestCloudBridgeServiceListRuns(t *testing.T) {
	userID := uuid.New()
	creatorID := uuid.New()
	agentID := uuid.New()
	started := time.Date(2026, 6, 20, 12, 30, 0, 0, time.UTC)
	runRow := db.ListRunsByUserWithAgentRow{
		Run: db.Run{
			ID:        uuid.New(),
			UserID:    userID,
			AgentID:   agentID,
			Status:    "success",
			CostCents: 35,
			StartedAt: started,
			Source:    "api",
		},
		AgentSlug: "writer",
		AgentName: "Writer",
	}

	queries := &fakeCloudBridgeQueries{
		listUserRows: []db.ListRunsByUserWithAgentRow{runRow},
		userRunCount: 12,
	}
	resp, err := (&Service{queries: queries}).ListUserRuns(context.Background(), userID, 2, maxSize+50)
	if err != nil {
		t.Fatalf("ListUserRuns error = %v", err)
	}
	if queries.listUserArg.UserID != userID || queries.listUserArg.Limit != maxSize || queries.listUserArg.Offset != maxSize {
		t.Fatalf("ListUserRuns query arg = %#v", queries.listUserArg)
	}
	if resp.Total != 12 || resp.Page != 2 || resp.Size != maxSize || len(resp.Items) != 1 {
		t.Fatalf("ListUserRuns response = %#v", resp)
	}
	if resp.Items[0].AgentSlug != "writer" || resp.Items[0].StartedAt != "2026-06-20T12:30:00Z" {
		t.Fatalf("ListUserRuns item = %#v", resp.Items[0])
	}

	creatorQueries := &fakeCloudBridgeQueries{
		ownerAgent: db.Agent{ID: agentID, CreatorID: creatorID},
		creatorRows: []db.ListRunsByCreatorAgentWithAgentRow{
			{Run: runRow.Run, AgentSlug: "writer", AgentName: "Writer"},
		},
		creatorRunCount: 3,
	}
	creatorResp, err := (&Service{queries: creatorQueries}).ListCreatorAgentRuns(context.Background(), creatorID, agentID, 3, 4)
	if err != nil {
		t.Fatalf("ListCreatorAgentRuns error = %v", err)
	}
	if creatorQueries.ownerArg.ID != agentID || creatorQueries.ownerArg.CreatorID != creatorID {
		t.Fatalf("owner check arg = %#v", creatorQueries.ownerArg)
	}
	if creatorQueries.creatorArg.CreatorID != creatorID || creatorQueries.creatorArg.AgentID != agentID || creatorQueries.creatorArg.Limit != 4 || creatorQueries.creatorArg.Offset != 8 {
		t.Fatalf("creator runs arg = %#v", creatorQueries.creatorArg)
	}
	if creatorResp.Total != 3 || creatorResp.Page != 3 || creatorResp.Size != 4 || len(creatorResp.Items) != 1 {
		t.Fatalf("creator response = %#v", creatorResp)
	}
}

func TestCloudBridgeServiceDashboards(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	recentRun := db.ListRunsByUserWithAgentRow{
		Run: db.Run{
			ID:        uuid.New(),
			UserID:    userID,
			AgentID:   agentID,
			Status:    "success",
			CostCents: 44,
			StartedAt: time.Date(2026, 6, 20, 8, 0, 0, 0, time.UTC),
			Source:    "runtime",
		},
		AgentSlug: "analyst",
		AgentName: "Analyst",
	}

	queries := &fakeCloudBridgeQueries{
		user:          db.User{ID: userID, IsCreator: true},
		userMonthRuns: 5,
		userSpent:     250,
		userRunCount:  18,
		creatorMonth:  7,
		creatorEarned: 330,
		agentCount:    4,
		pendingCount:  1,
		listUserRows:  []db.ListRunsByUserWithAgentRow{recentRun},
	}
	userResp, err := (&Service{queries: queries}).GetUserDashboard(context.Background(), userID)
	if err != nil {
		t.Fatalf("GetUserDashboard error = %v", err)
	}
	if queries.listUserArg.UserID != userID || queries.listUserArg.Limit != recentRuns || queries.listUserArg.Offset != 0 {
		t.Fatalf("recent query arg = %#v", queries.listUserArg)
	}
	if !userResp.IsCreator || userResp.Creator == nil || userResp.Creator.TotalAgents != 4 || userResp.Usage.TotalCalls != 18 {
		t.Fatalf("user dashboard = %#v", userResp)
	}
	if len(userResp.RecentRuns) != 1 || userResp.RecentRuns[0].AgentSlug != "analyst" {
		t.Fatalf("recent runs = %#v", userResp.RecentRuns)
	}

	statsID := uuid.New()
	creatorQueries := &fakeCloudBridgeQueries{
		user:          db.User{ID: userID, IsCreator: true},
		creatorMonth:  11,
		creatorEarned: 900,
		agentCount:    2,
		pendingCount:  1,
		agentStatsRows: []db.ListAgentStatsForCreatorRow{
			{
				ID:                statsID,
				Slug:              "analyst",
				Name:              "Analyst",
				Status:            "approved",
				PricePerCallCents: 20,
				LifetimeCalls:     99,
				LifetimeRevenue:   1980,
				CallsThisMonth:    12,
				RevenueThisMonth:  240,
			},
		},
	}
	creatorResp, err := (&Service{queries: creatorQueries}).GetCreatorDashboard(context.Background(), userID)
	if err != nil {
		t.Fatalf("GetCreatorDashboard error = %v", err)
	}
	if creatorResp.Summary.ThisMonthCalls != 11 || creatorResp.Summary.ThisMonthRevenue != 900 || len(creatorResp.Agents) != 1 {
		t.Fatalf("creator dashboard = %#v", creatorResp)
	}
	if creatorResp.Agents[0].ID != statsID.String() || creatorResp.Agents[0].CallsThisMonth != 12 {
		t.Fatalf("agent stats = %#v", creatorResp.Agents[0])
	}
}

func TestCloudBridgeServiceErrors(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	sentinel := errors.New("database stopped")

	for _, tc := range []struct {
		name string
		call func(*Service) error
		q    *fakeCloudBridgeQueries
		want int
	}{
		{
			name: "list user rows",
			call: func(s *Service) error {
				_, err := s.ListUserRuns(context.Background(), userID, 1, 20)
				return err
			},
			q:    &fakeCloudBridgeQueries{listUserErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "list user count",
			call: func(s *Service) error {
				_, err := s.ListUserRuns(context.Background(), userID, 1, 20)
				return err
			},
			q:    &fakeCloudBridgeQueries{countUserErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "creator owner not found",
			call: func(s *Service) error {
				_, err := s.ListCreatorAgentRuns(context.Background(), userID, agentID, 1, 20)
				return err
			},
			q:    &fakeCloudBridgeQueries{ownerErr: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "creator owner lookup",
			call: func(s *Service) error {
				_, err := s.ListCreatorAgentRuns(context.Background(), userID, agentID, 1, 20)
				return err
			},
			q:    &fakeCloudBridgeQueries{ownerErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "creator run rows",
			call: func(s *Service) error {
				_, err := s.ListCreatorAgentRuns(context.Background(), userID, agentID, 1, 20)
				return err
			},
			q:    &fakeCloudBridgeQueries{creatorErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "creator run count",
			call: func(s *Service) error {
				_, err := s.ListCreatorAgentRuns(context.Background(), userID, agentID, 1, 20)
				return err
			},
			q:    &fakeCloudBridgeQueries{creatorCountErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "user dashboard missing user",
			call: func(s *Service) error {
				_, err := s.GetUserDashboard(context.Background(), userID)
				return err
			},
			q:    &fakeCloudBridgeQueries{getUserErr: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "user dashboard usage count",
			call: func(s *Service) error {
				_, err := s.GetUserDashboard(context.Background(), userID)
				return err
			},
			q:    &fakeCloudBridgeQueries{user: db.User{ID: userID}, userMonthErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "user dashboard creator summary",
			call: func(s *Service) error {
				_, err := s.GetUserDashboard(context.Background(), userID)
				return err
			},
			q:    &fakeCloudBridgeQueries{user: db.User{ID: userID, IsCreator: true}, creatorMonthErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "user dashboard recent",
			call: func(s *Service) error {
				_, err := s.GetUserDashboard(context.Background(), userID)
				return err
			},
			q:    &fakeCloudBridgeQueries{user: db.User{ID: userID}, listUserErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "creator dashboard non creator",
			call: func(s *Service) error {
				_, err := s.GetCreatorDashboard(context.Background(), userID)
				return err
			},
			q:    &fakeCloudBridgeQueries{user: db.User{ID: userID}},
			want: http.StatusForbidden,
		},
		{
			name: "creator dashboard stats",
			call: func(s *Service) error {
				_, err := s.GetCreatorDashboard(context.Background(), userID)
				return err
			},
			q:    &fakeCloudBridgeQueries{user: db.User{ID: userID, IsCreator: true}, agentStatsErr: sentinel},
			want: http.StatusInternalServerError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireCloudBridgeHTTPStatus(t, tc.call(&Service{queries: tc.q}), tc.want)
		})
	}
}

type fakeCloudBridgeQueries struct {
	listUserArg  db.ListRunsByUserWithAgentParams
	listUserRows []db.ListRunsByUserWithAgentRow
	listUserErr  error

	userRunCount int32
	countUserErr error

	ownerArg   db.GetAgentByIDForOwnerParams
	ownerAgent db.Agent
	ownerErr   error

	creatorArg      db.ListRunsByCreatorAgentWithAgentParams
	creatorRows     []db.ListRunsByCreatorAgentWithAgentRow
	creatorErr      error
	creatorRunCount int32
	creatorCountErr error

	countCreatorArg db.CountRunsByCreatorAgentParams

	user       db.User
	getUserErr error

	userMonthRuns int32
	userMonthErr  error
	userSpent     int64
	userSpentErr  error

	creatorMonth    int32
	creatorMonthErr error
	creatorEarned   int64
	creatorEarnErr  error
	agentCount      int32
	agentCountErr   error
	pendingCount    int32
	pendingCountErr error

	agentStatsRows []db.ListAgentStatsForCreatorRow
	agentStatsErr  error
}

func (q *fakeCloudBridgeQueries) ListRunsByUserWithAgent(_ context.Context, arg db.ListRunsByUserWithAgentParams) ([]db.ListRunsByUserWithAgentRow, error) {
	q.listUserArg = arg
	return q.listUserRows, q.listUserErr
}

func (q *fakeCloudBridgeQueries) CountRunsByUser(context.Context, uuid.UUID) (int32, error) {
	return q.userRunCount, q.countUserErr
}

func (q *fakeCloudBridgeQueries) GetAgentByIDForOwner(_ context.Context, arg db.GetAgentByIDForOwnerParams) (db.Agent, error) {
	q.ownerArg = arg
	return q.ownerAgent, q.ownerErr
}

func (q *fakeCloudBridgeQueries) ListRunsByCreatorAgentWithAgent(_ context.Context, arg db.ListRunsByCreatorAgentWithAgentParams) ([]db.ListRunsByCreatorAgentWithAgentRow, error) {
	q.creatorArg = arg
	return q.creatorRows, q.creatorErr
}

func (q *fakeCloudBridgeQueries) CountRunsByCreatorAgent(_ context.Context, arg db.CountRunsByCreatorAgentParams) (int32, error) {
	q.countCreatorArg = arg
	return q.creatorRunCount, q.creatorCountErr
}

func (q *fakeCloudBridgeQueries) GetUserByID(context.Context, uuid.UUID) (db.User, error) {
	return q.user, q.getUserErr
}

func (q *fakeCloudBridgeQueries) CountRunsByUserThisMonth(context.Context, uuid.UUID) (int32, error) {
	return q.userMonthRuns, q.userMonthErr
}

func (q *fakeCloudBridgeQueries) SumSpentByUserThisMonth(context.Context, uuid.UUID) (int64, error) {
	return q.userSpent, q.userSpentErr
}

func (q *fakeCloudBridgeQueries) CountRunsForCreatorThisMonth(context.Context, uuid.UUID) (int32, error) {
	return q.creatorMonth, q.creatorMonthErr
}

func (q *fakeCloudBridgeQueries) SumEarningsByCreatorThisMonth(context.Context, uuid.UUID) (int64, error) {
	return q.creatorEarned, q.creatorEarnErr
}

func (q *fakeCloudBridgeQueries) CountAgentsByCreator(context.Context, uuid.UUID) (int32, error) {
	return q.agentCount, q.agentCountErr
}

func (q *fakeCloudBridgeQueries) CountPendingAgentsByCreator(context.Context, uuid.UUID) (int32, error) {
	return q.pendingCount, q.pendingCountErr
}

func (q *fakeCloudBridgeQueries) ListAgentStatsForCreator(context.Context, uuid.UUID) ([]db.ListAgentStatsForCreatorRow, error) {
	return q.agentStatsRows, q.agentStatsErr
}
