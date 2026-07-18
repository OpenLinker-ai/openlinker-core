package userdash

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

func TestUserDashServiceListRuns(t *testing.T) {
	userID := uuid.New()
	creatorID := uuid.New()
	agentID := uuid.New()
	latestAttemptID := uuid.New()
	replayOfRunID := uuid.New()
	started := time.Date(2026, 6, 20, 12, 30, 0, 0, time.UTC)
	nextAttemptAt := started.Add(time.Minute)
	runRow := db.ListRunsByUserWithAgentRow{
		Run: db.Run{
			ID:                uuid.New(),
			UserID:            userID,
			AgentID:           agentID,
			Status:            "success",
			CostCents:         35,
			StartedAt:         started,
			Source:            "api",
			RuntimeContractID: "openlinker.runtime.v2",
			DispatchState:     "retry_wait",
			AttemptCount:      2,
			MaxAttempts:       3,
			NextAttemptAt:     &nextAttemptAt,
			LatestAttemptID:   &latestAttemptID,
			ReplayOfRunID:     &replayOfRunID,
		},
		AgentSlug: "writer",
		AgentName: "Writer",
	}

	queries := &fakeDashboardQueries{
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
	if resp.Items[0].RuntimeContractID != "openlinker.runtime.v2" ||
		resp.Items[0].DispatchState != "retry_wait" || resp.Items[0].AttemptCount != 2 ||
		resp.Items[0].MaxAttempts != 3 || resp.Items[0].LatestAttemptID != latestAttemptID.String() ||
		resp.Items[0].ReplayOfRunID != replayOfRunID.String() {
		t.Fatalf("ListUserRuns runtime fields = %#v", resp.Items[0])
	}

	creatorQueries := &fakeDashboardQueries{
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

func TestUserDashServiceListCallRecords(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	callerID := uuid.New()
	parentRunID := uuid.New().String()
	started := time.Date(2026, 6, 20, 12, 30, 0, 0, time.UTC)
	transportChangedAt := started.Add(-30 * time.Second)
	duration := int32(850)
	rows := []db.ListCallRecordsForUserRow{
		{
			ID:                        uuid.New(),
			UserID:                    userID,
			AgentID:                   agentID,
			Status:                    "success",
			CostCents:                 0,
			CreatorRevenueCents:       0,
			DurationMs:                &duration,
			StartedAt:                 started,
			Source:                    "api",
			RuntimeContractID:         "openlinker.runtime.v2",
			AgentConnectionMode:       "runtime",
			RuntimeTransport:          "long_poll",
			RuntimeTransportReason:    "explicit",
			RuntimeTransportChangedAt: &transportChangedAt,
			DispatchState:             "terminal",
			AttemptCount:              1,
			MaxAttempts:               3,
			AgentSlug:                 "child",
			AgentName:                 "Child Agent",
			Direction:                 "made",
			ParentRunID:               parentRunID,
			CallerAgentID:             callerID.String(),
			CallerAgentSlug:           "parent",
			CallerAgentName:           "Parent Agent",
			ProtocolContextID:         "ctx-protocol",
			ProtocolTaskID:            "task-child",
			RootContextID:             "ctx-root",
			ParentContextID:           "ctx-parent",
			ParentTaskID:              "task-parent",
			TraceID:                   "trace-1",
			ReferenceTaskIDs:          []string{"task-parent"},
			ContextSource:             "agent_delegation",
			CallID:                    "task-child",
		},
	}
	queries := &fakeDashboardQueries{
		callRecordRows:  rows,
		callRecordCount: 12,
	}

	resp, err := (&Service{queries: queries}).ListCallRecords(context.Background(), userID, "made", " ctx-root ", "amount_desc", "success", "api", "a2a_child", 2, maxSize+50)
	if err != nil {
		t.Fatalf("ListCallRecords error = %v", err)
	}
	if queries.callRecordArg.UserID != userID || queries.callRecordArg.View != "made" ||
		queries.callRecordArg.Query != "ctx-root" || queries.callRecordArg.Sort != "amount_desc" ||
		queries.callRecordArg.Status != "success" || queries.callRecordArg.Source != "api" || queries.callRecordArg.Relation != "a2a_child" ||
		queries.callRecordArg.Limit != maxSize || queries.callRecordArg.Offset != maxSize {
		t.Fatalf("ListCallRecords query arg = %#v", queries.callRecordArg)
	}
	if queries.callRecordCountArg.UserID != userID || queries.callRecordCountArg.View != "made" || queries.callRecordCountArg.Query != "ctx-root" ||
		queries.callRecordCountArg.Status != "success" || queries.callRecordCountArg.Source != "api" || queries.callRecordCountArg.Relation != "a2a_child" {
		t.Fatalf("CountCallRecords query arg = %#v", queries.callRecordCountArg)
	}
	if resp.Total != 12 || resp.Page != 2 || resp.Size != maxSize || resp.View != "made" || resp.Query != "ctx-root" || resp.Sort != "amount_desc" ||
		resp.StatusFilter != "success" || resp.SourceFilter != "api" || resp.RelationFilter != "a2a_child" || len(resp.Items) != 1 {
		t.Fatalf("ListCallRecords response = %#v", resp)
	}
	got := resp.Items[0]
	if got.Relation != "a2a_child" || got.Direction != "made" || got.ParentRunID != parentRunID || got.CallID != "task-child" {
		t.Fatalf("call record identity = %#v", got)
	}
	if got.CallerAgent == nil || got.CallerAgent.ID != callerID.String() || got.TargetAgent.Slug != "child" {
		t.Fatalf("call record agents = %#v", got)
	}
	if got.A2AContext == nil || got.A2AContext.SessionID != "ctx-root" || got.A2AContext.ProtocolContextID != "ctx-protocol" {
		t.Fatalf("a2a context = %#v", got.A2AContext)
	}
	if got.RuntimeContractID != "openlinker.runtime.v2" || got.DispatchState != "terminal" ||
		got.AttemptCount != 1 || got.MaxAttempts != 3 {
		t.Fatalf("call record runtime fields = %#v", got)
	}
	if got.AgentConnectionMode != "runtime" || got.RuntimeTransport != "long_poll" ||
		got.RuntimeTransportReason != "explicit" || got.RuntimeTransportChangedAt == nil ||
		!got.RuntimeTransportChangedAt.Equal(transportChangedAt) {
		t.Fatalf("call record execution evidence = %#v", got)
	}
}

func TestUserDashServiceDashboards(t *testing.T) {
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

	queries := &fakeDashboardQueries{
		user: db.User{ID: userID, IsCreator: true},
		userUsage: db.GetUserDashboardUsageRow{
			ThisMonthCalls: 5,
			ThisMonthSpent: 250,
			TotalCalls:     18,
		},
		creatorSummary: db.GetCreatorDashboardSummaryRow{
			ThisMonthCalls:   7,
			ThisMonthRevenue: 330,
			TotalAgents:      4,
			PublicAgents:     3,
			PendingAgents:    1,
		},
		listUserRows: []db.ListRunsByUserWithAgentRow{recentRun},
	}
	userResp, err := (&Service{queries: queries}).GetUserDashboard(context.Background(), userID)
	if err != nil {
		t.Fatalf("GetUserDashboard error = %v", err)
	}
	if queries.listUserArg.UserID != userID || queries.listUserArg.Limit != recentRuns || queries.listUserArg.Offset != 0 {
		t.Fatalf("recent query arg = %#v", queries.listUserArg)
	}
	if !userResp.IsCreator || userResp.Creator == nil || userResp.Creator.TotalAgents != 4 || userResp.Creator.PublicAgents != 3 || userResp.Usage.TotalCalls != 18 {
		t.Fatalf("user dashboard = %#v", userResp)
	}
	if len(userResp.RecentRuns) != 1 || userResp.RecentRuns[0].AgentSlug != "analyst" {
		t.Fatalf("recent runs = %#v", userResp.RecentRuns)
	}

	statsID := uuid.New()
	creatorQueries := &fakeDashboardQueries{
		user: db.User{ID: userID, IsCreator: true},
		creatorSummary: db.GetCreatorDashboardSummaryRow{
			ThisMonthCalls:   11,
			ThisMonthRevenue: 900,
			TotalAgents:      2,
			PublicAgents:     1,
			PendingAgents:    1,
		},
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
	if creatorResp.Summary.ThisMonthCalls != 11 || creatorResp.Summary.ThisMonthRevenue != 900 || creatorResp.Summary.PublicAgents != 1 || len(creatorResp.Agents) != 1 {
		t.Fatalf("creator dashboard = %#v", creatorResp)
	}
	if creatorResp.Agents[0].ID != statsID.String() || creatorResp.Agents[0].CallsThisMonth != 12 {
		t.Fatalf("agent stats = %#v", creatorResp.Agents[0])
	}
}

func TestUserDashServiceErrors(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	sentinel := errors.New("database stopped")

	for _, tc := range []struct {
		name string
		call func(*Service) error
		q    *fakeDashboardQueries
		want int
	}{
		{
			name: "list user rows",
			call: func(s *Service) error {
				_, err := s.ListUserRuns(context.Background(), userID, 1, 20)
				return err
			},
			q:    &fakeDashboardQueries{listUserErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "list user count",
			call: func(s *Service) error {
				_, err := s.ListUserRuns(context.Background(), userID, 1, 20)
				return err
			},
			q:    &fakeDashboardQueries{countUserErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "creator owner not found",
			call: func(s *Service) error {
				_, err := s.ListCreatorAgentRuns(context.Background(), userID, agentID, 1, 20)
				return err
			},
			q:    &fakeDashboardQueries{ownerErr: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "creator owner lookup",
			call: func(s *Service) error {
				_, err := s.ListCreatorAgentRuns(context.Background(), userID, agentID, 1, 20)
				return err
			},
			q:    &fakeDashboardQueries{ownerErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "creator run rows",
			call: func(s *Service) error {
				_, err := s.ListCreatorAgentRuns(context.Background(), userID, agentID, 1, 20)
				return err
			},
			q:    &fakeDashboardQueries{creatorErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "creator run count",
			call: func(s *Service) error {
				_, err := s.ListCreatorAgentRuns(context.Background(), userID, agentID, 1, 20)
				return err
			},
			q:    &fakeDashboardQueries{creatorCountErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "call records rows",
			call: func(s *Service) error {
				_, err := s.ListCallRecords(context.Background(), userID, "all", "", "", "", "", "", 1, 20)
				return err
			},
			q:    &fakeDashboardQueries{callRecordErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "call records count",
			call: func(s *Service) error {
				_, err := s.ListCallRecords(context.Background(), userID, "all", "", "", "", "", "", 1, 20)
				return err
			},
			q:    &fakeDashboardQueries{callRecordCountErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "user dashboard missing user",
			call: func(s *Service) error {
				_, err := s.GetUserDashboard(context.Background(), userID)
				return err
			},
			q:    &fakeDashboardQueries{getUserErr: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "user dashboard usage count",
			call: func(s *Service) error {
				_, err := s.GetUserDashboard(context.Background(), userID)
				return err
			},
			q:    &fakeDashboardQueries{user: db.User{ID: userID}, userUsageErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "user dashboard creator summary",
			call: func(s *Service) error {
				_, err := s.GetUserDashboard(context.Background(), userID)
				return err
			},
			q:    &fakeDashboardQueries{user: db.User{ID: userID, IsCreator: true}, creatorSummaryErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "user dashboard recent",
			call: func(s *Service) error {
				_, err := s.GetUserDashboard(context.Background(), userID)
				return err
			},
			q:    &fakeDashboardQueries{user: db.User{ID: userID}, listUserErr: sentinel},
			want: http.StatusInternalServerError,
		},
		{
			name: "creator dashboard non creator",
			call: func(s *Service) error {
				_, err := s.GetCreatorDashboard(context.Background(), userID)
				return err
			},
			q:    &fakeDashboardQueries{user: db.User{ID: userID}},
			want: http.StatusForbidden,
		},
		{
			name: "creator dashboard stats",
			call: func(s *Service) error {
				_, err := s.GetCreatorDashboard(context.Background(), userID)
				return err
			},
			q:    &fakeDashboardQueries{user: db.User{ID: userID, IsCreator: true}, agentStatsErr: sentinel},
			want: http.StatusInternalServerError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireUserDashHTTPStatus(t, tc.call(&Service{queries: tc.q}), tc.want)
		})
	}
}

type fakeDashboardQueries struct {
	listUserArg  db.ListRunsByUserWithAgentParams
	listUserRows []db.ListRunsByUserWithAgentRow
	listUserErr  error

	userRunCount int32
	countUserErr error

	callRecordArg      db.ListCallRecordsForUserParams
	callRecordRows     []db.ListCallRecordsForUserRow
	callRecordErr      error
	callRecordCountArg db.CountCallRecordsForUserParams
	callRecordCount    int32
	callRecordCountErr error

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

	userUsage    db.GetUserDashboardUsageRow
	userUsageErr error

	creatorSummary    db.GetCreatorDashboardSummaryRow
	creatorSummaryErr error

	agentStatsRows []db.ListAgentStatsForCreatorRow
	agentStatsErr  error
}

func (q *fakeDashboardQueries) ListRunsByUserWithAgent(_ context.Context, arg db.ListRunsByUserWithAgentParams) ([]db.ListRunsByUserWithAgentRow, error) {
	q.listUserArg = arg
	return q.listUserRows, q.listUserErr
}

func (q *fakeDashboardQueries) CountRunsByUser(context.Context, uuid.UUID) (int32, error) {
	return q.userRunCount, q.countUserErr
}

func (q *fakeDashboardQueries) ListCallRecordsForUser(_ context.Context, arg db.ListCallRecordsForUserParams) ([]db.ListCallRecordsForUserRow, error) {
	q.callRecordArg = arg
	return q.callRecordRows, q.callRecordErr
}

func (q *fakeDashboardQueries) CountCallRecordsForUser(_ context.Context, arg db.CountCallRecordsForUserParams) (int32, error) {
	q.callRecordCountArg = arg
	return q.callRecordCount, q.callRecordCountErr
}

func (q *fakeDashboardQueries) GetAgentByIDForOwner(_ context.Context, arg db.GetAgentByIDForOwnerParams) (db.Agent, error) {
	q.ownerArg = arg
	return q.ownerAgent, q.ownerErr
}

func (q *fakeDashboardQueries) ListRunsByCreatorAgentWithAgent(_ context.Context, arg db.ListRunsByCreatorAgentWithAgentParams) ([]db.ListRunsByCreatorAgentWithAgentRow, error) {
	q.creatorArg = arg
	return q.creatorRows, q.creatorErr
}

func (q *fakeDashboardQueries) CountRunsByCreatorAgent(_ context.Context, arg db.CountRunsByCreatorAgentParams) (int32, error) {
	q.countCreatorArg = arg
	return q.creatorRunCount, q.creatorCountErr
}

func (q *fakeDashboardQueries) GetUserByID(context.Context, uuid.UUID) (db.User, error) {
	return q.user, q.getUserErr
}

func (q *fakeDashboardQueries) GetUserDashboardUsage(context.Context, uuid.UUID) (db.GetUserDashboardUsageRow, error) {
	return q.userUsage, q.userUsageErr
}

func (q *fakeDashboardQueries) GetCreatorDashboardSummary(context.Context, uuid.UUID) (db.GetCreatorDashboardSummaryRow, error) {
	return q.creatorSummary, q.creatorSummaryErr
}

func (q *fakeDashboardQueries) ListAgentStatsForCreator(context.Context, uuid.UUID) ([]db.ListAgentStatsForCreatorRow, error) {
	return q.agentStatsRows, q.agentStatsErr
}
