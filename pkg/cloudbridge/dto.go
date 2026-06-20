package cloudbridge

type RunListItem struct {
	ID         string `json:"id"`
	AgentID    string `json:"agent_id"`
	AgentSlug  string `json:"agent_slug"`
	AgentName  string `json:"agent_name"`
	Status     string `json:"status"`
	CostCents  int32  `json:"cost_cents"`
	DurationMs *int32 `json:"duration_ms,omitempty"`
	StartedAt  string `json:"started_at"`
	Source     string `json:"source,omitempty"`
}

type RunListResponse struct {
	Items []RunListItem `json:"items"`
	Total int32         `json:"total"`
	Page  int32         `json:"page"`
	Size  int32         `json:"size"`
}

type UsageStats struct {
	ThisMonthCalls int32 `json:"this_month_calls"`
	ThisMonthSpent int64 `json:"this_month_spent_cents"`
	TotalCalls     int32 `json:"total_calls"`
}

type CreatorSummary struct {
	ThisMonthCalls   int32 `json:"this_month_calls_received"`
	ThisMonthRevenue int64 `json:"this_month_revenue_cents"`
	TotalAgents      int32 `json:"total_agents"`
	PendingAgents    int32 `json:"pending_agents"`
}

type UserDashboardResponse struct {
	IsCreator  bool            `json:"is_creator"`
	Usage      UsageStats      `json:"usage"`
	Creator    *CreatorSummary `json:"creator,omitempty"`
	RecentRuns []RunListItem   `json:"recent_runs"`
}

type AgentStatsItem struct {
	ID               string `json:"id"`
	Slug             string `json:"slug"`
	Name             string `json:"name"`
	Status           string `json:"status"`
	PriceCents       int32  `json:"price_per_call_cents"`
	LifetimeCalls    int32  `json:"lifetime_calls"`
	LifetimeRevenue  int64  `json:"lifetime_revenue_cents"`
	CallsThisMonth   int64  `json:"calls_this_month"`
	RevenueThisMonth int64  `json:"revenue_this_month_cents"`
}

type CreatorDashboardResponse struct {
	Summary CreatorSummary   `json:"summary"`
	Agents  []AgentStatsItem `json:"agents"`
}
