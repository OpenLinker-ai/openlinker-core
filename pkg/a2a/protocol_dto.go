package a2a

import "encoding/json"

// JSONRPCRequest is the minimal JSON-RPC envelope used by the A2A adapter.
type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *JSONRPCError   `json:"error,omitempty"`
}

type JSONRPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// A2AMessageSendParams maps the A2A message/send params onto an OpenLinker run.
type A2AMessageSendParams struct {
	Message       A2AMessage             `json:"message"`
	Configuration *A2ASendConfiguration  `json:"configuration,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

type A2ASendConfiguration struct {
	AcceptedOutputModes        []string                       `json:"acceptedOutputModes,omitempty"`
	Blocking                   *bool                          `json:"blocking,omitempty"`
	ReturnImmediately          *bool                          `json:"returnImmediately,omitempty"`
	PushNotificationConfig     *A2APushNotificationConfig     `json:"pushNotificationConfig,omitempty"`
	TaskPushNotificationConfig *A2ATaskPushNotificationConfig `json:"taskPushNotificationConfig,omitempty"`
	HistoryLength              *int                           `json:"historyLength,omitempty"`
}

type A2APushNotificationConfig struct {
	ID              string                     `json:"id,omitempty"`
	URL             string                     `json:"url,omitempty"`
	Token           string                     `json:"token,omitempty"`
	Secret          string                     `json:"secret,omitempty"`
	Authentication  *A2APushAuthenticationInfo `json:"authentication,omitempty"`
	Metadata        map[string]interface{}     `json:"metadata,omitempty"`
	EventTypes      []string                   `json:"eventTypes,omitempty"`
	EventTypesAlias []string                   `json:"event_types,omitempty"`
}

type A2APushAuthenticationInfo struct {
	Scheme      string `json:"scheme,omitempty"`
	Credentials string `json:"credentials,omitempty"`
}

type A2ATaskPushNotificationConfig struct {
	Tenant                 string                     `json:"tenant,omitempty"`
	ID                     string                     `json:"id,omitempty"`
	TaskID                 string                     `json:"taskId,omitempty"`
	URL                    string                     `json:"url,omitempty"`
	Token                  string                     `json:"token,omitempty"`
	Secret                 string                     `json:"secret,omitempty"`
	Authentication         *A2APushAuthenticationInfo `json:"authentication,omitempty"`
	Metadata               map[string]interface{}     `json:"metadata,omitempty"`
	EventTypes             []string                   `json:"eventTypes,omitempty"`
	EventTypesAlias        []string                   `json:"event_types,omitempty"`
	PushNotificationConfig A2APushNotificationConfig  `json:"pushNotificationConfig,omitempty"`
}

type A2ATaskPushConfigParams struct {
	ID                       string                     `json:"id,omitempty"`
	TaskID                   string                     `json:"taskId,omitempty"`
	PushNotificationConfigID string                     `json:"pushNotificationConfigId,omitempty"`
	PushNotificationConfig   A2APushNotificationConfig  `json:"pushNotificationConfig,omitempty"`
	URL                      string                     `json:"url,omitempty"`
	Token                    string                     `json:"token,omitempty"`
	Secret                   string                     `json:"secret,omitempty"`
	Authentication           *A2APushAuthenticationInfo `json:"authentication,omitempty"`
	Metadata                 map[string]interface{}     `json:"metadata,omitempty"`
	EventTypes               []string                   `json:"eventTypes,omitempty"`
	EventTypesAlias          []string                   `json:"event_types,omitempty"`
	PageSize                 *int                       `json:"pageSize,omitempty"`
	PageToken                string                     `json:"pageToken,omitempty"`
}

type A2ATaskPushConfigList struct {
	Configs       []A2ATaskPushNotificationConfig `json:"configs,omitempty"`
	NextPageToken string                          `json:"nextPageToken,omitempty"`
	Items         []A2ATaskPushNotificationConfig `json:"items,omitempty"`
}

type A2ATaskQueryParams struct {
	ID            string `json:"id"`
	HistoryLength *int   `json:"historyLength,omitempty"`
}

type A2ATaskListParams struct {
	ContextID            string `json:"contextId,omitempty"`
	Status               string `json:"status,omitempty"`
	PageSize             *int   `json:"pageSize,omitempty"`
	PageToken            string `json:"pageToken,omitempty"`
	HistoryLength        *int   `json:"historyLength,omitempty"`
	StatusTimestampAfter string `json:"statusTimestampAfter,omitempty"`
	IncludeArtifacts     *bool  `json:"includeArtifacts,omitempty"`
}

type A2ATaskListResponse struct {
	Tasks         []A2ATask `json:"tasks"`
	NextPageToken string    `json:"nextPageToken"`
	PageSize      int32     `json:"pageSize"`
	TotalSize     int32     `json:"totalSize"`
}

type A2AMessage struct {
	Kind      string                   `json:"kind,omitempty"`
	MessageID string                   `json:"messageId,omitempty"`
	ContextID string                   `json:"contextId,omitempty"`
	TaskID    string                   `json:"taskId,omitempty"`
	Role      string                   `json:"role,omitempty"`
	Parts     []map[string]interface{} `json:"parts,omitempty"`
	Metadata  map[string]interface{}   `json:"metadata,omitempty"`
}

type A2ATask struct {
	Kind      string                 `json:"kind"`
	ID        string                 `json:"id"`
	ContextID string                 `json:"contextId,omitempty"`
	Status    A2ATaskStatus          `json:"status"`
	Artifacts []A2AArtifact          `json:"artifacts,omitempty"`
	History   []A2AMessage           `json:"history,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

type A2ATaskStatus struct {
	State     string      `json:"state"`
	Timestamp string      `json:"timestamp,omitempty"`
	Message   *A2AMessage `json:"message,omitempty"`
}

type A2AArtifact struct {
	ArtifactID string                   `json:"artifactId"`
	Name       string                   `json:"name,omitempty"`
	Parts      []map[string]interface{} `json:"parts,omitempty"`
	Metadata   map[string]interface{}   `json:"metadata,omitempty"`
}

type A2ATaskStatusUpdateEvent struct {
	Kind      string                 `json:"kind"`
	TaskID    string                 `json:"taskId"`
	ContextID string                 `json:"contextId"`
	Status    A2ATaskStatus          `json:"status"`
	Final     bool                   `json:"final"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

type A2ATaskArtifactUpdateEvent struct {
	Kind      string                 `json:"kind"`
	TaskID    string                 `json:"taskId"`
	ContextID string                 `json:"contextId"`
	Artifact  A2AArtifact            `json:"artifact"`
	Append    bool                   `json:"append,omitempty"`
	LastChunk bool                   `json:"lastChunk,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

type A2AStreamResponse struct {
	Task           *A2ATask                    `json:"task,omitempty"`
	Message        *A2AMessage                 `json:"message,omitempty"`
	StatusUpdate   *A2ATaskStatusUpdateEvent   `json:"statusUpdate,omitempty"`
	ArtifactUpdate *A2ATaskArtifactUpdateEvent `json:"artifactUpdate,omitempty"`
}

type A2ASendMessageResponse struct {
	Task    *A2ATask    `json:"task,omitempty"`
	Message *A2AMessage `json:"message,omitempty"`
}
