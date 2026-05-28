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
	AcceptedOutputModes    []string                   `json:"acceptedOutputModes,omitempty"`
	Blocking               *bool                      `json:"blocking,omitempty"`
	PushNotificationConfig *A2APushNotificationConfig `json:"pushNotificationConfig,omitempty"`
	HistoryLength          *int                       `json:"historyLength,omitempty"`
}

type A2APushNotificationConfig struct {
	ID             string                     `json:"id,omitempty"`
	URL            string                     `json:"url,omitempty"`
	Token          string                     `json:"token,omitempty"`
	Authentication *A2APushAuthenticationInfo `json:"authentication,omitempty"`
	Metadata       map[string]interface{}     `json:"metadata,omitempty"`
	EventTypes     []string                   `json:"eventTypes,omitempty"`
}

type A2APushAuthenticationInfo struct {
	Scheme      string `json:"scheme,omitempty"`
	Credentials string `json:"credentials,omitempty"`
}

type A2ATaskPushNotificationConfig struct {
	TaskID                 string                    `json:"taskId"`
	PushNotificationConfig A2APushNotificationConfig `json:"pushNotificationConfig"`
}

type A2ATaskPushConfigParams struct {
	ID                       string                    `json:"id,omitempty"`
	TaskID                   string                    `json:"taskId,omitempty"`
	PushNotificationConfigID string                    `json:"pushNotificationConfigId,omitempty"`
	PushNotificationConfig   A2APushNotificationConfig `json:"pushNotificationConfig,omitempty"`
}

type A2ATaskPushConfigList struct {
	Items []A2ATaskPushNotificationConfig `json:"items"`
}

type A2ATaskQueryParams struct {
	ID            string `json:"id"`
	HistoryLength *int   `json:"historyLength,omitempty"`
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
