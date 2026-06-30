package a2a

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	a2apb "github.com/OpenLinker-ai/openlinker-core/pkg/a2a/pb"
	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func messageSendParamsFromProto(req *a2apb.SendMessageRequest) *A2AMessageSendParams {
	if req == nil {
		return nil
	}
	return &A2AMessageSendParams{
		Message:       messageFromProto(req.GetMessage()),
		Configuration: sendConfigurationFromProto(req.GetConfiguration()),
		Metadata:      structToMap(req.GetMetadata()),
	}
}

func sendConfigurationFromProto(cfg *a2apb.SendMessageConfiguration) *A2ASendConfiguration {
	if cfg == nil {
		return nil
	}
	returnImmediately := cfg.GetReturnImmediately()
	out := &A2ASendConfiguration{
		AcceptedOutputModes:    append([]string{}, cfg.GetAcceptedOutputModes()...),
		ReturnImmediately:      &returnImmediately,
		PushNotificationConfig: pushConfigFromProto(cfg.GetTaskPushNotificationConfig()),
	}
	if cfg.HistoryLength != nil {
		historyLength := int(cfg.GetHistoryLength())
		out.HistoryLength = &historyLength
	}
	if taskCfg := taskPushConfigFromProto(cfg.GetTaskPushNotificationConfig()); taskCfg != nil {
		out.TaskPushNotificationConfig = taskCfg
	}
	return out
}

func messageFromProto(msg *a2apb.Message) A2AMessage {
	if msg == nil {
		return A2AMessage{}
	}
	parts := make([]map[string]interface{}, 0, len(msg.GetParts()))
	for _, part := range msg.GetParts() {
		parts = append(parts, partFromProto(part))
	}
	return A2AMessage{
		Kind:             "message",
		MessageID:        strings.TrimSpace(msg.GetMessageId()),
		ContextID:        strings.TrimSpace(msg.GetContextId()),
		TaskID:           strings.TrimSpace(msg.GetTaskId()),
		ReferenceTaskIDs: append([]string{}, msg.GetReferenceTaskIds()...),
		Extensions:       append([]string{}, msg.GetExtensions()...),
		Role:             roleFromProto(msg.GetRole()),
		Parts:            parts,
		Metadata:         structToMap(msg.GetMetadata()),
	}
}

func partFromProto(part *a2apb.Part) map[string]interface{} {
	if part == nil {
		return map[string]interface{}{}
	}
	out := map[string]interface{}{}
	switch content := part.GetContent().(type) {
	case *a2apb.Part_Text:
		out["kind"] = "text"
		out["text"] = content.Text
	case *a2apb.Part_Data:
		out["kind"] = "data"
		out["data"] = valueToInterface(content.Data)
	case *a2apb.Part_Url:
		out["kind"] = "file"
		out["file"] = filePartMap(content.Url, nil, part.GetFilename(), part.GetMediaType(), nil)
	case *a2apb.Part_Raw:
		out["kind"] = "file"
		out["file"] = filePartMap("", content.Raw, part.GetFilename(), part.GetMediaType(), nil)
	default:
		out["kind"] = "data"
		out["data"] = nil
	}
	if metadata := structToMap(part.GetMetadata()); len(metadata) > 0 {
		out["metadata"] = metadata
	}
	return out
}

func filePartMap(url string, raw []byte, filename, mediaType string, metadata map[string]interface{}) map[string]interface{} {
	file := map[string]interface{}{}
	if strings.TrimSpace(url) != "" {
		file["url"] = strings.TrimSpace(url)
		file["uri"] = strings.TrimSpace(url)
	}
	if len(raw) > 0 {
		file["bytes"] = base64.StdEncoding.EncodeToString(raw)
	}
	if strings.TrimSpace(filename) != "" {
		file["name"] = strings.TrimSpace(filename)
		file["filename"] = strings.TrimSpace(filename)
	}
	if strings.TrimSpace(mediaType) != "" {
		file["mimeType"] = strings.TrimSpace(mediaType)
		file["mediaType"] = strings.TrimSpace(mediaType)
	}
	if len(metadata) > 0 {
		file["metadata"] = metadata
	}
	return file
}

func taskPushConfigFromProto(cfg *a2apb.TaskPushNotificationConfig) *A2ATaskPushNotificationConfig {
	if cfg == nil {
		return nil
	}
	out := &A2ATaskPushNotificationConfig{
		Tenant: cfg.GetTenant(),
		ID:     cfg.GetId(),
		TaskID: cfg.GetTaskId(),
		URL:    cfg.GetUrl(),
		Token:  cfg.GetToken(),
	}
	if auth := cfg.GetAuthentication(); auth != nil {
		out.Authentication = &A2APushAuthenticationInfo{
			Scheme:      auth.GetScheme(),
			Credentials: auth.GetCredentials(),
		}
	}
	out.PushNotificationConfig = pushNotificationConfigFromTaskPush(*out)
	return out
}

func pushConfigFromProto(cfg *a2apb.TaskPushNotificationConfig) *A2APushNotificationConfig {
	taskCfg := taskPushConfigFromProto(cfg)
	if taskCfg == nil {
		return nil
	}
	out := pushNotificationConfigFromTaskPush(*taskCfg)
	return &out
}

func protoSendMessageResponse(task *A2ATask) *a2apb.SendMessageResponse {
	if task != nil && task.ResponseMessage != nil {
		return &a2apb.SendMessageResponse{Payload: &a2apb.SendMessageResponse_Message{Message: messageToProto(task.ResponseMessage)}}
	}
	return &a2apb.SendMessageResponse{Payload: &a2apb.SendMessageResponse_Task{Task: taskToProto(task)}}
}

func protoStreamResponseFromTask(task *A2ATask) *a2apb.StreamResponse {
	return &a2apb.StreamResponse{Payload: &a2apb.StreamResponse_Task{Task: taskToProto(task)}}
}

func protoStreamResponseFromEvent(event interface{}) *a2apb.StreamResponse {
	switch typed := event.(type) {
	case *A2ATaskStatusUpdateEvent:
		return &a2apb.StreamResponse{Payload: &a2apb.StreamResponse_StatusUpdate{StatusUpdate: statusUpdateToProto(typed)}}
	case A2ATaskStatusUpdateEvent:
		return &a2apb.StreamResponse{Payload: &a2apb.StreamResponse_StatusUpdate{StatusUpdate: statusUpdateToProto(&typed)}}
	case *A2ATaskArtifactUpdateEvent:
		return &a2apb.StreamResponse{Payload: &a2apb.StreamResponse_ArtifactUpdate{ArtifactUpdate: artifactUpdateToProto(typed)}}
	case A2ATaskArtifactUpdateEvent:
		return &a2apb.StreamResponse{Payload: &a2apb.StreamResponse_ArtifactUpdate{ArtifactUpdate: artifactUpdateToProto(&typed)}}
	case *A2AMessage:
		return &a2apb.StreamResponse{Payload: &a2apb.StreamResponse_Message{Message: messageToProto(typed)}}
	case A2AMessage:
		return &a2apb.StreamResponse{Payload: &a2apb.StreamResponse_Message{Message: messageToProto(&typed)}}
	case *A2ATask:
		return protoStreamResponseFromTask(typed)
	case A2ATask:
		return protoStreamResponseFromTask(&typed)
	default:
		return nil
	}
}

func taskToProto(task *A2ATask) *a2apb.Task {
	if task == nil {
		return nil
	}
	artifacts := make([]*a2apb.Artifact, 0, len(task.Artifacts))
	for i := range task.Artifacts {
		artifacts = append(artifacts, artifactToProto(&task.Artifacts[i]))
	}
	history := make([]*a2apb.Message, 0, len(task.History))
	for i := range task.History {
		history = append(history, messageToProto(&task.History[i]))
	}
	return &a2apb.Task{
		Id:        task.ID,
		ContextId: task.ContextID,
		Status:    taskStatusToProto(&task.Status),
		Artifacts: artifacts,
		History:   history,
		Metadata:  currentMetadataToProto(task.Metadata),
	}
}

func taskStatusToProto(status *A2ATaskStatus) *a2apb.TaskStatus {
	if status == nil {
		return nil
	}
	return &a2apb.TaskStatus{
		State:     taskStateToProto(status.State),
		Message:   messageToProto(status.Message),
		Timestamp: timestampFromRFC3339(status.Timestamp),
	}
}

func statusUpdateToProto(event *A2ATaskStatusUpdateEvent) *a2apb.TaskStatusUpdateEvent {
	if event == nil {
		return nil
	}
	return &a2apb.TaskStatusUpdateEvent{
		TaskId:    event.TaskID,
		ContextId: event.ContextID,
		Status:    taskStatusToProto(&event.Status),
		Metadata:  currentMetadataToProto(event.Metadata),
	}
}

func artifactUpdateToProto(event *A2ATaskArtifactUpdateEvent) *a2apb.TaskArtifactUpdateEvent {
	if event == nil {
		return nil
	}
	return &a2apb.TaskArtifactUpdateEvent{
		TaskId:    event.TaskID,
		ContextId: event.ContextID,
		Artifact:  artifactToProto(&event.Artifact),
		Append:    event.Append,
		LastChunk: event.LastChunk,
		Metadata:  currentMetadataToProto(event.Metadata),
	}
}

func messageToProto(msg *A2AMessage) *a2apb.Message {
	if msg == nil {
		return nil
	}
	parts := make([]*a2apb.Part, 0, len(msg.Parts))
	for _, part := range msg.Parts {
		parts = append(parts, partToProto(part))
	}
	return &a2apb.Message{
		MessageId:        msg.MessageID,
		ContextId:        msg.ContextID,
		TaskId:           msg.TaskID,
		Role:             roleToProto(msg.Role),
		Parts:            parts,
		Metadata:         currentMetadataToProto(msg.Metadata),
		Extensions:       append([]string{}, msg.Extensions...),
		ReferenceTaskIds: append([]string{}, msg.ReferenceTaskIDs...),
	}
}

func partToProto(part map[string]interface{}) *a2apb.Part {
	out := &a2apb.Part{Metadata: mapToStruct(nestedMap(part, "metadata"))}
	if filename := firstPartString(part, "filename", "fileName", "name"); filename != "" {
		out.Filename = filename
	}
	if mediaType := firstPartString(part, "mediaType", "mimeType"); mediaType != "" {
		out.MediaType = mediaType
	}

	switch partKind(part) {
	case "text":
		if text, ok := part["text"].(string); ok {
			out.Content = &a2apb.Part_Text{Text: text}
		} else {
			out.Content = &a2apb.Part_Text{Text: fmt.Sprint(part["text"])}
		}
	case "data":
		out.Content = &a2apb.Part_Data{Data: valueToProto(part["data"])}
	case "file":
		source := part
		if file, ok := part["file"].(map[string]interface{}); ok {
			source = file
		}
		if out.Filename == "" {
			out.Filename = firstPartString(source, "filename", "fileName", "name")
		}
		if out.MediaType == "" {
			out.MediaType = firstPartString(source, "mediaType", "mimeType")
		}
		if url := firstPartString(source, "url", "uri"); url != "" {
			out.Content = &a2apb.Part_Url{Url: url}
			return out
		}
		if raw := bytesFromFilePart(source); len(raw) > 0 {
			out.Content = &a2apb.Part_Raw{Raw: raw}
			return out
		}
		out.Content = &a2apb.Part_Data{Data: valueToProto(source)}
	default:
		out.Content = &a2apb.Part_Data{Data: valueToProto(part)}
	}
	return out
}

func nestedMap(source map[string]interface{}, key string) map[string]interface{} {
	if source == nil {
		return nil
	}
	value, _ := source[key].(map[string]interface{})
	return value
}

func bytesFromFilePart(source map[string]interface{}) []byte {
	for _, key := range []string{"raw", "bytes", "fileWithBytes"} {
		switch value := source[key].(type) {
		case []byte:
			return value
		case string:
			if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
				return decoded
			}
			if decoded, err := base64.RawStdEncoding.DecodeString(value); err == nil {
				return decoded
			}
			return []byte(value)
		}
	}
	return nil
}

func artifactToProto(artifact *A2AArtifact) *a2apb.Artifact {
	if artifact == nil {
		return nil
	}
	parts := make([]*a2apb.Part, 0, len(artifact.Parts))
	for _, part := range artifact.Parts {
		parts = append(parts, partToProto(part))
	}
	return &a2apb.Artifact{
		ArtifactId: artifact.ArtifactID,
		Name:       artifact.Name,
		Parts:      parts,
		Metadata:   mapToStruct(artifact.Metadata),
		Extensions: append([]string{}, artifact.Extensions...),
	}
}

func taskPushConfigToProto(cfg *A2ATaskPushNotificationConfig) *a2apb.TaskPushNotificationConfig {
	if cfg == nil {
		return nil
	}
	taskID := cfg.TaskID
	if taskID == "" {
		taskID = cfg.TaskIDAlias
	}
	out := &a2apb.TaskPushNotificationConfig{
		Tenant: cfg.Tenant,
		Id:     cfg.ID,
		TaskId: taskID,
		Url:    cfg.URL,
		Token:  cfg.Token,
	}
	if cfg.Authentication != nil {
		out.Authentication = &a2apb.AuthenticationInfo{
			Scheme:      cfg.Authentication.Scheme,
			Credentials: cfg.Authentication.Credentials,
		}
	}
	return out
}

func agentCardToProto(card *agent.AgentCardResponse) *a2apb.AgentCard {
	if card == nil {
		return nil
	}
	out := &a2apb.AgentCard{
		Name:                card.Name,
		Description:         card.Description,
		SupportedInterfaces: agentInterfacesToProto(card.SupportedInterfaces),
		Provider: &a2apb.AgentProvider{
			Organization: card.Provider.Organization,
			Url:          card.Provider.URL,
		},
		Version:              card.Version,
		Capabilities:         agentCapabilitiesToProto(card.Capabilities),
		SecuritySchemes:      bearerSecuritySchemesToProto(),
		SecurityRequirements: securityRequirementsToProto(card.SecurityRequirements),
		DefaultInputModes:    append([]string{}, card.DefaultInputModesCurrent...),
		DefaultOutputModes:   append([]string{}, card.DefaultOutputModesCurrent...),
		Skills:               agentSkillsToProto(card.Skills),
	}
	if len(out.DefaultInputModes) == 0 {
		out.DefaultInputModes = append([]string{}, card.DefaultInputModes...)
	}
	if len(out.DefaultOutputModes) == 0 {
		out.DefaultOutputModes = append([]string{}, card.DefaultOutputModes...)
	}
	return out
}

func agentInterfacesToProto(items []agent.AgentCardInterface) []*a2apb.AgentInterface {
	out := make([]*a2apb.AgentInterface, 0, len(items))
	for _, item := range items {
		out = append(out, &a2apb.AgentInterface{
			Url:             item.URL,
			ProtocolBinding: item.ProtocolBinding,
			Tenant:          item.Tenant,
			ProtocolVersion: item.ProtocolVersion,
		})
	}
	return out
}

func agentCapabilitiesToProto(cap agent.AgentCardCapabilities) *a2apb.AgentCapabilities {
	streaming := cap.Streaming
	push := cap.PushNotifications
	extended := cap.ExtendedAgentCard
	extensions := make([]*a2apb.AgentExtension, 0, len(cap.Extensions))
	for _, ext := range cap.Extensions {
		extensions = append(extensions, &a2apb.AgentExtension{
			Uri:         ext.URI,
			Description: ext.Description,
			Required:    ext.Required,
			Params:      mapToStruct(ext.Params),
		})
	}
	return &a2apb.AgentCapabilities{
		Streaming:         &streaming,
		PushNotifications: &push,
		ExtendedAgentCard: &extended,
		Extensions:        extensions,
	}
}

func bearerSecuritySchemesToProto() map[string]*a2apb.SecurityScheme {
	return map[string]*a2apb.SecurityScheme{
		"openlinker_bearer": {
			Scheme: &a2apb.SecurityScheme_HttpAuthSecurityScheme{
				HttpAuthSecurityScheme: &a2apb.HTTPAuthSecurityScheme{
					Scheme:       "Bearer",
					BearerFormat: "JWT or OpenLinker access token",
					Description:  "Use Authorization: Bearer <token> with agents:run and runs:read scopes.",
				},
			},
		},
	}
}

func securityRequirementsToProto(items []map[string][]string) []*a2apb.SecurityRequirement {
	out := make([]*a2apb.SecurityRequirement, 0, len(items))
	for _, item := range items {
		req := &a2apb.SecurityRequirement{Schemes: map[string]*a2apb.StringList{}}
		for scheme, scopes := range item {
			req.Schemes[scheme] = &a2apb.StringList{List: append([]string{}, scopes...)}
		}
		out = append(out, req)
	}
	if len(out) == 0 {
		out = append(out, &a2apb.SecurityRequirement{Schemes: map[string]*a2apb.StringList{
			"openlinker_bearer": {List: []string{"agents:run", "runs:read"}},
		}})
	}
	return out
}

func agentSkillsToProto(items []agent.AgentCardSkill) []*a2apb.AgentSkill {
	out := make([]*a2apb.AgentSkill, 0, len(items))
	for _, item := range items {
		out = append(out, &a2apb.AgentSkill{
			Id:          item.ID,
			Name:        item.Name,
			Description: item.Description,
			Tags:        append([]string{}, item.Tags...),
		})
	}
	return out
}

func roleFromProto(role a2apb.Role) string {
	switch role {
	case a2apb.Role_ROLE_AGENT:
		return "agent"
	case a2apb.Role_ROLE_USER, a2apb.Role_ROLE_UNSPECIFIED:
		return "user"
	default:
		return "user"
	}
}

func roleToProto(role string) a2apb.Role {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "agent", "assistant", "server":
		return a2apb.Role_ROLE_AGENT
	case "user", "client", "":
		return a2apb.Role_ROLE_USER
	default:
		return a2apb.Role_ROLE_USER
	}
}

func taskStateToProto(state string) a2apb.TaskState {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case a2aTaskStateSubmitted:
		return a2apb.TaskState_TASK_STATE_SUBMITTED
	case a2aTaskStateWorking:
		return a2apb.TaskState_TASK_STATE_WORKING
	case a2aTaskStateCompleted:
		return a2apb.TaskState_TASK_STATE_COMPLETED
	case a2aTaskStateFailed:
		return a2apb.TaskState_TASK_STATE_FAILED
	case a2aTaskStateCanceled, "cancelled":
		return a2apb.TaskState_TASK_STATE_CANCELED
	case "input-required", "input_required":
		return a2apb.TaskState_TASK_STATE_INPUT_REQUIRED
	case "auth-required", "auth_required":
		return a2apb.TaskState_TASK_STATE_AUTH_REQUIRED
	case "rejected":
		return a2apb.TaskState_TASK_STATE_REJECTED
	default:
		return a2apb.TaskState_TASK_STATE_UNSPECIFIED
	}
}

func taskStateFromProto(state a2apb.TaskState) string {
	switch state {
	case a2apb.TaskState_TASK_STATE_SUBMITTED:
		return a2aTaskStateSubmitted
	case a2apb.TaskState_TASK_STATE_WORKING:
		return a2aTaskStateWorking
	case a2apb.TaskState_TASK_STATE_COMPLETED:
		return a2aTaskStateCompleted
	case a2apb.TaskState_TASK_STATE_FAILED:
		return a2aTaskStateFailed
	case a2apb.TaskState_TASK_STATE_CANCELED:
		return a2aTaskStateCanceled
	case a2apb.TaskState_TASK_STATE_INPUT_REQUIRED:
		return "input_required"
	case a2apb.TaskState_TASK_STATE_AUTH_REQUIRED:
		return "auth_required"
	case a2apb.TaskState_TASK_STATE_REJECTED:
		return "rejected"
	default:
		return ""
	}
}

func timestampFromRFC3339(raw string) *timestamppb.Timestamp {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return timestamppb.New(parsed)
		}
	}
	return nil
}

func mapToStruct(value map[string]interface{}) *structpb.Struct {
	if len(value) == 0 {
		return nil
	}
	sanitized, ok := sanitizeProtoValue(value).(map[string]interface{})
	if !ok {
		return nil
	}
	out, err := structpb.NewStruct(sanitized)
	if err != nil {
		return nil
	}
	return out
}

func currentMetadataToProto(value map[string]interface{}) *structpb.Struct {
	if len(value) == 0 {
		return nil
	}
	return mapToStruct(normalizeA2AMetadataForCurrent(value))
}

func structToMap(value *structpb.Struct) map[string]interface{} {
	if value == nil {
		return nil
	}
	out, _ := valueToInterface(value).(map[string]interface{})
	return out
}

func valueToProto(value interface{}) *structpb.Value {
	out, err := structpb.NewValue(sanitizeProtoValue(value))
	if err != nil {
		return structpb.NewNullValue()
	}
	return out
}

func valueToInterface(value interface{}) interface{} {
	switch typed := value.(type) {
	case nil:
		return nil
	case *structpb.Struct:
		return typed.AsMap()
	case *structpb.Value:
		return typed.AsInterface()
	default:
		return typed
	}
}

func sanitizeProtoValue(value interface{}) interface{} {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	var out interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return fmt.Sprint(value)
	}
	return out
}
