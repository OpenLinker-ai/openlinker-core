package runtime

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestBrowserPayloadUint64AcceptsDecodedAndNativeNumbers(t *testing.T) {
	for _, value := range []any{
		float64(7),
		int(7),
		int64(7),
		uint64(7),
		json.Number("7"),
	} {
		got, err := browserPayloadUint64(map[string]any{"epoch": value}, "epoch")
		if err != nil || got != 7 {
			t.Fatalf("browserPayloadUint64(%T) = %d, %v", value, got, err)
		}
	}
	for _, value := range []any{float64(0), float64(1.5), -1, "7", json.Number("-1")} {
		if _, err := browserPayloadUint64(
			map[string]any{"epoch": value},
			"epoch",
		); err == nil {
			t.Fatalf("browserPayloadUint64(%T) accepted invalid value", value)
		}
	}
}

func TestBrowserViewerFrameRelayIsBoundedAndEpochFenced(t *testing.T) {
	now := time.Now().UTC()
	identity := AttemptIdentity{
		RunID:            uuid.New(),
		AttemptID:        uuid.New(),
		LeaseID:          uuid.New(),
		FencingToken:     1,
		NodeID:           uuid.New(),
		AgentID:          uuid.New(),
		WorkerID:         "worker-1",
		RuntimeSessionID: uuid.New(),
	}
	state := BrowserHumanControlState{
		RunID:            identity.RunID,
		UserID:           uuid.New(),
		AttemptID:        identity.AttemptID,
		RuntimeSessionID: identity.RuntimeSessionID,
		BrowserSessionID: uuid.New(),
		SessionEpoch:     1,
		AttachmentID:     uuid.New(),
		ControlEpoch:     9,
		Controller:       "human",
		State:            "human",
		HumanExpiresAt:   pointerTime(now.Add(time.Minute)),
	}
	control := NewBrowserHumanControl(nil)
	control.now = func() time.Time { return now }
	control.setLive(state)

	frame := BrowserViewerFramePayload{
		AttemptIdentity:  identity,
		BrowserSessionID: state.BrowserSessionID,
		SessionEpoch:     state.SessionEpoch,
		AttachmentID:     state.AttachmentID,
		ControlEpoch:     state.ControlEpoch,
		MIMEType:         "image/jpeg",
		Data:             []byte{0xff},
		Width:            1280,
		Height:           720,
	}
	for sequence := uint64(1); sequence <= browserViewerFrameLimit; sequence++ {
		frame.FrameSeq = sequence
		if err := control.PublishFrame(frame); err != nil {
			t.Fatalf("publish frame %d: %v", sequence, err)
		}
	}
	frame.FrameSeq++
	if err := control.PublishFrame(frame); err == nil {
		t.Fatal("frame-count limit was not enforced")
	}
	control.mu.Lock()
	live := control.live[state.RunID]
	if live.frameCount != browserViewerFrameLimit ||
		len(live.frame.Data) != 1 {
		t.Fatalf("bounded latest-frame relay = %#v", live)
	}
	control.mu.Unlock()

	stale := frame
	stale.ControlEpoch--
	if err := control.PublishFrame(stale); err == nil {
		t.Fatal("stale Viewer frame epoch was accepted")
	}
}

func TestBrowserViewerTerminateIsAClosedControlTransition(t *testing.T) {
	command := BrowserViewerCommandPayload{
		AttemptIdentity: AttemptIdentity{
			RunID:            uuid.New(),
			AttemptID:        uuid.New(),
			LeaseID:          uuid.New(),
			FencingToken:     1,
			NodeID:           uuid.New(),
			AgentID:          uuid.New(),
			WorkerID:         "worker-1",
			RuntimeSessionID: uuid.New(),
		},
		Action:               BrowserViewerActionTerminate,
		BrowserSessionID:     uuid.New(),
		SessionEpoch:         1,
		AttachmentID:         uuid.New(),
		PreviousControlEpoch: 3,
		ControlEpoch:         4,
		DeadlineAt:           time.Now().Add(time.Minute),
	}
	if err := validateBrowserViewerCommand(command); err != nil {
		t.Fatalf("terminate transition rejected: %v", err)
	}
	command.Input = &BrowserViewerInputPayload{Kind: "keyboard"}
	if err := validateBrowserViewerCommand(command); err == nil {
		t.Fatal("terminate transition accepted Viewer input")
	}
}

func pointerTime(value time.Time) *time.Time {
	return &value
}
