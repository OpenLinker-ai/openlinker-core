package runtime

const (
	RuntimeProtocolVersion = 2
	RuntimeContractID      = "openlinker.runtime.v2"
	RuntimeContractDigest  = "3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9"
)

var runtimeRequiredFeatures = [...]string{
	"lease_fence",
	"assignment_confirm",
	"renew",
	"resume",
	"event_ack",
	"result_ack",
	"cancel",
	"persistent_spool",
}

// RuntimeRequiredFeatures returns a copy so callers cannot mutate the
// handshake requirements for the running process.
func RuntimeRequiredFeatures() []string {
	features := make([]string, len(runtimeRequiredFeatures))
	copy(features, runtimeRequiredFeatures[:])
	return features
}
