package runtime

const (
	RuntimeProtocolVersion = 2
	RuntimeContractID      = "openlinker.runtime.v2"
	RuntimeContractDigest  = "fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53"
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
