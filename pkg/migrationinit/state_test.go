package migrationinit

import (
	"strings"
	"testing"
)

func TestValidateCoreUp(t *testing.T) {
	tests := []struct {
		name      string
		snapshot  Snapshot
		wantNoop  bool
		wantError string
	}{
		{name: "fresh", snapshot: Snapshot{}},
		{name: "current", snapshot: currentCoreSnapshot(), wantNoop: true},
		{name: "supported predecessor", snapshot: upgradeableCoreSnapshot()},
		{name: "nonempty without bookkeeping", snapshot: Snapshot{NonBookkeepingObjects: 1}, wantError: "requires an empty database"},
		{name: "Cloud bookkeeping before Core", snapshot: Snapshot{Cloud: MigrationTableState{Exists: true, Rows: 1, Version: CloudVersion}}, wantError: "Cloud migration bookkeeping"},
		{name: "legacy", snapshot: withCoreState(currentCoreSnapshot(), MigrationTableState{Exists: true, Rows: 1, Version: 81}), wantError: "unsupported"},
		{name: "predecessor definition drift", snapshot: withCoreDigest(upgradeableCoreSnapshot(), "wrong"), wantError: "fingerprint mismatch"},
		{name: "dirty", snapshot: withCoreState(currentCoreSnapshot(), MigrationTableState{Exists: true, Rows: 1, Version: CoreVersion, Dirty: true}), wantError: "dirty"},
		{name: "malformed", snapshot: withCoreState(currentCoreSnapshot(), MigrationTableState{Exists: true, Rows: 2}), wantError: "exactly one"},
		{name: "partial current", snapshot: withCoreShape(currentCoreSnapshot(), SchemaShape{Tables: 69}), wantError: "fingerprint mismatch"},
		{name: "definition drift with stable counts", snapshot: withCoreDigest(currentCoreSnapshot(), "wrong"), wantError: "fingerprint mismatch"},
		{name: "invalid callback owner index", snapshot: withCallbackOwnerIndexValidity(currentCoreSnapshot(), false), wantError: "idx_task_callback_subscriptions_owner is missing or invalid"},
		{name: "historical Browser Agent not backfilled", snapshot: withUnclassifiedBrowserAgents(currentCoreSnapshot(), 1), wantError: "backfill is incomplete"},
		{name: "unknown public relation", snapshot: withRelationCount(currentCoreSnapshot(), 73), wantError: "ownership mismatch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			noop, err := tt.snapshot.ValidateCoreUp()
			if noop != tt.wantNoop {
				t.Fatalf("noop = %v, want %v", noop, tt.wantNoop)
			}
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("error = %v, want contains %q", err, tt.wantError)
			}
		})
	}
}

func TestValidateCloudUp(t *testing.T) {
	fresh := currentCoreSnapshot()
	current := fresh
	current.Cloud = MigrationTableState{Exists: true, Rows: 1, Version: CloudVersion}
	current.CloudShape = currentCloudShape()
	current.NonBookkeepingObjects = 79

	tests := []struct {
		name      string
		snapshot  Snapshot
		wantNoop  bool
		wantError string
	}{
		{name: "fresh after current Core", snapshot: fresh},
		{name: "current", snapshot: current, wantNoop: true},
		{name: "current Cloud accepts exact Core predecessor preflight", snapshot: upgradeableCloudSnapshot(current), wantNoop: true},
		{name: "Core missing", snapshot: Snapshot{}, wantError: "current Core"},
		{name: "Core legacy", snapshot: withCoreState(fresh, MigrationTableState{Exists: true, Rows: 1, Version: 81}), wantError: "current Core"},
		{name: "Core predecessor", snapshot: withCoreShape(withCoreState(fresh, MigrationTableState{Exists: true, Rows: 1, Version: CoreUpgradeVersion}), upgradeableCoreShape()), wantError: "complete its supported migration"},
		{name: "Core Browser backfill incomplete", snapshot: withUnclassifiedBrowserAgents(current, 1), wantError: "backfill is incomplete"},
		{name: "partial without bookkeeping", snapshot: withCloudShape(fresh, SchemaShape{Tables: 1}), wantError: "partial or legacy"},
		{name: "obsolete table", snapshot: withObsoleteCloud(fresh, 1), wantError: "partial or legacy"},
		{name: "Cloud legacy", snapshot: withCloudState(current, MigrationTableState{Exists: true, Rows: 1, Version: 54}), wantError: "unsupported"},
		{name: "Cloud dirty", snapshot: withCloudState(current, MigrationTableState{Exists: true, Rows: 1, Version: CloudVersion, Dirty: true}), wantError: "dirty"},
		{name: "Cloud malformed", snapshot: withCloudState(current, MigrationTableState{Exists: true, Rows: 0}), wantError: "exactly one"},
		{name: "Core callback owner index invalid", snapshot: withCallbackOwnerIndexValidity(current, false), wantError: "idx_task_callback_subscriptions_owner is missing or invalid"},
		{name: "Cloud schema drift", snapshot: withCloudShape(current, SchemaShape{Tables: 7, Constraints: 71, Indexes: 30, Triggers: 5, GuardFunctions: 1}), wantError: "fingerprint mismatch"},
		{name: "Cloud definition drift with stable counts", snapshot: withCloudDigest(current, "wrong"), wantError: "fingerprint mismatch"},
		{name: "unknown public relation", snapshot: withRelationCount(current, 80), wantError: "ownership mismatch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			noop, err := tt.snapshot.ValidateCloudUp()
			if noop != tt.wantNoop {
				t.Fatalf("noop = %v, want %v", noop, tt.wantNoop)
			}
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("error = %v, want contains %q", err, tt.wantError)
			}
		})
	}
}

func currentCoreSnapshot() Snapshot {
	return Snapshot{
		Core:                    MigrationTableState{Exists: true, Rows: 1, Version: CoreVersion},
		CoreShape:               currentCoreShape(),
		NonBookkeepingObjects:   72,
		CallbackOwnerIndexValid: true,
	}
}

func currentCoreShape() SchemaShape {
	return SchemaShape{
		Digest:            CoreSchemaDigest,
		Tables:            72,
		Constraints:       616,
		Indexes:           266,
		Triggers:          70,
		CoreIdentities:    1,
		RuntimeControls:   1,
		RuntimeSchemas:    10,
		CurrentRuntime:    1,
		RuntimeWires:      5,
		CurrentWire:       1,
		PreviousWire:      1,
		BuiltInSkills:     30,
		BuiltInSkillCases: 15,
	}
}

func upgradeableCoreSnapshot() Snapshot {
	return Snapshot{
		Core: MigrationTableState{
			Exists:  true,
			Rows:    1,
			Version: CoreUpgradeVersion,
		},
		CoreShape:             upgradeableCoreShape(),
		NonBookkeepingObjects: 72,
	}
}

func upgradeableCoreShape() SchemaShape {
	return SchemaShape{
		Digest:            CoreUpgradeSchemaDigest,
		Tables:            72,
		Constraints:       615,
		Indexes:           265,
		Triggers:          70,
		CoreIdentities:    1,
		RuntimeControls:   1,
		RuntimeSchemas:    10,
		CurrentRuntime:    1,
		RuntimeWires:      5,
		CurrentWire:       1,
		PreviousWire:      1,
		BuiltInSkills:     30,
		BuiltInSkillCases: 15,
	}
}

func upgradeableCloudSnapshot(current Snapshot) Snapshot {
	current.Core = MigrationTableState{
		Exists:  true,
		Rows:    1,
		Version: CoreUpgradeVersion,
	}
	current.CoreShape = upgradeableCoreShape()
	current.NonBookkeepingObjects = 79
	return current
}

func currentCloudShape() SchemaShape {
	return SchemaShape{Digest: CloudSchemaDigest, Tables: 7, Constraints: 72, Indexes: 30, Triggers: 5, GuardFunctions: 1}
}

func withCoreState(snapshot Snapshot, state MigrationTableState) Snapshot {
	snapshot.Core = state
	return snapshot
}

func withCloudState(snapshot Snapshot, state MigrationTableState) Snapshot {
	snapshot.Cloud = state
	return snapshot
}

func withCoreShape(snapshot Snapshot, shape SchemaShape) Snapshot {
	snapshot.CoreShape = shape
	return snapshot
}

func withCloudShape(snapshot Snapshot, shape SchemaShape) Snapshot {
	snapshot.CloudShape = shape
	return snapshot
}

func withCoreDigest(snapshot Snapshot, digest string) Snapshot {
	snapshot.CoreShape.Digest = digest
	return snapshot
}

func withCloudDigest(snapshot Snapshot, digest string) Snapshot {
	snapshot.CloudShape.Digest = digest
	return snapshot
}

func withRelationCount(snapshot Snapshot, count int64) Snapshot {
	snapshot.NonBookkeepingObjects = count
	return snapshot
}

func withCallbackOwnerIndexValidity(snapshot Snapshot, valid bool) Snapshot {
	snapshot.CallbackOwnerIndexValid = valid
	return snapshot
}

func withObsoleteCloud(snapshot Snapshot, count int64) Snapshot {
	snapshot.ObsoleteCloudObjects = count
	return snapshot
}

func withUnclassifiedBrowserAgents(snapshot Snapshot, count int64) Snapshot {
	snapshot.UnclassifiedBrowserAgents = count
	return snapshot
}
