package skillpackage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func CompatibleProviders(features []string) []string {
	out := []string{}
	if !slices.Contains(features, Feature) {
		return out
	}
	for _, provider := range []string{"codex", "claude"} {
		if slices.Contains(features, "skill_packages."+provider+".v1") {
			out = append(out, provider)
		}
	}
	return out
}

func ValidateAssignment(value any, features []string) error {
	if value == nil {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var snapshot struct {
		Schema  int `json:"schema_version"`
		Bundles []struct {
			Payload string `json:"payload"`
		} `json:"bundles"`
	}
	if json.Unmarshal(raw, &snapshot) != nil || snapshot.Schema != 1 || len(snapshot.Bundles) > MaxBindings {
		return errors.New("invalid skill package snapshot")
	}
	supported := CompatibleProviders(features)
	for _, item := range snapshot.Bundles {
		var bundle Bundle
		if json.Unmarshal([]byte(item.Payload), &bundle) != nil {
			return errors.New("invalid skill package payload")
		}
		compatible := false
		for _, provider := range bundle.Providers {
			compatible = compatible || slices.Contains(supported, provider)
		}
		if !compatible {
			return errors.New("runtime does not support the snapshotted skill packages")
		}
	}
	return nil
}

// Snapshot uses the Run creation transaction. It never places private files in
// caller-visible request_metadata; only authenticated assignment queries join it.
func Snapshot(ctx context.Context, tx pgx.Tx, runID, agentID uuid.UUID) error {
	_, err := tx.Exec(ctx, `INSERT INTO run_skill_package_snapshots(run_id,package_id,version_id,binding_id,digest)
 SELECT $1,b.package_id,b.version_id,b.binding_id,v.digest FROM agent_skill_package_bindings b JOIN skill_package_versions v ON v.id=b.version_id WHERE b.agent_id=$2`, runID, agentID)
	if err != nil {
		return err
	}
	var incompatible bool
	// Session features remain valid historical evidence after disconnect/close.
	// Unknown history must not turn an otherwise queueable Run into an error.
	err = tx.QueryRow(ctx, `WITH latest_host AS (
 SELECT features FROM runtime_sessions WHERE agent_id=$2 ORDER BY created_at DESC,runtime_session_id DESC LIMIT 1)
 SELECT EXISTS(SELECT 1 FROM run_skill_package_snapshots s
 JOIN skill_package_versions v ON v.id=s.version_id CROSS JOIN latest_host h
 WHERE s.run_id=$1 AND NOT ('skill_packages.v1'=ANY(h.features) AND EXISTS (
 SELECT 1 FROM unnest(v.providers) p WHERE ('skill_packages.'||p||'.v1')=ANY(h.features))))`, runID, agentID).Scan(&incompatible)
	if err == nil && incompatible {
		// Caller-facing admission errors must not disclose private Agent configuration.
		return httpx.NewError(http.StatusServiceUnavailable, httpx.CodeServiceUnavailable, "Agent execution environment is temporarily unavailable")
	}
	return err
}

var ErrInvalidReceipt = errors.New("invalid skill package receipt")

func invalidReceipt(message string) error { return fmt.Errorf("%w: %s", ErrInvalidReceipt, message) }

type Receipt struct {
	Bindings  []ReceiptBinding `json:"bindings"`
	ErrorCode string           `json:"error_code,omitempty"`
}
type ReceiptBinding struct {
	BindingID uuid.UUID `json:"binding_id"`
	VersionID uuid.UUID `json:"version_id"`
	Digest    string    `json:"digest"`
}

// RecordReceipt is called only after Core's ordinary event principal, active
// Attempt, lease, fence and sequence checks, in that same transaction.
func RecordReceipt(ctx context.Context, tx pgx.Tx, runID, agentID uuid.UUID, event string, payload []byte) error {
	if event != "run.skill_packages.loaded" && event != "run.skill_packages.failed" {
		return nil
	}
	var receipt Receipt
	if json.Unmarshal(payload, &receipt) != nil || len(receipt.Bindings) < 1 || len(receipt.Bindings) > MaxBindings {
		return invalidReceipt("invalid skill package receipt")
	}
	var snapshot struct {
		Bundles []ReceiptBinding `json:"bundles"`
	}
	var raw []byte
	if err := tx.QueryRow(ctx, `SELECT jsonb_build_object('bundles',COALESCE(jsonb_agg(jsonb_build_object('binding_id',binding_id,'version_id',version_id,'digest',digest)),'[]'::jsonb)) FROM run_skill_package_snapshots WHERE run_id=$1`, runID).Scan(&raw); err != nil {
		return err
	}
	if json.Unmarshal(raw, &snapshot) != nil || len(snapshot.Bundles) != len(receipt.Bindings) {
		return invalidReceipt("skill package receipt does not match the Run snapshot")
	}
	want := map[uuid.UUID]ReceiptBinding{}
	for _, item := range snapshot.Bundles {
		want[item.BindingID] = item
	}
	status := "loaded"
	if event == "run.skill_packages.failed" {
		status = "failed"
		if receipt.ErrorCode != "package_invalid" && receipt.ErrorCode != "package_materialization_failed" && receipt.ErrorCode != "dependency_missing" {
			return invalidReceipt("invalid package failure code")
		}
	} else if receipt.ErrorCode != "" {
		return invalidReceipt("loaded receipt contains an error")
	}
	for _, item := range receipt.Bindings {
		if expected, ok := want[item.BindingID]; !ok || expected != item {
			return invalidReceipt("skill package receipt version mismatch")
		}
		delete(want, item.BindingID)
	}
	for _, item := range receipt.Bindings {
		// A removed or upgraded binding cannot be revived by an older Run.
		_, err := tx.Exec(ctx, `UPDATE agent_skill_package_bindings b SET status=$4,error_code=$5,last_run_id=$3,
   loaded_at=CASE WHEN $4='loaded' THEN now() ELSE NULL END
   WHERE agent_id=$1 AND binding_id=$2 AND (last_run_id IS NULL OR last_run_id=$3 OR
    (SELECT min(created_at) FROM run_skill_package_snapshots WHERE run_id=last_run_id) <= (SELECT min(created_at) FROM run_skill_package_snapshots WHERE run_id=$3))`, agentID, item.BindingID, runID, status, receipt.ErrorCode)
		if err != nil {
			return err
		}
	}
	return nil
}
