-- 086_current_schema_init.up.sql
-- Canonical fresh-database schema for OpenLinker Core.
--
-- This initializer intentionally contains only the final catalog and required
-- seed/control state. It must never be used as an upgrade migration.

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';

-- The final schema below is normalized from PostgreSQL 16 pg_dump output.


-- Dumped from database version 16.14 (Debian 16.14-1.pgdg13+1)
-- Dumped by pg_dump version 16.14 (Debian 16.14-1.pgdg13+1)

SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: emit_event_wake_notification(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.emit_event_wake_notification() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    channel_name TEXT;
    topic_name TEXT;
    resource_id TEXT;
    wake_generation BIGINT;
    payload TEXT;
BEGIN
    IF TG_NARGS <> 2 THEN
        RAISE EXCEPTION 'event wake trigger arguments are invalid';
    END IF;

    channel_name := TG_ARGV[0];
    topic_name := TG_ARGV[1];

    -- Keep the producer table/channel/topic mapping closed. Direct field
    -- access intentionally avoids converting a potentially large Run or
    -- outbox row (including input/output/payload) to JSON just to read its ID.
    IF TG_TABLE_NAME = 'run_events'
       AND channel_name = 'openlinker_run_v1'
       AND topic_name = 'run.changed' THEN
        resource_id := NEW.run_id::TEXT;
        wake_generation := NEW.sequence::BIGINT;
    ELSIF TG_TABLE_NAME = 'runs'
       AND channel_name = 'openlinker_run_v1'
       AND topic_name = 'run.changed' THEN
        resource_id := NEW.id::TEXT;
        wake_generation := floor(
            extract(epoch FROM clock_timestamp()) * 1000000
        )::BIGINT;
    ELSIF TG_TABLE_NAME = 'runtime_signal_outbox'
       AND channel_name = 'openlinker_work_v1'
       AND topic_name = 'work.runtime_signal.available' THEN
        resource_id := NEW.id::TEXT;
        wake_generation := NEW.attempt_count::BIGINT;
    ELSIF TG_TABLE_NAME = 'run_effect_outbox'
       AND channel_name = 'openlinker_work_v1'
       AND topic_name = 'work.run_effect.available' THEN
        resource_id := NEW.id::TEXT;
        wake_generation := NEW.attempt_count::BIGINT;
    ELSE
        RAISE EXCEPTION 'event wake channel/topic is not allowlisted';
    END IF;

    IF resource_id IS NULL OR resource_id = '' OR octet_length(resource_id) > 200 THEN
        RAISE EXCEPTION 'event wake resource identifier is invalid';
    END IF;
    IF wake_generation < 0 THEN
        RAISE EXCEPTION 'event wake generation is invalid';
    END IF;

    payload := jsonb_build_object(
        'version', 1,
        'topic', topic_name,
        'resource_id', resource_id,
        'generation', wake_generation,
        'produced_at', to_char(
            clock_timestamp() AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
    )::TEXT;
    IF octet_length(payload) > 1024 THEN
        RAISE EXCEPTION 'event wake payload is too large';
    END IF;

    PERFORM pg_notify(channel_name, payload);
    RETURN NEW;
END
$$;


--
-- Name: emit_external_execution_wake_notification(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.emit_external_execution_wake_notification() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    resource_id TEXT;
    payload TEXT;
BEGIN
    CASE TG_TABLE_NAME
        WHEN 'external_execution_cancellations' THEN
            resource_id := NEW.external_request_id::TEXT;
        WHEN 'external_executions' THEN
            resource_id := NEW.external_request_id::TEXT;
        WHEN 'run_cancellations' THEN
            resource_id := NEW.run_id::TEXT;
        WHEN 'workflow_run_cancellations' THEN
            resource_id := NEW.workflow_run_id::TEXT;
        ELSE
            RAISE EXCEPTION 'external execution wake table is not allowlisted';
    END CASE;

    IF resource_id IS NULL OR resource_id = '' OR octet_length(resource_id) > 200 THEN
        RAISE EXCEPTION 'external execution wake resource identifier is invalid';
    END IF;
    payload := jsonb_build_object(
        'version', 1,
        'topic', 'external_execution.changed',
        'resource_id', resource_id,
        'generation', floor(extract(epoch FROM clock_timestamp()) * 1000000)::BIGINT,
        'produced_at', to_char(
            clock_timestamp() AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
    )::TEXT;
    IF octet_length(payload) > 1024 THEN
        RAISE EXCEPTION 'external execution wake payload is too large';
    END IF;
    PERFORM pg_notify('openlinker_external_v1', payload);
    RETURN NEW;
END
$$;


--
-- Name: enforce_agent_token_identity_and_lifecycle(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_agent_token_identity_and_lifecycle() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    is_redemption BOOLEAN;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'agent token credential history cannot be deleted';
    END IF;

    is_redemption := OLD.status = 'pending_registration'
        AND OLD.agent_id IS NULL
        AND OLD.redeemed_at IS NULL
        AND NEW.status = 'active_runtime'
        AND NEW.agent_id IS NOT NULL
        AND NEW.redeemed_at IS NOT NULL
        AND NEW.revoked_at IS NULL
        AND NEW.revocation_kind IS NULL;

    IF ROW(
        NEW.id,
        NEW.creator_user_id,
        NEW.prefix,
        NEW.rotation_predecessor_id,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.creator_user_id,
        OLD.prefix,
        OLD.rotation_predecessor_id,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'agent token creation identity is immutable';
    END IF;

    IF ROW(
        NEW.agent_id,
        NEW.token_hash,
        NEW.scopes,
        NEW.redeemed_at
    ) IS DISTINCT FROM ROW(
        OLD.agent_id,
        OLD.token_hash,
        OLD.scopes,
        OLD.redeemed_at
    ) AND NOT is_redemption THEN
        RAISE EXCEPTION 'redeemed agent token credential identity is immutable';
    END IF;

    IF (OLD.status = 'active_runtime'
        AND NEW.status NOT IN ('active_runtime', 'revoked'))
       OR (OLD.status = 'revoked' AND NEW.status <> 'revoked') THEN
        RAISE EXCEPTION 'agent token lifecycle cannot move backwards';
    END IF;

    IF OLD.status = 'revoked'
       AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
        RAISE EXCEPTION 'revoked agent token is immutable';
    END IF;

    RETURN NEW;
END
$$;


--
-- Name: enforce_attempt_event_sequence_consistency(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_attempt_event_sequence_consistency() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    target_run_id UUID;
    target_attempt_id UUID;
    stored_last_sequence BIGINT;
    observed_last_sequence BIGINT;
    observed_event_count BIGINT;
    stored_result_id UUID;
BEGIN
    IF TG_TABLE_NAME = 'run_attempts' THEN
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.run_id;
            target_attempt_id := OLD.id;
        ELSE
            target_run_id := NEW.run_id;
            target_attempt_id := NEW.id;
        END IF;
    ELSE
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.run_id;
            target_attempt_id := OLD.attempt_id;
        ELSE
            target_run_id := NEW.run_id;
            target_attempt_id := NEW.attempt_id;
        END IF;
    END IF;

    IF target_attempt_id IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT last_client_event_seq, result_id
    INTO stored_last_sequence, stored_result_id
    FROM run_attempts
    WHERE run_id = target_run_id
      AND id = target_attempt_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT COALESCE(MAX(client_event_seq), 0), COUNT(client_event_seq)
    INTO observed_last_sequence, observed_event_count
    FROM run_events
    WHERE run_id = target_run_id
      AND attempt_id = target_attempt_id
      AND client_event_seq IS NOT NULL;

    IF stored_last_sequence <> observed_last_sequence THEN
        RAISE EXCEPTION 'run attempt event sequence summary does not match stored events';
    END IF;

    IF stored_result_id IS NOT NULL
       AND observed_event_count <> stored_last_sequence THEN
        RAISE EXCEPTION 'run attempt Result cannot finalize with missing client events';
    END IF;

    RETURN NULL;
END
$$;


--
-- Name: enforce_run_active_attempt_consistency(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_active_attempt_consistency() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    target_run_id UUID;
    current_run runs%ROWTYPE;
    active_attempt run_attempts%ROWTYPE;
    latest_attempt run_attempts%ROWTYPE;
    attempt_rows INTEGER;
    max_offer_no INTEGER;
    accepted_attempt_rows INTEGER;
    max_attempt_no INTEGER;
    max_fencing_token BIGINT;
    unfinished_attempt_rows INTEGER;
    unfinished_attempt_id UUID;
    cancellation_target_attempt_id UUID;
    cancellation_state TEXT;
BEGIN
    IF TG_TABLE_NAME = 'runs' THEN
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.id;
        ELSE
            target_run_id := NEW.id;
        END IF;
    ELSE
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.run_id;
        ELSE
            target_run_id := NEW.run_id;
        END IF;
    END IF;

    SELECT * INTO current_run
    FROM runs
    WHERE id = target_run_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT
        COUNT(*)::INTEGER,
        COALESCE(MAX(offer_no), 0)::INTEGER,
        COUNT(attempt_no)::INTEGER,
        COALESCE(MAX(attempt_no), 0)::INTEGER,
        COALESCE(MAX(fencing_token), 0),
        COUNT(*) FILTER (WHERE finished_at IS NULL)::INTEGER,
        (MIN(id::TEXT) FILTER (WHERE finished_at IS NULL))::UUID
    INTO
        attempt_rows,
        max_offer_no,
        accepted_attempt_rows,
        max_attempt_no,
        max_fencing_token,
        unfinished_attempt_rows,
        unfinished_attempt_id
    FROM run_attempts
    WHERE run_id = current_run.id;

    IF current_run.offer_count <> attempt_rows
       OR current_run.offer_count <> max_offer_no
       OR current_run.attempt_count <> accepted_attempt_rows
       OR current_run.attempt_count <> max_attempt_no
       OR current_run.fencing_token <> max_fencing_token THEN
        RAISE EXCEPTION 'run offer, attempt, or fence counters do not match attempt history';
    END IF;

    IF attempt_rows = 0 THEN
        IF current_run.latest_attempt_id IS NOT NULL THEN
            RAISE EXCEPTION 'run without attempts cannot keep a latest attempt';
        END IF;
    ELSE
        SELECT * INTO latest_attempt
        FROM run_attempts
        WHERE run_id = current_run.id
          AND offer_no = max_offer_no;

        IF NOT FOUND
           OR current_run.latest_attempt_id IS DISTINCT FROM latest_attempt.id THEN
            RAISE EXCEPTION 'run latest attempt does not match latest offer';
        END IF;

        IF current_run.dispatch_state = 'pending'
           AND (
               latest_attempt.finished_at IS NULL
               OR latest_attempt.outcome NOT IN ('offer_rejected', 'offer_expired')
           ) THEN
            RAISE EXCEPTION 'pending Run latest attempt is not a finished offer';
        END IF;

        IF current_run.dispatch_state = 'retry_wait'
           AND (
               latest_attempt.finished_at IS NULL
               OR latest_attempt.outcome NOT IN (
                   'retryable_failure',
                   'lease_expired',
                   'result_unknown'
               )
           ) THEN
            RAISE EXCEPTION 'retry-wait Run latest attempt is not retryable';
        END IF;
    END IF;

    IF current_run.active_attempt_id IS NULL THEN
        IF unfinished_attempt_rows <> 0 THEN
            SELECT target_attempt_id, state
            INTO cancellation_target_attempt_id, cancellation_state
            FROM run_cancellations
            WHERE run_id = current_run.id;

            IF current_run.status IS DISTINCT FROM 'canceled'
               OR current_run.dispatch_state IS DISTINCT FROM 'terminal'
               OR unfinished_attempt_rows <> 1
               OR cancellation_target_attempt_id IS DISTINCT FROM unfinished_attempt_id
               OR cancellation_state NOT IN ('requested', 'delivered', 'stopping', 'unsupported', 'failed')
               OR latest_attempt.id IS DISTINCT FROM unfinished_attempt_id
               OR latest_attempt.executor_type NOT IN ('runtime', 'core_http', 'core_mcp')
               OR latest_attempt.finished_at IS NOT NULL
               OR latest_attempt.outcome IS NOT NULL THEN
                RAISE EXCEPTION 'unfinished attempt must be the Run active attempt or unsettled cancellation target';
            END IF;
        END IF;
        RETURN NULL;
    END IF;

    IF unfinished_attempt_rows <> 1
       OR unfinished_attempt_id IS DISTINCT FROM current_run.active_attempt_id THEN
        RAISE EXCEPTION 'Run active attempt must be its only unfinished attempt';
    END IF;

    SELECT * INTO active_attempt
    FROM run_attempts
    WHERE run_id = current_run.id
      AND id = current_run.active_attempt_id;

    IF NOT FOUND
       OR active_attempt.finished_at IS NOT NULL
       OR active_attempt.outcome IS NOT NULL THEN
        RAISE EXCEPTION 'active run attempt does not exist';
    END IF;

    IF current_run.latest_attempt_id IS DISTINCT FROM active_attempt.id
       OR current_run.agent_id IS DISTINCT FROM active_attempt.agent_id
       OR current_run.lease_id IS DISTINCT FROM active_attempt.lease_id
       OR current_run.fencing_token IS DISTINCT FROM active_attempt.fencing_token
       OR current_run.executor_type IS DISTINCT FROM active_attempt.executor_type
       OR current_run.active_core_instance_id IS DISTINCT FROM active_attempt.attached_core_instance_id
       OR current_run.runtime_node_id IS DISTINCT FROM active_attempt.node_id
       OR current_run.runtime_worker_id IS DISTINCT FROM active_attempt.runtime_worker_id
       OR current_run.runtime_session_id IS DISTINCT FROM active_attempt.runtime_session_id
       OR current_run.lease_token_id IS DISTINCT FROM active_attempt.runtime_token_id
       OR current_run.offer_count IS DISTINCT FROM active_attempt.offer_no
       OR current_run.lease_offered_at IS DISTINCT FROM active_attempt.offered_at
       OR current_run.lease_accepted_at IS DISTINCT FROM active_attempt.accepted_at
       OR current_run.lease_expires_at IS DISTINCT FROM active_attempt.lease_expires_at
       OR current_run.attempt_deadline_at IS DISTINCT FROM active_attempt.attempt_deadline_at THEN
        RAISE EXCEPTION 'run active lease summary does not match active attempt';
    END IF;

    IF current_run.dispatch_state = 'offered'
       AND (
           active_attempt.accepted_at IS NOT NULL
           OR active_attempt.attempt_no IS NOT NULL
       ) THEN
        RAISE EXCEPTION 'offered run points to accepted attempt';
    END IF;

    IF current_run.dispatch_state = 'executing'
       AND (
           active_attempt.accepted_at IS NULL
           OR active_attempt.attempt_no IS DISTINCT FROM current_run.attempt_count
       ) THEN
        RAISE EXCEPTION 'executing run points to unaccepted attempt';
    END IF;

    RETURN NULL;
END
$$;


--
-- Name: enforce_run_attempt_identity_immutable(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_attempt_identity_immutable() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'run attempts are immutable history and cannot be deleted';
    END IF;

    IF ROW(
        OLD.id,
        OLD.run_id,
        OLD.agent_id,
        OLD.offer_no,
        OLD.executor_type,
        OLD.lease_id,
        OLD.fencing_token,
        OLD.runtime_token_id,
        OLD.runtime_worker_id,
        OLD.runtime_session_id,
        OLD.node_id,
        OLD.offered_by_core_instance_id,
        OLD.offered_at,
        OLD.offer_expires_at,
        OLD.attempt_deadline_at,
        OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.id,
        NEW.run_id,
        NEW.agent_id,
        NEW.offer_no,
        NEW.executor_type,
        NEW.lease_id,
        NEW.fencing_token,
        NEW.runtime_token_id,
        NEW.runtime_worker_id,
        NEW.runtime_session_id,
        NEW.node_id,
        NEW.offered_by_core_instance_id,
        NEW.offered_at,
        NEW.offer_expires_at,
        NEW.attempt_deadline_at,
        NEW.created_at
    ) THEN
        RAISE EXCEPTION 'run attempt immutable identity cannot change';
    END IF;

    IF OLD.attempt_no IS NOT NULL
       AND NEW.attempt_no IS DISTINCT FROM OLD.attempt_no THEN
        RAISE EXCEPTION 'run attempt number cannot change after acceptance';
    END IF;

    IF OLD.accepted_at IS NOT NULL
       AND NEW.accepted_at IS DISTINCT FROM OLD.accepted_at THEN
        RAISE EXCEPTION 'run attempt acceptance evidence is immutable';
    END IF;

    IF NEW.last_client_event_seq < OLD.last_client_event_seq THEN
        RAISE EXCEPTION 'run attempt event sequence cannot move backwards';
    END IF;

    IF OLD.final_client_event_seq IS NOT NULL
       AND NEW.final_client_event_seq IS DISTINCT FROM OLD.final_client_event_seq THEN
        RAISE EXCEPTION 'run attempt final event sequence is immutable';
    END IF;

    IF NEW.lease_expires_at < OLD.lease_expires_at THEN
        RAISE EXCEPTION 'run attempt lease expiry cannot move backwards';
    END IF;

    IF OLD.last_renewed_at IS NOT NULL
       AND (
           NEW.last_renewed_at IS NULL
           OR NEW.last_renewed_at < OLD.last_renewed_at
       ) THEN
        RAISE EXCEPTION 'run attempt renewal evidence cannot move backwards';
    END IF;

    IF OLD.result_id IS NOT NULL
       AND ROW(
           NEW.result_id,
           NEW.result_fingerprint,
           NEW.result_classification
       ) IS DISTINCT FROM ROW(
           OLD.result_id,
           OLD.result_fingerprint,
           OLD.result_classification
       ) THEN
        RAISE EXCEPTION 'run attempt result identity is immutable';
    END IF;

    IF OLD.finished_at IS NOT NULL
       AND ROW(
           NEW.finished_at,
           NEW.outcome,
           NEW.error_code,
           NEW.error_detail_redacted,
           NEW.attached_core_instance_id,
           NEW.lease_expires_at,
           NEW.last_renewed_at,
           NEW.last_client_event_seq,
           NEW.final_client_event_seq,
           NEW.result_id,
           NEW.result_fingerprint,
           NEW.result_classification
       ) IS DISTINCT FROM ROW(
           OLD.finished_at,
           OLD.outcome,
           OLD.error_code,
           OLD.error_detail_redacted,
           OLD.attached_core_instance_id,
           OLD.lease_expires_at,
           OLD.last_renewed_at,
           OLD.last_client_event_seq,
           OLD.final_client_event_seq,
           OLD.result_id,
           OLD.result_fingerprint,
           OLD.result_classification
       ) THEN
        RAISE EXCEPTION 'run attempt terminal evidence is immutable';
    END IF;

    IF OLD.result_acknowledged_at IS NOT NULL
       AND NEW.result_acknowledged_at IS DISTINCT FROM OLD.result_acknowledged_at THEN
        RAISE EXCEPTION 'run attempt result acknowledgement is immutable';
    END IF;

    RETURN NEW;
END
$$;


--
-- Name: enforce_run_attempt_runtime_attachment_evidence(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_attempt_runtime_attachment_evidence() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF OLD.runtime_attachment_id IS NOT NULL
           AND NEW.runtime_attachment_id IS DISTINCT FROM OLD.runtime_attachment_id THEN
            RAISE EXCEPTION 'run attempt Runtime Attachment evidence is immutable';
        END IF;
        IF OLD.accepted_at IS NOT NULL
           AND NEW.runtime_attachment_id IS DISTINCT FROM OLD.runtime_attachment_id THEN
            RAISE EXCEPTION 'accepted run attempt Runtime Attachment evidence cannot change';
        END IF;
    END IF;

    IF NEW.runtime_attachment_id IS NOT NULL
       AND (
           NEW.executor_type <> 'runtime'
           OR NEW.accepted_at IS NULL
           OR NEW.runtime_session_id IS NULL
       ) THEN
        RAISE EXCEPTION 'run attempt Runtime Attachment evidence requires an accepted Runtime Attempt';
    END IF;

    IF NEW.executor_type = 'runtime'
       AND NEW.accepted_at IS NOT NULL
       AND NEW.runtime_attachment_id IS NULL THEN
        IF TG_OP = 'INSERT' THEN
            RAISE EXCEPTION 'new Runtime acceptance requires Runtime Attachment evidence';
        ELSIF OLD.accepted_at IS NULL THEN
            RAISE EXCEPTION 'new Runtime acceptance requires Runtime Attachment evidence';
        END IF;
    END IF;

    RETURN NEW;
END
$$;


--
-- Name: enforce_run_attempt_slot_evidence(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_attempt_slot_evidence() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    owner_session runtime_sessions%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW.slot_acquired_at IS DISTINCT FROM OLD.slot_acquired_at THEN
            RAISE EXCEPTION 'run attempt slot acquisition evidence is immutable';
        END IF;

        IF OLD.slot_released_at IS NOT NULL
           AND ROW(
               NEW.slot_released_at,
               NEW.active_runtime_session_id
           ) IS DISTINCT FROM ROW(
               OLD.slot_released_at,
               OLD.active_runtime_session_id
           ) THEN
            RAISE EXCEPTION 'run attempt slot release evidence is immutable';
        END IF;

        IF OLD.active_runtime_session_id IS NULL
           AND NEW.active_runtime_session_id IS NOT NULL THEN
            RAISE EXCEPTION 'released run attempt slot cannot be reacquired';
        END IF;

        IF OLD.active_runtime_session_id IS NOT NULL
           AND NEW.active_runtime_session_id IS NULL
           AND NEW.slot_released_at IS NULL THEN
            RAISE EXCEPTION 'run attempt slot owner cannot clear without release evidence';
        END IF;
    END IF;

    IF NEW.executor_type = 'runtime'
       AND NEW.active_runtime_session_id IS NOT NULL THEN
        SELECT * INTO owner_session
        FROM runtime_sessions
        WHERE runtime_session_id = NEW.active_runtime_session_id;

        IF NOT FOUND
           OR owner_session.node_id IS DISTINCT FROM NEW.node_id
           OR owner_session.agent_id IS DISTINCT FROM NEW.agent_id
           OR owner_session.worker_id IS DISTINCT FROM NEW.runtime_worker_id THEN
            RAISE EXCEPTION 'run attempt active slot owner identity mismatch';
        END IF;
    END IF;

    RETURN NEW;
END
$$;


--
-- Name: enforce_run_attempt_slot_release_on_finish(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_attempt_slot_release_on_finish() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    current_attempt run_attempts%ROWTYPE;
BEGIN
    SELECT * INTO current_attempt
    FROM run_attempts
    WHERE id = NEW.id;

    IF NOT FOUND OR current_attempt.executor_type <> 'runtime' THEN
        RETURN NULL;
    END IF;

    IF (current_attempt.finished_at IS NULL)
       IS DISTINCT FROM (current_attempt.slot_released_at IS NULL) THEN
        RAISE EXCEPTION 'agent Node Attempt finish and slot release must commit together';
    END IF;

    RETURN NULL;
END
$$;


--
-- Name: enforce_run_cancellation_summary_consistency(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_cancellation_summary_consistency() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    target_run_id UUID;
    current_run runs%ROWTYPE;
    cancellation_record run_cancellations%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME = 'runs' THEN
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.id;
        ELSE
            target_run_id := NEW.id;
        END IF;
    ELSE
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.run_id;
        ELSE
            target_run_id := NEW.run_id;
        END IF;
    END IF;

    SELECT * INTO current_run
    FROM runs
    WHERE id = target_run_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT * INTO cancellation_record
    FROM run_cancellations
    WHERE run_id = current_run.id;

    IF current_run.cancel_request_id IS NULL THEN
        IF FOUND THEN
            RAISE EXCEPTION 'Run cancellation row is not reflected in its summary';
        END IF;
    ELSE
        IF NOT FOUND
           OR cancellation_record.id IS DISTINCT FROM current_run.cancel_request_id
           OR cancellation_record.state IS DISTINCT FROM current_run.cancel_state
           OR cancellation_record.requested_at IS DISTINCT FROM current_run.cancel_requested_at
           OR cancellation_record.acknowledged_at IS DISTINCT FROM current_run.cancel_acknowledged_at
           OR cancellation_record.reason IS DISTINCT FROM current_run.cancel_reason THEN
            RAISE EXCEPTION 'Run cancellation summary does not match cancellation evidence';
        END IF;
    END IF;

    IF current_run.runtime_contract_id = 'openlinker.runtime.v2' THEN
        IF current_run.status = 'canceled' THEN
            IF current_run.dispatch_state <> 'terminal'
               OR current_run.cancel_request_id IS NULL THEN
                RAISE EXCEPTION 'canceled Run requires cancellation request evidence';
            END IF;
        ELSIF current_run.cancel_request_id IS NOT NULL THEN
            RAISE EXCEPTION 'runtime v2 cancellation must atomically finalize the Run as canceled';
        END IF;
    END IF;

    RETURN NULL;
END
$$;


--
-- Name: enforce_run_cancellation_transition(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_cancellation_transition() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'run cancellation evidence cannot be deleted';
    END IF;

    IF ROW(
        NEW.id,
        NEW.run_id,
        NEW.target_attempt_id,
        NEW.requested_by_type,
        NEW.requested_by_id,
        NEW.reason,
        NEW.requested_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.run_id,
        OLD.target_attempt_id,
        OLD.requested_by_type,
        OLD.requested_by_id,
        OLD.reason,
        OLD.requested_at
    ) THEN
        RAISE EXCEPTION 'cancellation request identity is immutable';
    END IF;

    IF OLD.state IN ('stopped', 'unsupported', 'failed')
       AND NEW.state <> OLD.state THEN
        RAISE EXCEPTION 'terminal cancellation state cannot change';
    END IF;

    IF NEW.state <> OLD.state
       AND NOT (
           (OLD.state = 'requested' AND NEW.state IN (
               'delivered', 'stopping', 'stopped', 'unsupported', 'failed', 'unconfirmed'
           ))
           OR (OLD.state = 'delivered' AND NEW.state IN (
               'stopping', 'stopped', 'unsupported', 'failed', 'unconfirmed'
           ))
           OR (OLD.state = 'stopping' AND NEW.state IN (
               'stopped', 'failed', 'unconfirmed'
           ))
           OR (OLD.state = 'unconfirmed' AND NEW.state IN (
               'stopped', 'unsupported', 'failed'
           ))
       ) THEN
        RAISE EXCEPTION 'invalid cancellation state transition';
    END IF;

    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'cancellation updated_at cannot move backwards';
    END IF;

    IF (OLD.delivered_at IS NOT NULL
        AND NEW.delivered_at IS DISTINCT FROM OLD.delivered_at)
       OR (OLD.stopping_at IS NOT NULL
           AND NEW.stopping_at IS DISTINCT FROM OLD.stopping_at)
       OR (OLD.stopped_at IS NOT NULL
           AND NEW.stopped_at IS DISTINCT FROM OLD.stopped_at)
       OR (OLD.acknowledged_at IS NOT NULL
           AND NEW.acknowledged_at IS DISTINCT FROM OLD.acknowledged_at) THEN
        RAISE EXCEPTION 'cancellation evidence timestamps are immutable once recorded';
    END IF;

    IF OLD.state IN ('stopped', 'unsupported', 'failed')
       AND (
           to_jsonb(NEW) - 'updated_at'
       ) IS DISTINCT FROM (
           to_jsonb(OLD) - 'updated_at'
       ) THEN
        RAISE EXCEPTION 'terminal cancellation evidence is immutable';
    END IF;

    RETURN NEW;
END
$$;


--
-- Name: enforce_run_effect_identity_immutable(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_effect_identity_immutable() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Run effect outbox history cannot be deleted';
    END IF;

    IF ROW(
        NEW.id,
        NEW.run_id,
        NEW.terminal_event_id,
        NEW.effect_type,
        NEW.target_key,
        NEW.metadata,
        NEW.max_attempts,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.run_id,
        OLD.terminal_event_id,
        OLD.effect_type,
        OLD.target_key,
        OLD.metadata,
        OLD.max_attempts,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'Run effect delivery identity is immutable';
    END IF;

    RETURN NEW;
END
$$;


--
-- Name: enforce_run_effect_replay_immutable(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_effect_replay_immutable() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'Run effect replay audit is immutable';
END
$$;


--
-- Name: enforce_run_event_immutable(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_event_immutable() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'run events are append-only';
    END IF;
    RETURN NEW;
END
$$;


--
-- Name: enforce_run_event_retention_watermark(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_event_retention_watermark() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    latest_sequence INTEGER;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'run event retention watermark evidence cannot be deleted';
    END IF;

    IF TG_OP = 'UPDATE' AND NEW.run_id IS DISTINCT FROM OLD.run_id THEN
        RAISE EXCEPTION 'run event retention watermark identity cannot change';
    END IF;

    IF TG_OP = 'UPDATE'
       AND NEW.retained_through_sequence < OLD.retained_through_sequence THEN
        RAISE EXCEPTION 'run event retention watermark cannot move backwards';
    END IF;

    -- Every Event/retention writer takes locks in Run row -> advisory order.
    -- Taking the row lock first also prevents the INSERT's FK check from
    -- creating the inverse advisory -> Run dependency.
    PERFORM 1
    FROM runs
    WHERE id = NEW.run_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'run event retention watermark references a missing Run';
    END IF;

    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.run_id::text, 0));

    SELECT COALESCE(MAX(sequence), 0)::INTEGER
    INTO latest_sequence
    FROM run_events
    WHERE run_id = NEW.run_id;

    IF NEW.retained_through_sequence > latest_sequence THEN
        RAISE EXCEPTION
            'run event retention watermark cannot exceed latest event sequence';
    END IF;

    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END
$$;


--
-- Name: enforce_run_terminal_artifact_immutable(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_terminal_artifact_immutable() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' OR NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal Run artifact is immutable';
    END IF;
    RETURN NEW;
END
$$;


--
-- Name: enforce_run_terminal_artifacts_consistency(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_terminal_artifacts_consistency() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    target_run_id UUID;
    current_run runs%ROWTYPE;
    ledger_record run_accounting_ledger%ROWTYPE;
    dead_letter_record run_dead_letters%ROWTYPE;
    latest_attempt run_attempts%ROWTYPE;
    terminal_event run_events%ROWTYPE;
    terminal_event_count INTEGER;
    cancellation_target_attempt_id UUID;
    cancellation_state TEXT;
    cancellation_requested_at TIMESTAMPTZ;
BEGIN
    IF TG_TABLE_NAME = 'runs' THEN
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.id;
        ELSE
            target_run_id := NEW.id;
        END IF;
    ELSE
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.run_id;
        ELSE
            target_run_id := NEW.run_id;
        END IF;
    END IF;

    SELECT * INTO current_run
    FROM runs
    WHERE id = target_run_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT * INTO ledger_record
    FROM run_accounting_ledger
    WHERE run_id = current_run.id;

    IF current_run.dispatch_state IN ('terminal', 'dead_letter') THEN
        IF NOT FOUND
           OR ledger_record.terminal_event_id IS DISTINCT FROM current_run.terminal_event_id
           OR ledger_record.agent_id IS DISTINCT FROM current_run.agent_id
           OR ledger_record.success_delta IS DISTINCT FROM (
                CASE WHEN current_run.status = 'success' THEN 1 ELSE 0 END
           )
           OR ledger_record.revenue_delta_cents IS DISTINCT FROM (
                CASE
                    WHEN current_run.status = 'success'
                        THEN current_run.creator_revenue_cents::BIGINT
                    ELSE 0::BIGINT
                END
           ) THEN
            RAISE EXCEPTION 'terminal Run accounting ledger is missing or inconsistent';
        END IF;
    ELSIF FOUND THEN
        RAISE EXCEPTION 'nonterminal Run cannot have an accounting ledger';
    END IF;

    IF current_run.runtime_contract_id = 'openlinker.runtime.v2'
       THEN
        SELECT COUNT(*)::INTEGER
        INTO terminal_event_count
        FROM run_events
        WHERE run_id = current_run.id
          AND (
              event_type IN ('run.completed', 'run.failed', 'run.canceled')
              OR payload->>'terminal' = 'true'
          );

        IF current_run.dispatch_state IN ('terminal', 'dead_letter')
           AND terminal_event_count <> 1 THEN
            RAISE EXCEPTION 'terminal Run must have exactly one terminal event';
        ELSIF current_run.dispatch_state NOT IN ('terminal', 'dead_letter')
              AND terminal_event_count <> 0 THEN
            RAISE EXCEPTION 'nonterminal Run cannot have a terminal event';
        END IF;
    END IF;

    IF current_run.runtime_contract_id = 'openlinker.runtime.v2'
       AND current_run.dispatch_state IN ('terminal', 'dead_letter') THEN
        SELECT * INTO terminal_event
        FROM run_events
        WHERE run_id = current_run.id
          AND id = current_run.terminal_event_id;

        IF NOT FOUND
           OR terminal_event.payload->>'terminal' IS DISTINCT FROM 'true'
           OR terminal_event.payload->>'status' IS DISTINCT FROM current_run.status
           OR terminal_event.event_type IS DISTINCT FROM (
                CASE current_run.status
                    WHEN 'success' THEN 'run.completed'
                    WHEN 'canceled' THEN 'run.canceled'
                    ELSE 'run.failed'
                END
           ) THEN
            RAISE EXCEPTION 'terminal Run event semantics are inconsistent';
        END IF;
    END IF;

    SELECT * INTO dead_letter_record
    FROM run_dead_letters
    WHERE run_id = current_run.id;

    IF current_run.dispatch_state = 'dead_letter' THEN
        IF NOT FOUND
           OR dead_letter_record.final_attempt_no IS DISTINCT FROM current_run.attempt_count THEN
            RAISE EXCEPTION 'dead-letter Run is missing matching DLQ evidence';
        END IF;

        SELECT * INTO latest_attempt
        FROM run_attempts
        WHERE run_id = current_run.id
          AND id = current_run.latest_attempt_id;

        IF NOT FOUND
           OR latest_attempt.attempt_no IS DISTINCT FROM dead_letter_record.final_attempt_no
           OR latest_attempt.finished_at IS NULL
           OR latest_attempt.outcome NOT IN (
               'retryable_failure',
               'lease_expired',
               'result_unknown'
           ) THEN
            RAISE EXCEPTION 'dead-letter Run final attempt evidence is inconsistent';
        END IF;
    ELSIF FOUND THEN
        RAISE EXCEPTION 'non-DLQ Run cannot have dead-letter evidence';
    END IF;

    IF current_run.result_id IS NOT NULL THEN
        SELECT * INTO latest_attempt
        FROM run_attempts
        WHERE run_id = current_run.id
          AND id = current_run.latest_attempt_id;

        IF NOT FOUND
           OR latest_attempt.result_id IS DISTINCT FROM current_run.result_id
           OR latest_attempt.result_fingerprint IS DISTINCT FROM current_run.result_fingerprint THEN
            RAISE EXCEPTION 'Run result summary does not match its final attempt';
        END IF;
    END IF;

    IF current_run.runtime_contract_id = 'openlinker.runtime.v2'
       AND current_run.status IN ('success', 'failed')
       AND current_run.dispatch_state = 'terminal' THEN
        SELECT * INTO latest_attempt
        FROM run_attempts
        WHERE run_id = current_run.id
          AND id = current_run.latest_attempt_id;

        IF NOT FOUND
           OR latest_attempt.finished_at IS NULL
           OR latest_attempt.outcome IS DISTINCT FROM (
               CASE current_run.status
                   WHEN 'success' THEN 'success'
                   ELSE 'non_retryable_failure'
               END
           )
           OR latest_attempt.result_classification IS DISTINCT FROM (
               CASE current_run.status
                   WHEN 'success' THEN 'success'
                   ELSE 'non_retryable_failure'
               END
           )
           OR latest_attempt.result_id IS DISTINCT FROM current_run.result_id
           OR latest_attempt.result_fingerprint IS DISTINCT FROM current_run.result_fingerprint THEN
            RAISE EXCEPTION 'terminal Run status does not match its final attempt Result';
        END IF;
    END IF;

    IF current_run.runtime_contract_id = 'openlinker.runtime.v2'
       AND current_run.status IN ('timeout', 'canceled') THEN
        IF current_run.status = 'canceled' THEN
            SELECT target_attempt_id, state, requested_at
            INTO cancellation_target_attempt_id, cancellation_state, cancellation_requested_at
            FROM run_cancellations
            WHERE run_id = current_run.id;
        END IF;

        IF current_run.result_id IS NOT NULL
           OR current_run.result_fingerprint IS NOT NULL
           OR current_run.output IS NOT NULL THEN
            RAISE EXCEPTION 'timeout or canceled Run cannot publish a Result';
        END IF;

        IF current_run.latest_attempt_id IS NOT NULL THEN
            SELECT * INTO latest_attempt
            FROM run_attempts
            WHERE run_id = current_run.id
              AND id = current_run.latest_attempt_id;

            IF NOT FOUND
               OR (
                   current_run.status = 'timeout'
                   AND (
                       latest_attempt.finished_at IS NULL
                       OR latest_attempt.outcome NOT IN (
                           'offer_rejected',
                           'offer_expired',
                           'retryable_failure',
                           'lease_expired',
                           'timeout',
                           'result_unknown'
                       )
                   )
               )
               OR (
                   current_run.status = 'canceled'
                   AND cancellation_target_attempt_id IS NULL
                   AND (
                       latest_attempt.finished_at IS NULL
                       OR latest_attempt.outcome NOT IN (
                           'offer_rejected',
                           'offer_expired',
                           'retryable_failure',
                           'lease_expired',
                           'result_unknown'
                       )
                   )
               )
               OR (
                   current_run.status = 'canceled'
                   AND cancellation_target_attempt_id IS NOT NULL
                   AND (
                       current_run.latest_attempt_id IS DISTINCT FROM cancellation_target_attempt_id
                       OR latest_attempt.id IS DISTINCT FROM cancellation_target_attempt_id
                       OR (
                           cancellation_state IN ('requested', 'delivered', 'stopping')
                           AND (
                               latest_attempt.executor_type NOT IN ('runtime', 'core_http', 'core_mcp')
                               OR latest_attempt.finished_at IS NOT NULL
                               OR latest_attempt.outcome IS NOT NULL
                           )
                       )
                       OR (
                           cancellation_state IN ('unsupported', 'failed')
                           AND (
                               latest_attempt.executor_type IS DISTINCT FROM 'runtime'
                               OR (
                                   latest_attempt.finished_at IS NULL
                                   AND latest_attempt.outcome IS NOT NULL
                               )
                               OR (
                                   latest_attempt.finished_at IS NOT NULL
                                   AND (
                                       latest_attempt.outcome IS DISTINCT FROM 'canceled'
                                       OR latest_attempt.error_code IS DISTINCT FROM 'CANCEL_UNCONFIRMED'
                                       OR cancellation_requested_at IS NULL
                                       OR latest_attempt.finished_at
                                           < cancellation_requested_at + INTERVAL '30 seconds'
                                   )
                               )
                           )
                       )
                       OR (
                           cancellation_state IN ('stopped', 'unconfirmed')
                           AND (
                               latest_attempt.finished_at IS NULL
                               OR latest_attempt.outcome IS DISTINCT FROM 'canceled'
                           )
                       )
                       OR cancellation_state NOT IN (
                           'requested', 'delivered', 'stopping', 'unsupported', 'failed',
                           'stopped', 'unconfirmed'
                       )
                   )
               ) THEN
                RAISE EXCEPTION 'timeout or canceled Run contradicts its latest Attempt or cancellation lifecycle';
            END IF;
        END IF;
    END IF;

    IF current_run.replay_of_run_id IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM runs original
           JOIN run_dead_letters dlq ON dlq.run_id = original.id
           WHERE original.id = current_run.replay_of_run_id
             AND original.dispatch_state = 'dead_letter'
       ) THEN
        RAISE EXCEPTION 'Run replay must reference a real dead-letter Run';
    END IF;

    IF current_run.dispatch_state <> 'dead_letter'
       AND EXISTS (
           SELECT 1
           FROM runs replay
           WHERE replay.replay_of_run_id = current_run.id
       ) THEN
        RAISE EXCEPTION 'a replay source must remain a dead-letter Run';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM run_effect_outbox effect
        WHERE effect.run_id = current_run.id
          AND (
              current_run.dispatch_state NOT IN ('terminal', 'dead_letter')
              OR effect.terminal_event_id IS DISTINCT FROM current_run.terminal_event_id
          )
    ) THEN
        RAISE EXCEPTION 'Run effect outbox does not match the Run terminal event';
    END IF;

    RETURN NULL;
END
$$;


--
-- Name: enforce_run_v2_contract_identity(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_run_v2_contract_identity() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.runtime_contract_id = 'openlinker.runtime.v2' THEN
            RAISE EXCEPTION 'runtime v2 runs cannot be deleted';
        END IF;
        RETURN OLD;
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.runtime_contract_id <> 'openlinker.runtime.v2'
           OR NEW.idempotency_key_hash IS NULL
           OR octet_length(NEW.idempotency_key_hash) <> 32
           OR NEW.idempotency_fingerprint IS NULL
           OR octet_length(NEW.idempotency_fingerprint) <> 32
           OR NEW.connection_mode_snapshot IS NULL
           OR NEW.dispatch_deadline_at IS NULL
           OR NEW.run_deadline_at IS NULL
           OR NEW.dispatch_deadline_at <= clock_timestamp()
           OR NEW.run_deadline_at <= NEW.dispatch_deadline_at
           OR (
               NEW.connection_mode_snapshot IN ('direct_http', 'mcp_server')
               AND NEW.endpoint_idempotency_snapshot IS NULL
           )
           OR NEW.status <> 'running'
           OR NEW.dispatch_state <> 'pending'
           OR NEW.output IS NOT NULL
           OR NEW.error_code IS NOT NULL
           OR NEW.error_message IS NOT NULL
           OR NEW.duration_ms IS NOT NULL
           OR NEW.finished_at IS NOT NULL
           OR NEW.offer_count <> 0
           OR NEW.attempt_count <> 0
           OR NEW.fencing_token <> 0
           OR NEW.latest_attempt_id IS NOT NULL
           OR NEW.active_attempt_id IS NOT NULL
           OR NEW.lease_id IS NOT NULL
           OR NEW.executor_type IS NOT NULL
           OR NEW.cancel_request_id IS NOT NULL
           OR NEW.result_id IS NOT NULL
           OR NEW.terminal_event_id IS NOT NULL
           OR NEW.dead_lettered_at IS NOT NULL THEN
            RAISE EXCEPTION 'runtime v2 run insert requires complete v2 creation identity';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.runtime_contract_id IS DISTINCT FROM OLD.runtime_contract_id THEN
        RAISE EXCEPTION 'run runtime contract is immutable';
    END IF;

    IF ROW(
        NEW.id,
        NEW.user_id,
        NEW.agent_id,
        NEW.input,
        NEW.cost_cents,
        NEW.platform_fee_cents,
        NEW.creator_revenue_cents,
        NEW.source,
        NEW.started_at,
        NEW.idempotency_key_hash,
        NEW.idempotency_fingerprint,
        NEW.request_metadata,
        NEW.connection_mode_snapshot,
        NEW.endpoint_idempotency_snapshot,
        NEW.max_offer_count,
        NEW.max_attempts,
        NEW.dispatch_deadline_at,
        NEW.run_deadline_at,
        NEW.replay_of_run_id
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.user_id,
        OLD.agent_id,
        OLD.input,
        OLD.cost_cents,
        OLD.platform_fee_cents,
        OLD.creator_revenue_cents,
        OLD.source,
        OLD.started_at,
        OLD.idempotency_key_hash,
        OLD.idempotency_fingerprint,
        OLD.request_metadata,
        OLD.connection_mode_snapshot,
        OLD.endpoint_idempotency_snapshot,
        OLD.max_offer_count,
        OLD.max_attempts,
        OLD.dispatch_deadline_at,
        OLD.run_deadline_at,
        OLD.replay_of_run_id
    ) THEN
        RAISE EXCEPTION 'run creation identity is immutable';
    END IF;

    IF OLD.cancel_request_id IS NOT NULL
       AND ROW(
           NEW.cancel_request_id,
           NEW.cancel_requested_at,
           NEW.cancel_reason
       ) IS DISTINCT FROM ROW(
           OLD.cancel_request_id,
           OLD.cancel_requested_at,
           OLD.cancel_reason
       ) THEN
        RAISE EXCEPTION 'run cancellation request identity is immutable';
    END IF;

    IF OLD.dispatch_state IN ('terminal', 'dead_letter')
       AND OLD.cancel_request_id IS NULL
       AND ROW(
           NEW.cancel_request_id,
           NEW.cancel_state,
           NEW.cancel_requested_at,
           NEW.cancel_acknowledged_at,
           NEW.cancel_reason
       ) IS DISTINCT FROM ROW(
           OLD.cancel_request_id,
           OLD.cancel_state,
           OLD.cancel_requested_at,
           OLD.cancel_acknowledged_at,
           OLD.cancel_reason
       ) THEN
        RAISE EXCEPTION 'terminal run cannot acquire a cancellation request';
    END IF;

    IF OLD.dispatch_state IN ('terminal', 'dead_letter')
       AND (
           to_jsonb(NEW) - ARRAY[
               'cancel_request_id',
               'cancel_state',
               'cancel_requested_at',
               'cancel_acknowledged_at',
               'cancel_reason'
           ]::TEXT[]
       ) IS DISTINCT FROM (
           to_jsonb(OLD) - ARRAY[
               'cancel_request_id',
               'cancel_state',
               'cancel_requested_at',
               'cancel_acknowledged_at',
               'cancel_reason'
           ]::TEXT[]
       ) THEN
        RAISE EXCEPTION 'terminal run facts are immutable';
    END IF;

    RETURN NEW;
END
$$;


--
-- Name: enforce_runtime_node_identity_and_lifecycle(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_runtime_node_identity_and_lifecycle() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    is_guarded_activation BOOLEAN;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'runtime node enrollment history cannot be deleted';
    END IF;

    IF ROW(
        NEW.node_id,
        NEW.device_certificate_serial,
        NEW.device_public_key_thumbprint,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.node_id,
        OLD.device_certificate_serial,
        OLD.device_public_key_thumbprint,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'runtime node device identity is immutable';
    END IF;

    -- A wire-generation switch is legal only while the Node has no live
    -- Sessions, and the target generation must use exactly its Server-owned
    -- feature set. Steady-state generations may advertise extensions, but
    -- extensions cannot be silently carried across an adapter boundary.
    IF OLD.runtime_contract_digest IS DISTINCT FROM NEW.runtime_contract_digest THEN
        IF EXISTS (
            SELECT 1 FROM runtime_sessions
            WHERE node_id = OLD.node_id
              AND (
                  status IN ('active', 'draining')
                  OR (
                      status = 'offline'
                      AND runtime_contract_digest <> NEW.runtime_contract_digest
                  )
              )
        ) THEN
            RAISE EXCEPTION 'runtime node generation cannot change with live or resumable sessions';
        END IF;

        IF NEW.runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481' THEN
            IF NOT (
                NEW.features @> ARRAY[
                    'lease_fence', 'assignment_confirm', 'renew', 'resume',
                    'event_ack', 'result_ack', 'cancel', 'persistent_spool',
                    'session_drain'
                ]::TEXT[]
                AND ARRAY[
                    'lease_fence', 'assignment_confirm', 'renew', 'resume',
                    'event_ack', 'result_ack', 'cancel', 'persistent_spool',
                    'session_drain'
                ]::TEXT[] @> NEW.features
            ) THEN
                RAISE EXCEPTION 'runtime node target generation features must be exact';
            END IF;
        ELSIF NEW.runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9' THEN
            IF NOT (
                NEW.features @> ARRAY[
                    'lease_fence', 'assignment_confirm', 'renew', 'resume',
                    'event_ack', 'result_ack', 'cancel', 'persistent_spool'
                ]::TEXT[]
                AND ARRAY[
                    'lease_fence', 'assignment_confirm', 'renew', 'resume',
                    'event_ack', 'result_ack', 'cancel', 'persistent_spool'
                ]::TEXT[] @> NEW.features
            ) THEN
                RAISE EXCEPTION 'runtime node target generation features must be exact';
            END IF;
        ELSE
            RAISE EXCEPTION 'runtime node target generation is unsupported';
        END IF;
    END IF;

    is_guarded_activation := OLD.status = 'draining'
        AND NEW.status = 'active'
        AND NEW.draining_at IS NULL
        AND NEW.revoked_at IS NULL
        AND current_setting('openlinker.runtime_node_activation', TRUE)
            IS NOT DISTINCT FROM OLD.node_id::TEXT;

    IF (OLD.status = 'draining' AND NEW.status NOT IN ('draining', 'revoked') AND NOT is_guarded_activation)
       OR (OLD.status = 'revoked' AND NEW.status <> 'revoked') THEN
        RAISE EXCEPTION 'runtime node lifecycle cannot move backwards';
    END IF;

    IF OLD.draining_at IS NOT NULL
       AND NEW.draining_at IS DISTINCT FROM OLD.draining_at
       AND NOT is_guarded_activation THEN
        RAISE EXCEPTION 'runtime node draining evidence is immutable';
    END IF;

    IF OLD.status = 'revoked'
       AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
        RAISE EXCEPTION 'revoked runtime node is immutable';
    END IF;

    RETURN NEW;
END
$$;


--
-- Name: enforce_runtime_node_revocation_guard(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_runtime_node_revocation_guard() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.status = 'revoked'
       AND (
           OLD.status IS DISTINCT FROM NEW.status
           OR OLD.revoked_at IS DISTINCT FROM NEW.revoked_at
       )
       AND EXISTS (
           SELECT 1
           FROM runtime_sessions
           WHERE node_id = NEW.node_id
             AND status IN ('active', 'draining')
       ) THEN
        RAISE EXCEPTION 'runtime node sessions must close before node revocation';
    END IF;
    RETURN NEW;
END
$$;


--
-- Name: enforce_runtime_node_session_contract_consistency(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_runtime_node_session_contract_consistency() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    target_node_id UUID;
    node_record runtime_nodes%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME = 'runtime_nodes' THEN
        IF TG_OP = 'DELETE' THEN
            target_node_id := OLD.node_id;
        ELSE
            target_node_id := NEW.node_id;
        END IF;
    ELSE
        IF TG_OP = 'DELETE' THEN
            target_node_id := OLD.node_id;
        ELSE
            target_node_id := NEW.node_id;
        END IF;
    END IF;

    SELECT * INTO node_record
    FROM runtime_nodes
    WHERE node_id = target_node_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runtime_sessions s
        WHERE s.node_id = node_record.node_id
          AND s.status IN ('active', 'draining')
          AND (
              s.device_certificate_serial <> node_record.device_certificate_serial
              OR s.node_version <> node_record.node_version
              OR s.protocol_version <> node_record.protocol_version
              OR s.runtime_contract_id <> node_record.runtime_contract_id
              OR s.runtime_contract_digest <> node_record.runtime_contract_digest
              OR NOT (s.features @> node_record.features)
              OR NOT (node_record.features @> s.features)
          )
    ) THEN
        RAISE EXCEPTION 'active runtime sessions do not match their Node contract';
    END IF;

    RETURN NULL;
END
$$;


--
-- Name: enforce_runtime_resume_grant_identity(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_runtime_resume_grant_identity() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    source_attempt run_attempts%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'runtime resume grant evidence cannot be deleted';
    END IF;

    IF TG_OP = 'INSERT' THEN
        SELECT * INTO source_attempt
        FROM run_attempts
        WHERE run_id = NEW.run_id
          AND id = NEW.attempt_id;

        IF NOT FOUND
           OR source_attempt.lease_id IS DISTINCT FROM NEW.lease_id
           OR source_attempt.fencing_token IS DISTINCT FROM NEW.fencing_token
           OR source_attempt.agent_id IS DISTINCT FROM NEW.agent_id
           OR source_attempt.node_id IS DISTINCT FROM NEW.node_id
           OR source_attempt.runtime_worker_id IS DISTINCT FROM NEW.worker_id
           OR source_attempt.runtime_session_id IS DISTINCT FROM NEW.source_session_id
           OR source_attempt.runtime_token_id IS DISTINCT FROM NEW.source_credential_id THEN
            RAISE EXCEPTION 'runtime resume grant does not match immutable Attempt identity';
        END IF;

        IF NEW.permission = 'continue_execution'
           AND (
               source_attempt.accepted_at IS NULL
               OR source_attempt.finished_at IS NOT NULL
           ) THEN
            RAISE EXCEPTION 'continue_execution requires an unfinished accepted Attempt';
        END IF;

        RETURN NEW;
    END IF;

    IF ROW(
        NEW.id,
        NEW.run_id,
        NEW.attempt_id,
        NEW.lease_id,
        NEW.fencing_token,
        NEW.agent_id,
        NEW.node_id,
        NEW.worker_id,
        NEW.source_session_id,
        NEW.source_credential_id,
        NEW.target_session_id,
        NEW.target_credential_id,
        NEW.permission,
        NEW.granted_by_core_instance_id,
        NEW.granted_at,
        NEW.expires_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.run_id,
        OLD.attempt_id,
        OLD.lease_id,
        OLD.fencing_token,
        OLD.agent_id,
        OLD.node_id,
        OLD.worker_id,
        OLD.source_session_id,
        OLD.source_credential_id,
        OLD.target_session_id,
        OLD.target_credential_id,
        OLD.permission,
        OLD.granted_by_core_instance_id,
        OLD.granted_at,
        OLD.expires_at
    ) THEN
        RAISE EXCEPTION 'runtime resume grant immutable identity cannot change';
    END IF;

    IF OLD.first_used_at IS NOT NULL
       AND NEW.first_used_at IS DISTINCT FROM OLD.first_used_at THEN
        RAISE EXCEPTION 'runtime resume grant first-use evidence is immutable';
    END IF;

    IF OLD.revoked_at IS NOT NULL
       AND ROW(
           NEW.revoked_at,
           NEW.revoked_by_type,
           NEW.revoked_by_id,
           NEW.revoke_reason
       ) IS DISTINCT FROM ROW(
           OLD.revoked_at,
           OLD.revoked_by_type,
           OLD.revoked_by_id,
           OLD.revoke_reason
       ) THEN
        RAISE EXCEPTION 'runtime resume grant revocation evidence is immutable';
    END IF;

    RETURN NEW;
END
$$;


--
-- Name: enforce_runtime_session_attachment_consistency(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_runtime_session_attachment_consistency() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    target_session_id UUID;
    session_record runtime_sessions%ROWTYPE;
    active_attachment_count INTEGER;
    active_attachment_core_id UUID;
BEGIN
    IF TG_TABLE_NAME = 'runtime_sessions' THEN
        IF TG_OP = 'DELETE' THEN
            target_session_id := OLD.runtime_session_id;
        ELSE
            target_session_id := NEW.runtime_session_id;
        END IF;
    ELSE
        IF TG_OP = 'DELETE' THEN
            target_session_id := OLD.runtime_session_id;
        ELSE
            target_session_id := NEW.runtime_session_id;
        END IF;
    END IF;

    SELECT * INTO session_record
    FROM runtime_sessions
    WHERE runtime_session_id = target_session_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT COUNT(*), MIN(core_instance_id::TEXT)::UUID
    INTO active_attachment_count, active_attachment_core_id
    FROM runtime_session_attachments
    WHERE runtime_session_id = target_session_id
      AND detached_at IS NULL;

    IF session_record.status IN ('active', 'draining') THEN
        IF active_attachment_count <> 1
           OR active_attachment_core_id IS DISTINCT FROM session_record.attached_core_instance_id THEN
            RAISE EXCEPTION 'active runtime session attachment does not match its Core instance';
        END IF;
    ELSIF active_attachment_count <> 0 THEN
        RAISE EXCEPTION 'inactive runtime session cannot keep an active attachment';
    END IF;

    RETURN NULL;
END
$$;


--
-- Name: enforce_runtime_session_attachment_history(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_runtime_session_attachment_history() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'runtime session attachment history cannot be deleted';
    END IF;

    IF ROW(
        NEW.id,
        NEW.runtime_session_id,
        NEW.core_instance_id,
        NEW.attachment_kind,
        NEW.attached_at,
        NEW.transport,
        NEW.transport_reason,
        NEW.transport_changed_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.runtime_session_id,
        OLD.core_instance_id,
        OLD.attachment_kind,
        OLD.attached_at,
        OLD.transport,
        OLD.transport_reason,
        OLD.transport_changed_at
    ) THEN
        RAISE EXCEPTION 'runtime session attachment identity is immutable';
    END IF;

    IF OLD.detached_at IS NOT NULL
       AND ROW(
           NEW.detached_at,
           NEW.disconnect_reason
       ) IS DISTINCT FROM ROW(
           OLD.detached_at,
           OLD.disconnect_reason
       ) THEN
        RAISE EXCEPTION 'detached runtime session attachment is immutable';
    END IF;

    RETURN NEW;
END
$$;


--
-- Name: enforce_runtime_session_identity_immutable(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_runtime_session_identity_immutable() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'runtime session history cannot be deleted';
    END IF;

    IF ROW(
        OLD.runtime_session_id,
        OLD.node_id,
        OLD.agent_id,
        OLD.credential_id,
        OLD.worker_id,
        OLD.session_epoch,
        OLD.device_certificate_serial,
        OLD.node_version,
        OLD.protocol_version,
        OLD.runtime_contract_id,
        OLD.runtime_contract_digest,
        OLD.features,
        OLD.connected_at,
        OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.runtime_session_id,
        NEW.node_id,
        NEW.agent_id,
        NEW.credential_id,
        NEW.worker_id,
        NEW.session_epoch,
        NEW.device_certificate_serial,
        NEW.node_version,
        NEW.protocol_version,
        NEW.runtime_contract_id,
        NEW.runtime_contract_digest,
        NEW.features,
        NEW.connected_at,
        NEW.created_at
    ) THEN
        RAISE EXCEPTION 'runtime session immutable identity cannot change';
    END IF;

    IF NEW.heartbeat_at < OLD.heartbeat_at
       OR NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'runtime session clocks cannot move backwards';
    END IF;

    IF OLD.status IN ('revoked', 'closed')
       AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
        IF NEW.inflight = OLD.inflight - 1
           AND (
               to_jsonb(NEW) - ARRAY['inflight', 'updated_at']::TEXT[]
           ) IS NOT DISTINCT FROM (
               to_jsonb(OLD) - ARRAY['inflight', 'updated_at']::TEXT[]
           ) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'terminal runtime session is immutable except for a fenced slot release';
    END IF;

    RETURN NEW;
END
$$;


--
-- Name: enforce_runtime_session_principal(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_runtime_session_principal() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    node_record runtime_nodes%ROWTYPE;
    token_record agent_tokens%ROWTYPE;
    must_lock_principal BOOLEAN;
BEGIN
    must_lock_principal := TG_OP = 'INSERT';

    IF TG_OP = 'UPDATE' THEN
        must_lock_principal := (
            OLD.status NOT IN ('active', 'draining')
            AND NEW.status IN ('active', 'draining')
        ) OR ROW(
            NEW.node_id,
            NEW.agent_id,
            NEW.credential_id,
            NEW.device_certificate_serial,
            NEW.node_version,
            NEW.protocol_version,
            NEW.runtime_contract_id,
            NEW.runtime_contract_digest,
            NEW.features
        ) IS DISTINCT FROM ROW(
            OLD.node_id,
            OLD.agent_id,
            OLD.credential_id,
            OLD.device_certificate_serial,
            OLD.node_version,
            OLD.protocol_version,
            OLD.runtime_contract_id,
            OLD.runtime_contract_digest,
            OLD.features
        );
    END IF;

    IF NOT must_lock_principal THEN
        RETURN NEW;
    END IF;

    SELECT * INTO node_record
    FROM runtime_nodes
    WHERE node_id = NEW.node_id
    FOR SHARE;

    IF NOT FOUND
       OR node_record.device_certificate_serial <> NEW.device_certificate_serial THEN
        RAISE EXCEPTION 'runtime session node certificate identity mismatch';
    END IF;

    IF node_record.protocol_version <> NEW.protocol_version
       OR node_record.runtime_contract_id <> NEW.runtime_contract_id
       OR node_record.runtime_contract_digest <> NEW.runtime_contract_digest
       OR node_record.node_version <> NEW.node_version
       OR NOT (node_record.features @> NEW.features)
       OR NOT (NEW.features @> node_record.features) THEN
        RAISE EXCEPTION 'runtime session node contract identity mismatch';
    END IF;

    SELECT * INTO token_record
    FROM agent_tokens
    WHERE id = NEW.credential_id
      AND agent_id = NEW.agent_id
    FOR SHARE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'runtime session credential principal mismatch';
    END IF;

    IF NEW.status IN ('active', 'draining') THEN
        IF node_record.status = 'revoked'
           OR (NEW.status = 'active' AND node_record.status <> 'active') THEN
            RAISE EXCEPTION 'inactive runtime node cannot keep an active session';
        END IF;
        IF token_record.status <> 'active_runtime'
           OR token_record.revoked_at IS NOT NULL
           OR NOT ('agent:pull' = ANY(token_record.scopes))
           OR (
               token_record.expires_at IS NOT NULL
               AND token_record.expires_at <= clock_timestamp()
           ) THEN
            RAISE EXCEPTION 'inactive runtime credential cannot keep an active session';
        END IF;
    END IF;

    RETURN NEW;
END
$$;


--
-- Name: enforce_runtime_token_revocation_guard(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_runtime_token_revocation_guard() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF (
        NEW.revoked_at IS NOT NULL
        OR NEW.status <> 'active_runtime'
        OR (
            NEW.expires_at IS NOT NULL
            AND NEW.expires_at <= clock_timestamp()
        )
    )
    AND (
        OLD.revoked_at IS DISTINCT FROM NEW.revoked_at
        OR OLD.status IS DISTINCT FROM NEW.status
        OR OLD.expires_at IS DISTINCT FROM NEW.expires_at
    )
    AND EXISTS (
        SELECT 1
        FROM runtime_sessions
        WHERE credential_id = NEW.id
          AND status IN ('active', 'draining')
    ) THEN
        RAISE EXCEPTION 'runtime credential sessions must close before token revocation';
    END IF;
    RETURN NEW;
END
$$;


--
-- Name: runtime_v2_feature_set_is_valid(text[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.runtime_v2_feature_set_is_valid(feature_set text[]) RETURNS boolean
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
    SELECT cardinality(feature_set) >= 8
       AND COUNT(*) = COUNT(DISTINCT feature)
       AND BOOL_AND(char_length(feature) BETWEEN 1 AND 100)
    FROM unnest(feature_set) AS feature
$$;


--
-- Name: trigger_set_updated_at(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trigger_set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: a2a_context_mappings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.a2a_context_mappings (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    user_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    protocol_context_id text NOT NULL,
    protocol_task_id text NOT NULL,
    root_context_id text NOT NULL,
    parent_context_id text DEFAULT ''::text NOT NULL,
    parent_task_id text DEFAULT ''::text NOT NULL,
    parent_run_id uuid,
    caller_agent_id uuid,
    target_agent_id uuid,
    trace_id text DEFAULT ''::text NOT NULL,
    reference_task_ids text[] DEFAULT ARRAY[]::text[] NOT NULL,
    source text DEFAULT 'a2a_protocol'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT a2a_context_mappings_parent_context_len CHECK ((char_length(parent_context_id) <= 200)),
    CONSTRAINT a2a_context_mappings_parent_task_len CHECK ((char_length(parent_task_id) <= 200)),
    CONSTRAINT a2a_context_mappings_protocol_context_len CHECK (((char_length(protocol_context_id) >= 1) AND (char_length(protocol_context_id) <= 200))),
    CONSTRAINT a2a_context_mappings_protocol_task_len CHECK (((char_length(protocol_task_id) >= 1) AND (char_length(protocol_task_id) <= 200))),
    CONSTRAINT a2a_context_mappings_root_context_len CHECK (((char_length(root_context_id) >= 1) AND (char_length(root_context_id) <= 200))),
    CONSTRAINT a2a_context_mappings_source_valid CHECK ((source = ANY (ARRAY['a2a_protocol'::text, 'agent_delegation'::text]))),
    CONSTRAINT a2a_context_mappings_trace_len CHECK ((char_length(trace_id) <= 200))
);


--
-- Name: agent_action_approval_requests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_action_approval_requests (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    agent_id uuid NOT NULL,
    requested_by_user_id uuid,
    requested_by_token_id uuid,
    action text NOT NULL,
    payload_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    approval_url_slug text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    decided_at timestamp with time zone,
    decided_by_user_id uuid,
    decision_note text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_action_approval_action_len CHECK (((char_length(action) >= 1) AND (char_length(action) <= 80))),
    CONSTRAINT agent_action_approval_decision_consistent CHECK ((((status = 'pending'::text) AND (decided_at IS NULL) AND (decided_by_user_id IS NULL)) OR ((status = ANY (ARRAY['confirmed'::text, 'rejected'::text])) AND (decided_at IS NOT NULL)) OR (status = 'expired'::text))),
    CONSTRAINT agent_action_approval_slug_format CHECK ((approval_url_slug ~ '^[a-zA-Z0-9_-]{16,64}$'::text)),
    CONSTRAINT agent_action_approval_status_valid CHECK ((status = ANY (ARRAY['pending'::text, 'confirmed'::text, 'rejected'::text, 'expired'::text])))
);


--
-- Name: agent_availability_alerts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_availability_alerts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    agent_id uuid NOT NULL,
    creator_id uuid NOT NULL,
    alert_type text NOT NULL,
    severity text NOT NULL,
    availability_status text NOT NULL,
    consecutive_failures integer DEFAULT 0 NOT NULL,
    title text NOT NULL,
    message text NOT NULL,
    last_error text,
    repair_hints text[] DEFAULT ARRAY[]::text[] NOT NULL,
    read_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_availability_alert_failures_nonneg CHECK ((consecutive_failures >= 0)),
    CONSTRAINT agent_availability_alert_message_len CHECK (((char_length(message) >= 1) AND (char_length(message) <= 1000))),
    CONSTRAINT agent_availability_alert_severity_valid CHECK ((severity = ANY (ARRAY['info'::text, 'warning'::text, 'critical'::text]))),
    CONSTRAINT agent_availability_alert_status_valid CHECK ((availability_status = ANY (ARRAY['unknown'::text, 'healthy'::text, 'degraded'::text, 'unreachable'::text]))),
    CONSTRAINT agent_availability_alert_title_len CHECK (((char_length(title) >= 1) AND (char_length(title) <= 160))),
    CONSTRAINT agent_availability_alert_type_valid CHECK ((alert_type = ANY (ARRAY['availability_failed'::text, 'availability_recovered'::text])))
);


--
-- Name: agent_availability_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_availability_snapshots (
    agent_id uuid NOT NULL,
    availability_status text DEFAULT 'unknown'::text NOT NULL,
    last_successful_run_at timestamp with time zone,
    last_failed_run_at timestamp with time zone,
    last_checked_at timestamp with time zone,
    consecutive_failures integer DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_availability_failures_nonneg CHECK ((consecutive_failures >= 0)),
    CONSTRAINT agent_availability_status_valid CHECK ((availability_status = ANY (ARRAY['unknown'::text, 'healthy'::text, 'degraded'::text, 'unreachable'::text])))
);


--
-- Name: agent_call_policies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_call_policies (
    agent_id uuid NOT NULL,
    callable_by text DEFAULT 'public'::text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_call_policies_callable_by_valid CHECK ((callable_by = ANY (ARRAY['public'::text, 'same_creator'::text, 'private'::text])))
);


--
-- Name: agent_capabilities; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_capabilities (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    agent_id uuid NOT NULL,
    input_schema jsonb NOT NULL,
    output_schema jsonb NOT NULL,
    summary text DEFAULT ''::text NOT NULL,
    version integer DEFAULT 1 NOT NULL,
    published_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_capabilities_version_positive CHECK ((version >= 1))
);


--
-- Name: agent_examples; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_examples (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    agent_id uuid NOT NULL,
    title text NOT NULL,
    input_json jsonb NOT NULL,
    expected_output_json jsonb,
    sort_order integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_examples_title_len CHECK (((char_length(title) >= 1) AND (char_length(title) <= 120)))
);


--
-- Name: agent_metric_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_metric_snapshots (
    agent_id uuid NOT NULL,
    time_window text NOT NULL,
    call_count integer DEFAULT 0 NOT NULL,
    success_count integer DEFAULT 0 NOT NULL,
    failure_count integer DEFAULT 0 NOT NULL,
    success_rate_bps integer DEFAULT 0 NOT NULL,
    median_latency_ms integer,
    p95_latency_ms integer,
    snapshotted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_metric_snapshots_counts_nonneg CHECK (((call_count >= 0) AND (success_count >= 0) AND (failure_count >= 0))),
    CONSTRAINT agent_metric_snapshots_rate_range CHECK (((success_rate_bps >= 0) AND (success_rate_bps <= 10000))),
    CONSTRAINT agent_metric_snapshots_time_window_valid CHECK ((time_window = ANY (ARRAY['24h'::text, '7d'::text, '30d'::text])))
);


--
-- Name: agent_onboarding_status; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_onboarding_status (
    agent_id uuid NOT NULL,
    endpoint_set boolean DEFAULT false NOT NULL,
    capabilities_set boolean DEFAULT false NOT NULL,
    examples_set boolean DEFAULT false NOT NULL,
    dry_run_passed boolean DEFAULT false NOT NULL,
    dry_run_last_result text DEFAULT 'pending'::text NOT NULL,
    dry_run_error text,
    dry_run_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_onboarding_dry_run_result_valid CHECK ((dry_run_last_result = ANY (ARRAY['pending'::text, 'pass'::text, 'fail'::text])))
);


--
-- Name: agent_skill_benchmark_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_skill_benchmark_runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    batch_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    skill_id text NOT NULL,
    test_case_id uuid NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    score integer,
    raw_output jsonb,
    judge_reasoning text,
    error_message text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT agent_skill_benchmark_score_range CHECK (((score IS NULL) OR ((score >= 0) AND (score <= 100)))),
    CONSTRAINT agent_skill_benchmark_status_valid CHECK ((status = ANY (ARRAY['pending'::text, 'success'::text, 'failed'::text])))
);


--
-- Name: agent_skill_scores; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_skill_scores (
    agent_id uuid NOT NULL,
    skill_id text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    average_score integer,
    pass_count integer DEFAULT 0 NOT NULL,
    total_count integer DEFAULT 0 NOT NULL,
    last_batch_id uuid,
    verified_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_skill_scores_avg_range CHECK (((average_score IS NULL) OR ((average_score >= 0) AND (average_score <= 100)))),
    CONSTRAINT agent_skill_scores_status_valid CHECK ((status = ANY (ARRAY['pending'::text, 'verified'::text, 'failed'::text])))
);


--
-- Name: agent_skills; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_skills (
    agent_id uuid NOT NULL,
    skill_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: agent_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_tokens (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    agent_id uuid,
    creator_user_id uuid NOT NULL,
    name text NOT NULL,
    prefix text NOT NULL,
    token_hash text NOT NULL,
    scopes text[] DEFAULT ARRAY['agent:call'::text] NOT NULL,
    last_used_at timestamp with time zone,
    revoked_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    status text DEFAULT 'active_runtime'::text NOT NULL,
    expires_at timestamp with time zone,
    redeemed_at timestamp with time zone,
    rotation_predecessor_id uuid,
    revocation_kind text,
    CONSTRAINT agent_tokens_name_len CHECK (((char_length(name) >= 1) AND (char_length(name) <= 80))),
    CONSTRAINT agent_tokens_pending_shape CHECK ((((status = 'pending_registration'::text) AND (agent_id IS NULL) AND (redeemed_at IS NULL)) OR (status <> 'pending_registration'::text))),
    CONSTRAINT agent_tokens_prefix_format CHECK ((prefix ~ '^ol_agent_[a-f0-9]+$'::text)),
    CONSTRAINT agent_tokens_revocation_consistent CHECK ((((status <> 'revoked'::text) AND (revoked_at IS NULL) AND (revocation_kind IS NULL)) OR ((status = 'revoked'::text) AND (revoked_at IS NOT NULL) AND (revocation_kind IS NOT NULL)))),
    CONSTRAINT agent_tokens_revocation_kind_valid CHECK (((revocation_kind IS NULL) OR (revocation_kind = ANY (ARRAY['planned_rotation'::text, 'security'::text, 'manual'::text, 'expired'::text])))),
    CONSTRAINT agent_tokens_rotation_distinct CHECK (((rotation_predecessor_id IS NULL) OR (rotation_predecessor_id <> id))),
    CONSTRAINT agent_tokens_runtime_shape CHECK ((((status = 'active_runtime'::text) AND (agent_id IS NOT NULL) AND (redeemed_at IS NOT NULL)) OR (status <> 'active_runtime'::text))),
    CONSTRAINT agent_tokens_scopes_known CHECK ((scopes <@ ARRAY['agent:call'::text, 'agent:pull'::text])),
    CONSTRAINT agent_tokens_scopes_nonempty CHECK ((cardinality(scopes) > 0)),
    CONSTRAINT agent_tokens_status_valid CHECK ((status = ANY (ARRAY['pending_registration'::text, 'active_runtime'::text, 'revoked'::text])))
);


--
-- Name: agents; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agents (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    creator_id uuid NOT NULL,
    slug text NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    endpoint_url text NOT NULL,
    endpoint_auth_header text,
    price_per_call_cents integer NOT NULL,
    tags text[] DEFAULT '{}'::text[] NOT NULL,
    rejection_reason text,
    total_calls integer DEFAULT 0 NOT NULL,
    total_revenue_cents bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    webhook_url text,
    webhook_secret text,
    lifecycle_status text DEFAULT 'active'::text NOT NULL,
    visibility text DEFAULT 'public'::text NOT NULL,
    certification_status text DEFAULT 'unreviewed'::text NOT NULL,
    certified_at timestamp with time zone,
    connection_mode text DEFAULT 'direct_http'::text NOT NULL,
    mcp_tool_name text,
    CONSTRAINT agents_certification_status_valid CHECK ((certification_status = ANY (ARRAY['unreviewed'::text, 'pending'::text, 'certified'::text, 'rejected'::text]))),
    CONSTRAINT agents_connection_mode_valid CHECK ((connection_mode = ANY (ARRAY['direct_http'::text, 'mcp_server'::text, 'runtime'::text]))),
    CONSTRAINT agents_endpoint_https CHECK (((endpoint_url ~~ 'https://%'::text) OR (endpoint_url = 'http://localhost'::text) OR (endpoint_url ~~ 'http://localhost:%'::text) OR (endpoint_url ~~ 'http://localhost/%'::text) OR (endpoint_url = 'http://127.0.0.1'::text) OR (endpoint_url ~~ 'http://127.0.0.1:%'::text) OR (endpoint_url ~~ 'http://127.0.0.1/%'::text) OR (endpoint_url = 'http://[::1]'::text) OR (endpoint_url ~~ 'http://[::1]:%'::text) OR (endpoint_url ~~ 'http://[::1]/%'::text) OR (endpoint_url ~~ 'openlinker-runtime://%'::text))),
    CONSTRAINT agents_lifecycle_status_valid CHECK ((lifecycle_status = ANY (ARRAY['active'::text, 'disabled'::text]))),
    CONSTRAINT agents_mcp_tool_required CHECK (((connection_mode <> 'mcp_server'::text) OR ((mcp_tool_name IS NOT NULL) AND ((char_length(TRIM(BOTH FROM mcp_tool_name)) >= 1) AND (char_length(TRIM(BOTH FROM mcp_tool_name)) <= 120))))),
    CONSTRAINT agents_price_nonnegative CHECK (((price_per_call_cents >= 0) AND (price_per_call_cents <= 1000000))),
    CONSTRAINT agents_runtime_queue_endpoint CHECK (((connection_mode <> 'runtime'::text) OR (endpoint_url ~~ 'openlinker-runtime://%'::text))),
    CONSTRAINT agents_slug_format CHECK ((slug ~ '^[a-z0-9][a-z0-9-]*[a-z0-9]$'::text)),
    CONSTRAINT agents_visibility_valid CHECK ((visibility = ANY (ARRAY['public'::text, 'unlisted'::text, 'private'::text]))),
    CONSTRAINT agents_webhook_https CHECK (((webhook_url IS NULL) OR (webhook_url ~~ 'https://%'::text)))
);


--
-- Name: core_instance_identity; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.core_instance_identity (
    singleton boolean DEFAULT true NOT NULL,
    issuer_instance_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT core_instance_identity_singleton_check CHECK (singleton)
);


--
-- Name: delivery_targets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.delivery_targets (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    name text NOT NULL,
    type text NOT NULL,
    config jsonb DEFAULT '{}'::jsonb NOT NULL,
    secret text NOT NULL,
    is_default boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT delivery_targets_name_len CHECK (((char_length(name) >= 1) AND (char_length(name) <= 80))),
    CONSTRAINT delivery_targets_type_valid CHECK ((type = ANY (ARRAY['webhook'::text, 'slack'::text])))
);


--
-- Name: external_execution_cancellations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.external_execution_cancellations (
    id uuid NOT NULL,
    caller_service_id text NOT NULL,
    external_request_id uuid NOT NULL,
    actor_user_id uuid NOT NULL,
    reason_code text NOT NULL,
    state text NOT NULL,
    execution_kind_snapshot text,
    execution_id_snapshot uuid,
    error_code text,
    requested_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    applied_at timestamp with time zone,
    finished_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT external_execution_cancellations_attachment_complete CHECK (((execution_kind_snapshot IS NULL) = (execution_id_snapshot IS NULL))),
    CONSTRAINT external_execution_cancellations_error_valid CHECK (((((state = 'unconfirmed'::text) AND (error_code = 'CANCEL_UNCONFIRMED'::text)) OR ((state <> 'unconfirmed'::text) AND (error_code IS NULL))) IS TRUE)),
    CONSTRAINT external_execution_cancellations_execution_kind_snapshot_check CHECK (((execution_kind_snapshot IS NULL) OR (execution_kind_snapshot = ANY (ARRAY['run'::text, 'workflow_run'::text])))),
    CONSTRAINT external_execution_cancellations_reason_code_check CHECK ((reason_code = ANY (ARRAY['CALLER_REQUESTED'::text, 'DEADLINE_EXCEEDED'::text]))),
    CONSTRAINT external_execution_cancellations_state_check CHECK ((state = ANY (ARRAY['requested'::text, 'stopping'::text, 'stopped'::text, 'unconfirmed'::text, 'not_applied'::text]))),
    CONSTRAINT external_execution_cancellations_time_order CHECK ((((applied_at IS NULL) OR (applied_at >= requested_at)) AND ((finished_at IS NULL) OR ((applied_at IS NOT NULL) AND (finished_at >= applied_at))))),
    CONSTRAINT external_execution_cancellations_timestamps_valid CHECK (((((state = 'requested'::text) AND (applied_at IS NULL) AND (finished_at IS NULL)) OR ((state = 'stopping'::text) AND (applied_at IS NOT NULL) AND (finished_at IS NULL)) OR ((state = ANY (ARRAY['stopped'::text, 'unconfirmed'::text, 'not_applied'::text])) AND (applied_at IS NOT NULL) AND (finished_at IS NOT NULL))) IS TRUE))
);


--
-- Name: external_execution_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.external_execution_keys (
    caller_service_id text NOT NULL,
    external_request_id uuid NOT NULL,
    actor_user_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT external_execution_keys_caller_service_id_valid CHECK (((caller_service_id = btrim(caller_service_id)) AND ((length(caller_service_id) >= 1) AND (length(caller_service_id) <= 200))))
);


--
-- Name: external_executions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.external_executions (
    external_request_id uuid NOT NULL,
    actor_user_id uuid NOT NULL,
    legacy_rollback_target_owner_id uuid,
    target_type text NOT NULL,
    target_id uuid NOT NULL,
    input_fingerprint bytea NOT NULL,
    trace_id text NOT NULL,
    execution_kind text,
    execution_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    expected_contract_hash text,
    input_schema_fingerprint bytea,
    downstream_replay_identity jsonb,
    caller_service_id text NOT NULL,
    request_fingerprint_version smallint DEFAULT 2 NOT NULL,
    start_state text DEFAULT 'pending'::text NOT NULL,
    start_token uuid,
    start_lease_until timestamp with time zone,
    authorized_target_owner_id uuid,
    rejection_code text,
    downstream_idempotency_key_hash bytea,
    downstream_creation_fingerprint bytea,
    CONSTRAINT external_executions_attachment_complete CHECK (((execution_kind IS NULL) = (execution_id IS NULL))),
    CONSTRAINT external_executions_caller_service_id_valid CHECK (((caller_service_id = btrim(caller_service_id)) AND ((length(caller_service_id) >= 1) AND (length(caller_service_id) <= 200)))),
    CONSTRAINT external_executions_contract_hash_valid CHECK (((expected_contract_hash IS NULL) OR (expected_contract_hash ~ '^hct:v1:[a-f0-9]{64}$'::text))),
    CONSTRAINT external_executions_downstream_fingerprint_valid CHECK (((downstream_creation_fingerprint IS NULL) OR (octet_length(downstream_creation_fingerprint) = 32))),
    CONSTRAINT external_executions_downstream_identity_complete CHECK (((downstream_idempotency_key_hash IS NULL) = (downstream_creation_fingerprint IS NULL))),
    CONSTRAINT external_executions_downstream_key_hash_valid CHECK (((downstream_idempotency_key_hash IS NULL) OR (octet_length(downstream_idempotency_key_hash) = 32))),
    CONSTRAINT external_executions_downstream_replay_identity_valid CHECK (((((downstream_replay_identity IS NULL) OR ((request_fingerprint_version = 1) AND (target_type = 'agent'::text) AND (jsonb_typeof(downstream_replay_identity) = 'object'::text) AND (downstream_replay_identity ?& ARRAY['version'::text, 'kind'::text, 'source'::text, 'idempotency_key'::text, 'creation_protocol'::text, 'creation_method'::text, 'metadata'::text]) AND ((downstream_replay_identity - ARRAY['version'::text, 'kind'::text, 'source'::text, 'idempotency_key'::text, 'creation_protocol'::text, 'creation_method'::text, 'metadata'::text]) = '{}'::jsonb) AND (jsonb_typeof((downstream_replay_identity -> 'version'::text)) = 'number'::text) AND ((downstream_replay_identity -> 'version'::text) = '1'::jsonb) AND (jsonb_typeof((downstream_replay_identity -> 'kind'::text)) = 'string'::text) AND ((downstream_replay_identity ->> 'kind'::text) = 'run'::text) AND (jsonb_typeof((downstream_replay_identity -> 'source'::text)) = 'string'::text) AND ((downstream_replay_identity ->> 'source'::text) = 'api'::text) AND (jsonb_typeof((downstream_replay_identity -> 'idempotency_key'::text)) = 'string'::text) AND ((length((downstream_replay_identity ->> 'idempotency_key'::text)) >= 1) AND (length((downstream_replay_identity ->> 'idempotency_key'::text)) <= 255)) AND ((downstream_replay_identity ->> 'idempotency_key'::text) ~ '^[ -~]+$'::text) AND (jsonb_typeof((downstream_replay_identity -> 'creation_protocol'::text)) = 'string'::text) AND ((length((downstream_replay_identity ->> 'creation_protocol'::text)) >= 1) AND (length((downstream_replay_identity ->> 'creation_protocol'::text)) <= 80)) AND ((downstream_replay_identity ->> 'creation_protocol'::text) = btrim((downstream_replay_identity ->> 'creation_protocol'::text))) AND ((downstream_replay_identity ->> 'creation_protocol'::text) = lower((downstream_replay_identity ->> 'creation_protocol'::text))) AND (jsonb_typeof((downstream_replay_identity -> 'creation_method'::text)) = 'string'::text) AND ((length((downstream_replay_identity ->> 'creation_method'::text)) >= 1) AND (length((downstream_replay_identity ->> 'creation_method'::text)) <= 120)) AND ((downstream_replay_identity ->> 'creation_method'::text) = btrim((downstream_replay_identity ->> 'creation_method'::text))) AND ((downstream_replay_identity ->> 'creation_method'::text) = lower((downstream_replay_identity ->> 'creation_method'::text))) AND (jsonb_typeof((downstream_replay_identity -> 'metadata'::text)) = 'object'::text))) AND ((request_fingerprint_version <> 1) OR (target_type <> 'agent'::text) OR (execution_id IS NOT NULL) OR (downstream_replay_identity IS NOT NULL))) IS TRUE)),
    CONSTRAINT external_executions_execution_kind_check CHECK ((execution_kind = ANY (ARRAY['run'::text, 'workflow_run'::text]))),
    CONSTRAINT external_executions_input_fingerprint_check CHECK ((octet_length(input_fingerprint) = 32)),
    CONSTRAINT external_executions_request_fingerprint_version_valid CHECK ((request_fingerprint_version = ANY (ARRAY[1, 2]))),
    CONSTRAINT external_executions_schema_fingerprint_valid CHECK (((input_schema_fingerprint IS NULL) OR (octet_length(input_schema_fingerprint) = 32))),
    CONSTRAINT external_executions_start_state_valid CHECK (((((start_state = 'pending'::text) AND (start_token IS NULL) AND (start_lease_until IS NULL) AND (authorized_target_owner_id IS NULL) AND (rejection_code IS NULL) AND (execution_id IS NULL)) OR ((start_state = 'evaluating'::text) AND (start_token IS NOT NULL) AND (start_lease_until IS NOT NULL) AND (authorized_target_owner_id IS NULL) AND (rejection_code IS NULL) AND (execution_id IS NULL)) OR ((start_state = 'authorized'::text) AND (start_token IS NULL) AND (start_lease_until IS NULL) AND (authorized_target_owner_id IS NOT NULL) AND (rejection_code IS NULL) AND (execution_id IS NULL)) OR ((start_state = 'launching'::text) AND (start_token IS NOT NULL) AND (start_lease_until IS NOT NULL) AND (authorized_target_owner_id IS NOT NULL) AND (rejection_code IS NULL) AND (execution_id IS NULL)) OR ((start_state = 'attached'::text) AND (start_token IS NULL) AND (start_lease_until IS NULL) AND (rejection_code IS NULL) AND (execution_id IS NOT NULL)) OR ((start_state = 'rejected'::text) AND (start_token IS NULL) AND (start_lease_until IS NULL) AND (authorized_target_owner_id IS NULL) AND (rejection_code = ANY (ARRAY['TARGET_UNAVAILABLE'::text, 'TARGET_CONTRACT_CHANGED'::text, 'DOWNSTREAM_IDENTITY_CONFLICT'::text])) AND (execution_id IS NULL)) OR ((start_state = 'canceled'::text) AND (start_token IS NULL) AND (start_lease_until IS NULL) AND (rejection_code IS NULL))) IS TRUE)),
    CONSTRAINT external_executions_target_type_check CHECK ((target_type = ANY (ARRAY['agent'::text, 'workflow'::text]))),
    CONSTRAINT external_executions_trace_id_check CHECK (((length(trace_id) >= 1) AND (length(trace_id) <= 200)))
);


--
-- Name: oauth_login_codes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.oauth_login_codes (
    code_hash text NOT NULL,
    user_id uuid NOT NULL,
    jwt text,
    expires_at timestamp with time zone NOT NULL,
    consumed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT oauth_login_codes_hash_len CHECK ((char_length(code_hash) = 64)),
    CONSTRAINT oauth_login_codes_jwt_nonempty CHECK ((char_length(jwt) > 0))
);


--
-- Name: proxy_run_artifacts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_run_artifacts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    proxy_run_id uuid NOT NULL,
    registry_run_id uuid NOT NULL,
    source_artifact_id text NOT NULL,
    artifact_type text DEFAULT 'data'::text NOT NULL,
    title text NOT NULL,
    content jsonb DEFAULT '{}'::jsonb NOT NULL,
    mime_type text,
    file_uri text,
    file_name text,
    file_sha256 text,
    file_size_bytes bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT proxy_run_artifacts_mime_len CHECK (((mime_type IS NULL) OR (char_length(mime_type) <= 200))),
    CONSTRAINT proxy_run_artifacts_name_len CHECK (((file_name IS NULL) OR (char_length(file_name) <= 500))),
    CONSTRAINT proxy_run_artifacts_sha256_format CHECK (((file_sha256 IS NULL) OR (file_sha256 ~ '^[a-f0-9]{64}$'::text))),
    CONSTRAINT proxy_run_artifacts_size_nonnegative CHECK (((file_size_bytes IS NULL) OR (file_size_bytes >= 0))),
    CONSTRAINT proxy_run_artifacts_source_len CHECK (((char_length(source_artifact_id) >= 1) AND (char_length(source_artifact_id) <= 160))),
    CONSTRAINT proxy_run_artifacts_title_len CHECK (((char_length(title) >= 1) AND (char_length(title) <= 300))),
    CONSTRAINT proxy_run_artifacts_type_len CHECK (((char_length(artifact_type) >= 1) AND (char_length(artifact_type) <= 80))),
    CONSTRAINT proxy_run_artifacts_uri_len CHECK (((file_uri IS NULL) OR (char_length(file_uri) <= 2000)))
);


--
-- Name: proxy_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    registry_run_id uuid DEFAULT gen_random_uuid() NOT NULL,
    registry_listing_link_id uuid NOT NULL,
    registry_listing_id uuid NOT NULL,
    registry_node_id uuid NOT NULL,
    local_agent_id uuid NOT NULL,
    requesting_user_id uuid NOT NULL,
    idempotency_key text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    payload_policy text DEFAULT 'metadata_only'::text NOT NULL,
    input jsonb DEFAULT '{}'::jsonb NOT NULL,
    input_summary text,
    output jsonb DEFAULT '{}'::jsonb NOT NULL,
    output_summary text,
    error_code text,
    error_message text,
    claimed_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    node_input jsonb,
    attempt_count integer DEFAULT 0 NOT NULL,
    max_attempts integer DEFAULT 3 NOT NULL,
    next_retry_at timestamp with time zone,
    payload_redaction_keys text[] DEFAULT ARRAY[]::text[] NOT NULL,
    CONSTRAINT proxy_runs_attempt_count_nonnegative CHECK ((attempt_count >= 0)),
    CONSTRAINT proxy_runs_claimed_at_status CHECK ((((status = 'pending'::text) AND (claimed_at IS NULL)) OR ((status <> 'pending'::text) AND (claimed_at IS NOT NULL)))),
    CONSTRAINT proxy_runs_error_code_len CHECK (((error_code IS NULL) OR (char_length(error_code) <= 80))),
    CONSTRAINT proxy_runs_error_message_len CHECK (((error_message IS NULL) OR (char_length(error_message) <= 1000))),
    CONSTRAINT proxy_runs_finished_at_status CHECK ((((status = ANY (ARRAY['success'::text, 'failed'::text, 'timeout'::text])) AND (finished_at IS NOT NULL)) OR ((status = ANY (ARRAY['pending'::text, 'claimed'::text])) AND (finished_at IS NULL)))),
    CONSTRAINT proxy_runs_idempotency_key_len CHECK (((char_length(idempotency_key) >= 8) AND (char_length(idempotency_key) <= 160))),
    CONSTRAINT proxy_runs_input_summary_len CHECK (((input_summary IS NULL) OR (char_length(input_summary) <= 500))),
    CONSTRAINT proxy_runs_max_attempts_range CHECK (((max_attempts >= 1) AND (max_attempts <= 10))),
    CONSTRAINT proxy_runs_output_summary_len CHECK (((output_summary IS NULL) OR (char_length(output_summary) <= 1000))),
    CONSTRAINT proxy_runs_payload_policy_check CHECK ((payload_policy = ANY (ARRAY['metadata_only'::text, 'store_run_summary'::text, 'store_full_payload'::text]))),
    CONSTRAINT proxy_runs_redaction_keys_limit CHECK ((cardinality(payload_redaction_keys) <= 20)),
    CONSTRAINT proxy_runs_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'claimed'::text, 'success'::text, 'failed'::text, 'timeout'::text])))
);


--
-- Name: registry_federation_invites; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.registry_federation_invites (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    owner_user_id uuid NOT NULL,
    name text NOT NULL,
    api_base_url text NOT NULL,
    bearer_token text NOT NULL,
    token_prefix text NOT NULL,
    token_hash text NOT NULL,
    credential_hint text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    consumed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT registry_federation_invites_api_base_url_len CHECK (((char_length(api_base_url) >= 8) AND (char_length(api_base_url) <= 500))),
    CONSTRAINT registry_federation_invites_bearer_token_len CHECK (((char_length(bearer_token) >= 8) AND (char_length(bearer_token) <= 4096))),
    CONSTRAINT registry_federation_invites_name_len CHECK (((char_length(name) >= 2) AND (char_length(name) <= 120))),
    CONSTRAINT registry_federation_invites_status_check CHECK ((status = ANY (ARRAY['active'::text, 'consumed'::text, 'expired'::text, 'revoked'::text])))
);


--
-- Name: registry_listing_links; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.registry_listing_links (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    registry_listing_id uuid DEFAULT gen_random_uuid() NOT NULL,
    registry_node_id uuid NOT NULL,
    local_agent_id uuid NOT NULL,
    routing_mode text DEFAULT 'pull_proxy'::text NOT NULL,
    payload_policy text DEFAULT 'metadata_only'::text NOT NULL,
    sync_status text DEFAULT 'linked'::text NOT NULL,
    last_sync_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    synced_agent_slug text DEFAULT ''::text NOT NULL,
    synced_agent_name text DEFAULT ''::text NOT NULL,
    synced_agent_description text DEFAULT ''::text NOT NULL,
    synced_agent_tags text[] DEFAULT ARRAY[]::text[] NOT NULL,
    synced_availability_status text DEFAULT 'unknown'::text NOT NULL,
    metadata_synced_at timestamp with time zone,
    metadata_sync_error text,
    payload_redaction_keys text[] DEFAULT ARRAY[]::text[] NOT NULL,
    CONSTRAINT cloud_listing_links_payload_policy_check CHECK ((payload_policy = ANY (ARRAY['metadata_only'::text, 'store_run_summary'::text, 'store_full_payload'::text]))),
    CONSTRAINT cloud_listing_links_routing_mode_check CHECK ((routing_mode = ANY (ARRAY['direct_endpoint'::text, 'pull_proxy'::text]))),
    CONSTRAINT cloud_listing_links_sync_status_check CHECK ((sync_status = ANY (ARRAY['linked'::text, 'paused'::text, 'error'::text]))),
    CONSTRAINT registry_listing_links_metadata_sync_error_len CHECK (((metadata_sync_error IS NULL) OR (char_length(metadata_sync_error) <= 1000))),
    CONSTRAINT registry_listing_links_redaction_keys_limit CHECK ((cardinality(payload_redaction_keys) <= 20)),
    CONSTRAINT registry_listing_links_synced_availability_valid CHECK ((synced_availability_status = ANY (ARRAY['unknown'::text, 'healthy'::text, 'degraded'::text, 'unreachable'::text]))),
    CONSTRAINT registry_listing_links_synced_description_len CHECK ((char_length(synced_agent_description) <= 500)),
    CONSTRAINT registry_listing_links_synced_name_len CHECK ((char_length(synced_agent_name) <= 80)),
    CONSTRAINT registry_listing_links_synced_slug_len CHECK ((char_length(synced_agent_slug) <= 80))
);


--
-- Name: registry_nodes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.registry_nodes (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    owner_user_id uuid NOT NULL,
    node_name text NOT NULL,
    node_type text DEFAULT 'bridge_proxy'::text NOT NULL,
    base_url text,
    secret_prefix text NOT NULL,
    secret_hash text NOT NULL,
    scopes text[] DEFAULT ARRAY['heartbeat'::text, 'listing:sync'::text, 'proxy:pull'::text, 'proxy:result'::text] NOT NULL,
    heartbeat_status text DEFAULT 'unknown'::text NOT NULL,
    last_heartbeat_at timestamp with time zone,
    revoked_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT registry_nodes_base_url_len CHECK (((base_url IS NULL) OR (char_length(base_url) <= 500))),
    CONSTRAINT registry_nodes_heartbeat_status_check CHECK ((heartbeat_status = ANY (ARRAY['unknown'::text, 'healthy'::text, 'stale'::text, 'revoked'::text]))),
    CONSTRAINT registry_nodes_name_len CHECK (((char_length(node_name) >= 2) AND (char_length(node_name) <= 120))),
    CONSTRAINT registry_nodes_node_type_check CHECK ((node_type = ANY (ARRAY['self_hosted'::text, 'bridge_proxy'::text]))),
    CONSTRAINT registry_nodes_scopes_known CHECK ((scopes <@ ARRAY['heartbeat'::text, 'listing:sync'::text, 'proxy:pull'::text, 'proxy:result'::text])),
    CONSTRAINT registry_nodes_secret_prefix_format CHECK ((secret_prefix ~ '^rn_live_[a-f0-9]+$'::text))
);


--
-- Name: registry_peers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.registry_peers (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    owner_user_id uuid NOT NULL,
    name text NOT NULL,
    api_base_url text NOT NULL,
    bearer_token text NOT NULL,
    credential_hint text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    last_used_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT registry_peers_api_base_url_len CHECK (((char_length(api_base_url) >= 8) AND (char_length(api_base_url) <= 500))),
    CONSTRAINT registry_peers_bearer_token_len CHECK (((char_length(bearer_token) >= 8) AND (char_length(bearer_token) <= 4096))),
    CONSTRAINT registry_peers_name_len CHECK (((char_length(name) >= 2) AND (char_length(name) <= 120))),
    CONSTRAINT registry_peers_status_check CHECK ((status = ANY (ARRAY['active'::text, 'paused'::text])))
);


--
-- Name: run_accounting_ledger; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_accounting_ledger (
    run_id uuid NOT NULL,
    terminal_event_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    success_delta integer NOT NULL,
    revenue_delta_cents bigint NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT run_accounting_ledger_revenue_nonnegative CHECK ((revenue_delta_cents >= 0)),
    CONSTRAINT run_accounting_ledger_success_delta CHECK ((success_delta = ANY (ARRAY[0, 1])))
);


--
-- Name: run_artifact_chunks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_artifact_chunks (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    run_artifact_id uuid NOT NULL,
    source_artifact_id text NOT NULL,
    event_sequence integer,
    chunk_index integer NOT NULL,
    append boolean DEFAULT true NOT NULL,
    last_chunk boolean DEFAULT false NOT NULL,
    parts jsonb DEFAULT '[]'::jsonb NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    parts_sha256 text,
    payload_sha256 text,
    declared_sha256 text,
    checksum_status text DEFAULT 'not_provided'::text NOT NULL,
    CONSTRAINT run_artifact_chunks_checksum_status_valid CHECK ((checksum_status = ANY (ARRAY['not_provided'::text, 'verified'::text, 'mismatch'::text, 'invalid'::text]))),
    CONSTRAINT run_artifact_chunks_declared_sha256_len CHECK (((declared_sha256 IS NULL) OR (declared_sha256 ~ '^[a-f0-9]{64}$'::text))),
    CONSTRAINT run_artifact_chunks_index_nonnegative CHECK ((chunk_index >= 0)),
    CONSTRAINT run_artifact_chunks_parts_array CHECK ((jsonb_typeof(parts) = 'array'::text)),
    CONSTRAINT run_artifact_chunks_parts_sha256_len CHECK (((parts_sha256 IS NULL) OR (parts_sha256 ~ '^[a-f0-9]{64}$'::text))),
    CONSTRAINT run_artifact_chunks_payload_sha256_len CHECK (((payload_sha256 IS NULL) OR (payload_sha256 ~ '^[a-f0-9]{64}$'::text))),
    CONSTRAINT run_artifact_chunks_source_len CHECK (((char_length(source_artifact_id) >= 1) AND (char_length(source_artifact_id) <= 200)))
);


--
-- Name: run_artifacts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_artifacts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    artifact_type text DEFAULT 'json'::text NOT NULL,
    title text NOT NULL,
    content jsonb NOT NULL,
    visibility text DEFAULT 'private'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    source_artifact_id text,
    mime_type text,
    file_uri text,
    file_name text,
    file_sha256 text,
    file_size_bytes bigint,
    CONSTRAINT run_artifacts_file_name_len CHECK (((file_name IS NULL) OR ((char_length(file_name) >= 1) AND (char_length(file_name) <= 500)))),
    CONSTRAINT run_artifacts_file_sha256_len CHECK (((file_sha256 IS NULL) OR (file_sha256 ~ '^[A-Fa-f0-9]{64}$'::text))),
    CONSTRAINT run_artifacts_file_size_nonnegative CHECK (((file_size_bytes IS NULL) OR (file_size_bytes >= 0))),
    CONSTRAINT run_artifacts_file_uri_len CHECK (((file_uri IS NULL) OR ((char_length(file_uri) >= 1) AND (char_length(file_uri) <= 2000)))),
    CONSTRAINT run_artifacts_mime_type_len CHECK (((mime_type IS NULL) OR ((char_length(mime_type) >= 1) AND (char_length(mime_type) <= 200)))),
    CONSTRAINT run_artifacts_title_len CHECK (((char_length(title) >= 1) AND (char_length(title) <= 200))),
    CONSTRAINT run_artifacts_type_valid CHECK ((artifact_type = ANY (ARRAY['json'::text, 'text'::text, 'file'::text, 'data'::text]))),
    CONSTRAINT run_artifacts_visibility_valid CHECK ((visibility = ANY (ARRAY['private'::text, 'shared'::text, 'public_example'::text])))
);


--
-- Name: run_attempts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_attempts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    offer_no integer NOT NULL,
    attempt_no integer,
    executor_type text NOT NULL,
    lease_id uuid NOT NULL,
    fencing_token bigint NOT NULL,
    runtime_token_id uuid,
    runtime_worker_id text,
    runtime_session_id uuid,
    node_id uuid,
    offered_by_core_instance_id uuid NOT NULL,
    attached_core_instance_id uuid NOT NULL,
    offered_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    offer_expires_at timestamp with time zone NOT NULL,
    accepted_at timestamp with time zone,
    last_renewed_at timestamp with time zone,
    lease_expires_at timestamp with time zone NOT NULL,
    attempt_deadline_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone,
    outcome text,
    result_id uuid,
    result_fingerprint bytea,
    result_classification text,
    result_acknowledged_at timestamp with time zone,
    last_client_event_seq bigint DEFAULT 0 NOT NULL,
    final_client_event_seq bigint,
    error_code text,
    error_detail_redacted text,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    slot_acquired_at timestamp with time zone,
    slot_released_at timestamp with time zone,
    active_runtime_session_id uuid,
    runtime_attachment_id uuid,
    CONSTRAINT run_attempts_acceptance_consistent CHECK ((((accepted_at IS NULL) AND (attempt_no IS NULL)) OR ((accepted_at IS NOT NULL) AND (attempt_no IS NOT NULL)))),
    CONSTRAINT run_attempts_attempt_positive CHECK (((attempt_no IS NULL) OR (attempt_no > 0))),
    CONSTRAINT run_attempts_event_sequences CHECK (((last_client_event_seq >= 0) AND ((final_client_event_seq IS NULL) OR (final_client_event_seq >= 0)))),
    CONSTRAINT run_attempts_executor_identity CHECK ((((executor_type = 'runtime'::text) AND (runtime_token_id IS NOT NULL) AND (runtime_worker_id IS NOT NULL) AND (runtime_session_id IS NOT NULL) AND (node_id IS NOT NULL)) OR ((executor_type = ANY (ARRAY['core_http'::text, 'core_mcp'::text])) AND (runtime_token_id IS NULL) AND (runtime_worker_id IS NULL) AND (runtime_session_id IS NULL) AND (node_id IS NULL)))),
    CONSTRAINT run_attempts_executor_valid CHECK ((executor_type = ANY (ARRAY['runtime'::text, 'core_http'::text, 'core_mcp'::text]))),
    CONSTRAINT run_attempts_fence_positive CHECK ((fencing_token > 0)),
    CONSTRAINT run_attempts_finished_consistent CHECK ((((finished_at IS NULL) AND (outcome IS NULL)) OR ((finished_at IS NOT NULL) AND (outcome IS NOT NULL)))),
    CONSTRAINT run_attempts_offer_positive CHECK ((offer_no > 0)),
    CONSTRAINT run_attempts_outcome_attempt_consistent CHECK (((outcome IS NULL) OR ((outcome = ANY (ARRAY['offer_rejected'::text, 'offer_expired'::text])) AND (attempt_no IS NULL)) OR (outcome = 'canceled'::text) OR ((outcome <> ALL (ARRAY['offer_rejected'::text, 'offer_expired'::text, 'canceled'::text])) AND (attempt_no IS NOT NULL)))),
    CONSTRAINT run_attempts_outcome_result_consistent CHECK (
CASE
    WHEN (outcome = 'success'::text) THEN ((NOT (result_classification IS DISTINCT FROM 'success'::text)) AND (result_id IS NOT NULL))
    WHEN (outcome = 'retryable_failure'::text) THEN ((NOT (result_classification IS DISTINCT FROM 'retryable_failure'::text)) AND (result_id IS NOT NULL))
    WHEN (outcome = 'non_retryable_failure'::text) THEN ((NOT (result_classification IS DISTINCT FROM 'non_retryable_failure'::text)) AND (result_id IS NOT NULL))
    WHEN (outcome = ANY (ARRAY['offer_rejected'::text, 'offer_expired'::text, 'lease_expired'::text, 'canceled'::text, 'result_unknown'::text])) THEN (result_id IS NULL)
    WHEN (outcome = 'timeout'::text) THEN ((result_id IS NULL) OR (result_classification = ANY (ARRAY['success'::text, 'retryable_failure'::text, 'non_retryable_failure'::text])))
    ELSE (result_id IS NULL)
END),
    CONSTRAINT run_attempts_outcome_valid CHECK (((outcome IS NULL) OR (outcome = ANY (ARRAY['offer_rejected'::text, 'offer_expired'::text, 'success'::text, 'retryable_failure'::text, 'non_retryable_failure'::text, 'lease_expired'::text, 'canceled'::text, 'timeout'::text, 'result_unknown'::text])))),
    CONSTRAINT run_attempts_result_ack_consistent CHECK (((result_acknowledged_at IS NULL) OR (result_id IS NOT NULL))),
    CONSTRAINT run_attempts_result_classification_valid CHECK (((result_classification IS NULL) OR (result_classification = ANY (ARRAY['success'::text, 'retryable_failure'::text, 'non_retryable_failure'::text])))),
    CONSTRAINT run_attempts_result_consistent CHECK ((((result_id IS NULL) AND (result_fingerprint IS NULL) AND (result_classification IS NULL) AND (final_client_event_seq IS NULL)) OR ((result_id IS NOT NULL) AND (result_fingerprint IS NOT NULL) AND (result_classification IS NOT NULL) AND (final_client_event_seq IS NOT NULL) AND (octet_length(result_fingerprint) = 32) AND (((outcome = 'timeout'::text) AND (final_client_event_seq >= last_client_event_seq)) OR ((outcome IS DISTINCT FROM 'timeout'::text) AND (final_client_event_seq = last_client_event_seq)))))),
    CONSTRAINT run_attempts_runtime_attachment_state CHECK (((runtime_attachment_id IS NULL) OR ((executor_type = 'runtime'::text) AND (accepted_at IS NOT NULL) AND (runtime_session_id IS NOT NULL)))),
    CONSTRAINT run_attempts_slot_shape CHECK ((((executor_type = 'runtime'::text) AND (slot_acquired_at IS NOT NULL) AND (((slot_released_at IS NULL) AND (active_runtime_session_id IS NOT NULL)) OR ((slot_released_at IS NOT NULL) AND (active_runtime_session_id IS NULL)))) OR ((executor_type = ANY (ARRAY['core_http'::text, 'core_mcp'::text])) AND (slot_acquired_at IS NULL) AND (slot_released_at IS NULL) AND (active_runtime_session_id IS NULL)))),
    CONSTRAINT run_attempts_slot_time_order CHECK (((slot_released_at IS NULL) OR (slot_released_at >= slot_acquired_at))),
    CONSTRAINT run_attempts_time_order CHECK (((offer_expires_at >= offered_at) AND (lease_expires_at >= offered_at) AND (attempt_deadline_at >= offered_at) AND (lease_expires_at <= attempt_deadline_at) AND ((accepted_at IS NULL) OR (accepted_at >= offered_at)) AND ((accepted_at IS NULL) OR (accepted_at <= lease_expires_at)) AND ((last_renewed_at IS NULL) OR ((accepted_at IS NOT NULL) AND (last_renewed_at >= accepted_at))) AND ((finished_at IS NULL) OR (finished_at >= offered_at))))
);


--
-- Name: run_cancellations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_cancellations (
    id uuid NOT NULL,
    run_id uuid NOT NULL,
    target_attempt_id uuid,
    state text DEFAULT 'requested'::text NOT NULL,
    requested_by_type text NOT NULL,
    requested_by_id uuid NOT NULL,
    reason text,
    requested_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    delivered_at timestamp with time zone,
    stopping_at timestamp with time zone,
    stopped_at timestamp with time zone,
    acknowledged_at timestamp with time zone,
    error_code text,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT run_cancellations_reason_len CHECK (((reason IS NULL) OR (char_length(reason) <= 500))),
    CONSTRAINT run_cancellations_requester_valid CHECK ((requested_by_type = ANY (ARRAY['user'::text, 'service_account'::text, 'agent'::text, 'instance_admin'::text, 'system'::text]))),
    CONSTRAINT run_cancellations_state_evidence CHECK (
CASE state
    WHEN 'requested'::text THEN ((delivered_at IS NULL) AND (stopping_at IS NULL) AND (stopped_at IS NULL) AND (acknowledged_at IS NULL) AND (error_code IS NULL))
    WHEN 'delivered'::text THEN ((target_attempt_id IS NOT NULL) AND (delivered_at IS NOT NULL) AND (stopping_at IS NULL) AND (stopped_at IS NULL) AND (acknowledged_at IS NULL) AND (error_code IS NULL))
    WHEN 'stopping'::text THEN ((target_attempt_id IS NOT NULL) AND (delivered_at IS NOT NULL) AND (stopping_at IS NOT NULL) AND (stopped_at IS NULL) AND (acknowledged_at IS NOT NULL) AND (error_code IS NULL))
    WHEN 'stopped'::text THEN ((stopped_at IS NOT NULL) AND (error_code IS NULL) AND (((target_attempt_id IS NULL) AND (delivered_at IS NULL) AND (stopping_at IS NULL) AND (acknowledged_at IS NULL)) OR ((target_attempt_id IS NOT NULL) AND (delivered_at IS NOT NULL) AND (acknowledged_at IS NOT NULL))))
    WHEN 'unsupported'::text THEN ((target_attempt_id IS NOT NULL) AND (delivered_at IS NOT NULL) AND (stopping_at IS NULL) AND (stopped_at IS NULL) AND (acknowledged_at IS NOT NULL) AND (error_code IS NOT NULL))
    WHEN 'failed'::text THEN ((target_attempt_id IS NOT NULL) AND (stopped_at IS NULL) AND (error_code IS NOT NULL))
    WHEN 'unconfirmed'::text THEN ((target_attempt_id IS NOT NULL) AND (stopped_at IS NULL) AND (error_code IS NOT NULL))
    ELSE false
END),
    CONSTRAINT run_cancellations_state_valid CHECK ((state = ANY (ARRAY['requested'::text, 'delivered'::text, 'stopping'::text, 'stopped'::text, 'unsupported'::text, 'failed'::text, 'unconfirmed'::text]))),
    CONSTRAINT run_cancellations_time_order CHECK ((((delivered_at IS NULL) OR (delivered_at >= requested_at)) AND ((stopping_at IS NULL) OR (stopping_at >= COALESCE(delivered_at, requested_at))) AND ((stopped_at IS NULL) OR (stopped_at >= COALESCE(stopping_at, delivered_at, requested_at))) AND ((acknowledged_at IS NULL) OR (acknowledged_at >= requested_at)) AND (updated_at >= requested_at)))
);


--
-- Name: run_dead_letters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_dead_letters (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    final_attempt_no integer NOT NULL,
    reason_code text NOT NULL,
    reason_redacted text,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT run_dead_letters_final_attempt_positive CHECK ((final_attempt_no > 0))
);


--
-- Name: run_delegations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_delegations (
    child_run_id uuid NOT NULL,
    parent_run_id uuid NOT NULL,
    caller_agent_id uuid NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT run_delegations_distinct_runs CHECK ((child_run_id <> parent_run_id)),
    CONSTRAINT run_delegations_reason_len CHECK ((char_length(reason) <= 500))
);


--
-- Name: run_deliveries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_deliveries (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    target_id uuid,
    user_id uuid NOT NULL,
    target_type text NOT NULL,
    target_url text NOT NULL,
    payload jsonb NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    response_status integer,
    response_body text,
    error_message text,
    attempt_count integer DEFAULT 0 NOT NULL,
    next_retry_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    effect_outbox_id uuid,
    CONSTRAINT run_deliveries_status_valid CHECK ((status = ANY (ARRAY['pending'::text, 'success'::text, 'failed'::text])))
);


--
-- Name: run_effect_outbox; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_effect_outbox (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    terminal_event_id uuid NOT NULL,
    effect_type text NOT NULL,
    target_key text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    available_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    lease_owner uuid,
    lease_expires_at timestamp with time zone,
    attempt_count integer DEFAULT 0 NOT NULL,
    max_attempts integer DEFAULT 12 NOT NULL,
    completed_at timestamp with time zone,
    dead_lettered_at timestamp with time zone,
    last_error text,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT run_effect_outbox_attempts CHECK (((attempt_count >= 0) AND ((max_attempts >= 1) AND (max_attempts <= 100)) AND (attempt_count <= max_attempts))),
    CONSTRAINT run_effect_outbox_completed_consistent CHECK ((((status = 'succeeded'::text) AND (completed_at IS NOT NULL)) OR ((status <> 'succeeded'::text) AND (completed_at IS NULL)))),
    CONSTRAINT run_effect_outbox_dead_letter_consistent CHECK ((((status = 'dead_letter'::text) AND (dead_lettered_at IS NOT NULL)) OR ((status <> 'dead_letter'::text) AND (dead_lettered_at IS NULL)))),
    CONSTRAINT run_effect_outbox_last_error_len CHECK (((last_error IS NULL) OR (char_length(last_error) <= 500))),
    CONSTRAINT run_effect_outbox_lease_consistent CHECK ((((status = 'processing'::text) AND (lease_owner IS NOT NULL) AND (lease_expires_at IS NOT NULL)) OR ((status <> 'processing'::text) AND (lease_owner IS NULL) AND (lease_expires_at IS NULL)))),
    CONSTRAINT run_effect_outbox_metadata_object CHECK ((jsonb_typeof(metadata) = 'object'::text)),
    CONSTRAINT run_effect_outbox_status_valid CHECK ((status = ANY (ARRAY['pending'::text, 'processing'::text, 'succeeded'::text, 'dead_letter'::text]))),
    CONSTRAINT run_effect_outbox_target_len CHECK (((char_length(target_key) >= 1) AND (char_length(target_key) <= 500))),
    CONSTRAINT run_effect_outbox_type_format CHECK ((effect_type ~ '^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$'::text))
);


--
-- Name: run_effect_replays; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_effect_replays (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    effect_outbox_id uuid NOT NULL,
    actor_type text NOT NULL,
    actor_id uuid,
    reason text NOT NULL,
    replayed_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT run_effect_replays_actor_type_valid CHECK ((((char_length(actor_type) >= 1) AND (char_length(actor_type) <= 100)) AND (actor_type ~ '^[a-z][a-z0-9_.-]*$'::text))),
    CONSTRAINT run_effect_replays_reason_len CHECK (((char_length(btrim(reason)) >= 1) AND (char_length(btrim(reason)) <= 500)))
);


--
-- Name: run_event_retention_watermarks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_event_retention_watermarks (
    run_id uuid NOT NULL,
    retained_through_sequence integer DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT run_event_retention_watermarks_sequence_nonnegative CHECK ((retained_through_sequence >= 0))
);


--
-- Name: run_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_events (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    parent_run_id uuid,
    sequence integer NOT NULL,
    event_type text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    client_event_id uuid,
    client_event_seq bigint,
    payload_fingerprint bytea,
    attempt_id uuid,
    attempt_no integer,
    fencing_token bigint,
    CONSTRAINT run_events_client_identity_consistent CHECK ((((client_event_id IS NULL) AND (client_event_seq IS NULL) AND (payload_fingerprint IS NULL) AND (attempt_id IS NULL) AND (attempt_no IS NULL) AND (fencing_token IS NULL)) OR ((client_event_id IS NOT NULL) AND (client_event_seq IS NOT NULL) AND (payload_fingerprint IS NOT NULL) AND (attempt_id IS NOT NULL) AND (attempt_no IS NOT NULL) AND (fencing_token IS NOT NULL) AND (client_event_seq > 0) AND (attempt_no > 0) AND (fencing_token > 0) AND (octet_length(payload_fingerprint) = 32)))),
    CONSTRAINT run_events_payload_object CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT run_events_sequence_positive CHECK ((sequence > 0)),
    CONSTRAINT run_events_type_format CHECK ((event_type ~ '^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$'::text))
);


--
-- Name: run_messages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_messages (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    event_sequence integer,
    role text NOT NULL,
    content text DEFAULT ''::text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT run_messages_content_len CHECK ((char_length(content) <= 10000)),
    CONSTRAINT run_messages_role_valid CHECK ((role = ANY (ARRAY['user'::text, 'agent'::text, 'tool'::text, 'platform'::text])))
);


--
-- Name: run_requirement_evidence; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_requirement_evidence (
    run_id uuid NOT NULL,
    task_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    user_id uuid NOT NULL,
    required_skill_ids text[] DEFAULT '{}'::text[] NOT NULL,
    required_mcp_tools text[] DEFAULT '{}'::text[] NOT NULL,
    agent_skill_ids text[] DEFAULT '{}'::text[] NOT NULL,
    matched_skill_ids text[] DEFAULT '{}'::text[] NOT NULL,
    missing_skill_ids text[] DEFAULT '{}'::text[] NOT NULL,
    used_mcp_tools text[] DEFAULT '{}'::text[] NOT NULL,
    missing_mcp_tools text[] DEFAULT '{}'::text[] NOT NULL,
    coverage_status text DEFAULT 'no_requirements'::text NOT NULL,
    evidence_source text DEFAULT 'web'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT run_requirement_evidence_coverage_status_check CHECK ((coverage_status = ANY (ARRAY['covered'::text, 'partial'::text, 'missing_requirements'::text, 'no_requirements'::text]))),
    CONSTRAINT run_requirement_evidence_evidence_source_check CHECK ((evidence_source = ANY (ARRAY['web'::text, 'mcp'::text, 'api'::text])))
);


--
-- Name: runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    input jsonb NOT NULL,
    output jsonb,
    status text DEFAULT 'running'::text NOT NULL,
    error_code text,
    error_message text,
    cost_cents integer NOT NULL,
    platform_fee_cents integer NOT NULL,
    creator_revenue_cents integer NOT NULL,
    duration_ms integer,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    source text DEFAULT 'web'::text NOT NULL,
    runtime_contract_id text DEFAULT 'openlinker.runtime.v2'::text NOT NULL,
    idempotency_key_hash bytea,
    idempotency_fingerprint bytea,
    request_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    connection_mode_snapshot text,
    endpoint_idempotency_snapshot boolean,
    dispatch_state text DEFAULT 'pending'::text NOT NULL,
    offer_count integer DEFAULT 0 NOT NULL,
    max_offer_count integer DEFAULT 20 NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    max_attempts integer DEFAULT 3 NOT NULL,
    next_attempt_at timestamp with time zone,
    dispatch_deadline_at timestamp with time zone,
    run_deadline_at timestamp with time zone,
    latest_attempt_id uuid,
    active_attempt_id uuid,
    lease_id uuid,
    fencing_token bigint DEFAULT 0 NOT NULL,
    executor_type text,
    active_core_instance_id uuid,
    runtime_node_id uuid,
    runtime_worker_id text,
    runtime_session_id uuid,
    lease_token_id uuid,
    lease_offered_at timestamp with time zone,
    lease_accepted_at timestamp with time zone,
    lease_expires_at timestamp with time zone,
    attempt_deadline_at timestamp with time zone,
    cancel_request_id uuid,
    cancel_state text,
    cancel_requested_at timestamp with time zone,
    cancel_acknowledged_at timestamp with time zone,
    cancel_reason text,
    result_id uuid,
    result_fingerprint bytea,
    terminal_event_id uuid,
    dead_lettered_at timestamp with time zone,
    replay_of_run_id uuid,
    CONSTRAINT runs_active_attempt_state CHECK ((((dispatch_state = ANY (ARRAY['offered'::text, 'executing'::text])) AND (active_attempt_id IS NOT NULL) AND (latest_attempt_id = active_attempt_id) AND (lease_id IS NOT NULL) AND (fencing_token > 0) AND (executor_type IS NOT NULL) AND (active_core_instance_id IS NOT NULL) AND (lease_offered_at IS NOT NULL) AND (lease_expires_at IS NOT NULL) AND (attempt_deadline_at IS NOT NULL)) OR ((dispatch_state <> ALL (ARRAY['offered'::text, 'executing'::text])) AND (active_attempt_id IS NULL) AND (lease_id IS NULL) AND (executor_type IS NULL) AND (active_core_instance_id IS NULL) AND (runtime_node_id IS NULL) AND (runtime_worker_id IS NULL) AND (runtime_session_id IS NULL) AND (lease_token_id IS NULL) AND (lease_offered_at IS NULL) AND (lease_accepted_at IS NULL) AND (lease_expires_at IS NULL) AND (attempt_deadline_at IS NULL)))),
    CONSTRAINT runs_cancel_state_valid CHECK (((cancel_state IS NULL) OR (cancel_state = ANY (ARRAY['requested'::text, 'delivered'::text, 'stopping'::text, 'stopped'::text, 'unsupported'::text, 'failed'::text, 'unconfirmed'::text])))),
    CONSTRAINT runs_cancel_summary_consistent CHECK ((((cancel_request_id IS NULL) AND (cancel_state IS NULL) AND (cancel_requested_at IS NULL) AND (cancel_acknowledged_at IS NULL) AND (cancel_reason IS NULL)) OR ((cancel_request_id IS NOT NULL) AND (cancel_state IS NOT NULL) AND (cancel_requested_at IS NOT NULL)))),
    CONSTRAINT runs_connection_mode_snapshot_valid CHECK (((connection_mode_snapshot IS NULL) OR (connection_mode_snapshot = ANY (ARRAY['direct_http'::text, 'mcp_server'::text, 'runtime'::text])))),
    CONSTRAINT runs_connection_snapshot_consistent CHECK ((((runtime_contract_id = 'legacy.pre-v2'::text) AND (connection_mode_snapshot IS NULL) AND (endpoint_idempotency_snapshot IS NULL)) OR ((runtime_contract_id = 'openlinker.runtime.v2'::text) AND (connection_mode_snapshot = ANY (ARRAY['direct_http'::text, 'mcp_server'::text, 'runtime'::text])) AND ((connection_mode_snapshot <> ALL (ARRAY['direct_http'::text, 'mcp_server'::text])) OR (endpoint_idempotency_snapshot IS NOT NULL))))),
    CONSTRAINT runs_cost_nonneg CHECK ((cost_cents >= 0)),
    CONSTRAINT runs_counter_ranges CHECK (((offer_count >= 0) AND ((max_offer_count >= 1) AND (max_offer_count <= 100)) AND (offer_count <= max_offer_count) AND (attempt_count >= 0) AND ((max_attempts >= 1) AND (max_attempts <= 20)) AND (attempt_count <= max_attempts) AND (fencing_token >= 0))),
    CONSTRAINT runs_dead_letter_consistent CHECK ((((dispatch_state = 'dead_letter'::text) AND (dead_lettered_at IS NOT NULL) AND (error_code = 'RUNTIME_RETRY_EXHAUSTED'::text)) OR ((dispatch_state <> 'dead_letter'::text) AND (dead_lettered_at IS NULL) AND ((runtime_contract_id = 'legacy.pre-v2'::text) OR (error_code IS DISTINCT FROM 'RUNTIME_RETRY_EXHAUSTED'::text))))),
    CONSTRAINT runs_deadline_order CHECK ((((runtime_contract_id = 'legacy.pre-v2'::text) AND (dispatch_deadline_at IS NULL) AND (run_deadline_at IS NULL)) OR ((runtime_contract_id = 'openlinker.runtime.v2'::text) AND (dispatch_deadline_at IS NOT NULL) AND (run_deadline_at IS NOT NULL) AND (dispatch_deadline_at <= run_deadline_at) AND ((lease_expires_at IS NULL) OR (lease_offered_at IS NULL) OR (lease_expires_at >= lease_offered_at)) AND ((attempt_deadline_at IS NULL) OR (lease_offered_at IS NULL) OR (attempt_deadline_at >= lease_offered_at)) AND ((attempt_deadline_at IS NULL) OR (attempt_deadline_at <= run_deadline_at))))),
    CONSTRAINT runs_dispatch_state_valid CHECK ((dispatch_state = ANY (ARRAY['pending'::text, 'offered'::text, 'executing'::text, 'retry_wait'::text, 'terminal'::text, 'dead_letter'::text]))),
    CONSTRAINT runs_executor_identity CHECK (((dispatch_state <> ALL (ARRAY['offered'::text, 'executing'::text])) OR ((executor_type = 'runtime'::text) AND (runtime_node_id IS NOT NULL) AND (runtime_worker_id IS NOT NULL) AND (runtime_session_id IS NOT NULL) AND (lease_token_id IS NOT NULL)) OR ((executor_type = ANY (ARRAY['core_http'::text, 'core_mcp'::text])) AND (runtime_node_id IS NULL) AND (runtime_worker_id IS NULL) AND (runtime_session_id IS NULL) AND (lease_token_id IS NULL)))),
    CONSTRAINT runs_fee_consistent CHECK ((cost_cents = (platform_fee_cents + creator_revenue_cents))),
    CONSTRAINT runs_finished_state_consistent CHECK ((((status = 'running'::text) AND (finished_at IS NULL)) OR ((status <> 'running'::text) AND (finished_at IS NOT NULL)))),
    CONSTRAINT runs_idempotency_consistent CHECK ((((runtime_contract_id = 'legacy.pre-v2'::text) AND (idempotency_key_hash IS NULL) AND (idempotency_fingerprint IS NULL)) OR ((runtime_contract_id = 'openlinker.runtime.v2'::text) AND (idempotency_key_hash IS NOT NULL) AND (idempotency_fingerprint IS NOT NULL) AND (octet_length(idempotency_key_hash) = 32) AND (octet_length(idempotency_fingerprint) = 32)))),
    CONSTRAINT runs_nonresult_terminal_output_consistent CHECK (((status <> ALL (ARRAY['timeout'::text, 'canceled'::text])) OR (output IS NULL))),
    CONSTRAINT runs_offer_acceptance_state CHECK ((((dispatch_state = 'offered'::text) AND (lease_accepted_at IS NULL)) OR ((dispatch_state = 'executing'::text) AND (lease_accepted_at IS NOT NULL)) OR (dispatch_state <> ALL (ARRAY['offered'::text, 'executing'::text])))),
    CONSTRAINT runs_replay_distinct CHECK (((replay_of_run_id IS NULL) OR (replay_of_run_id <> id))),
    CONSTRAINT runs_request_metadata_object CHECK ((jsonb_typeof(request_metadata) = 'object'::text)),
    CONSTRAINT runs_result_consistent CHECK ((((result_id IS NULL) AND (result_fingerprint IS NULL)) OR ((status = ANY (ARRAY['success'::text, 'failed'::text])) AND (result_id IS NOT NULL) AND (result_fingerprint IS NOT NULL) AND (octet_length(result_fingerprint) = 32)))),
    CONSTRAINT runs_retry_state_consistent CHECK ((((dispatch_state = 'retry_wait'::text) AND (next_attempt_at IS NOT NULL)) OR ((dispatch_state <> 'retry_wait'::text) AND (next_attempt_at IS NULL)))),
    CONSTRAINT runs_runtime_contract_id_len CHECK (((char_length(runtime_contract_id) >= 1) AND (char_length(runtime_contract_id) <= 200))),
    CONSTRAINT runs_runtime_contract_id_valid CHECK ((runtime_contract_id = ANY (ARRAY['legacy.pre-v2'::text, 'openlinker.runtime.v2'::text]))),
    CONSTRAINT runs_source_check CHECK ((source = ANY (ARRAY['web'::text, 'mcp'::text, 'api'::text]))),
    CONSTRAINT runs_status_dispatch_consistent CHECK ((((status = 'running'::text) AND (dispatch_state = ANY (ARRAY['pending'::text, 'offered'::text, 'executing'::text, 'retry_wait'::text]))) OR ((status = ANY (ARRAY['success'::text, 'failed'::text, 'timeout'::text, 'canceled'::text])) AND (dispatch_state = 'terminal'::text)) OR ((status = 'failed'::text) AND (dispatch_state = 'dead_letter'::text) AND (error_code = 'RUNTIME_RETRY_EXHAUSTED'::text)))),
    CONSTRAINT runs_status_valid CHECK ((status = ANY (ARRAY['running'::text, 'success'::text, 'failed'::text, 'timeout'::text, 'canceled'::text]))),
    CONSTRAINT runs_terminal_attempt_evidence CHECK (((status <> ALL (ARRAY['success'::text, 'failed'::text])) OR (latest_attempt_id IS NOT NULL) OR (runtime_contract_id = 'legacy.pre-v2'::text))),
    CONSTRAINT runs_terminal_event_consistent CHECK ((((dispatch_state = ANY (ARRAY['terminal'::text, 'dead_letter'::text])) AND (terminal_event_id IS NOT NULL)) OR ((dispatch_state <> ALL (ARRAY['terminal'::text, 'dead_letter'::text])) AND (terminal_event_id IS NULL))))
);


--
-- Name: runtime_cluster_control; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_cluster_control (
    singleton_id smallint DEFAULT 1 NOT NULL,
    mode text DEFAULT 'hard_maintenance'::text NOT NULL,
    expected_replicas integer DEFAULT 1 NOT NULL,
    cutover_id uuid DEFAULT gen_random_uuid() NOT NULL,
    drain_started_at timestamp with time zone,
    drain_deadline_at timestamp with time zone,
    hard_maintenance_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    reopened_at timestamp with time zone,
    version bigint DEFAULT 1 NOT NULL,
    updated_by_type text DEFAULT 'migration'::text NOT NULL,
    updated_by_id uuid,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT runtime_cluster_control_drain_order CHECK (((drain_deadline_at IS NULL) OR (drain_started_at IS NULL) OR (drain_deadline_at >= drain_started_at))),
    CONSTRAINT runtime_cluster_control_expected_replicas CHECK (((expected_replicas >= 1) AND (expected_replicas <= 1024))),
    CONSTRAINT runtime_cluster_control_mode_valid CHECK ((mode = ANY (ARRAY['normal'::text, 'draining'::text, 'hard_maintenance'::text]))),
    CONSTRAINT runtime_cluster_control_singleton CHECK ((singleton_id = 1)),
    CONSTRAINT runtime_cluster_control_version_positive CHECK ((version > 0))
);


--
-- Name: runtime_cluster_members; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_cluster_members (
    instance_id uuid NOT NULL,
    release_version text NOT NULL,
    release_commit text NOT NULL,
    schema_version integer NOT NULL,
    schema_checksum text NOT NULL,
    runtime_contract_id text NOT NULL,
    runtime_contract_digest text NOT NULL,
    started_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    heartbeat_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    draining boolean DEFAULT false NOT NULL,
    ready boolean DEFAULT false NOT NULL,
    CONSTRAINT runtime_cluster_members_checksum_format CHECK (((schema_checksum ~ '^[a-f0-9]{64}$'::text) AND (runtime_contract_digest ~ '^[a-f0-9]{64}$'::text))),
    CONSTRAINT runtime_cluster_members_release_len CHECK ((((char_length(release_version) >= 1) AND (char_length(release_version) <= 100)) AND ((char_length(release_commit) >= 1) AND (char_length(release_commit) <= 100)))),
    CONSTRAINT runtime_cluster_members_schema_version CHECK ((schema_version > 0))
);


--
-- Name: runtime_node_bindings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_node_bindings (
    credential_id uuid NOT NULL,
    node_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    public_key_thumbprint text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    binding_mode text DEFAULT 'mtls'::text NOT NULL,
    CONSTRAINT runtime_node_bindings_mode_valid CHECK ((binding_mode = ANY (ARRAY['mtls'::text, 'token_only'::text]))),
    CONSTRAINT runtime_node_bindings_thumbprint_format CHECK ((public_key_thumbprint ~ '^[a-f0-9]{64}$'::text))
);


--
-- Name: COLUMN runtime_node_bindings.binding_mode; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.runtime_node_bindings.binding_mode IS 'Authentication mode for this immutable credential-to-Node binding. token_only identity digests are selectors, never certificate factors.';


--
-- Name: runtime_node_certificates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_node_certificates (
    certificate_serial text NOT NULL,
    node_id uuid NOT NULL,
    public_key_thumbprint text NOT NULL,
    certificate_fingerprint text NOT NULL,
    not_before timestamp with time zone NOT NULL,
    not_after timestamp with time zone NOT NULL,
    issued_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    revoked_at timestamp with time zone,
    certificate_pem text,
    certificate_chain_pem text,
    trust_bundle_pem text,
    renew_after timestamp with time zone,
    CONSTRAINT runtime_node_certificates_fingerprint_format CHECK ((certificate_fingerprint ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT runtime_node_certificates_replay_material_shape CHECK ((((certificate_pem IS NULL) AND (certificate_chain_pem IS NULL) AND (trust_bundle_pem IS NULL) AND (renew_after IS NULL)) OR ((certificate_pem IS NOT NULL) AND (certificate_chain_pem IS NOT NULL) AND (trust_bundle_pem IS NOT NULL) AND (renew_after IS NOT NULL) AND (not_before < renew_after) AND (renew_after < not_after)))),
    CONSTRAINT runtime_node_certificates_revocation_time CHECK (((revoked_at IS NULL) OR (revoked_at >= not_before))),
    CONSTRAINT runtime_node_certificates_serial_format CHECK ((certificate_serial ~ '^[a-f0-9]+$'::text)),
    CONSTRAINT runtime_node_certificates_thumbprint_format CHECK ((public_key_thumbprint ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT runtime_node_certificates_validity CHECK ((not_before < not_after))
);


--
-- Name: runtime_nodes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_nodes (
    node_id uuid NOT NULL,
    display_name text NOT NULL,
    device_certificate_serial text NOT NULL,
    device_public_key_thumbprint text NOT NULL,
    node_version text NOT NULL,
    protocol_version integer NOT NULL,
    runtime_contract_id text NOT NULL,
    runtime_contract_digest text NOT NULL,
    features text[] NOT NULL,
    capacity integer DEFAULT 1 NOT NULL,
    inflight integer DEFAULT 0 NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    last_seen_at timestamp with time zone,
    draining_at timestamp with time zone,
    revoked_at timestamp with time zone,
    revoke_reason text,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT runtime_nodes_capacity CHECK ((((capacity >= 0) AND (capacity <= 1024)) AND ((inflight >= 0) AND (inflight <= 1024)) AND ((inflight <= capacity) OR (status = 'draining'::text)))),
    CONSTRAINT runtime_nodes_contract_current CHECK (((protocol_version = 2) AND (runtime_contract_id = 'openlinker.runtime.v2'::text) AND (((status = ANY (ARRAY['active'::text, 'draining'::text])) AND (runtime_contract_digest = ANY (ARRAY['3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'::text, '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'::text]))) OR (status = 'revoked'::text)))),
    CONSTRAINT runtime_nodes_contract_digest CHECK ((runtime_contract_digest ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT runtime_nodes_display_name_len CHECK (((char_length(display_name) >= 1) AND (char_length(display_name) <= 200))),
    CONSTRAINT runtime_nodes_lifecycle_consistent CHECK ((((status = 'active'::text) AND (draining_at IS NULL)) OR ((status = 'draining'::text) AND (draining_at IS NOT NULL)) OR (status = 'revoked'::text))),
    CONSTRAINT runtime_nodes_required_features CHECK ((public.runtime_v2_feature_set_is_valid(features) AND (features @> ARRAY['lease_fence'::text, 'assignment_confirm'::text, 'renew'::text, 'resume'::text, 'event_ack'::text, 'result_ack'::text, 'cancel'::text, 'persistent_spool'::text]) AND ((runtime_contract_digest <> '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'::text) OR (features @> ARRAY['session_drain'::text])))),
    CONSTRAINT runtime_nodes_revoke_consistent CHECK ((((status = 'revoked'::text) AND (revoked_at IS NOT NULL)) OR ((status <> 'revoked'::text) AND (revoked_at IS NULL)))),
    CONSTRAINT runtime_nodes_status_valid CHECK ((status = ANY (ARRAY['active'::text, 'draining'::text, 'revoked'::text]))),
    CONSTRAINT runtime_nodes_version_len CHECK (((char_length(node_version) >= 1) AND (char_length(node_version) <= 100)))
);


--
-- Name: runtime_pki_authorities; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_pki_authorities (
    authority_id text NOT NULL,
    certificate_pem text NOT NULL,
    encrypted_private_key bytea NOT NULL,
    not_before timestamp with time zone NOT NULL,
    not_after timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT runtime_pki_authorities_id_valid CHECK ((authority_id = ANY (ARRAY['root'::text, 'client-intermediate'::text, 'server-intermediate'::text]))),
    CONSTRAINT runtime_pki_authorities_validity CHECK ((not_before < not_after))
);


--
-- Name: runtime_resume_grants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_resume_grants (
    id uuid NOT NULL,
    run_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    lease_id uuid NOT NULL,
    fencing_token bigint NOT NULL,
    agent_id uuid NOT NULL,
    node_id uuid NOT NULL,
    worker_id text NOT NULL,
    source_session_id uuid NOT NULL,
    source_credential_id uuid NOT NULL,
    target_session_id uuid NOT NULL,
    target_credential_id uuid NOT NULL,
    permission text NOT NULL,
    granted_by_core_instance_id uuid NOT NULL,
    granted_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    first_used_at timestamp with time zone,
    revoked_at timestamp with time zone,
    revoked_by_type text,
    revoked_by_id uuid,
    revoke_reason text,
    CONSTRAINT runtime_resume_grants_distinct_sessions CHECK ((source_session_id <> target_session_id)),
    CONSTRAINT runtime_resume_grants_fence_positive CHECK ((fencing_token > 0)),
    CONSTRAINT runtime_resume_grants_permission_valid CHECK ((permission = ANY (ARRAY['upload_spool_only'::text, 'continue_execution'::text]))),
    CONSTRAINT runtime_resume_grants_revoke_evidence CHECK ((((revoked_at IS NULL) AND (revoked_by_type IS NULL) AND (revoked_by_id IS NULL) AND (revoke_reason IS NULL)) OR ((revoked_at IS NOT NULL) AND (revoked_by_type IS NOT NULL) AND (revoked_by_type = ANY (ARRAY['runtime_session'::text, 'core_instance'::text, 'system'::text, 'operator'::text])) AND (revoke_reason IS NOT NULL) AND ((char_length(revoke_reason) >= 1) AND (char_length(revoke_reason) <= 500)) AND ((revoked_by_type = 'system'::text) OR (revoked_by_id IS NOT NULL))))),
    CONSTRAINT runtime_resume_grants_time_order CHECK (((expires_at > granted_at) AND ((first_used_at IS NULL) OR (first_used_at >= granted_at)) AND ((revoked_at IS NULL) OR (revoked_at >= granted_at)) AND ((first_used_at IS NULL) OR (revoked_at IS NULL) OR (first_used_at <= revoked_at)))),
    CONSTRAINT runtime_resume_grants_worker_len CHECK (((char_length(worker_id) >= 1) AND (char_length(worker_id) <= 200)))
);


--
-- Name: runtime_schema_contracts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_schema_contracts (
    schema_version integer NOT NULL,
    migration_name text NOT NULL,
    runtime_contract_id text NOT NULL,
    runtime_contract_digest text NOT NULL,
    applied_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    is_current boolean DEFAULT true NOT NULL,
    CONSTRAINT runtime_schema_contracts_contract_id_len CHECK (((char_length(runtime_contract_id) >= 1) AND (char_length(runtime_contract_id) <= 200))),
    CONSTRAINT runtime_schema_contracts_digest_format CHECK ((runtime_contract_digest ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT runtime_schema_contracts_version_positive CHECK ((schema_version > 0))
);


--
-- Name: runtime_session_attachments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_session_attachments (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    runtime_session_id uuid NOT NULL,
    core_instance_id uuid NOT NULL,
    attachment_kind text NOT NULL,
    attached_at timestamp with time zone DEFAULT statement_timestamp() NOT NULL,
    detached_at timestamp with time zone,
    disconnect_reason text,
    transport text DEFAULT 'long_poll'::text NOT NULL,
    transport_reason text DEFAULT 'explicit'::text,
    transport_changed_at timestamp with time zone DEFAULT statement_timestamp() NOT NULL,
    CONSTRAINT runtime_session_attachments_detach_evidence CHECK ((((detached_at IS NULL) AND (disconnect_reason IS NULL)) OR ((detached_at IS NOT NULL) AND (disconnect_reason IS NOT NULL) AND ((char_length(disconnect_reason) >= 1) AND (char_length(disconnect_reason) <= 200))))),
    CONSTRAINT runtime_session_attachments_kind_valid CHECK ((attachment_kind = ANY (ARRAY['connected'::text, 'resumed'::text]))),
    CONSTRAINT runtime_session_attachments_live_transport_known CHECK (((detached_at IS NOT NULL) OR (transport = ANY (ARRAY['websocket'::text, 'long_poll'::text])))),
    CONSTRAINT runtime_session_attachments_time_order CHECK (((detached_at IS NULL) OR (detached_at >= attached_at))),
    CONSTRAINT runtime_session_attachments_transport_reason_valid CHECK ((((transport = 'unknown'::text) AND (detached_at IS NOT NULL) AND (transport_reason IS NULL)) OR ((transport = ANY (ARRAY['websocket'::text, 'long_poll'::text])) AND (transport_reason = ANY (ARRAY['explicit'::text, 'websocket_unavailable'::text, 'policy_forced'::text, 'recovery'::text]))))),
    CONSTRAINT runtime_session_attachments_transport_time_order CHECK (((transport_changed_at >= attached_at) AND ((detached_at IS NULL) OR (transport_changed_at <= detached_at)))),
    CONSTRAINT runtime_session_attachments_transport_valid CHECK ((transport = ANY (ARRAY['websocket'::text, 'long_poll'::text, 'unknown'::text])))
);


--
-- Name: runtime_sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_sessions (
    runtime_session_id uuid NOT NULL,
    node_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    credential_id uuid NOT NULL,
    worker_id text NOT NULL,
    session_epoch bigint NOT NULL,
    device_certificate_serial text NOT NULL,
    node_version text NOT NULL,
    protocol_version integer NOT NULL,
    runtime_contract_id text NOT NULL,
    runtime_contract_digest text NOT NULL,
    features text[] NOT NULL,
    capacity integer NOT NULL,
    inflight integer DEFAULT 0 NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    attached_core_instance_id uuid,
    connected_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    heartbeat_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    disconnected_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    drain_requested_at timestamp with time zone,
    drain_deadline_at timestamp with time zone,
    drain_reason_code text,
    resume_capacity integer,
    CONSTRAINT runtime_sessions_capacity CHECK ((((capacity >= 0) AND (capacity <= 1024)) AND ((inflight >= 0) AND (inflight <= 1024)) AND ((inflight <= capacity) OR (status = 'draining'::text)))),
    CONSTRAINT runtime_sessions_contract_current CHECK (((protocol_version = 2) AND (runtime_contract_id = 'openlinker.runtime.v2'::text) AND (((status = ANY (ARRAY['active'::text, 'draining'::text])) AND (runtime_contract_digest = ANY (ARRAY['3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'::text, '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'::text]))) OR (status = ANY (ARRAY['offline'::text, 'revoked'::text, 'closed'::text]))))),
    CONSTRAINT runtime_sessions_contract_digest CHECK ((runtime_contract_digest ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT runtime_sessions_disconnect_consistent CHECK ((((status = ANY (ARRAY['active'::text, 'draining'::text])) AND (disconnected_at IS NULL) AND (attached_core_instance_id IS NOT NULL)) OR ((status = ANY (ARRAY['offline'::text, 'revoked'::text, 'closed'::text])) AND (disconnected_at IS NOT NULL) AND (attached_core_instance_id IS NULL)))),
    CONSTRAINT runtime_sessions_drain_evidence_consistent CHECK ((((drain_requested_at IS NULL) AND (drain_deadline_at IS NULL) AND (drain_reason_code IS NULL) AND (resume_capacity IS NULL) AND (status <> 'draining'::text)) OR ((drain_requested_at IS NOT NULL) AND (drain_deadline_at IS NOT NULL) AND (drain_reason_code = btrim(drain_reason_code)) AND ((char_length(drain_reason_code) >= 1) AND (char_length(drain_reason_code) <= 120)) AND ((resume_capacity >= 0) AND (resume_capacity <= 1024)) AND (capacity = 0) AND (status = ANY (ARRAY['draining'::text, 'offline'::text, 'revoked'::text, 'closed'::text]))))),
    CONSTRAINT runtime_sessions_epoch_positive CHECK ((session_epoch > 0)),
    CONSTRAINT runtime_sessions_required_features CHECK ((public.runtime_v2_feature_set_is_valid(features) AND (features @> ARRAY['lease_fence'::text, 'assignment_confirm'::text, 'renew'::text, 'resume'::text, 'event_ack'::text, 'result_ack'::text, 'cancel'::text, 'persistent_spool'::text]) AND ((runtime_contract_digest <> '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'::text) OR (features @> ARRAY['session_drain'::text])))),
    CONSTRAINT runtime_sessions_status_valid CHECK ((status = ANY (ARRAY['active'::text, 'draining'::text, 'offline'::text, 'revoked'::text, 'closed'::text]))),
    CONSTRAINT runtime_sessions_worker_len CHECK (((char_length(worker_id) >= 1) AND (char_length(worker_id) <= 200)))
);


--
-- Name: runtime_signal_outbox; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_signal_outbox (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    event_type text NOT NULL,
    agent_id uuid NOT NULL,
    run_id uuid,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    available_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    lease_owner uuid,
    lease_expires_at timestamp with time zone,
    published_at timestamp with time zone,
    attempt_count integer DEFAULT 0 NOT NULL,
    last_error text,
    CONSTRAINT runtime_signal_outbox_attempts_nonnegative CHECK ((attempt_count >= 0)),
    CONSTRAINT runtime_signal_outbox_event_type_format CHECK ((event_type ~ '^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$'::text)),
    CONSTRAINT runtime_signal_outbox_lease_consistent CHECK ((((status = 'processing'::text) AND (lease_owner IS NOT NULL) AND (lease_expires_at IS NOT NULL)) OR ((status <> 'processing'::text) AND (lease_owner IS NULL) AND (lease_expires_at IS NULL)))),
    CONSTRAINT runtime_signal_outbox_payload_object CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT runtime_signal_outbox_published_consistent CHECK ((((status = 'published'::text) AND (published_at IS NOT NULL)) OR ((status <> 'published'::text) AND (published_at IS NULL)))),
    CONSTRAINT runtime_signal_outbox_status_valid CHECK ((status = ANY (ARRAY['pending'::text, 'processing'::text, 'published'::text])))
);


--
-- Name: runtime_wire_contracts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runtime_wire_contracts (
    runtime_contract_id text NOT NULL,
    runtime_contract_digest text NOT NULL,
    registered_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    support_tier text NOT NULL,
    CONSTRAINT runtime_wire_contracts_contract_id_len CHECK (((char_length(runtime_contract_id) >= 1) AND (char_length(runtime_contract_id) <= 200))),
    CONSTRAINT runtime_wire_contracts_digest_format CHECK ((runtime_contract_digest ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT runtime_wire_contracts_support_identity CHECK (((runtime_contract_id = 'openlinker.runtime.v2'::text) AND (((support_tier = 'current'::text) AND (runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'::text)) OR ((support_tier = 'previous'::text) AND (runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'::text)) OR ((support_tier = 'historical'::text) AND (runtime_contract_digest <> ALL (ARRAY['4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'::text, '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'::text])))))),
    CONSTRAINT runtime_wire_contracts_support_tier_valid CHECK ((support_tier = ANY (ARRAY['current'::text, 'previous'::text, 'historical'::text])))
);


--
-- Name: skill_proposals; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.skill_proposals (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    owner_user_id uuid NOT NULL,
    agent_id uuid,
    proposed_skill_id text NOT NULL,
    category text NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    source text DEFAULT 'manual'::text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    matched_skill_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT skill_proposals_category_format CHECK ((category ~ '^[a-z][a-z0-9_-]{1,79}$'::text)),
    CONSTRAINT skill_proposals_description_len CHECK (((char_length(description) >= 4) AND (char_length(description) <= 1000))),
    CONSTRAINT skill_proposals_id_format CHECK ((proposed_skill_id ~ '^[a-z][a-z0-9]*(?:[/_-][a-z0-9]+)*$'::text)),
    CONSTRAINT skill_proposals_name_len CHECK (((char_length(name) >= 1) AND (char_length(name) <= 120))),
    CONSTRAINT skill_proposals_source_valid CHECK ((source = ANY (ARRAY['manual'::text, 'imported_text'::text, 'imported_json'::text]))),
    CONSTRAINT skill_proposals_status_valid CHECK ((status = ANY (ARRAY['pending'::text, 'merged'::text, 'rejected'::text])))
);


--
-- Name: skill_test_cases; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.skill_test_cases (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    skill_id text NOT NULL,
    title text NOT NULL,
    input_json jsonb NOT NULL,
    judge_prompt text NOT NULL,
    sort_order integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT skill_test_cases_title_len CHECK (((char_length(title) >= 1) AND (char_length(title) <= 200)))
);


--
-- Name: skills; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.skills (
    id text NOT NULL,
    category text NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    sort_order integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT skills_id_format CHECK ((id ~ '^[a-z]+/[a-z0-9-]+$'::text))
);


--
-- Name: task_callback_deliveries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_callback_deliveries (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    subscription_id uuid NOT NULL,
    run_event_id uuid NOT NULL,
    payload jsonb NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    response_status integer,
    response_body text,
    error_message text,
    attempt_count integer DEFAULT 0 NOT NULL,
    next_retry_at timestamp with time zone,
    delivered_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    effect_outbox_id uuid,
    CONSTRAINT task_callback_deliveries_status_valid CHECK ((status = ANY (ARRAY['pending'::text, 'success'::text, 'failed'::text])))
);


--
-- Name: task_callback_subscriptions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_callback_subscriptions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    owner_user_id uuid NOT NULL,
    caller_agent_id uuid,
    target_url text NOT NULL,
    secret text NOT NULL,
    event_types text[] DEFAULT ARRAY['run.completed'::text, 'run.failed'::text] NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    consecutive_failures integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    auth_scheme text,
    auth_credentials text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT task_callback_subscriptions_auth_credentials_len CHECK (((auth_credentials IS NULL) OR (char_length(auth_credentials) <= 1000))),
    CONSTRAINT task_callback_subscriptions_auth_scheme_len CHECK (((auth_scheme IS NULL) OR ((char_length(auth_scheme) >= 1) AND (char_length(auth_scheme) <= 80)))),
    CONSTRAINT task_callback_subscriptions_event_types_nonempty CHECK ((cardinality(event_types) > 0)),
    CONSTRAINT task_callback_subscriptions_failures_nonneg CHECK ((consecutive_failures >= 0)),
    CONSTRAINT task_callback_subscriptions_metadata_object CHECK ((jsonb_typeof(metadata) = 'object'::text)),
    CONSTRAINT task_callback_subscriptions_status_valid CHECK ((status = ANY (ARRAY['active'::text, 'paused'::text, 'failed'::text, 'deleted'::text]))),
    CONSTRAINT task_callback_subscriptions_url_len CHECK (((char_length(target_url) >= 1) AND (char_length(target_url) <= 500)))
);


--
-- Name: task_queries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_queries (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    query text NOT NULL,
    parsed_skills text[] DEFAULT '{}'::text[] NOT NULL,
    recommended_agent_ids uuid[] DEFAULT '{}'::uuid[] NOT NULL,
    chosen_agent_id uuid,
    chosen_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    mcp_tools text[] DEFAULT '{}'::text[] NOT NULL,
    completed_at timestamp with time zone,
    completion_summary text,
    completion_run_id uuid,
    CONSTRAINT task_queries_completion_consistency CHECK ((((completed_at IS NULL) AND (completion_run_id IS NULL) AND (completion_summary IS NULL)) OR ((completed_at IS NOT NULL) AND (completion_run_id IS NOT NULL) AND (chosen_agent_id IS NOT NULL)))),
    CONSTRAINT task_queries_completion_summary_len CHECK (((completion_summary IS NULL) OR (char_length(completion_summary) <= 2000))),
    CONSTRAINT task_queries_query_len CHECK (((char_length(query) >= 4) AND (char_length(query) <= 500)))
);


--
-- Name: user_token_core_grants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_token_core_grants (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    token_id uuid NOT NULL,
    permission text NOT NULL,
    resource_type text NOT NULL,
    resource_id uuid,
    constraints jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT user_token_core_grants_constraints_object CHECK ((jsonb_typeof(constraints) = 'object'::text)),
    CONSTRAINT user_token_core_grants_permission_nonempty CHECK (((char_length(permission) >= 1) AND (char_length(permission) <= 80))),
    CONSTRAINT user_token_core_grants_resource_type_nonempty CHECK (((char_length(resource_type) >= 1) AND (char_length(resource_type) <= 40)))
);


--
-- Name: user_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_tokens (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    name text NOT NULL,
    prefix text NOT NULL,
    token_hash text NOT NULL,
    last_used_at timestamp with time zone,
    revoked_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    scopes text[] DEFAULT ARRAY['agents:run'::text] NOT NULL,
    expires_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT user_tokens_name_len CHECK (((char_length(name) >= 1) AND (char_length(name) <= 80))),
    CONSTRAINT user_tokens_prefix_format CHECK ((prefix ~ '^ol_user_[a-f0-9]+$'::text))
);


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    email text NOT NULL,
    password_hash text,
    oauth_provider text,
    oauth_id text,
    display_name text NOT NULL,
    avatar_url text,
    is_creator boolean DEFAULT false NOT NULL,
    creator_verified boolean DEFAULT false NOT NULL,
    is_admin boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    disabled_at timestamp with time zone,
    CONSTRAINT users_must_have_credential CHECK (((password_hash IS NOT NULL) OR ((oauth_provider IS NOT NULL) AND (oauth_id IS NOT NULL))))
);


--
-- Name: webhook_deliveries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.webhook_deliveries (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    agent_id uuid NOT NULL,
    run_id uuid NOT NULL,
    url text NOT NULL,
    payload jsonb NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    response_status integer,
    response_body text,
    error_message text,
    attempt_count integer DEFAULT 0 NOT NULL,
    next_retry_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    effect_outbox_id uuid,
    CONSTRAINT webhook_deliveries_status_valid CHECK ((status = ANY (ARRAY['pending'::text, 'success'::text, 'failed'::text])))
);


--
-- Name: workflow_nodes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflow_nodes (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    workflow_id uuid NOT NULL,
    node_key text NOT NULL,
    node_type text DEFAULT 'agent'::text NOT NULL,
    agent_id uuid NOT NULL,
    title text NOT NULL,
    config jsonb DEFAULT '{}'::jsonb NOT NULL,
    "position" integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT workflow_nodes_key_len CHECK (((char_length(node_key) >= 1) AND (char_length(node_key) <= 80))),
    CONSTRAINT workflow_nodes_position_nonnegative CHECK (("position" >= 0)),
    CONSTRAINT workflow_nodes_title_len CHECK (((char_length(title) >= 1) AND (char_length(title) <= 160))),
    CONSTRAINT workflow_nodes_type_valid CHECK ((node_type = 'agent'::text))
);


--
-- Name: workflow_run_cancellations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflow_run_cancellations (
    workflow_run_id uuid NOT NULL,
    id uuid NOT NULL,
    actor_user_id uuid NOT NULL,
    reason_code text NOT NULL,
    state text NOT NULL,
    error_code text,
    requested_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    applied_at timestamp with time zone,
    finished_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT workflow_run_cancellations_error_valid CHECK (((((state = 'unconfirmed'::text) AND (error_code = 'CANCEL_UNCONFIRMED'::text)) OR ((state <> 'unconfirmed'::text) AND (error_code IS NULL))) IS TRUE)),
    CONSTRAINT workflow_run_cancellations_reason_code_check CHECK ((reason_code = ANY (ARRAY['OWNER_CANCEL_REQUESTED'::text, 'CALLER_REQUESTED'::text, 'DEADLINE_EXCEEDED'::text]))),
    CONSTRAINT workflow_run_cancellations_state_check CHECK ((state = ANY (ARRAY['requested'::text, 'stopping'::text, 'stopped'::text, 'unconfirmed'::text, 'not_applied'::text]))),
    CONSTRAINT workflow_run_cancellations_time_order CHECK ((((applied_at IS NULL) OR (applied_at >= requested_at)) AND ((finished_at IS NULL) OR ((applied_at IS NOT NULL) AND (finished_at >= applied_at))))),
    CONSTRAINT workflow_run_cancellations_timestamps_valid CHECK (((((state = 'requested'::text) AND (applied_at IS NULL) AND (finished_at IS NULL)) OR ((state = 'stopping'::text) AND (applied_at IS NOT NULL) AND (finished_at IS NULL)) OR ((state = ANY (ARRAY['stopped'::text, 'unconfirmed'::text, 'not_applied'::text])) AND (applied_at IS NOT NULL) AND (finished_at IS NOT NULL))) IS TRUE))
);


--
-- Name: workflow_run_steps; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflow_run_steps (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    workflow_run_id uuid NOT NULL,
    workflow_node_id uuid NOT NULL,
    node_key text NOT NULL,
    agent_id uuid NOT NULL,
    run_id uuid,
    status text DEFAULT 'running'::text NOT NULL,
    input jsonb DEFAULT '{}'::jsonb NOT NULL,
    output jsonb,
    error_message text,
    sequence integer NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT workflow_run_steps_sequence_nonnegative CHECK ((sequence >= 0)),
    CONSTRAINT workflow_run_steps_status_valid CHECK ((status = ANY (ARRAY['running'::text, 'success'::text, 'failed'::text])))
);


--
-- Name: workflow_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflow_runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    workflow_id uuid NOT NULL,
    user_id uuid NOT NULL,
    status text DEFAULT 'running'::text NOT NULL,
    input jsonb DEFAULT '{}'::jsonb NOT NULL,
    output jsonb,
    error_message text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    max_attempts integer DEFAULT 3 NOT NULL,
    next_retry_at timestamp with time zone,
    claimed_at timestamp with time zone,
    last_worker_error text,
    CONSTRAINT workflow_runs_attempt_count_nonnegative CHECK ((attempt_count >= 0)),
    CONSTRAINT workflow_runs_max_attempts_valid CHECK (((max_attempts >= 1) AND (max_attempts <= 10))),
    CONSTRAINT workflow_runs_status_valid CHECK ((status = ANY (ARRAY['pending'::text, 'running'::text, 'paused'::text, 'canceled'::text, 'success'::text, 'failed'::text])))
);


--
-- Name: workflow_step_launches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflow_step_launches (
    workflow_run_id uuid NOT NULL,
    workflow_node_id uuid NOT NULL,
    workflow_run_step_id uuid,
    node_key text NOT NULL,
    launch_token uuid NOT NULL,
    idempotency_key_hash bytea NOT NULL,
    creation_fingerprint bytea NOT NULL,
    state text NOT NULL,
    run_id uuid,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT workflow_step_launches_creation_fingerprint_check CHECK ((octet_length(creation_fingerprint) = 32)),
    CONSTRAINT workflow_step_launches_idempotency_key_hash_check CHECK ((octet_length(idempotency_key_hash) = 32)),
    CONSTRAINT workflow_step_launches_node_key_valid CHECK (((node_key = btrim(node_key)) AND ((length(node_key) >= 1) AND (length(node_key) <= 80)))),
    CONSTRAINT workflow_step_launches_state_check CHECK ((state = ANY (ARRAY['claimed'::text, 'created'::text, 'attached'::text, 'invalidated'::text]))),
    CONSTRAINT workflow_step_launches_state_valid CHECK (((((state = 'claimed'::text) AND (run_id IS NULL)) OR ((state = ANY (ARRAY['created'::text, 'attached'::text])) AND (run_id IS NOT NULL)) OR ((state = 'invalidated'::text) AND (run_id IS NULL))) IS TRUE))
);


--
-- Name: workflows; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflows (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    name text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    edges jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT workflows_edges_array CHECK ((jsonb_typeof(edges) = 'array'::text)),
    CONSTRAINT workflows_name_len CHECK (((char_length(name) >= 1) AND (char_length(name) <= 120))),
    CONSTRAINT workflows_status_valid CHECK ((status = ANY (ARRAY['active'::text, 'archived'::text])))
);


--
-- Name: a2a_context_mappings a2a_context_mappings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.a2a_context_mappings
    ADD CONSTRAINT a2a_context_mappings_pkey PRIMARY KEY (id);


--
-- Name: a2a_context_mappings a2a_context_mappings_run_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.a2a_context_mappings
    ADD CONSTRAINT a2a_context_mappings_run_id_key UNIQUE (run_id);


--
-- Name: agent_action_approval_requests agent_action_approval_requests_approval_url_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_action_approval_requests
    ADD CONSTRAINT agent_action_approval_requests_approval_url_slug_key UNIQUE (approval_url_slug);


--
-- Name: agent_action_approval_requests agent_action_approval_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_action_approval_requests
    ADD CONSTRAINT agent_action_approval_requests_pkey PRIMARY KEY (id);


--
-- Name: agent_availability_alerts agent_availability_alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_availability_alerts
    ADD CONSTRAINT agent_availability_alerts_pkey PRIMARY KEY (id);


--
-- Name: agent_availability_snapshots agent_availability_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_availability_snapshots
    ADD CONSTRAINT agent_availability_snapshots_pkey PRIMARY KEY (agent_id);


--
-- Name: agent_call_policies agent_call_policies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_call_policies
    ADD CONSTRAINT agent_call_policies_pkey PRIMARY KEY (agent_id);


--
-- Name: agent_capabilities agent_capabilities_agent_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_capabilities
    ADD CONSTRAINT agent_capabilities_agent_unique UNIQUE (agent_id);


--
-- Name: agent_capabilities agent_capabilities_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_capabilities
    ADD CONSTRAINT agent_capabilities_pkey PRIMARY KEY (id);


--
-- Name: agent_examples agent_examples_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_examples
    ADD CONSTRAINT agent_examples_pkey PRIMARY KEY (id);


--
-- Name: agent_metric_snapshots agent_metric_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_metric_snapshots
    ADD CONSTRAINT agent_metric_snapshots_pkey PRIMARY KEY (agent_id, time_window);


--
-- Name: agent_onboarding_status agent_onboarding_status_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_onboarding_status
    ADD CONSTRAINT agent_onboarding_status_pkey PRIMARY KEY (agent_id);


--
-- Name: agent_tokens agent_runtime_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_tokens
    ADD CONSTRAINT agent_runtime_tokens_pkey PRIMARY KEY (id);


--
-- Name: agent_skill_benchmark_runs agent_skill_benchmark_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_skill_benchmark_runs
    ADD CONSTRAINT agent_skill_benchmark_runs_pkey PRIMARY KEY (id);


--
-- Name: agent_skill_scores agent_skill_scores_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_skill_scores
    ADD CONSTRAINT agent_skill_scores_pkey PRIMARY KEY (agent_id, skill_id);


--
-- Name: agent_skills agent_skills_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_skills
    ADD CONSTRAINT agent_skills_pkey PRIMARY KEY (agent_id, skill_id);


--
-- Name: agent_tokens agent_tokens_id_agent_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_tokens
    ADD CONSTRAINT agent_tokens_id_agent_unique UNIQUE (id, agent_id);


--
-- Name: agents agents_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agents
    ADD CONSTRAINT agents_pkey PRIMARY KEY (id);


--
-- Name: agents agents_slug_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agents
    ADD CONSTRAINT agents_slug_unique UNIQUE (slug);


--
-- Name: registry_listing_links cloud_listing_links_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registry_listing_links
    ADD CONSTRAINT cloud_listing_links_pkey PRIMARY KEY (id);


--
-- Name: core_instance_identity core_instance_identity_issuer_instance_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.core_instance_identity
    ADD CONSTRAINT core_instance_identity_issuer_instance_id_key UNIQUE (issuer_instance_id);


--
-- Name: core_instance_identity core_instance_identity_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.core_instance_identity
    ADD CONSTRAINT core_instance_identity_pkey PRIMARY KEY (singleton);


--
-- Name: delivery_targets delivery_targets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.delivery_targets
    ADD CONSTRAINT delivery_targets_pkey PRIMARY KEY (id);


--
-- Name: external_execution_cancellations external_execution_cancellati_caller_service_id_external_re_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_execution_cancellations
    ADD CONSTRAINT external_execution_cancellati_caller_service_id_external_re_key UNIQUE (caller_service_id, external_request_id);


--
-- Name: external_execution_cancellations external_execution_cancellations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_execution_cancellations
    ADD CONSTRAINT external_execution_cancellations_pkey PRIMARY KEY (id);


--
-- Name: external_execution_keys external_execution_keys_caller_service_id_external_request__key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_execution_keys
    ADD CONSTRAINT external_execution_keys_caller_service_id_external_request__key UNIQUE (caller_service_id, external_request_id, actor_user_id);


--
-- Name: external_execution_keys external_execution_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_execution_keys
    ADD CONSTRAINT external_execution_keys_pkey PRIMARY KEY (caller_service_id, external_request_id);


--
-- Name: external_executions external_executions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_executions
    ADD CONSTRAINT external_executions_pkey PRIMARY KEY (caller_service_id, external_request_id);


--
-- Name: oauth_login_codes oauth_login_codes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.oauth_login_codes
    ADD CONSTRAINT oauth_login_codes_pkey PRIMARY KEY (code_hash);


--
-- Name: proxy_run_artifacts proxy_run_artifacts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_run_artifacts
    ADD CONSTRAINT proxy_run_artifacts_pkey PRIMARY KEY (id);


--
-- Name: proxy_run_artifacts proxy_run_artifacts_unique_source; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_run_artifacts
    ADD CONSTRAINT proxy_run_artifacts_unique_source UNIQUE (proxy_run_id, source_artifact_id);


--
-- Name: proxy_runs proxy_runs_idempotency_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_runs
    ADD CONSTRAINT proxy_runs_idempotency_unique UNIQUE (registry_node_id, idempotency_key);


--
-- Name: proxy_runs proxy_runs_listing_idempotency_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_runs
    ADD CONSTRAINT proxy_runs_listing_idempotency_unique UNIQUE (registry_listing_id, idempotency_key);


--
-- Name: proxy_runs proxy_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_runs
    ADD CONSTRAINT proxy_runs_pkey PRIMARY KEY (id);


--
-- Name: registry_federation_invites registry_federation_invites_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registry_federation_invites
    ADD CONSTRAINT registry_federation_invites_pkey PRIMARY KEY (id);


--
-- Name: registry_listing_links registry_listing_links_node_agent_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registry_listing_links
    ADD CONSTRAINT registry_listing_links_node_agent_unique UNIQUE (registry_node_id, local_agent_id);


--
-- Name: registry_nodes registry_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registry_nodes
    ADD CONSTRAINT registry_nodes_pkey PRIMARY KEY (id);


--
-- Name: registry_peers registry_peers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registry_peers
    ADD CONSTRAINT registry_peers_pkey PRIMARY KEY (id);


--
-- Name: run_accounting_ledger run_accounting_ledger_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_accounting_ledger
    ADD CONSTRAINT run_accounting_ledger_pkey PRIMARY KEY (run_id);


--
-- Name: run_accounting_ledger run_accounting_ledger_terminal_event_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_accounting_ledger
    ADD CONSTRAINT run_accounting_ledger_terminal_event_id_key UNIQUE (terminal_event_id);


--
-- Name: run_artifact_chunks run_artifact_chunks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_artifact_chunks
    ADD CONSTRAINT run_artifact_chunks_pkey PRIMARY KEY (id);


--
-- Name: run_artifacts run_artifacts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_artifacts
    ADD CONSTRAINT run_artifacts_pkey PRIMARY KEY (id);


--
-- Name: run_attempts run_attempts_event_identity_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_event_identity_unique UNIQUE (run_id, id, attempt_no, fencing_token);


--
-- Name: run_attempts run_attempts_lease_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_lease_id_key UNIQUE (lease_id);


--
-- Name: run_attempts run_attempts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_pkey PRIMARY KEY (id);


--
-- Name: run_attempts run_attempts_run_attempt_fence_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_run_attempt_fence_unique UNIQUE (run_id, attempt_no, fencing_token);


--
-- Name: run_attempts run_attempts_run_attempt_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_run_attempt_unique UNIQUE (run_id, attempt_no);


--
-- Name: run_attempts run_attempts_run_fence_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_run_fence_unique UNIQUE (run_id, fencing_token);


--
-- Name: run_attempts run_attempts_run_id_id_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_run_id_id_unique UNIQUE (run_id, id);


--
-- Name: run_attempts run_attempts_run_offer_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_run_offer_unique UNIQUE (run_id, offer_no);


--
-- Name: run_attempts run_attempts_run_result_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_run_result_unique UNIQUE (run_id, result_id);


--
-- Name: run_cancellations run_cancellations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_cancellations
    ADD CONSTRAINT run_cancellations_pkey PRIMARY KEY (id);


--
-- Name: run_cancellations run_cancellations_run_id_id_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_cancellations
    ADD CONSTRAINT run_cancellations_run_id_id_unique UNIQUE (run_id, id);


--
-- Name: run_cancellations run_cancellations_run_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_cancellations
    ADD CONSTRAINT run_cancellations_run_unique UNIQUE (run_id);


--
-- Name: run_dead_letters run_dead_letters_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_dead_letters
    ADD CONSTRAINT run_dead_letters_pkey PRIMARY KEY (id);


--
-- Name: run_dead_letters run_dead_letters_run_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_dead_letters
    ADD CONSTRAINT run_dead_letters_run_id_key UNIQUE (run_id);


--
-- Name: run_delegations run_delegations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_delegations
    ADD CONSTRAINT run_delegations_pkey PRIMARY KEY (child_run_id);


--
-- Name: run_deliveries run_deliveries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_deliveries
    ADD CONSTRAINT run_deliveries_pkey PRIMARY KEY (id);


--
-- Name: run_effect_outbox run_effect_outbox_business_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_effect_outbox
    ADD CONSTRAINT run_effect_outbox_business_unique UNIQUE (run_id, effect_type, target_key);


--
-- Name: run_effect_outbox run_effect_outbox_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_effect_outbox
    ADD CONSTRAINT run_effect_outbox_pkey PRIMARY KEY (id);


--
-- Name: run_effect_replays run_effect_replays_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_effect_replays
    ADD CONSTRAINT run_effect_replays_pkey PRIMARY KEY (id);


--
-- Name: run_event_retention_watermarks run_event_retention_watermarks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_event_retention_watermarks
    ADD CONSTRAINT run_event_retention_watermarks_pkey PRIMARY KEY (run_id);


--
-- Name: run_events run_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_events
    ADD CONSTRAINT run_events_pkey PRIMARY KEY (id);


--
-- Name: run_events run_events_run_id_id_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_events
    ADD CONSTRAINT run_events_run_id_id_unique UNIQUE (run_id, id);


--
-- Name: run_events run_events_run_sequence_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_events
    ADD CONSTRAINT run_events_run_sequence_unique UNIQUE (run_id, sequence);


--
-- Name: run_messages run_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_messages
    ADD CONSTRAINT run_messages_pkey PRIMARY KEY (id);


--
-- Name: run_requirement_evidence run_requirement_evidence_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_requirement_evidence
    ADD CONSTRAINT run_requirement_evidence_pkey PRIMARY KEY (run_id);


--
-- Name: runs runs_id_agent_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_id_agent_unique UNIQUE (id, agent_id);


--
-- Name: runs runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_pkey PRIMARY KEY (id);


--
-- Name: runtime_cluster_control runtime_cluster_control_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_cluster_control
    ADD CONSTRAINT runtime_cluster_control_pkey PRIMARY KEY (singleton_id);


--
-- Name: runtime_cluster_members runtime_cluster_members_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_cluster_members
    ADD CONSTRAINT runtime_cluster_members_pkey PRIMARY KEY (instance_id);


--
-- Name: runtime_node_bindings runtime_node_bindings_node_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_node_bindings
    ADD CONSTRAINT runtime_node_bindings_node_id_key UNIQUE (node_id);


--
-- Name: runtime_node_bindings runtime_node_bindings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_node_bindings
    ADD CONSTRAINT runtime_node_bindings_pkey PRIMARY KEY (credential_id);


--
-- Name: runtime_node_bindings runtime_node_bindings_public_key_thumbprint_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_node_bindings
    ADD CONSTRAINT runtime_node_bindings_public_key_thumbprint_key UNIQUE (public_key_thumbprint);


--
-- Name: runtime_node_certificates runtime_node_certificates_certificate_fingerprint_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_node_certificates
    ADD CONSTRAINT runtime_node_certificates_certificate_fingerprint_key UNIQUE (certificate_fingerprint);


--
-- Name: runtime_node_certificates runtime_node_certificates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_node_certificates
    ADD CONSTRAINT runtime_node_certificates_pkey PRIMARY KEY (certificate_serial);


--
-- Name: runtime_nodes runtime_nodes_device_certificate_serial_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_nodes
    ADD CONSTRAINT runtime_nodes_device_certificate_serial_key UNIQUE (device_certificate_serial);


--
-- Name: runtime_nodes runtime_nodes_device_public_key_thumbprint_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_nodes
    ADD CONSTRAINT runtime_nodes_device_public_key_thumbprint_key UNIQUE (device_public_key_thumbprint);


--
-- Name: runtime_nodes runtime_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_nodes
    ADD CONSTRAINT runtime_nodes_pkey PRIMARY KEY (node_id);


--
-- Name: runtime_pki_authorities runtime_pki_authorities_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_pki_authorities
    ADD CONSTRAINT runtime_pki_authorities_pkey PRIMARY KEY (authority_id);


--
-- Name: runtime_resume_grants runtime_resume_grants_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_resume_grants
    ADD CONSTRAINT runtime_resume_grants_pkey PRIMARY KEY (id);


--
-- Name: runtime_schema_contracts runtime_schema_contracts_contract_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_schema_contracts
    ADD CONSTRAINT runtime_schema_contracts_contract_unique UNIQUE (schema_version, runtime_contract_id, runtime_contract_digest);


--
-- Name: runtime_schema_contracts runtime_schema_contracts_migration_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_schema_contracts
    ADD CONSTRAINT runtime_schema_contracts_migration_name_key UNIQUE (migration_name);


--
-- Name: runtime_schema_contracts runtime_schema_contracts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_schema_contracts
    ADD CONSTRAINT runtime_schema_contracts_pkey PRIMARY KEY (schema_version);


--
-- Name: runtime_session_attachments runtime_session_attachments_attempt_identity_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_session_attachments
    ADD CONSTRAINT runtime_session_attachments_attempt_identity_unique UNIQUE (id, runtime_session_id);


--
-- Name: runtime_session_attachments runtime_session_attachments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_session_attachments
    ADD CONSTRAINT runtime_session_attachments_pkey PRIMARY KEY (id);


--
-- Name: runtime_sessions runtime_sessions_attempt_identity_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_sessions
    ADD CONSTRAINT runtime_sessions_attempt_identity_unique UNIQUE (runtime_session_id, node_id, agent_id, credential_id, worker_id);


--
-- Name: runtime_sessions runtime_sessions_identity_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_sessions
    ADD CONSTRAINT runtime_sessions_identity_unique UNIQUE (node_id, agent_id, worker_id, session_epoch);


--
-- Name: runtime_sessions runtime_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_sessions
    ADD CONSTRAINT runtime_sessions_pkey PRIMARY KEY (runtime_session_id);


--
-- Name: runtime_signal_outbox runtime_signal_outbox_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_signal_outbox
    ADD CONSTRAINT runtime_signal_outbox_pkey PRIMARY KEY (id);


--
-- Name: runtime_wire_contracts runtime_wire_contracts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_wire_contracts
    ADD CONSTRAINT runtime_wire_contracts_pkey PRIMARY KEY (runtime_contract_id, runtime_contract_digest);


--
-- Name: skill_proposals skill_proposals_owner_skill_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.skill_proposals
    ADD CONSTRAINT skill_proposals_owner_skill_unique UNIQUE (owner_user_id, proposed_skill_id);


--
-- Name: skill_proposals skill_proposals_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.skill_proposals
    ADD CONSTRAINT skill_proposals_pkey PRIMARY KEY (id);


--
-- Name: skill_test_cases skill_test_cases_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.skill_test_cases
    ADD CONSTRAINT skill_test_cases_pkey PRIMARY KEY (id);


--
-- Name: skills skills_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.skills
    ADD CONSTRAINT skills_pkey PRIMARY KEY (id);


--
-- Name: task_callback_deliveries task_callback_deliveries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_callback_deliveries
    ADD CONSTRAINT task_callback_deliveries_pkey PRIMARY KEY (id);


--
-- Name: task_callback_subscriptions task_callback_subscriptions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_callback_subscriptions
    ADD CONSTRAINT task_callback_subscriptions_pkey PRIMARY KEY (id);


--
-- Name: task_queries task_queries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_queries
    ADD CONSTRAINT task_queries_pkey PRIMARY KEY (id);


--
-- Name: user_token_core_grants user_token_core_grants_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_token_core_grants
    ADD CONSTRAINT user_token_core_grants_pkey PRIMARY KEY (id);


--
-- Name: user_tokens api_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_tokens
    ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id);


--
-- Name: users users_email_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_email_unique UNIQUE (email);


--
-- Name: users users_oauth_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_oauth_unique UNIQUE (oauth_provider, oauth_id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: webhook_deliveries webhook_deliveries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhook_deliveries
    ADD CONSTRAINT webhook_deliveries_pkey PRIMARY KEY (id);


--
-- Name: workflow_nodes workflow_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_nodes
    ADD CONSTRAINT workflow_nodes_pkey PRIMARY KEY (id);


--
-- Name: workflow_run_cancellations workflow_run_cancellations_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run_cancellations
    ADD CONSTRAINT workflow_run_cancellations_id_key UNIQUE (id);


--
-- Name: workflow_run_cancellations workflow_run_cancellations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run_cancellations
    ADD CONSTRAINT workflow_run_cancellations_pkey PRIMARY KEY (workflow_run_id);


--
-- Name: workflow_run_steps workflow_run_steps_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run_steps
    ADD CONSTRAINT workflow_run_steps_pkey PRIMARY KEY (id);


--
-- Name: workflow_runs workflow_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_runs
    ADD CONSTRAINT workflow_runs_pkey PRIMARY KEY (id);


--
-- Name: workflow_step_launches workflow_step_launches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_step_launches
    ADD CONSTRAINT workflow_step_launches_pkey PRIMARY KEY (workflow_run_id, workflow_node_id);


--
-- Name: workflow_step_launches workflow_step_launches_workflow_run_step_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_step_launches
    ADD CONSTRAINT workflow_step_launches_workflow_run_step_id_key UNIQUE (workflow_run_step_id);


--
-- Name: workflows workflows_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflows
    ADD CONSTRAINT workflows_pkey PRIMARY KEY (id);


--
-- Name: idx_a2a_context_mappings_parent_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_a2a_context_mappings_parent_run ON public.a2a_context_mappings USING btree (parent_run_id, created_at) WHERE (parent_run_id IS NOT NULL);


--
-- Name: idx_a2a_context_mappings_trace; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_a2a_context_mappings_trace ON public.a2a_context_mappings USING btree (trace_id, created_at) WHERE (trace_id <> ''::text);


--
-- Name: idx_a2a_context_mappings_user_protocol_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_a2a_context_mappings_user_protocol_context ON public.a2a_context_mappings USING btree (user_id, agent_id, protocol_context_id, created_at DESC);


--
-- Name: idx_a2a_context_mappings_user_root; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_a2a_context_mappings_user_root ON public.a2a_context_mappings USING btree (user_id, root_context_id, created_at);


--
-- Name: idx_agent_approvals_agent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_approvals_agent ON public.agent_action_approval_requests USING btree (agent_id, created_at DESC);


--
-- Name: idx_agent_approvals_creator_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_approvals_creator_pending ON public.agent_action_approval_requests USING btree (requested_by_user_id, created_at DESC) WHERE (status = 'pending'::text);


--
-- Name: idx_agent_approvals_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_approvals_expiry ON public.agent_action_approval_requests USING btree (expires_at) WHERE (status = 'pending'::text);


--
-- Name: idx_agent_availability_alerts_creator; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_availability_alerts_creator ON public.agent_availability_alerts USING btree (creator_id, created_at DESC);


--
-- Name: idx_agent_availability_alerts_open; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_agent_availability_alerts_open ON public.agent_availability_alerts USING btree (agent_id, alert_type) WHERE (read_at IS NULL);


--
-- Name: idx_agent_availability_alerts_unread; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_availability_alerts_unread ON public.agent_availability_alerts USING btree (creator_id, created_at DESC) WHERE (read_at IS NULL);


--
-- Name: idx_agent_availability_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_availability_status ON public.agent_availability_snapshots USING btree (availability_status, updated_at DESC);


--
-- Name: idx_agent_capabilities_agent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_capabilities_agent ON public.agent_capabilities USING btree (agent_id);


--
-- Name: idx_agent_examples_agent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_examples_agent ON public.agent_examples USING btree (agent_id, sort_order, created_at);


--
-- Name: idx_agent_metric_snapshots_freshness; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_metric_snapshots_freshness ON public.agent_metric_snapshots USING btree (snapshotted_at);


--
-- Name: idx_agent_skill_scores_skill; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_skill_scores_skill ON public.agent_skill_scores USING btree (skill_id, status, average_score DESC);


--
-- Name: idx_agent_skills_skill; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_skills_skill ON public.agent_skills USING btree (skill_id, agent_id);


--
-- Name: idx_agent_tokens_agent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_tokens_agent ON public.agent_tokens USING btree (agent_id, created_at DESC) WHERE (agent_id IS NOT NULL);


--
-- Name: idx_agent_tokens_creator; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_tokens_creator ON public.agent_tokens USING btree (creator_user_id, created_at DESC);


--
-- Name: idx_agent_tokens_pending_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_tokens_pending_expiry ON public.agent_tokens USING btree (expires_at) WHERE ((status = 'pending_registration'::text) AND (revoked_at IS NULL));


--
-- Name: idx_agent_tokens_prefix_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agent_tokens_prefix_active ON public.agent_tokens USING btree (prefix) WHERE (revoked_at IS NULL);


--
-- Name: idx_agent_tokens_rotation_predecessor; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_agent_tokens_rotation_predecessor ON public.agent_tokens USING btree (rotation_predecessor_id) WHERE (rotation_predecessor_id IS NOT NULL);


--
-- Name: idx_agents_creator; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agents_creator ON public.agents USING btree (creator_id);


--
-- Name: idx_agents_lifecycle; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agents_lifecycle ON public.agents USING btree (lifecycle_status, created_at DESC);


--
-- Name: idx_agents_market_listing; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agents_market_listing ON public.agents USING btree (created_at DESC) WHERE ((visibility = 'public'::text) AND (lifecycle_status = 'active'::text));


--
-- Name: idx_agents_pending_certification; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agents_pending_certification ON public.agents USING btree (created_at DESC) WHERE (certification_status = 'pending'::text);


--
-- Name: idx_agents_tags; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_agents_tags ON public.agents USING gin (tags);


--
-- Name: idx_benchmark_runs_agent_skill; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_benchmark_runs_agent_skill ON public.agent_skill_benchmark_runs USING btree (agent_id, skill_id, started_at DESC);


--
-- Name: idx_benchmark_runs_batch; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_benchmark_runs_batch ON public.agent_skill_benchmark_runs USING btree (batch_id);


--
-- Name: idx_delivery_targets_default_per_user; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_delivery_targets_default_per_user ON public.delivery_targets USING btree (user_id) WHERE (is_default = true);


--
-- Name: idx_delivery_targets_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_delivery_targets_user ON public.delivery_targets USING btree (user_id, created_at DESC);


--
-- Name: idx_external_execution_cancellations_reconcile; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_external_execution_cancellations_reconcile ON public.external_execution_cancellations USING btree (state, requested_at, caller_service_id, external_request_id) WHERE (state = ANY (ARRAY['requested'::text, 'stopping'::text]));


--
-- Name: idx_external_executions_actor; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_external_executions_actor ON public.external_executions USING btree (actor_user_id, created_at DESC);


--
-- Name: idx_external_executions_execution; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_external_executions_execution ON public.external_executions USING btree (execution_kind, execution_id) WHERE (execution_id IS NOT NULL);


--
-- Name: idx_oauth_login_codes_expires_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_oauth_login_codes_expires_at ON public.oauth_login_codes USING btree (expires_at);


--
-- Name: idx_proxy_run_artifacts_proxy_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_run_artifacts_proxy_run ON public.proxy_run_artifacts USING btree (proxy_run_id, created_at);


--
-- Name: idx_proxy_runs_listing; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_runs_listing ON public.proxy_runs USING btree (registry_listing_id, created_at DESC);


--
-- Name: idx_proxy_runs_node_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_runs_node_pending ON public.proxy_runs USING btree (registry_node_id, status, created_at);


--
-- Name: idx_proxy_runs_node_retry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_runs_node_retry ON public.proxy_runs USING btree (registry_node_id, status, next_retry_at, created_at) WHERE (status = 'pending'::text);


--
-- Name: idx_proxy_runs_registry_run; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_proxy_runs_registry_run ON public.proxy_runs USING btree (registry_run_id);


--
-- Name: idx_proxy_runs_requester; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_runs_requester ON public.proxy_runs USING btree (requesting_user_id, created_at DESC);


--
-- Name: idx_registry_federation_invites_owner; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_registry_federation_invites_owner ON public.registry_federation_invites USING btree (owner_user_id, created_at DESC);


--
-- Name: idx_registry_federation_invites_token_prefix; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_registry_federation_invites_token_prefix ON public.registry_federation_invites USING btree (token_prefix, status);


--
-- Name: idx_registry_listing_links_agent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_registry_listing_links_agent ON public.registry_listing_links USING btree (local_agent_id, created_at DESC);


--
-- Name: idx_registry_listing_links_node; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_registry_listing_links_node ON public.registry_listing_links USING btree (registry_node_id, created_at DESC);


--
-- Name: idx_registry_listing_links_registry_listing; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_registry_listing_links_registry_listing ON public.registry_listing_links USING btree (registry_listing_id);


--
-- Name: idx_registry_nodes_owner; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_registry_nodes_owner ON public.registry_nodes USING btree (owner_user_id, created_at DESC);


--
-- Name: idx_registry_nodes_secret_prefix; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_registry_nodes_secret_prefix ON public.registry_nodes USING btree (secret_prefix) WHERE (revoked_at IS NULL);


--
-- Name: idx_registry_peers_owner; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_registry_peers_owner ON public.registry_peers USING btree (owner_user_id, created_at DESC);


--
-- Name: idx_registry_peers_owner_name; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_registry_peers_owner_name ON public.registry_peers USING btree (owner_user_id, lower(name));


--
-- Name: idx_run_artifact_chunks_artifact; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_artifact_chunks_artifact ON public.run_artifact_chunks USING btree (run_artifact_id, chunk_index);


--
-- Name: idx_run_artifact_chunks_run_source_index; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_run_artifact_chunks_run_source_index ON public.run_artifact_chunks USING btree (run_id, source_artifact_id, chunk_index);


--
-- Name: idx_run_artifacts_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_artifacts_run ON public.run_artifacts USING btree (run_id, created_at);


--
-- Name: idx_run_artifacts_run_source; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_run_artifacts_run_source ON public.run_artifacts USING btree (run_id, source_artifact_id) WHERE (source_artifact_id IS NOT NULL);


--
-- Name: idx_run_attempts_active_node; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_attempts_active_node ON public.run_attempts USING btree (node_id, run_id, attempt_no) WHERE ((finished_at IS NULL) AND (node_id IS NOT NULL));


--
-- Name: idx_run_attempts_active_runtime_session_slot; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_attempts_active_runtime_session_slot ON public.run_attempts USING btree (active_runtime_session_id, run_id, id) WHERE ((slot_released_at IS NULL) AND (active_runtime_session_id IS NOT NULL));


--
-- Name: idx_run_attempts_active_session; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_attempts_active_session ON public.run_attempts USING btree (runtime_session_id, run_id, attempt_no) WHERE ((finished_at IS NULL) AND (runtime_session_id IS NOT NULL));


--
-- Name: idx_run_attempts_lease_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_attempts_lease_expiry ON public.run_attempts USING btree (lease_expires_at, run_id) WHERE (finished_at IS NULL);


--
-- Name: idx_run_attempts_runtime_v2_execution_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_attempts_runtime_v2_execution_due ON public.run_attempts USING btree (lease_expires_at, attempt_deadline_at, run_id, id) WHERE ((finished_at IS NULL) AND (accepted_at IS NOT NULL));


--
-- Name: idx_run_attempts_runtime_v2_offer_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_attempts_runtime_v2_offer_due ON public.run_attempts USING btree (offer_expires_at, run_id, id) WHERE ((finished_at IS NULL) AND (accepted_at IS NULL));


--
-- Name: idx_run_attempts_unaccepted_session; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_run_attempts_unaccepted_session ON public.run_attempts USING btree (runtime_session_id) WHERE ((runtime_session_id IS NOT NULL) AND (accepted_at IS NULL) AND (finished_at IS NULL));


--
-- Name: idx_run_attempts_unfinished_run; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_run_attempts_unfinished_run ON public.run_attempts USING btree (run_id) WHERE (finished_at IS NULL);


--
-- Name: idx_run_cancellations_unsettled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_cancellations_unsettled ON public.run_cancellations USING btree (updated_at, id) WHERE (state = ANY (ARRAY['requested'::text, 'delivered'::text, 'stopping'::text, 'unconfirmed'::text]));


--
-- Name: idx_run_delegations_caller; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_delegations_caller ON public.run_delegations USING btree (caller_agent_id, created_at DESC);


--
-- Name: idx_run_delegations_parent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_delegations_parent ON public.run_delegations USING btree (parent_run_id, created_at);


--
-- Name: idx_run_deliveries_effect_outbox; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_run_deliveries_effect_outbox ON public.run_deliveries USING btree (effect_outbox_id) WHERE (effect_outbox_id IS NOT NULL);


--
-- Name: idx_run_deliveries_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_deliveries_pending ON public.run_deliveries USING btree (next_retry_at) WHERE ((status = 'pending'::text) AND (next_retry_at IS NOT NULL));


--
-- Name: idx_run_deliveries_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_deliveries_run ON public.run_deliveries USING btree (run_id, created_at DESC);


--
-- Name: idx_run_deliveries_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_deliveries_user ON public.run_deliveries USING btree (user_id, created_at DESC);


--
-- Name: idx_run_effect_outbox_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_effect_outbox_pending ON public.run_effect_outbox USING btree (available_at, created_at, id) WHERE (status = 'pending'::text);


--
-- Name: idx_run_effect_outbox_processing_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_effect_outbox_processing_expiry ON public.run_effect_outbox USING btree (lease_expires_at, id) WHERE (status = 'processing'::text);


--
-- Name: idx_run_effect_replays_effect; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_effect_replays_effect ON public.run_effect_replays USING btree (effect_outbox_id, replayed_at DESC, id DESC);


--
-- Name: idx_run_events_attempt_client_sequence; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_run_events_attempt_client_sequence ON public.run_events USING btree (run_id, attempt_no, client_event_seq) WHERE (client_event_seq IS NOT NULL);


--
-- Name: idx_run_events_client_event_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_run_events_client_event_id ON public.run_events USING btree (run_id, client_event_id) WHERE (client_event_id IS NOT NULL);


--
-- Name: idx_run_events_metric_cursor; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_events_metric_cursor ON public.run_events USING btree (created_at, id) INCLUDE (run_id);


--
-- Name: idx_run_events_parent_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_events_parent_run ON public.run_events USING btree (parent_run_id) WHERE (parent_run_id IS NOT NULL);


--
-- Name: idx_run_events_type_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_events_type_time ON public.run_events USING btree (event_type, created_at DESC);


--
-- Name: idx_run_messages_event_sequence; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_messages_event_sequence ON public.run_messages USING btree (run_id, event_sequence) WHERE (event_sequence IS NOT NULL);


--
-- Name: idx_run_messages_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_messages_run ON public.run_messages USING btree (run_id, created_at, id);


--
-- Name: idx_run_requirement_evidence_agent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_requirement_evidence_agent ON public.run_requirement_evidence USING btree (agent_id, created_at DESC);


--
-- Name: idx_run_requirement_evidence_task; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_run_requirement_evidence_task ON public.run_requirement_evidence USING btree (task_id, created_at DESC);


--
-- Name: idx_runs_agent_success_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_agent_success_time ON public.runs USING btree (agent_id, started_at DESC) WHERE (status = 'success'::text);


--
-- Name: idx_runs_agent_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_agent_time ON public.runs USING btree (agent_id, started_at DESC);


--
-- Name: idx_runs_idempotency_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_runs_idempotency_key ON public.runs USING btree (user_id, idempotency_key_hash) WHERE (idempotency_key_hash IS NOT NULL);


--
-- Name: idx_runs_replay_lineage; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_replay_lineage ON public.runs USING btree (replay_of_run_id, started_at, id) WHERE (replay_of_run_id IS NOT NULL);


--
-- Name: idx_runs_runtime_deadline; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_runtime_deadline ON public.runs USING btree (run_deadline_at, id) WHERE ((status = 'running'::text) AND (run_deadline_at IS NOT NULL));


--
-- Name: idx_runs_runtime_execution_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_runtime_execution_expiry ON public.runs USING btree (lease_expires_at, id) WHERE ((status = 'running'::text) AND (dispatch_state = 'executing'::text));


--
-- Name: idx_runs_runtime_offer_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_runtime_offer_expiry ON public.runs USING btree (lease_expires_at, id) WHERE ((status = 'running'::text) AND (dispatch_state = 'offered'::text));


--
-- Name: idx_runs_runtime_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_runtime_pending ON public.runs USING btree (agent_id, started_at, id) WHERE ((status = 'running'::text) AND (dispatch_state = 'pending'::text));


--
-- Name: idx_runs_runtime_pending_global; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_runtime_pending_global ON public.runs USING btree (started_at, id) WHERE ((status = 'running'::text) AND (dispatch_state = 'pending'::text));


--
-- Name: idx_runs_runtime_retry_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_runtime_retry_due ON public.runs USING btree (agent_id, next_attempt_at, started_at, id) WHERE ((status = 'running'::text) AND (dispatch_state = 'retry_wait'::text));


--
-- Name: idx_runs_runtime_retry_due_global; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_runtime_retry_due_global ON public.runs USING btree (next_attempt_at, started_at, id) WHERE ((status = 'running'::text) AND (dispatch_state = 'retry_wait'::text));


--
-- Name: idx_runs_runtime_v2_dispatch_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_runtime_v2_dispatch_due ON public.runs USING btree (dispatch_deadline_at, id) WHERE ((runtime_contract_id = 'openlinker.runtime.v2'::text) AND (status = 'running'::text) AND (cancel_request_id IS NULL) AND (dispatch_state = ANY (ARRAY['pending'::text, 'offered'::text, 'retry_wait'::text])));


--
-- Name: idx_runs_runtime_v2_run_deadline_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_runtime_v2_run_deadline_due ON public.runs USING btree (run_deadline_at, id) WHERE ((runtime_contract_id = 'openlinker.runtime.v2'::text) AND (status = 'running'::text) AND (cancel_request_id IS NULL));


--
-- Name: idx_runs_source_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_source_time ON public.runs USING btree (source, started_at DESC);


--
-- Name: idx_runs_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_status ON public.runs USING btree (status, started_at DESC);


--
-- Name: idx_runs_user_success_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_user_success_time ON public.runs USING btree (user_id, started_at DESC) WHERE (status = 'success'::text);


--
-- Name: idx_runs_user_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runs_user_time ON public.runs USING btree (user_id, started_at DESC);


--
-- Name: idx_runtime_cluster_members_heartbeat; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_cluster_members_heartbeat ON public.runtime_cluster_members USING btree (heartbeat_at, instance_id);


--
-- Name: idx_runtime_node_certificates_active_node; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_node_certificates_active_node ON public.runtime_node_certificates USING btree (node_id, not_after DESC) WHERE (revoked_at IS NULL);


--
-- Name: idx_runtime_node_certificates_retention; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_node_certificates_retention ON public.runtime_node_certificates USING btree (not_after, certificate_serial);


--
-- Name: idx_runtime_nodes_last_seen; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_nodes_last_seen ON public.runtime_nodes USING btree (last_seen_at, node_id) WHERE (status <> 'revoked'::text);


--
-- Name: idx_runtime_resume_grants_source_history; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_resume_grants_source_history ON public.runtime_resume_grants USING btree (source_session_id, granted_at DESC, id DESC);


--
-- Name: idx_runtime_resume_grants_target_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_resume_grants_target_active ON public.runtime_resume_grants USING btree (target_session_id, expires_at, attempt_id) WHERE (revoked_at IS NULL);


--
-- Name: idx_runtime_resume_grants_unrevoked_attempt; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_runtime_resume_grants_unrevoked_attempt ON public.runtime_resume_grants USING btree (attempt_id) WHERE (revoked_at IS NULL);


--
-- Name: idx_runtime_schema_contracts_current; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_runtime_schema_contracts_current ON public.runtime_schema_contracts USING btree (is_current) WHERE is_current;


--
-- Name: idx_runtime_session_attachments_active; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_runtime_session_attachments_active ON public.runtime_session_attachments USING btree (runtime_session_id) WHERE (detached_at IS NULL);


--
-- Name: idx_runtime_session_attachments_instance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_session_attachments_instance ON public.runtime_session_attachments USING btree (core_instance_id, attached_at DESC);


--
-- Name: idx_runtime_sessions_active_worker; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_runtime_sessions_active_worker ON public.runtime_sessions USING btree (node_id, agent_id, worker_id) WHERE (status = ANY (ARRAY['active'::text, 'draining'::text]));


--
-- Name: idx_runtime_sessions_agent_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_sessions_agent_status ON public.runtime_sessions USING btree (agent_id, status, heartbeat_at DESC);


--
-- Name: idx_runtime_sessions_credential_lifecycle; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_sessions_credential_lifecycle ON public.runtime_sessions USING btree (credential_id, runtime_session_id) WHERE (status = ANY (ARRAY['active'::text, 'draining'::text, 'offline'::text]));


--
-- Name: idx_runtime_sessions_heartbeat; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_sessions_heartbeat ON public.runtime_sessions USING btree (heartbeat_at, runtime_session_id) WHERE (status = ANY (ARRAY['active'::text, 'draining'::text]));


--
-- Name: idx_runtime_signal_outbox_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_signal_outbox_pending ON public.runtime_signal_outbox USING btree (available_at, created_at, id) WHERE (status = 'pending'::text);


--
-- Name: idx_runtime_signal_outbox_processing_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_runtime_signal_outbox_processing_expiry ON public.runtime_signal_outbox USING btree (lease_expires_at, id) WHERE (status = 'processing'::text);


--
-- Name: idx_runtime_wire_contracts_current; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_runtime_wire_contracts_current ON public.runtime_wire_contracts USING btree ((1)) WHERE (support_tier = 'current'::text);


--
-- Name: idx_runtime_wire_contracts_previous; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_runtime_wire_contracts_previous ON public.runtime_wire_contracts USING btree ((1)) WHERE (support_tier = 'previous'::text);


--
-- Name: idx_skill_proposals_matched_skill; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_skill_proposals_matched_skill ON public.skill_proposals USING btree (matched_skill_id);


--
-- Name: idx_skill_proposals_owner; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_skill_proposals_owner ON public.skill_proposals USING btree (owner_user_id, updated_at DESC);


--
-- Name: idx_skill_proposals_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_skill_proposals_status ON public.skill_proposals USING btree (status, updated_at DESC);


--
-- Name: idx_skill_test_cases_skill; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_skill_test_cases_skill ON public.skill_test_cases USING btree (skill_id, sort_order);


--
-- Name: idx_skills_category; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_skills_category ON public.skills USING btree (category, sort_order);


--
-- Name: idx_task_callback_deliveries_effect_outbox; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_task_callback_deliveries_effect_outbox ON public.task_callback_deliveries USING btree (effect_outbox_id) WHERE (effect_outbox_id IS NOT NULL);


--
-- Name: idx_task_callback_deliveries_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_callback_deliveries_pending ON public.task_callback_deliveries USING btree (next_retry_at) WHERE ((status = 'pending'::text) AND (next_retry_at IS NOT NULL));


--
-- Name: idx_task_callback_deliveries_subscription; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_callback_deliveries_subscription ON public.task_callback_deliveries USING btree (subscription_id, created_at DESC);


--
-- Name: idx_task_callback_deliveries_subscription_event; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_task_callback_deliveries_subscription_event ON public.task_callback_deliveries USING btree (subscription_id, run_event_id);


--
-- Name: idx_task_callback_subscriptions_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_callback_subscriptions_active ON public.task_callback_subscriptions USING btree (run_id) WHERE (status = 'active'::text);


--
-- Name: idx_task_callback_subscriptions_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_callback_subscriptions_run ON public.task_callback_subscriptions USING btree (run_id, created_at DESC) WHERE (status <> 'deleted'::text);


--
-- Name: idx_task_queries_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_queries_user ON public.task_queries USING btree (user_id, created_at DESC);


--
-- Name: idx_user_token_core_grants_identity; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_user_token_core_grants_identity ON public.user_token_core_grants USING btree (token_id, permission, resource_type, COALESCE(resource_id, '00000000-0000-0000-0000-000000000000'::uuid));


--
-- Name: idx_user_token_core_grants_token; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_token_core_grants_token ON public.user_token_core_grants USING btree (token_id, permission);


--
-- Name: idx_user_tokens_prefix_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_tokens_prefix_active ON public.user_tokens USING btree (prefix) WHERE (revoked_at IS NULL);


--
-- Name: idx_user_tokens_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_tokens_user ON public.user_tokens USING btree (user_id, created_at DESC);


--
-- Name: idx_users_creator; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_users_creator ON public.users USING btree (is_creator) WHERE ((is_creator = true) AND (deleted_at IS NULL));


--
-- Name: idx_users_disabled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_users_disabled ON public.users USING btree (disabled_at) WHERE ((disabled_at IS NOT NULL) AND (deleted_at IS NULL));


--
-- Name: idx_users_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_users_email ON public.users USING btree (email) WHERE (deleted_at IS NULL);


--
-- Name: idx_webhook_deliveries_agent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webhook_deliveries_agent ON public.webhook_deliveries USING btree (agent_id, created_at DESC);


--
-- Name: idx_webhook_deliveries_effect_outbox; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_webhook_deliveries_effect_outbox ON public.webhook_deliveries USING btree (effect_outbox_id) WHERE (effect_outbox_id IS NOT NULL);


--
-- Name: idx_webhook_deliveries_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webhook_deliveries_pending ON public.webhook_deliveries USING btree (next_retry_at) WHERE ((status = 'pending'::text) AND (next_retry_at IS NOT NULL));


--
-- Name: idx_webhook_deliveries_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webhook_deliveries_run ON public.webhook_deliveries USING btree (run_id);


--
-- Name: idx_workflow_nodes_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_workflow_nodes_key ON public.workflow_nodes USING btree (workflow_id, node_key);


--
-- Name: idx_workflow_nodes_order; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_workflow_nodes_order ON public.workflow_nodes USING btree (workflow_id, "position", created_at);


--
-- Name: idx_workflow_run_cancellations_reconcile; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_workflow_run_cancellations_reconcile ON public.workflow_run_cancellations USING btree (state, requested_at, workflow_run_id) WHERE (state = ANY (ARRAY['requested'::text, 'stopping'::text]));


--
-- Name: idx_workflow_run_steps_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_workflow_run_steps_run ON public.workflow_run_steps USING btree (workflow_run_id, sequence);


--
-- Name: idx_workflow_runs_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_workflow_runs_pending ON public.workflow_runs USING btree (status, next_retry_at, created_at) WHERE (status = ANY (ARRAY['pending'::text, 'running'::text]));


--
-- Name: idx_workflow_runs_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_workflow_runs_user ON public.workflow_runs USING btree (user_id, created_at DESC);


--
-- Name: idx_workflow_runs_workflow; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_workflow_runs_workflow ON public.workflow_runs USING btree (workflow_id, created_at DESC);


--
-- Name: idx_workflow_step_launches_reconcile; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_workflow_step_launches_reconcile ON public.workflow_step_launches USING btree (workflow_run_id, state, workflow_node_id) WHERE (state = ANY (ARRAY['claimed'::text, 'created'::text]));


--
-- Name: idx_workflows_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_workflows_user ON public.workflows USING btree (user_id, created_at DESC);


--
-- Name: a2a_context_mappings a2a_context_mappings_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER a2a_context_mappings_set_updated_at BEFORE UPDATE ON public.a2a_context_mappings FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: agent_availability_alerts agent_availability_alerts_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_availability_alerts_set_updated_at BEFORE UPDATE ON public.agent_availability_alerts FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: agent_call_policies agent_call_policies_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_call_policies_set_updated_at BEFORE UPDATE ON public.agent_call_policies FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: agent_tokens agent_tokens_identity_and_lifecycle; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_tokens_identity_and_lifecycle BEFORE DELETE OR UPDATE ON public.agent_tokens FOR EACH ROW EXECUTE FUNCTION public.enforce_agent_token_identity_and_lifecycle();


--
-- Name: agent_tokens agent_tokens_runtime_revocation_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_tokens_runtime_revocation_guard BEFORE UPDATE OF status, revoked_at, expires_at ON public.agent_tokens FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_token_revocation_guard();


--
-- Name: agents agents_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agents_set_updated_at BEFORE UPDATE ON public.agents FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: delivery_targets delivery_targets_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER delivery_targets_set_updated_at BEFORE UPDATE ON public.delivery_targets FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: external_execution_cancellations external_execution_cancellations_wake_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER external_execution_cancellations_wake_insert AFTER INSERT ON public.external_execution_cancellations FOR EACH ROW EXECUTE FUNCTION public.emit_external_execution_wake_notification();


--
-- Name: external_execution_cancellations external_execution_cancellations_wake_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER external_execution_cancellations_wake_update AFTER UPDATE ON public.external_execution_cancellations FOR EACH ROW WHEN (((old.state IS DISTINCT FROM new.state) OR (old.execution_kind_snapshot IS DISTINCT FROM new.execution_kind_snapshot) OR (old.execution_id_snapshot IS DISTINCT FROM new.execution_id_snapshot))) EXECUTE FUNCTION public.emit_external_execution_wake_notification();


--
-- Name: external_executions external_executions_cancellation_wake_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER external_executions_cancellation_wake_update AFTER UPDATE ON public.external_executions FOR EACH ROW WHEN (((old.start_state IS DISTINCT FROM new.start_state) OR (old.execution_kind IS DISTINCT FROM new.execution_kind) OR (old.execution_id IS DISTINCT FROM new.execution_id) OR (old.downstream_idempotency_key_hash IS DISTINCT FROM new.downstream_idempotency_key_hash) OR (old.downstream_creation_fingerprint IS DISTINCT FROM new.downstream_creation_fingerprint))) EXECUTE FUNCTION public.emit_external_execution_wake_notification();


--
-- Name: external_executions external_executions_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER external_executions_set_updated_at BEFORE UPDATE ON public.external_executions FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: proxy_runs proxy_runs_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER proxy_runs_set_updated_at BEFORE UPDATE ON public.proxy_runs FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: registry_federation_invites registry_federation_invites_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER registry_federation_invites_set_updated_at BEFORE UPDATE ON public.registry_federation_invites FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: registry_listing_links registry_listing_links_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER registry_listing_links_set_updated_at BEFORE UPDATE ON public.registry_listing_links FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: registry_nodes registry_nodes_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER registry_nodes_set_updated_at BEFORE UPDATE ON public.registry_nodes FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: registry_peers registry_peers_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER registry_peers_set_updated_at BEFORE UPDATE ON public.registry_peers FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: run_accounting_ledger run_accounting_ledger_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_accounting_ledger_immutable BEFORE DELETE OR UPDATE ON public.run_accounting_ledger FOR EACH ROW EXECUTE FUNCTION public.enforce_run_terminal_artifact_immutable();


--
-- Name: run_accounting_ledger run_accounting_ledger_run_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER run_accounting_ledger_run_consistency AFTER INSERT OR DELETE OR UPDATE ON public.run_accounting_ledger DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_run_terminal_artifacts_consistency();


--
-- Name: run_attempts run_attempts_active_run_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER run_attempts_active_run_consistency AFTER INSERT OR DELETE OR UPDATE ON public.run_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_run_active_attempt_consistency();


--
-- Name: run_attempts run_attempts_event_sequence_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER run_attempts_event_sequence_consistency AFTER INSERT OR DELETE OR UPDATE ON public.run_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_attempt_event_sequence_consistency();


--
-- Name: run_attempts run_attempts_identity_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_attempts_identity_immutable BEFORE DELETE OR UPDATE ON public.run_attempts FOR EACH ROW EXECUTE FUNCTION public.enforce_run_attempt_identity_immutable();


--
-- Name: run_attempts run_attempts_runtime_attachment_evidence; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_attempts_runtime_attachment_evidence BEFORE INSERT OR UPDATE ON public.run_attempts FOR EACH ROW EXECUTE FUNCTION public.enforce_run_attempt_runtime_attachment_evidence();


--
-- Name: run_attempts run_attempts_slot_evidence_forward_only; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_attempts_slot_evidence_forward_only BEFORE INSERT OR UPDATE ON public.run_attempts FOR EACH ROW EXECUTE FUNCTION public.enforce_run_attempt_slot_evidence();


--
-- Name: run_attempts run_attempts_slot_release_on_finish; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER run_attempts_slot_release_on_finish AFTER INSERT OR UPDATE ON public.run_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_run_attempt_slot_release_on_finish();


--
-- Name: run_cancellations run_cancellations_external_execution_wake_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_cancellations_external_execution_wake_insert AFTER INSERT ON public.run_cancellations FOR EACH ROW EXECUTE FUNCTION public.emit_external_execution_wake_notification();


--
-- Name: run_cancellations run_cancellations_external_execution_wake_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_cancellations_external_execution_wake_update AFTER UPDATE ON public.run_cancellations FOR EACH ROW WHEN ((old.state IS DISTINCT FROM new.state)) EXECUTE FUNCTION public.emit_external_execution_wake_notification();


--
-- Name: run_cancellations run_cancellations_run_summary_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER run_cancellations_run_summary_consistency AFTER INSERT OR DELETE OR UPDATE ON public.run_cancellations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_run_cancellation_summary_consistency();


--
-- Name: run_cancellations run_cancellations_state_forward_only; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_cancellations_state_forward_only BEFORE DELETE OR UPDATE ON public.run_cancellations FOR EACH ROW EXECUTE FUNCTION public.enforce_run_cancellation_transition();


--
-- Name: run_dead_letters run_dead_letters_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_dead_letters_immutable BEFORE DELETE OR UPDATE ON public.run_dead_letters FOR EACH ROW EXECUTE FUNCTION public.enforce_run_terminal_artifact_immutable();


--
-- Name: run_dead_letters run_dead_letters_run_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER run_dead_letters_run_consistency AFTER INSERT OR DELETE OR UPDATE ON public.run_dead_letters DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_run_terminal_artifacts_consistency();


--
-- Name: run_deliveries run_deliveries_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_deliveries_set_updated_at BEFORE UPDATE ON public.run_deliveries FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: run_effect_outbox run_effect_outbox_event_wake_due_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_effect_outbox_event_wake_due_update AFTER UPDATE ON public.run_effect_outbox FOR EACH ROW WHEN ((((new.status = 'pending'::text) AND (old.status IS DISTINCT FROM new.status)) OR ((new.status = 'pending'::text) AND (new.available_at < old.available_at)) OR ((new.status = 'processing'::text) AND (old.status = 'processing'::text) AND (new.lease_expires_at IS NOT NULL) AND ((old.lease_expires_at IS NULL) OR (new.lease_expires_at < old.lease_expires_at))))) EXECUTE FUNCTION public.emit_event_wake_notification('openlinker_work_v1', 'work.run_effect.available');


--
-- Name: run_effect_outbox run_effect_outbox_event_wake_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_effect_outbox_event_wake_insert AFTER INSERT ON public.run_effect_outbox FOR EACH ROW EXECUTE FUNCTION public.emit_event_wake_notification('openlinker_work_v1', 'work.run_effect.available');


--
-- Name: run_effect_outbox run_effect_outbox_identity_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_effect_outbox_identity_immutable BEFORE DELETE OR UPDATE ON public.run_effect_outbox FOR EACH ROW EXECUTE FUNCTION public.enforce_run_effect_identity_immutable();


--
-- Name: run_effect_outbox run_effect_outbox_run_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER run_effect_outbox_run_consistency AFTER INSERT OR DELETE OR UPDATE ON public.run_effect_outbox DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_run_terminal_artifacts_consistency();


--
-- Name: run_effect_replays run_effect_replays_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_effect_replays_immutable BEFORE DELETE OR UPDATE ON public.run_effect_replays FOR EACH ROW EXECUTE FUNCTION public.enforce_run_effect_replay_immutable();


--
-- Name: run_event_retention_watermarks run_event_retention_watermarks_forward_only; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_event_retention_watermarks_forward_only BEFORE INSERT OR DELETE OR UPDATE ON public.run_event_retention_watermarks FOR EACH ROW EXECUTE FUNCTION public.enforce_run_event_retention_watermark();


--
-- Name: run_events run_events_attempt_sequence_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER run_events_attempt_sequence_consistency AFTER INSERT OR DELETE ON public.run_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_attempt_event_sequence_consistency();


--
-- Name: run_events run_events_event_wake_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_events_event_wake_insert AFTER INSERT ON public.run_events FOR EACH ROW EXECUTE FUNCTION public.emit_event_wake_notification('openlinker_run_v1', 'run.changed');


--
-- Name: run_events run_events_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER run_events_immutable BEFORE DELETE OR UPDATE ON public.run_events FOR EACH ROW EXECUTE FUNCTION public.enforce_run_event_immutable();


--
-- Name: run_events run_events_terminal_artifacts_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER run_events_terminal_artifacts_consistency AFTER INSERT OR DELETE OR UPDATE ON public.run_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_run_terminal_artifacts_consistency();


--
-- Name: runs runs_active_attempt_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER runs_active_attempt_consistency AFTER INSERT OR DELETE OR UPDATE ON public.runs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_run_active_attempt_consistency();


--
-- Name: runs runs_cancellation_summary_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER runs_cancellation_summary_consistency AFTER INSERT OR DELETE OR UPDATE ON public.runs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_run_cancellation_summary_consistency();


--
-- Name: runs runs_event_wake_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER runs_event_wake_insert AFTER INSERT ON public.runs FOR EACH ROW EXECUTE FUNCTION public.emit_event_wake_notification('openlinker_run_v1', 'run.changed');


--
-- Name: runs runs_event_wake_state_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER runs_event_wake_state_update AFTER UPDATE ON public.runs FOR EACH ROW WHEN (((old.status IS DISTINCT FROM new.status) OR (old.dispatch_state IS DISTINCT FROM new.dispatch_state) OR (old.active_attempt_id IS DISTINCT FROM new.active_attempt_id) OR (old.attempt_count IS DISTINCT FROM new.attempt_count) OR (old.cancel_state IS DISTINCT FROM new.cancel_state) OR (old.result_id IS DISTINCT FROM new.result_id) OR (old.terminal_event_id IS DISTINCT FROM new.terminal_event_id))) EXECUTE FUNCTION public.emit_event_wake_notification('openlinker_run_v1', 'run.changed');


--
-- Name: runs runs_terminal_artifacts_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER runs_terminal_artifacts_consistency AFTER INSERT OR DELETE OR UPDATE ON public.runs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_run_terminal_artifacts_consistency();


--
-- Name: runs runs_v2_contract_identity; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER runs_v2_contract_identity BEFORE INSERT OR DELETE OR UPDATE ON public.runs FOR EACH ROW EXECUTE FUNCTION public.enforce_run_v2_contract_identity();


--
-- Name: runtime_nodes runtime_nodes_identity_and_lifecycle; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER runtime_nodes_identity_and_lifecycle BEFORE DELETE OR UPDATE ON public.runtime_nodes FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_node_identity_and_lifecycle();


--
-- Name: runtime_nodes runtime_nodes_revocation_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER runtime_nodes_revocation_guard BEFORE UPDATE OF status, revoked_at ON public.runtime_nodes FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_node_revocation_guard();


--
-- Name: runtime_nodes runtime_nodes_session_contract_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER runtime_nodes_session_contract_consistency AFTER INSERT OR DELETE OR UPDATE ON public.runtime_nodes DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_node_session_contract_consistency();


--
-- Name: runtime_resume_grants runtime_resume_grants_identity_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER runtime_resume_grants_identity_immutable BEFORE INSERT OR DELETE OR UPDATE ON public.runtime_resume_grants FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_resume_grant_identity();


--
-- Name: runtime_session_attachments runtime_session_attachments_history; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER runtime_session_attachments_history BEFORE DELETE OR UPDATE ON public.runtime_session_attachments FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_session_attachment_history();


--
-- Name: runtime_session_attachments runtime_session_attachments_session_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER runtime_session_attachments_session_consistency AFTER INSERT OR DELETE OR UPDATE ON public.runtime_session_attachments DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_session_attachment_consistency();


--
-- Name: runtime_sessions runtime_sessions_attachment_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER runtime_sessions_attachment_consistency AFTER INSERT OR DELETE OR UPDATE ON public.runtime_sessions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_session_attachment_consistency();


--
-- Name: runtime_sessions runtime_sessions_identity_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER runtime_sessions_identity_immutable BEFORE DELETE OR UPDATE ON public.runtime_sessions FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_session_identity_immutable();


--
-- Name: runtime_sessions runtime_sessions_node_contract_consistency; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER runtime_sessions_node_contract_consistency AFTER INSERT OR DELETE OR UPDATE ON public.runtime_sessions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_node_session_contract_consistency();


--
-- Name: runtime_sessions runtime_sessions_principal_valid; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER runtime_sessions_principal_valid BEFORE INSERT OR UPDATE ON public.runtime_sessions FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_session_principal();


--
-- Name: runtime_signal_outbox runtime_signal_outbox_event_wake_due_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER runtime_signal_outbox_event_wake_due_update AFTER UPDATE ON public.runtime_signal_outbox FOR EACH ROW WHEN ((((new.status = 'pending'::text) AND (old.status IS DISTINCT FROM new.status)) OR ((new.status = 'pending'::text) AND (new.available_at < old.available_at)) OR ((new.status = 'processing'::text) AND (old.status = 'processing'::text) AND (new.lease_expires_at IS NOT NULL) AND ((old.lease_expires_at IS NULL) OR (new.lease_expires_at < old.lease_expires_at))))) EXECUTE FUNCTION public.emit_event_wake_notification('openlinker_work_v1', 'work.runtime_signal.available');


--
-- Name: runtime_signal_outbox runtime_signal_outbox_event_wake_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER runtime_signal_outbox_event_wake_insert AFTER INSERT ON public.runtime_signal_outbox FOR EACH ROW EXECUTE FUNCTION public.emit_event_wake_notification('openlinker_work_v1', 'work.runtime_signal.available');


--
-- Name: skill_proposals skill_proposals_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER skill_proposals_set_updated_at BEFORE UPDATE ON public.skill_proposals FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: task_callback_deliveries task_callback_deliveries_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER task_callback_deliveries_set_updated_at BEFORE UPDATE ON public.task_callback_deliveries FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: task_callback_subscriptions task_callback_subscriptions_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER task_callback_subscriptions_set_updated_at BEFORE UPDATE ON public.task_callback_subscriptions FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: user_tokens user_tokens_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER user_tokens_set_updated_at BEFORE UPDATE ON public.user_tokens FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: users users_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER users_set_updated_at BEFORE UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: webhook_deliveries webhook_deliveries_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER webhook_deliveries_set_updated_at BEFORE UPDATE ON public.webhook_deliveries FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: workflow_run_cancellations workflow_run_cancellations_external_execution_wake_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER workflow_run_cancellations_external_execution_wake_insert AFTER INSERT ON public.workflow_run_cancellations FOR EACH ROW EXECUTE FUNCTION public.emit_external_execution_wake_notification();


--
-- Name: workflow_run_cancellations workflow_run_cancellations_external_execution_wake_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER workflow_run_cancellations_external_execution_wake_update AFTER UPDATE ON public.workflow_run_cancellations FOR EACH ROW WHEN ((old.state IS DISTINCT FROM new.state)) EXECUTE FUNCTION public.emit_external_execution_wake_notification();


--
-- Name: workflow_run_steps workflow_run_steps_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER workflow_run_steps_set_updated_at BEFORE UPDATE ON public.workflow_run_steps FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: workflow_runs workflow_runs_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER workflow_runs_set_updated_at BEFORE UPDATE ON public.workflow_runs FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: workflows workflows_set_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER workflows_set_updated_at BEFORE UPDATE ON public.workflows FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: a2a_context_mappings a2a_context_mappings_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.a2a_context_mappings
    ADD CONSTRAINT a2a_context_mappings_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: a2a_context_mappings a2a_context_mappings_caller_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.a2a_context_mappings
    ADD CONSTRAINT a2a_context_mappings_caller_agent_id_fkey FOREIGN KEY (caller_agent_id) REFERENCES public.agents(id) ON DELETE SET NULL;


--
-- Name: a2a_context_mappings a2a_context_mappings_parent_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.a2a_context_mappings
    ADD CONSTRAINT a2a_context_mappings_parent_run_id_fkey FOREIGN KEY (parent_run_id) REFERENCES public.runs(id) ON DELETE SET NULL;


--
-- Name: a2a_context_mappings a2a_context_mappings_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.a2a_context_mappings
    ADD CONSTRAINT a2a_context_mappings_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE;


--
-- Name: a2a_context_mappings a2a_context_mappings_target_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.a2a_context_mappings
    ADD CONSTRAINT a2a_context_mappings_target_agent_id_fkey FOREIGN KEY (target_agent_id) REFERENCES public.agents(id) ON DELETE SET NULL;


--
-- Name: a2a_context_mappings a2a_context_mappings_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.a2a_context_mappings
    ADD CONSTRAINT a2a_context_mappings_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: agent_action_approval_requests agent_action_approval_requests_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_action_approval_requests
    ADD CONSTRAINT agent_action_approval_requests_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: agent_action_approval_requests agent_action_approval_requests_decided_by_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_action_approval_requests
    ADD CONSTRAINT agent_action_approval_requests_decided_by_user_id_fkey FOREIGN KEY (decided_by_user_id) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: agent_action_approval_requests agent_action_approval_requests_requested_by_token_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_action_approval_requests
    ADD CONSTRAINT agent_action_approval_requests_requested_by_token_id_fkey FOREIGN KEY (requested_by_token_id) REFERENCES public.agent_tokens(id) ON DELETE SET NULL;


--
-- Name: agent_action_approval_requests agent_action_approval_requests_requested_by_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_action_approval_requests
    ADD CONSTRAINT agent_action_approval_requests_requested_by_user_id_fkey FOREIGN KEY (requested_by_user_id) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: agent_availability_alerts agent_availability_alerts_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_availability_alerts
    ADD CONSTRAINT agent_availability_alerts_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: agent_availability_alerts agent_availability_alerts_creator_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_availability_alerts
    ADD CONSTRAINT agent_availability_alerts_creator_id_fkey FOREIGN KEY (creator_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: agent_availability_snapshots agent_availability_snapshots_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_availability_snapshots
    ADD CONSTRAINT agent_availability_snapshots_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: agent_call_policies agent_call_policies_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_call_policies
    ADD CONSTRAINT agent_call_policies_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: agent_capabilities agent_capabilities_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_capabilities
    ADD CONSTRAINT agent_capabilities_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: agent_examples agent_examples_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_examples
    ADD CONSTRAINT agent_examples_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: agent_metric_snapshots agent_metric_snapshots_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_metric_snapshots
    ADD CONSTRAINT agent_metric_snapshots_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: agent_onboarding_status agent_onboarding_status_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_onboarding_status
    ADD CONSTRAINT agent_onboarding_status_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: agent_tokens agent_runtime_tokens_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_tokens
    ADD CONSTRAINT agent_runtime_tokens_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: agent_tokens agent_runtime_tokens_created_by_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_tokens
    ADD CONSTRAINT agent_runtime_tokens_created_by_user_id_fkey FOREIGN KEY (creator_user_id) REFERENCES public.users(id) ON DELETE RESTRICT;


--
-- Name: agent_skill_benchmark_runs agent_skill_benchmark_runs_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_skill_benchmark_runs
    ADD CONSTRAINT agent_skill_benchmark_runs_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: agent_skill_benchmark_runs agent_skill_benchmark_runs_skill_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_skill_benchmark_runs
    ADD CONSTRAINT agent_skill_benchmark_runs_skill_id_fkey FOREIGN KEY (skill_id) REFERENCES public.skills(id) ON DELETE CASCADE;


--
-- Name: agent_skill_benchmark_runs agent_skill_benchmark_runs_test_case_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_skill_benchmark_runs
    ADD CONSTRAINT agent_skill_benchmark_runs_test_case_id_fkey FOREIGN KEY (test_case_id) REFERENCES public.skill_test_cases(id) ON DELETE CASCADE;


--
-- Name: agent_skill_scores agent_skill_scores_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_skill_scores
    ADD CONSTRAINT agent_skill_scores_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: agent_skill_scores agent_skill_scores_skill_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_skill_scores
    ADD CONSTRAINT agent_skill_scores_skill_id_fkey FOREIGN KEY (skill_id) REFERENCES public.skills(id) ON DELETE CASCADE;


--
-- Name: agent_skills agent_skills_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_skills
    ADD CONSTRAINT agent_skills_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: agent_skills agent_skills_skill_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_skills
    ADD CONSTRAINT agent_skills_skill_id_fkey FOREIGN KEY (skill_id) REFERENCES public.skills(id) ON DELETE RESTRICT;


--
-- Name: agent_tokens agent_tokens_rotation_same_agent; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_tokens
    ADD CONSTRAINT agent_tokens_rotation_same_agent FOREIGN KEY (rotation_predecessor_id, agent_id) REFERENCES public.agent_tokens(id, agent_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: agents agents_creator_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agents
    ADD CONSTRAINT agents_creator_id_fkey FOREIGN KEY (creator_id) REFERENCES public.users(id) ON DELETE RESTRICT;


--
-- Name: registry_listing_links cloud_listing_links_local_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registry_listing_links
    ADD CONSTRAINT cloud_listing_links_local_agent_id_fkey FOREIGN KEY (local_agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: registry_listing_links cloud_listing_links_registry_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registry_listing_links
    ADD CONSTRAINT cloud_listing_links_registry_node_id_fkey FOREIGN KEY (registry_node_id) REFERENCES public.registry_nodes(id) ON DELETE CASCADE;


--
-- Name: delivery_targets delivery_targets_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.delivery_targets
    ADD CONSTRAINT delivery_targets_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: external_execution_cancellations external_execution_cancellations_key_actor_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_execution_cancellations
    ADD CONSTRAINT external_execution_cancellations_key_actor_fkey FOREIGN KEY (caller_service_id, external_request_id, actor_user_id) REFERENCES public.external_execution_keys(caller_service_id, external_request_id, actor_user_id) ON DELETE RESTRICT;


--
-- Name: external_execution_keys external_execution_keys_actor_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_execution_keys
    ADD CONSTRAINT external_execution_keys_actor_user_id_fkey FOREIGN KEY (actor_user_id) REFERENCES public.users(id) ON DELETE RESTRICT;


--
-- Name: external_executions external_executions_actor_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_executions
    ADD CONSTRAINT external_executions_actor_user_id_fkey FOREIGN KEY (actor_user_id) REFERENCES public.users(id) ON DELETE RESTRICT;


--
-- Name: external_executions external_executions_key_actor_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_executions
    ADD CONSTRAINT external_executions_key_actor_fkey FOREIGN KEY (caller_service_id, external_request_id, actor_user_id) REFERENCES public.external_execution_keys(caller_service_id, external_request_id, actor_user_id) ON DELETE RESTRICT;


--
-- Name: external_executions external_executions_legacy_rollback_target_owner_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_executions
    ADD CONSTRAINT external_executions_legacy_rollback_target_owner_id_fkey FOREIGN KEY (legacy_rollback_target_owner_id) REFERENCES public.users(id) ON DELETE RESTRICT;


--
-- Name: oauth_login_codes oauth_login_codes_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.oauth_login_codes
    ADD CONSTRAINT oauth_login_codes_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: proxy_run_artifacts proxy_run_artifacts_proxy_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_run_artifacts
    ADD CONSTRAINT proxy_run_artifacts_proxy_run_id_fkey FOREIGN KEY (proxy_run_id) REFERENCES public.proxy_runs(id) ON DELETE CASCADE;


--
-- Name: proxy_runs proxy_runs_cloud_listing_link_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_runs
    ADD CONSTRAINT proxy_runs_cloud_listing_link_id_fkey FOREIGN KEY (registry_listing_link_id) REFERENCES public.registry_listing_links(id) ON DELETE CASCADE;


--
-- Name: proxy_runs proxy_runs_local_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_runs
    ADD CONSTRAINT proxy_runs_local_agent_id_fkey FOREIGN KEY (local_agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: proxy_runs proxy_runs_registry_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_runs
    ADD CONSTRAINT proxy_runs_registry_node_id_fkey FOREIGN KEY (registry_node_id) REFERENCES public.registry_nodes(id) ON DELETE CASCADE;


--
-- Name: proxy_runs proxy_runs_requesting_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_runs
    ADD CONSTRAINT proxy_runs_requesting_user_id_fkey FOREIGN KEY (requesting_user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: registry_federation_invites registry_federation_invites_owner_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registry_federation_invites
    ADD CONSTRAINT registry_federation_invites_owner_user_id_fkey FOREIGN KEY (owner_user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: registry_nodes registry_nodes_owner_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registry_nodes
    ADD CONSTRAINT registry_nodes_owner_user_id_fkey FOREIGN KEY (owner_user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: registry_peers registry_peers_owner_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registry_peers
    ADD CONSTRAINT registry_peers_owner_user_id_fkey FOREIGN KEY (owner_user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: run_accounting_ledger run_accounting_ledger_run_agent_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_accounting_ledger
    ADD CONSTRAINT run_accounting_ledger_run_agent_fk FOREIGN KEY (run_id, agent_id) REFERENCES public.runs(id, agent_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: run_accounting_ledger run_accounting_ledger_terminal_event_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_accounting_ledger
    ADD CONSTRAINT run_accounting_ledger_terminal_event_fk FOREIGN KEY (run_id, terminal_event_id) REFERENCES public.run_events(run_id, id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: run_artifact_chunks run_artifact_chunks_run_artifact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_artifact_chunks
    ADD CONSTRAINT run_artifact_chunks_run_artifact_id_fkey FOREIGN KEY (run_artifact_id) REFERENCES public.run_artifacts(id) ON DELETE CASCADE;


--
-- Name: run_artifact_chunks run_artifact_chunks_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_artifact_chunks
    ADD CONSTRAINT run_artifact_chunks_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE;


--
-- Name: run_artifacts run_artifacts_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_artifacts
    ADD CONSTRAINT run_artifacts_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE;


--
-- Name: run_attempts run_attempts_active_runtime_session_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_active_runtime_session_fk FOREIGN KEY (active_runtime_session_id) REFERENCES public.runtime_sessions(runtime_session_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;


--
-- Name: run_attempts run_attempts_run_agent_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_run_agent_fk FOREIGN KEY (run_id, agent_id) REFERENCES public.runs(id, agent_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: run_attempts run_attempts_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id);


--
-- Name: run_attempts run_attempts_runtime_attachment_identity_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_runtime_attachment_identity_fk FOREIGN KEY (runtime_attachment_id, runtime_session_id) REFERENCES public.runtime_session_attachments(id, runtime_session_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;


--
-- Name: run_attempts run_attempts_session_identity_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_attempts
    ADD CONSTRAINT run_attempts_session_identity_fk FOREIGN KEY (runtime_session_id, node_id, agent_id, runtime_token_id, runtime_worker_id) REFERENCES public.runtime_sessions(runtime_session_id, node_id, agent_id, credential_id, worker_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;


--
-- Name: run_cancellations run_cancellations_run_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_cancellations
    ADD CONSTRAINT run_cancellations_run_fk FOREIGN KEY (run_id) REFERENCES public.runs(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: run_cancellations run_cancellations_target_attempt_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_cancellations
    ADD CONSTRAINT run_cancellations_target_attempt_fk FOREIGN KEY (run_id, target_attempt_id) REFERENCES public.run_attempts(run_id, id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: run_dead_letters run_dead_letters_run_attempt_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_dead_letters
    ADD CONSTRAINT run_dead_letters_run_attempt_fk FOREIGN KEY (run_id, final_attempt_no) REFERENCES public.run_attempts(run_id, attempt_no) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: run_delegations run_delegations_caller_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_delegations
    ADD CONSTRAINT run_delegations_caller_agent_id_fkey FOREIGN KEY (caller_agent_id) REFERENCES public.agents(id) ON DELETE RESTRICT;


--
-- Name: run_delegations run_delegations_child_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_delegations
    ADD CONSTRAINT run_delegations_child_run_id_fkey FOREIGN KEY (child_run_id) REFERENCES public.runs(id) ON DELETE CASCADE;


--
-- Name: run_delegations run_delegations_parent_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_delegations
    ADD CONSTRAINT run_delegations_parent_run_id_fkey FOREIGN KEY (parent_run_id) REFERENCES public.runs(id) ON DELETE CASCADE;


--
-- Name: run_deliveries run_deliveries_effect_outbox_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_deliveries
    ADD CONSTRAINT run_deliveries_effect_outbox_fk FOREIGN KEY (effect_outbox_id) REFERENCES public.run_effect_outbox(id) ON DELETE RESTRICT;


--
-- Name: run_deliveries run_deliveries_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_deliveries
    ADD CONSTRAINT run_deliveries_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE;


--
-- Name: run_deliveries run_deliveries_target_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_deliveries
    ADD CONSTRAINT run_deliveries_target_id_fkey FOREIGN KEY (target_id) REFERENCES public.delivery_targets(id) ON DELETE SET NULL;


--
-- Name: run_deliveries run_deliveries_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_deliveries
    ADD CONSTRAINT run_deliveries_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: run_effect_outbox run_effect_outbox_terminal_event_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_effect_outbox
    ADD CONSTRAINT run_effect_outbox_terminal_event_fk FOREIGN KEY (run_id, terminal_event_id) REFERENCES public.run_events(run_id, id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: run_effect_replays run_effect_replays_effect_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_effect_replays
    ADD CONSTRAINT run_effect_replays_effect_fk FOREIGN KEY (effect_outbox_id) REFERENCES public.run_effect_outbox(id);


--
-- Name: run_event_retention_watermarks run_event_retention_watermarks_run_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_event_retention_watermarks
    ADD CONSTRAINT run_event_retention_watermarks_run_fk FOREIGN KEY (run_id) REFERENCES public.runs(id);


--
-- Name: run_events run_events_attempt_identity_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_events
    ADD CONSTRAINT run_events_attempt_identity_fk FOREIGN KEY (run_id, attempt_id, attempt_no, fencing_token) REFERENCES public.run_attempts(run_id, id, attempt_no, fencing_token) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: run_events run_events_parent_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_events
    ADD CONSTRAINT run_events_parent_run_id_fkey FOREIGN KEY (parent_run_id) REFERENCES public.runs(id) ON DELETE SET NULL;


--
-- Name: run_events run_events_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_events
    ADD CONSTRAINT run_events_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE;


--
-- Name: run_messages run_messages_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_messages
    ADD CONSTRAINT run_messages_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE;


--
-- Name: run_requirement_evidence run_requirement_evidence_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_requirement_evidence
    ADD CONSTRAINT run_requirement_evidence_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: run_requirement_evidence run_requirement_evidence_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_requirement_evidence
    ADD CONSTRAINT run_requirement_evidence_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE;


--
-- Name: run_requirement_evidence run_requirement_evidence_task_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_requirement_evidence
    ADD CONSTRAINT run_requirement_evidence_task_id_fkey FOREIGN KEY (task_id) REFERENCES public.task_queries(id) ON DELETE CASCADE;


--
-- Name: run_requirement_evidence run_requirement_evidence_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_requirement_evidence
    ADD CONSTRAINT run_requirement_evidence_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: task_callback_subscriptions run_webhook_subscriptions_caller_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_callback_subscriptions
    ADD CONSTRAINT run_webhook_subscriptions_caller_agent_id_fkey FOREIGN KEY (caller_agent_id) REFERENCES public.agents(id) ON DELETE SET NULL;


--
-- Name: task_callback_subscriptions run_webhook_subscriptions_owner_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_callback_subscriptions
    ADD CONSTRAINT run_webhook_subscriptions_owner_user_id_fkey FOREIGN KEY (owner_user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: task_callback_subscriptions run_webhook_subscriptions_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_callback_subscriptions
    ADD CONSTRAINT run_webhook_subscriptions_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE;


--
-- Name: runs runs_active_attempt_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_active_attempt_fk FOREIGN KEY (id, active_attempt_id) REFERENCES public.run_attempts(run_id, id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: runs runs_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE RESTRICT;


--
-- Name: runs runs_cancellation_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_cancellation_fk FOREIGN KEY (id, cancel_request_id) REFERENCES public.run_cancellations(run_id, id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: runs runs_latest_attempt_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_latest_attempt_fk FOREIGN KEY (id, latest_attempt_id) REFERENCES public.run_attempts(run_id, id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: runs runs_lease_token_agent_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_lease_token_agent_fk FOREIGN KEY (lease_token_id, agent_id) REFERENCES public.agent_tokens(id, agent_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;


--
-- Name: runs runs_replay_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_replay_fk FOREIGN KEY (replay_of_run_id) REFERENCES public.runs(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: runs runs_result_attempt_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_result_attempt_fk FOREIGN KEY (id, result_id) REFERENCES public.run_attempts(run_id, result_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: runs runs_runtime_node_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_runtime_node_fk FOREIGN KEY (runtime_node_id) REFERENCES public.runtime_nodes(node_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;


--
-- Name: runs runs_runtime_session_identity_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_runtime_session_identity_fk FOREIGN KEY (runtime_session_id, runtime_node_id, agent_id, lease_token_id, runtime_worker_id) REFERENCES public.runtime_sessions(runtime_session_id, node_id, agent_id, credential_id, worker_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;


--
-- Name: runs runs_terminal_event_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_terminal_event_fk FOREIGN KEY (id, terminal_event_id) REFERENCES public.run_events(run_id, id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: runs runs_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE RESTRICT;


--
-- Name: runtime_cluster_members runtime_cluster_members_schema_contract_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_cluster_members
    ADD CONSTRAINT runtime_cluster_members_schema_contract_fk FOREIGN KEY (schema_version, runtime_contract_id, runtime_contract_digest) REFERENCES public.runtime_schema_contracts(schema_version, runtime_contract_id, runtime_contract_digest) ON DELETE RESTRICT;


--
-- Name: runtime_node_bindings runtime_node_bindings_credential_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_node_bindings
    ADD CONSTRAINT runtime_node_bindings_credential_id_fkey FOREIGN KEY (credential_id) REFERENCES public.agent_tokens(id) ON DELETE RESTRICT;


--
-- Name: runtime_node_bindings runtime_node_bindings_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_node_bindings
    ADD CONSTRAINT runtime_node_bindings_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.runtime_nodes(node_id) ON DELETE RESTRICT;


--
-- Name: runtime_node_bindings runtime_node_bindings_token_agent_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_node_bindings
    ADD CONSTRAINT runtime_node_bindings_token_agent_fk FOREIGN KEY (credential_id, agent_id) REFERENCES public.agent_tokens(id, agent_id) ON DELETE RESTRICT;


--
-- Name: runtime_node_certificates runtime_node_certificates_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_node_certificates
    ADD CONSTRAINT runtime_node_certificates_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.runtime_nodes(node_id) ON DELETE RESTRICT;


--
-- Name: runtime_nodes runtime_nodes_contract_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_nodes
    ADD CONSTRAINT runtime_nodes_contract_fk FOREIGN KEY (runtime_contract_id, runtime_contract_digest) REFERENCES public.runtime_wire_contracts(runtime_contract_id, runtime_contract_digest) ON DELETE RESTRICT;


--
-- Name: runtime_resume_grants runtime_resume_grants_attempt_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_resume_grants
    ADD CONSTRAINT runtime_resume_grants_attempt_fk FOREIGN KEY (run_id, attempt_id) REFERENCES public.run_attempts(run_id, id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: runtime_resume_grants runtime_resume_grants_source_session_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_resume_grants
    ADD CONSTRAINT runtime_resume_grants_source_session_fk FOREIGN KEY (source_session_id, node_id, agent_id, source_credential_id, worker_id) REFERENCES public.runtime_sessions(runtime_session_id, node_id, agent_id, credential_id, worker_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;


--
-- Name: runtime_resume_grants runtime_resume_grants_target_session_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_resume_grants
    ADD CONSTRAINT runtime_resume_grants_target_session_fk FOREIGN KEY (target_session_id, node_id, agent_id, target_credential_id, worker_id) REFERENCES public.runtime_sessions(runtime_session_id, node_id, agent_id, credential_id, worker_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;


--
-- Name: runtime_schema_contracts runtime_schema_contracts_wire_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_schema_contracts
    ADD CONSTRAINT runtime_schema_contracts_wire_fk FOREIGN KEY (runtime_contract_id, runtime_contract_digest) REFERENCES public.runtime_wire_contracts(runtime_contract_id, runtime_contract_digest) ON DELETE RESTRICT;


--
-- Name: runtime_session_attachments runtime_session_attachments_runtime_session_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_session_attachments
    ADD CONSTRAINT runtime_session_attachments_runtime_session_id_fkey FOREIGN KEY (runtime_session_id) REFERENCES public.runtime_sessions(runtime_session_id) ON DELETE RESTRICT;


--
-- Name: runtime_sessions runtime_sessions_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_sessions
    ADD CONSTRAINT runtime_sessions_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE RESTRICT;


--
-- Name: runtime_sessions runtime_sessions_contract_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_sessions
    ADD CONSTRAINT runtime_sessions_contract_fk FOREIGN KEY (runtime_contract_id, runtime_contract_digest) REFERENCES public.runtime_wire_contracts(runtime_contract_id, runtime_contract_digest) ON DELETE RESTRICT;


--
-- Name: runtime_sessions runtime_sessions_credential_agent_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_sessions
    ADD CONSTRAINT runtime_sessions_credential_agent_fk FOREIGN KEY (credential_id, agent_id) REFERENCES public.agent_tokens(id, agent_id) ON DELETE RESTRICT;


--
-- Name: runtime_sessions runtime_sessions_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_sessions
    ADD CONSTRAINT runtime_sessions_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.runtime_nodes(node_id) ON DELETE RESTRICT;


--
-- Name: runtime_signal_outbox runtime_signal_outbox_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_signal_outbox
    ADD CONSTRAINT runtime_signal_outbox_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE RESTRICT;


--
-- Name: runtime_signal_outbox runtime_signal_outbox_run_agent_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runtime_signal_outbox
    ADD CONSTRAINT runtime_signal_outbox_run_agent_fk FOREIGN KEY (run_id, agent_id) REFERENCES public.runs(id, agent_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: skill_proposals skill_proposals_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.skill_proposals
    ADD CONSTRAINT skill_proposals_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE SET NULL;


--
-- Name: skill_proposals skill_proposals_matched_skill_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.skill_proposals
    ADD CONSTRAINT skill_proposals_matched_skill_id_fkey FOREIGN KEY (matched_skill_id) REFERENCES public.skills(id) ON DELETE SET NULL;


--
-- Name: skill_proposals skill_proposals_owner_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.skill_proposals
    ADD CONSTRAINT skill_proposals_owner_user_id_fkey FOREIGN KEY (owner_user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: skill_test_cases skill_test_cases_skill_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.skill_test_cases
    ADD CONSTRAINT skill_test_cases_skill_id_fkey FOREIGN KEY (skill_id) REFERENCES public.skills(id) ON DELETE CASCADE;


--
-- Name: task_callback_deliveries task_callback_deliveries_effect_outbox_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_callback_deliveries
    ADD CONSTRAINT task_callback_deliveries_effect_outbox_fk FOREIGN KEY (effect_outbox_id) REFERENCES public.run_effect_outbox(id) ON DELETE RESTRICT;


--
-- Name: task_callback_deliveries task_callback_deliveries_run_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_callback_deliveries
    ADD CONSTRAINT task_callback_deliveries_run_event_id_fkey FOREIGN KEY (run_event_id) REFERENCES public.run_events(id) ON DELETE CASCADE;


--
-- Name: task_callback_deliveries task_callback_deliveries_subscription_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_callback_deliveries
    ADD CONSTRAINT task_callback_deliveries_subscription_id_fkey FOREIGN KEY (subscription_id) REFERENCES public.task_callback_subscriptions(id) ON DELETE CASCADE;


--
-- Name: task_queries task_queries_chosen_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_queries
    ADD CONSTRAINT task_queries_chosen_agent_id_fkey FOREIGN KEY (chosen_agent_id) REFERENCES public.agents(id) ON DELETE SET NULL;


--
-- Name: task_queries task_queries_completion_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_queries
    ADD CONSTRAINT task_queries_completion_run_id_fkey FOREIGN KEY (completion_run_id) REFERENCES public.runs(id) ON DELETE SET NULL;


--
-- Name: task_queries task_queries_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_queries
    ADD CONSTRAINT task_queries_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: user_token_core_grants user_token_core_grants_token_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_token_core_grants
    ADD CONSTRAINT user_token_core_grants_token_id_fkey FOREIGN KEY (token_id) REFERENCES public.user_tokens(id) ON DELETE CASCADE;


--
-- Name: user_tokens api_keys_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_tokens
    ADD CONSTRAINT api_keys_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: webhook_deliveries webhook_deliveries_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhook_deliveries
    ADD CONSTRAINT webhook_deliveries_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE;


--
-- Name: webhook_deliveries webhook_deliveries_effect_outbox_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhook_deliveries
    ADD CONSTRAINT webhook_deliveries_effect_outbox_fk FOREIGN KEY (effect_outbox_id) REFERENCES public.run_effect_outbox(id) ON DELETE RESTRICT;


--
-- Name: webhook_deliveries webhook_deliveries_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhook_deliveries
    ADD CONSTRAINT webhook_deliveries_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE;


--
-- Name: workflow_nodes workflow_nodes_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_nodes
    ADD CONSTRAINT workflow_nodes_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE RESTRICT;


--
-- Name: workflow_nodes workflow_nodes_workflow_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_nodes
    ADD CONSTRAINT workflow_nodes_workflow_id_fkey FOREIGN KEY (workflow_id) REFERENCES public.workflows(id) ON DELETE CASCADE;


--
-- Name: workflow_run_cancellations workflow_run_cancellations_actor_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run_cancellations
    ADD CONSTRAINT workflow_run_cancellations_actor_user_id_fkey FOREIGN KEY (actor_user_id) REFERENCES public.users(id) ON DELETE RESTRICT;


--
-- Name: workflow_run_cancellations workflow_run_cancellations_workflow_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run_cancellations
    ADD CONSTRAINT workflow_run_cancellations_workflow_run_id_fkey FOREIGN KEY (workflow_run_id) REFERENCES public.workflow_runs(id) ON DELETE RESTRICT;


--
-- Name: workflow_run_steps workflow_run_steps_agent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run_steps
    ADD CONSTRAINT workflow_run_steps_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE RESTRICT;


--
-- Name: workflow_run_steps workflow_run_steps_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run_steps
    ADD CONSTRAINT workflow_run_steps_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE SET NULL;


--
-- Name: workflow_run_steps workflow_run_steps_workflow_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run_steps
    ADD CONSTRAINT workflow_run_steps_workflow_node_id_fkey FOREIGN KEY (workflow_node_id) REFERENCES public.workflow_nodes(id) ON DELETE RESTRICT;


--
-- Name: workflow_run_steps workflow_run_steps_workflow_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run_steps
    ADD CONSTRAINT workflow_run_steps_workflow_run_id_fkey FOREIGN KEY (workflow_run_id) REFERENCES public.workflow_runs(id) ON DELETE CASCADE;


--
-- Name: workflow_runs workflow_runs_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_runs
    ADD CONSTRAINT workflow_runs_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: workflow_runs workflow_runs_workflow_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_runs
    ADD CONSTRAINT workflow_runs_workflow_id_fkey FOREIGN KEY (workflow_id) REFERENCES public.workflows(id) ON DELETE CASCADE;


--
-- Name: workflow_step_launches workflow_step_launches_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_step_launches
    ADD CONSTRAINT workflow_step_launches_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE RESTRICT;


--
-- Name: workflow_step_launches workflow_step_launches_workflow_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_step_launches
    ADD CONSTRAINT workflow_step_launches_workflow_node_id_fkey FOREIGN KEY (workflow_node_id) REFERENCES public.workflow_nodes(id) ON DELETE RESTRICT;


--
-- Name: workflow_step_launches workflow_step_launches_workflow_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_step_launches
    ADD CONSTRAINT workflow_step_launches_workflow_run_id_fkey FOREIGN KEY (workflow_run_id) REFERENCES public.workflow_runs(id) ON DELETE RESTRICT;


--
-- Name: workflow_step_launches workflow_step_launches_workflow_run_step_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_step_launches
    ADD CONSTRAINT workflow_step_launches_workflow_run_step_id_fkey FOREIGN KEY (workflow_run_step_id) REFERENCES public.workflow_run_steps(id) ON DELETE SET NULL;


--
-- Name: workflows workflows_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflows
    ADD CONSTRAINT workflows_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--


SET LOCAL search_path = public, pg_catalog;

INSERT INTO skills (id, category, name, description, sort_order) VALUES
    -- content（内容创作 / 文本处理）
    ('content/translation',     'content', '翻译',          '中英互译、多语言翻译、术语库本地化',                   1),
    ('content/summarization',   'content', '摘要',          '长文压缩、要点提取、会议纪要生成',                     2),
    ('content/copywriting',     'content', '文案',          '营销文案、社媒推文、产品描述、广告创意',               3),
    ('content/proofreading',    'content', '校对润色',      '错别字、语法、风格统一、SEO 优化',                     4),
    ('content/structured-data', 'content', '结构化抽取',    '从非结构化文本中抽取人名/地名/日期/金额/字段',         5),

    -- dev（研发工程）
    ('dev/code-review',         'dev',     '代码审查',      'PR 评审、风格检查、潜在 bug 提示',                     1),
    ('dev/code-generation',     'dev',     '代码生成',      '按 spec 生成函数 / 测试 / 脚手架',                     2),
    ('dev/code-explanation',    'dev',     '代码解释',      '解读 legacy 代码、生成文档、画时序图',                 3),
    ('dev/test-generation',     'dev',     '测试生成',      '单测 / 集成测试 / mock 数据生成',                      4),
    ('dev/devops-ci',           'dev',     'CI/CD',         'GitHub Actions、流水线、部署脚本',                     5),

    -- data（数据 / 分析）
    ('data/sql-query',          'data',    'SQL 查询',      '自然语言转 SQL、慢查询优化、schema 解读',              1),
    ('data/data-cleaning',      'data',    '数据清洗',      '去重 / 补全 / 类型转换 / 异常值检测',                  2),
    ('data/analysis',           'data',    '数据分析',      '统计 / 趋势 / 同比环比、生成洞察文字',                 3),
    ('data/visualization',      'data',    '可视化',        '生成 chart 配置 / 仪表盘 spec / Mermaid 图',           4),
    ('data/forecasting',        'data',    '预测',          '时序预测、销量 / 流量 / 库存预估',                     5),

    -- media（图像 / 音视频）
    ('media/image-generate',    'media',   '图像生成',      '文生图、产品图、海报、avatar',                         1),
    ('media/image-edit',        'media',   '图像编辑',      '抠图 / 换背景 / 修复 / 风格化 / 超分',                 2),
    ('media/audio-transcribe',  'media',   '语音转写',      '中英多语言 ASR、字幕生成、说话人分离',                 3),
    ('media/audio-generate',    'media',   '语音合成',      '多角色 TTS、克隆音色、语调 / 情绪控制',                4),
    ('media/video-process',     'media',   '视频处理',      '剪辑 / 字幕 / 搬运 / 切片 / 摘要',                     5),

    -- ops（业务流程 / 运维）
    ('ops/document-generate',   'ops',     '文档生成',      'PDF / Word / 报告 / 合同 / 简历',                      1),
    ('ops/email-process',       'ops',     '邮件处理',      '分类 / 自动回复 / 内容抽取 / 抄送决策',                2),
    ('ops/scheduling',          'ops',     '日程安排',      '会议时间协调、提醒、跨时区',                           3),
    ('ops/web-scraping',        'ops',     '网页抓取',      '抓取站点 / API / 监控 / 价格追踪',                     4),
    ('ops/notification',        'ops',     '通知推送',      '微信 / Slack / 邮件 / 短信投递',                       5),

    -- ai（AI 工程 / 多 Agent）
    ('ai/rag',                  'ai',      'RAG 检索',      '知识库问答、语义检索、文档级 QA',                      1),
    ('ai/agent-orchestration',  'ai',      'Agent 编排',    '多步 Agent 协作 / 工具链调度',                         2),
    ('ai/finetune',             'ai',      '模型微调',      'LoRA / SFT / 数据准备 / 评测',                         3),
    ('ai/prompt-engineering',   'ai',      'Prompt 工程',   '迭代 prompt、A/B、few-shot 设计',                      4),
    ('ai/safety-eval',          'ai',      '安全评测',      '幻觉检测、jailbreak 测试、合规审查',                   5);

INSERT INTO skill_test_cases (skill_id, title, input_json, judge_prompt, sort_order) VALUES
    -- content/translation
    ('content/translation', '中译英 · 商务邮件',
     '{"query":"请把下面这段商务邮件翻成英文，保持正式语气：\n您好，我们已收到贵司的报价单，正在内部评估，预计本周内给出答复。"}'::jsonb,
     '请评估下面的英文翻译是否准确、自然、符合商务邮件语境。给 0-100 分（90+ 母语级；75-89 准确达意；60-74 基本正确有小瑕疵；< 60 有明显错误）。\n\n译文：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     1),
    ('content/translation', '英译中 · 技术文档',
     '{"query":"Translate the following to Chinese: A Goroutine is a lightweight thread of execution managed by the Go runtime, multiplexed onto multiple OS threads."}'::jsonb,
     '请评估下面的中文翻译是否准确传达原文技术含义且术语得当。给 0-100 分。\n\n译文：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     2),
    ('content/translation', '多语言混合 · 保留专有名词',
     '{"query":"Translate to Chinese, keep brand names in English: Apple released the new MacBook Pro with M5 chip yesterday, alongside iOS 19 beta."}'::jsonb,
     '请评估翻译是否准确，是否按要求保留品牌名英文。给 0-100 分。\n\n译文：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     3),

    -- content/summarization
    ('content/summarization', '长文压缩 · 新闻',
     '{"query":"用 2-3 句话总结这段新闻：\n本周三，美联储宣布将基准利率维持在 5.25%-5.50% 不变，这是连续第 6 次按兵不动。鲍威尔在新闻发布会上表示，通胀降温的进展放缓，距离 2% 目标仍有距离；同时强调当前数据不支持立即降息。市场反应方面，10 年期美债收益率上行 8bp 至 4.6%，标普 500 收跌 0.4%。"}'::jsonb,
     '请评估摘要是否准确、信息完整、长度适当（2-3 句）。给 0-100 分。\n\n摘要：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     1),
    ('content/summarization', '要点提取 · 会议纪要',
     '{"query":"从下面对话提取 3-5 条决议要点（项目符号格式）：\nA：下周二的 demo 我们决定用方案 B，理由是稳定性更好。\nB：好，那我去对接客户确认时间。\nA：另外预算超了 15%，需要找 CFO 走特批。\nC：我去联系 CFO 王总，目标周五前拿到批复。\nB：UI 改稿明天给到设计组。"}'::jsonb,
     '请评估要点是否覆盖关键决议、责任人清晰、格式规范。给 0-100 分。\n\n要点：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     2),
    ('content/summarization', '一句话标题',
     '{"query":"给下面的产品介绍写一句不超过 20 字的标题：\nOpenLinker 是一个让 AI Agent 互联互通的市场和协议层。开发者可以把自己写的 Agent 注册上来公开售卖，调用方既能通过网页直接试用，也能通过 MCP 让 Claude/Cursor 等工具一键接入。"}'::jsonb,
     '请评估标题是否抓住核心卖点、是否在 20 字内。给 0-100 分。\n\n标题：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     3),

    -- dev/code-review
    ('dev/code-review', '审查 SQL 注入风险',
     '{"query":"请审查这段 Go 代码并指出问题：\nfunc getUser(db *sql.DB, name string) (*User, error) {\n    q := fmt.Sprintf(\"SELECT * FROM users WHERE name = ''%s''\", name)\n    row := db.QueryRow(q)\n    var u User\n    if err := row.Scan(&u.ID, &u.Name); err != nil {\n        return nil, err\n    }\n    return &u, nil\n}"}'::jsonb,
     '请评估审查是否准确指出 SQL 注入问题并给出修复建议（参数化查询）。给 0-100 分。\n\n审查意见：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     1),
    ('dev/code-review', '审查并发风险',
     '{"query":"审查下面代码：\nvar counter int\nfunc inc() { counter++ }\nfunc main() {\n    var wg sync.WaitGroup\n    for i := 0; i < 1000; i++ {\n        wg.Add(1)\n        go func() { defer wg.Done(); inc() }()\n    }\n    wg.Wait()\n    fmt.Println(counter)\n}"}'::jsonb,
     '请评估是否准确指出 data race 并给出修复（mutex / atomic）。给 0-100 分。\n\n审查意见：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     2),
    ('dev/code-review', '审查错误处理',
     '{"query":"审查：\nfunc fetch(url string) string {\n    resp, _ := http.Get(url)\n    body, _ := io.ReadAll(resp.Body)\n    return string(body)\n}"}'::jsonb,
     '请评估是否指出忽略 error、未关闭 Body、未检查 nil resp 等问题。给 0-100 分。\n\n审查意见：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     3),

    -- data/sql-query
    ('data/sql-query', '自然语言转 SQL · 简单聚合',
     '{"query":"表 orders(id, user_id, amount_cents, created_at)。请写 SQL 查询本月总销售额（cents），按 user_id 分组，取 top 10。"}'::jsonb,
     '请评估 SQL 是否正确：日期过滤、SUM 聚合、GROUP BY、ORDER BY DESC LIMIT 10。给 0-100 分。\n\nSQL：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     1),
    ('data/sql-query', '解读 schema',
     '{"query":"解读这段建表：\nCREATE TABLE runs (\n  id UUID PRIMARY KEY,\n  user_id UUID REFERENCES users(id),\n  agent_id UUID REFERENCES agents(id),\n  status TEXT CHECK (status IN (''running'',''success'',''failed'')),\n  cost_cents INT,\n  created_at TIMESTAMPTZ DEFAULT NOW()\n);"}'::jsonb,
     '请评估解读是否覆盖主键、外键、status 取值、计费字段含义。给 0-100 分。\n\n解读：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     2),
    ('data/sql-query', '慢查询优化建议',
     '{"query":"下面查询很慢，给出 2-3 条优化建议：\nSELECT a.name, COUNT(r.id) FROM agents a\nLEFT JOIN runs r ON r.agent_id = a.id\nWHERE r.created_at > NOW() - INTERVAL ''30 days''\nGROUP BY a.name ORDER BY COUNT(r.id) DESC;"}'::jsonb,
     '请评估优化建议（索引、JOIN 顺序、分区、谓词下推等）是否切中要害。给 0-100 分。\n\n建议：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     3),

    -- ops/email-process
    ('ops/email-process', '邮件分类',
     '{"query":"把下面邮件归到一个类别（销售线索/客户支持/账单/垃圾邮件/其他）并给一句理由：\n主题：关于贵司 SaaS 产品的 API 限流策略\n正文：你好，我司在评估接入贵司 API，想咨询免费版每分钟调用上限，以及付费后是否能突破。"}'::jsonb,
     '请评估分类是否准确（应为"销售线索"或"客户支持"）。给 0-100 分。\n\n输出：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     1),
    ('ops/email-process', '邮件抽取关键信息',
     '{"query":"从邮件中抽取：发件人、发票号、金额、币种，以 JSON 返回：\nFrom: billing@aws.com\nDear customer, your invoice INV-2026-04-887 totaling $1,234.56 USD is now available."}'::jsonb,
     '请评估抽取的 JSON 字段是否完整准确（4 个字段）。给 0-100 分。\n\n输出：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     2),
    ('ops/email-process', '邮件自动回复',
     '{"query":"客户发来：贵司产品 X 是否支持 SSO?\n请生成一段简短礼貌的自动回复（中文，3-5 句）。"}'::jsonb,
     '请评估回复是否礼貌、信息明确（哪怕说不确定也要给后续路径）。给 0-100 分。\n\n回复：\n{output}\n\n输出 JSON：{"score": 数字, "reason": "一句话点评"}',
     3);

-- Per-installation identity must be generated locally and remain stable after
-- initialization.
INSERT INTO public.core_instance_identity (singleton, issuer_instance_id)
VALUES (TRUE, 'core_' || replace(gen_random_uuid()::text, '-', ''));

-- Runtime starts in hard maintenance and is reopened only by the existing
-- cutover controller after every owner has finished initialization.
INSERT INTO public.runtime_cluster_control (singleton_id)
VALUES (1);

INSERT INTO public.runtime_wire_contracts (
    runtime_contract_id,
    runtime_contract_digest,
    support_tier
) VALUES
    ('openlinker.runtime.v2', 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53', 'historical'),
    ('openlinker.runtime.v2', '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f', 'historical'),
    ('openlinker.runtime.v2', '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9', 'previous'),
    ('openlinker.runtime.v2', '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61', 'historical'),
    ('openlinker.runtime.v2', '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481', 'current');

INSERT INTO public.runtime_schema_contracts (
    schema_version,
    migration_name,
    runtime_contract_id,
    runtime_contract_digest,
    is_current
) VALUES
    (66, '066_runtime_v2_deadline_reconciler', 'openlinker.runtime.v2', '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61', FALSE),
    (67, '067_runtime_v2_core_execution', 'openlinker.runtime.v2', '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f', FALSE),
    (70, '070_sdk_first_runtime_boundary', 'openlinker.runtime.v2', 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53', FALSE),
    (71, '071_runtime_attachment_generation', 'openlinker.runtime.v2', '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9', FALSE),
    (73, '073_runtime_transport_observability', 'openlinker.runtime.v2', '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9', FALSE),
    (75, '075_runtime_wire_compatibility', 'openlinker.runtime.v2', '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9', FALSE),
    (76, '076_runtime_cancellation_terminal_reap', 'openlinker.runtime.v2', '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9', FALSE),
    (77, '077_external_execution_cancellation', 'openlinker.runtime.v2', '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481', FALSE),
    (79, '079_runtime_attempt_transport_evidence', 'openlinker.runtime.v2', '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481', FALSE),
    (80, '080_runtime_attempt_transport_evidence', 'openlinker.runtime.v2', '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481', TRUE);

COMMIT;
