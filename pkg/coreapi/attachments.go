package coreapi

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/attachment"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// configureAttachments enables Run attachments when a dedicated directory is
// configured. Misconfiguration disables the feature with a clear log instead
// of failing Core start, so deployments without the directory keep working.
func configureAttachments(rootCtx context.Context, api *echo.Group, pool *pgxpool.Pool, cfg *config.Config, authMw echo.MiddlewareFunc, runtimeSvc *runtime.Service) {
	dir := strings.TrimSpace(cfg.AttachmentsDir)
	if dir == "" {
		log.Info().Msg("run attachments disabled: OPENLINKER_ATTACHMENTS_DIR is not set")
		return
	}
	if cfg.RuntimeHAMode && !cfg.AttachmentsShared {
		log.Error().Msg("run attachments disabled: RUNTIME_HA_MODE requires one shared attachment volume and OPENLINKER_ATTACHMENTS_SHARED=true")
		return
	}
	origin := strings.TrimSpace(cfg.AttachmentsPublicOrigin)
	if origin == "" {
		origin = cfg.APIURL
	}
	links, err := attachment.NewLinks(origin)
	if err != nil {
		log.Error().Err(err).Msg("run attachments disabled: invalid public origin")
		return
	}
	store, err := attachment.Open(attachment.Config{
		Dir: dir, OwnerMaxCount: cfg.AttachmentsOwnerMaxCount, OwnerMaxBytes: cfg.AttachmentsOwnerMaxBytes,
		InstanceMaxBytes: cfg.AttachmentsInstanceMaxBytes, UnboundTTL: time.Duration(cfg.AttachmentsUnboundTTLSeconds) * time.Second,
	})
	if err != nil {
		log.Error().Err(err).Msg("run attachments disabled: attachment directory is not usable")
		return
	}
	attachment.NewHandler(store, links).Register(api, authMw)
	runtimeSvc.SetRunAttachments(attachment.RunBinder{Store: store, Links: links})
	queries := db.New(pool)
	status := func(ctx context.Context, run uuid.UUID) (string, bool, error) {
		found, err := queries.GetRunByID(ctx, run)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		if err != nil {
			return "", false, err
		}
		return found.Status, true, nil
	}
	go store.Run(rootCtx, 5*time.Minute, status, runtime.IsTerminalRunStatus, func(result attachment.SweepResult, err error) {
		if err != nil {
			log.Warn().Err(err).Msg("run attachment sweep incomplete")
		}
		if result.ReleasedRuns+result.Expired+result.Orphans > 0 {
			log.Info().Int("runs", result.ReleasedRuns).Int("expired", result.Expired).Int("orphans", result.Orphans).Msg("run attachments released")
		}
	})
	log.Info().Str("dir", store.Dir()).Msg("run attachments enabled")
}
