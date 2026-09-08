package delivery

import (
	"context"
	"testing"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestHistoryRetainsDeliveriesAfterTargetDeletion(t *testing.T) {
	userID, runID, targetID := uuid.New(), uuid.New(), uuid.New()
	for _, status := range []string{"pending", "success", "failed"} {
		t.Run(status, func(t *testing.T) {
			deleted := db.RunDelivery{
				ID: uuid.New(), RunID: runID, UserID: userID,
				TargetURL: "https://deleted.example/hook", TargetType: "webhook",
				Status: status, AttemptCount: 1,
			}
			retained := deleted
			retained.ID, retained.TargetID = uuid.New(), &targetID
			queries := &fakeDeliveryQueries{
				run:        db.Run{ID: runID, UserID: userID},
				deliveries: []db.RunDelivery{deleted, retained},
			}
			svc := &Service{queries: queries}
			byRun, err := svc.ListByRun(context.Background(), runID, userID)
			require.NoError(t, err)
			global, err := svc.List(context.Background(), userID, DeliveryListFilter{})
			require.NoError(t, err)
			for _, items := range [][]DeliveryItem{byRun, global} {
				require.Len(t, items, 2)
				require.Empty(t, items[0].TargetID)
				require.Equal(t, deleted.TargetURL, items[0].TargetURL)
				require.Equal(t, status, items[0].Status)
				require.Equal(t, targetID.String(), items[1].TargetID)
			}
		})
	}
}
