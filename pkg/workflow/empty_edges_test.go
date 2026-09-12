package workflow

import (
	"encoding/json"
	"testing"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/stretchr/testify/require"
)

func TestWorkflowExplicitEmptyEdgesSurviveRequestAndStorage(t *testing.T) {
	for _, tc := range []struct {
		name, json string
		levels     int
	}{
		{"omitted defaults to sequential", `{}`, 2},
		{"null defaults to sequential", `{"edges":null}`, 2},
		{"explicit empty is parallel", `{"edges":[]}`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var request CreateWorkflowRequest
			require.NoError(t, json.Unmarshal([]byte(tc.json), &request))
			request.Nodes = []WorkflowNodeRequest{{Key: "a"}, {Key: "b"}}
			// Exercise Go caller JSON as well: omitempty must not discard [].
			wire, err := json.Marshal(request)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(wire, &request))
			edges, err := normalizeWorkflowEdgesFromRequest(request.Nodes, request.Edges)
			require.NoError(t, err)
			require.NoError(t, validateWorkflowGraphFromRequest(request.Nodes, edges))
			stored, err := json.Marshal(edges)
			require.NoError(t, err)
			graph, err := workflowGraphFromDefinition(db.Workflow{Edges: stored}, []db.WorkflowNode{{NodeKey: "a"}, {NodeKey: "b"}})
			require.NoError(t, err)
			require.Len(t, graph.Levels, tc.levels)
			if tc.levels == 1 {
				require.Empty(t, graph.Parents["b"])
				require.Equal(t, []string{"a", "b"}, graph.Sinks)
			} else {
				require.Equal(t, []string{"a"}, graph.Parents["b"])
			}
		})
	}
}
