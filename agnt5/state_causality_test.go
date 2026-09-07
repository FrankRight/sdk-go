package agnt5

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type causalStateClient struct {
	pb.EngineServiceClient
	projected *pb.GetEntityStateResponse
	current   int64
	gets      int
	puts      []*pb.PutEntityStateRequest
	onPut     func(context.Context, *pb.PutEntityStateRequest, int) (*pb.PutEntityStateResponse, error)
}

func (c *causalStateClient) GetEntityState(context.Context, *pb.GetEntityStateRequest, ...grpc.CallOption) (*pb.GetEntityStateResponse, error) {
	c.gets++
	if c.projected == nil {
		return &pb.GetEntityStateResponse{}, nil
	}
	return c.projected, nil
}

func (c *causalStateClient) PutEntityState(ctx context.Context, r *pb.PutEntityStateRequest, _ ...grpc.CallOption) (*pb.PutEntityStateResponse, error) {
	c.puts = append(c.puts, proto.Clone(r).(*pb.PutEntityStateRequest))
	if c.onPut != nil {
		return c.onPut(ctx, r, len(c.puts))
	}
	if r.ExpectedVersion != c.current {
		return nil, status.Error(codes.FailedPrecondition, "version conflict: stale projection")
	}
	c.current++
	return &pb.PutEntityStateResponse{NewVersion: c.current}, nil
}

func expireStateObservation(ctx *Context) {
	s := ctx.stateStore.(*engineStateStore)
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, entry := range s.cache {
		entry.expires = time.Time{}
		s.cache[key] = entry
	}
}

func stateTestInvocation() Invocation {
	return Invocation{ID: "run", RunID: "run", ComponentName: "test", ComponentType: ComponentTypeFunction, LeaseID: "lease",
		Metadata: map[string]string{"worker_id": "worker", "worker_session_id": "session", "project_id": "project"}}
}

func TestRunStateDoesNotRegressAfterReceiptCacheExpires(t *testing.T) {
	client := &causalStateClient{}
	base := newEngineStateStore(client, "project").(*engineStateStore)
	worker := NewWorker("state", WithProjectID("project"))
	worker.stateStore = base
	if err := RegisterFunction(worker, "test", func(ctx *Context, _ any) (string, error) {
		if err := ctx.State().Set(ctx, "stage", "validated"); err != nil {
			return "", err
		}
		expireStateObservation(ctx)
		stage, err := ctx.State().Get(ctx, "stage")
		if err != nil || stage != "validated" {
			t.Fatalf("acknowledged value regressed after TTL: %v %v", stage, err)
		}
		if err := ctx.State().Set(ctx, "stage", "priced"); err != nil {
			return "", err
		}
		// A fresher projection must remain visible, including another field.
		client.current = 3
		client.projected = &pb.GetEntityStateResponse{Found: true, Version: 3, StateJson: []byte(`{"stage":"charged","other":true}`)}
		expireStateObservation(ctx)
		stage, err = ctx.State().Get(ctx, "stage")
		if err != nil || stage != "charged" {
			t.Fatalf("fresh state not observed: %v %v", stage, err)
		}
		client.projected = &pb.GetEntityStateResponse{}
		expireStateObservation(ctx)
		if err := ctx.State().Set(ctx, "stage", "receipted"); err != nil {
			return "", err
		}
		var values map[string]any
		if err := json.Unmarshal(client.puts[len(client.puts)-1].StateJson, &values); err != nil {
			t.Fatal(err)
		}
		if values["other"] != true {
			t.Fatal("newer observed state was overwritten by an older projection")
		}
		return "ok", nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.invoke(context.Background(), stateTestInvocation()); err != nil {
		t.Fatal(err)
	}
	if len(base.cache) != 0 {
		t.Fatal("worker retains completed invocation state")
	}
}

func TestStateUnknownOutcomeKeepsExactRequestAcrossVersionConflict(t *testing.T) {
	client := &causalStateClient{}
	client.onPut = func(ctx context.Context, r *pb.PutEntityStateRequest, call int) (*pb.PutEntityStateResponse, error) {
		if call == 1 {
			client.projected = &pb.GetEntityStateResponse{Found: true, Version: 1, StateJson: r.StateJson}
			expireStateObservation(ctx.Value(stateAuthorityContextKey).(*Context))
			return nil, status.Error(codes.Unavailable, "accepted response lost")
		}
		if !proto.Equal(client.puts[0], r) {
			t.Fatal("uncertain mutation changed its request identity or payload")
		}
		if call == 2 {
			return nil, status.Error(codes.FailedPrecondition, "version conflict: routing has not resolved original acceptance")
		}
		return &pb.PutEntityStateResponse{NewVersion: 1}, nil
	}
	worker := NewWorker("state", WithProjectID("project"))
	worker.stateStore = newEngineStateStore(client, "project")
	if err := RegisterFunction(worker, "test", func(ctx *Context, _ any) (string, error) { return "", ctx.State().Set(ctx, "stage", "done") }); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.invoke(context.Background(), stateTestInvocation()); err != nil {
		t.Fatal(err)
	}
	if client.gets != 1 || len(client.puts) != 3 {
		t.Fatalf("gets=%d puts=%d", client.gets, len(client.puts))
	}
}

func TestStateDefiniteVersionConflictRefreshesBeforeRetry(t *testing.T) {
	client := &causalStateClient{}
	client.onPut = func(_ context.Context, r *pb.PutEntityStateRequest, call int) (*pb.PutEntityStateResponse, error) {
		if call == 1 {
			client.projected = &pb.GetEntityStateResponse{Found: true, Version: 2, StateJson: []byte(`{"other":true}`)}
			return nil, status.Error(codes.FailedPrecondition, "version conflict: concurrent write")
		}
		var values map[string]any
		if err := json.Unmarshal(r.StateJson, &values); err != nil {
			t.Fatal(err)
		}
		if r.ExpectedVersion != 2 || values["other"] != true || values["stage"] != "done" {
			t.Fatal("definite conflict did not safely rebase")
		}
		return &pb.PutEntityStateResponse{NewVersion: 3}, nil
	}
	worker := NewWorker("state", WithProjectID("project"))
	worker.stateStore = newEngineStateStore(client, "project")
	if err := RegisterFunction(worker, "test", func(ctx *Context, _ any) (string, error) { return "", ctx.State().Set(ctx, "stage", "done") }); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.invoke(context.Background(), stateTestInvocation()); err != nil {
		t.Fatal(err)
	}
}

func TestCausalStateOwnsSnapshotsAndRejectsOlderReceipts(t *testing.T) {
	client := &causalStateClient{}
	worker := NewWorker("state", WithProjectID("project"))
	worker.stateStore = newEngineStateStore(client, "project")
	if err := RegisterFunction(worker, "test", func(ctx *Context, _ any) (string, error) {
		input := map[string]any{"value": "original"}
		if err := ctx.State().Set(ctx, "nested", input); err != nil {
			return "", err
		}
		input["value"] = "mutated after acknowledgment"
		value, err := ctx.State().Get(ctx, "nested")
		if err != nil || value.(map[string]any)["value"] != "original" {
			t.Fatal("caller mutated acknowledged snapshot")
		}
		value.(map[string]any)["value"] = "mutated read result"
		value, err = ctx.State().Get(ctx, "nested")
		if err != nil || value.(map[string]any)["value"] != "original" {
			t.Fatal("read result aliases cached snapshot")
		}
		store := ctx.stateStore.(*engineStateStore)
		store.storeCached(StateScopeRun, ctx.RunID(), []byte(`{"stage":"newer"}`), 3)
		store.storeCached(StateScopeRun, ctx.RunID(), []byte(`{"stage":"older"}`), 2)
		value, err = ctx.State().Get(ctx, "stage")
		if err != nil || value != "newer" {
			t.Fatal("late receipt regressed state")
		}
		return "ok", nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.invoke(context.Background(), stateTestInvocation()); err != nil {
		t.Fatal(err)
	}
}
