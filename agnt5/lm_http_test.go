package agnt5

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const okChatCompletion = `{"id":"c1","choices":[{"finish_reason":"stop","message":{"content":"done"}}]}`

// fakeProvider answers each call with the next behaviour: a status code, or
// -1 to hang past the client timeout.
func fakeProvider(t *testing.T, behaviours ...int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"model":"gpt-test"`) {
			t.Errorf("attempt %d sent body %q", calls.Load()+1, body)
		}
		n := int(calls.Add(1)) - 1
		behaviour := http.StatusOK
		if n < len(behaviours) {
			behaviour = behaviours[n]
		}
		switch behaviour {
		case -1:
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		case http.StatusOK:
			_, _ = io.WriteString(w, okChatCompletion)
		default:
			w.WriteHeader(behaviour)
			_, _ = io.WriteString(w, `{"error":{"message":"transient"}}`)
		}
	}))
	t.Cleanup(server.Close)
	return server, &calls
}

func fastRetries(t *testing.T) {
	t.Setenv("AGNT5_LM_INITIAL_DELAY_MS", "1")
	t.Setenv("AGNT5_LM_MAX_DELAY_MS", "5")
}

func testOpenAIModel(server *httptest.Server) *OpenAIModel {
	return NewOpenAIModel(OpenAIConfig{
		BaseURL:    server.URL,
		Model:      "gpt-test",
		HTTPClient: &http.Client{Timeout: 100 * time.Millisecond},
	})
}

func generateOnce(model LanguageModel) (GenerateResponse, error) {
	return model.Generate(context.Background(), GenerateRequest{Messages: []Message{{Role: "user", Content: "hi"}}})
}

// One slow provider response is retried instead of failing the run
// (AGNT5-1251).
func TestModelRequestRetriesAProviderTimeout(t *testing.T) {
	fastRetries(t)
	server, calls := fakeProvider(t, -1, http.StatusOK)

	resp, err := generateOnce(testOpenAIModel(server))
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if resp.Content != "done" || calls.Load() != 2 {
		t.Fatalf("content %q after %d call(s), want \"done\" after 2", resp.Content, calls.Load())
	}
}

func TestModelRequestRetriesTransientStatuses(t *testing.T) {
	fastRetries(t)
	server, calls := fakeProvider(t, http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusOK)

	if _, err := generateOnce(testOpenAIModel(server)); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("%d call(s), want 3", calls.Load())
	}
}

func TestModelRequestDoesNotRetryAClientError(t *testing.T) {
	fastRetries(t)
	server, calls := fakeProvider(t, http.StatusBadRequest)

	_, err := generateOnce(testOpenAIModel(server))
	if err == nil || !strings.Contains(err.Error(), "HTTP 400") {
		t.Fatalf("err = %v, want the HTTP 400", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("%d call(s), want 1", calls.Load())
	}
}

func TestModelRequestReportsATimeoutAfterTheLastRetry(t *testing.T) {
	fastRetries(t)
	server, calls := fakeProvider(t, -1, -1, -1)

	_, err := generateOnce(testOpenAIModel(server))
	var requestErr *ModelRequestError
	if !errors.As(err, &requestErr) || !requestErr.Timeout || requestErr.Attempts != 3 || requestErr.Provider != "openai" {
		t.Fatalf("err = %#v, want an openai timeout after 3 attempts", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("%d call(s), want 3", calls.Load())
	}
}

func TestModelRequestRetriesAreConfigurable(t *testing.T) {
	fastRetries(t)
	t.Setenv("AGNT5_LM_MAX_RETRIES", "0")
	server, calls := fakeProvider(t, http.StatusServiceUnavailable)

	if _, err := generateOnce(testOpenAIModel(server)); err == nil {
		t.Fatal("want the 503")
	}
	if calls.Load() != 1 {
		t.Fatalf("%d call(s), want 1", calls.Load())
	}
}

// The run's own deadline is final: it is not a slow provider to retry.
func TestModelRequestDoesNotRetryAfterTheRunDeadline(t *testing.T) {
	fastRetries(t)
	server, calls := fakeProvider(t, -1, -1, -1)
	model := NewOpenAIModel(OpenAIConfig{BaseURL: server.URL, Model: "gpt-test"})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := model.Generate(ctx, GenerateRequest{Messages: []Message{{Role: "user", Content: "hi"}}})
	var requestErr *ModelRequestError
	if !errors.As(err, &requestErr) || requestErr.Timeout || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %#v, want the run deadline, not a provider timeout", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("%d call(s), want 1", calls.Load())
	}
}

func TestModelProvidersDefaultToALongRequestTimeout(t *testing.T) {
	for name, client := range map[string]*http.Client{
		"openai":    NewOpenAIModel(OpenAIConfig{}).config.HTTPClient,
		"anthropic": NewAnthropicModel(AnthropicConfig{}).config.HTTPClient,
		"google":    NewGoogleModel(GoogleConfig{}).config.HTTPClient,
	} {
		if client.Timeout != defaultModelRequestTimeout {
			t.Errorf("%s client timeout = %s, want %s", name, client.Timeout, defaultModelRequestTimeout)
		}
	}
}

func TestModelRetryDelay(t *testing.T) {
	policy := modelRetryPolicy{initialDelay: 500 * time.Millisecond, maxDelay: 8 * time.Second}
	for retry, base := range map[int]time.Duration{1: 500 * time.Millisecond, 2: time.Second, 5: 8 * time.Second, 40: 8 * time.Second} {
		got := policy.delay(retry, 0)
		if got < base*3/4 || got > base*5/4 {
			t.Errorf("delay(%d) = %s, want %s ±25%%", retry, got, base)
		}
	}
	if got := policy.delay(1, 30*time.Second); got != 30*time.Second {
		t.Errorf("delay with Retry-After 30s = %s, want 30s", got)
	}
}
