package agnt5

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

// Model provider calls follow the Rust core's policy (sdk-core src/lm/http.rs),
// so the Go, Python and TypeScript SDKs fail the same way: a generous
// per-request timeout bounded by the run's own context, and up to two retries
// with jittered exponential backoff when the provider times out or answers
// with a transient status. A single slow response used to fail the whole run
// on a fixed 60-second client timeout (AGNT5-1251).
const (
	defaultModelRequestTimeout    = 10 * time.Minute
	defaultModelConnectTimeout    = 10 * time.Second
	defaultModelMaxRetries        = 2
	defaultModelInitialRetryDelay = 500 * time.Millisecond
	defaultModelMaxRetryDelay     = 8 * time.Second
	modelRetryJitter              = 0.25
)

// ModelRequestError is a model provider request that failed before a response
// arrived, after any retries.
type ModelRequestError struct {
	Provider string
	Attempts int
	// Timeout reports whether the provider did not answer in time, as opposed
	// to the run's context ending or the connection failing.
	Timeout bool
	Err     error
}

func (e *ModelRequestError) Error() string {
	kind := "request failed"
	if e.Timeout {
		kind = "request timed out"
	}
	return fmt.Sprintf("agnt5: %s provider %s after %d attempt(s): %v", e.Provider, kind, e.Attempts, e.Err)
}

func (e *ModelRequestError) Unwrap() error { return e.Err }

func newModelHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: defaultModelConnectTimeout, KeepAlive: 30 * time.Second}).DialContext
	return &http.Client{Timeout: defaultModelRequestTimeout, Transport: transport}
}

type modelRetryPolicy struct {
	maxRetries   int
	initialDelay time.Duration
	maxDelay     time.Duration
}

// modelRetryPolicyFromEnv reads the same variables as the Rust core:
// AGNT5_LM_MAX_RETRIES, AGNT5_LM_INITIAL_DELAY_MS and AGNT5_LM_MAX_DELAY_MS.
func modelRetryPolicyFromEnv() modelRetryPolicy {
	policy := modelRetryPolicy{
		maxRetries:   defaultModelMaxRetries,
		initialDelay: defaultModelInitialRetryDelay,
		maxDelay:     defaultModelMaxRetryDelay,
	}
	if n, err := strconv.Atoi(os.Getenv("AGNT5_LM_MAX_RETRIES")); err == nil && n >= 0 {
		policy.maxRetries = n
	}
	if ms, err := strconv.Atoi(os.Getenv("AGNT5_LM_INITIAL_DELAY_MS")); err == nil && ms >= 0 {
		policy.initialDelay = time.Duration(ms) * time.Millisecond
	}
	if ms, err := strconv.Atoi(os.Getenv("AGNT5_LM_MAX_DELAY_MS")); err == nil && ms >= 0 {
		policy.maxDelay = time.Duration(ms) * time.Millisecond
	}
	return policy
}

// delay is initialDelay * 2^(retry-1), capped at maxDelay, jittered by ±25%,
// and never shorter than the provider's Retry-After.
func (p modelRetryPolicy) delay(retry int, retryAfter time.Duration) time.Duration {
	base := p.initialDelay << min(retry-1, 10)
	if base > p.maxDelay || base < 0 {
		base = p.maxDelay
	}
	jittered := time.Duration(float64(base) * (1 + modelRetryJitter*(2*rand.Float64()-1)))
	return max(jittered, retryAfter)
}

func retryableModelStatus(status int) bool {
	switch status {
	case 408, 429, 500, 502, 503, 504, 529:
		return true
	}
	return false
}

// sendModelRequest sends req, retrying on a provider timeout or a transient
// status. The retries run inside the caller's model activation attempt, so
// they share its idempotency key. A non-retryable or final response is
// returned as is for the caller to check.
func sendModelRequest(client *http.Client, provider string, req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	policy := modelRetryPolicyFromEnv()
	var retryAfter time.Duration
	for attempt := 1; ; attempt++ {
		if attempt > 1 {
			if err := sleepContext(ctx, policy.delay(attempt-1, retryAfter)); err != nil {
				return nil, &ModelRequestError{Provider: provider, Attempts: attempt - 1, Err: err}
			}
			next := req.Clone(ctx)
			if req.GetBody != nil {
				body, err := req.GetBody()
				if err != nil {
					return nil, err
				}
				next.Body = body
			}
			req = next
		}

		resp, err := client.Do(req)
		if err != nil {
			// The run's own deadline or cancellation is final; only the
			// provider being slow is worth another attempt.
			var netErr net.Error
			timeout := ctx.Err() == nil && errors.As(err, &netErr) && netErr.Timeout()
			if timeout && attempt <= policy.maxRetries {
				retryAfter = 0
				continue
			}
			return nil, &ModelRequestError{Provider: provider, Attempts: attempt, Timeout: timeout, Err: err}
		}
		if !retryableModelStatus(resp.StatusCode) || attempt > policy.maxRetries {
			return resp, nil
		}
		retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
	}
}

// parseRetryAfter reads a Retry-After header given in seconds. The HTTP-date
// form is ignored.
func parseRetryAfter(value string) time.Duration {
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}
