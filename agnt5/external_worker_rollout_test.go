package agnt5

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestExternalWorkerRollingOptInAndDowngradeProtection(t *testing.T) {
	credential := externalWorkerCredential{file: "bootstrap-key"}
	legacy, err := prepareExternalWorkerRollout("", "", credential)
	if err != nil || legacy.ready || legacy.pinned || len(legacy.profiles()) != 1 || legacy.profiles()[0] != authProfileTokenAuth {
		t.Fatalf("legacy=%+v err=%v", legacy, err)
	}
	if legacy.accept(authProfileBootstrapMTLS) == nil {
		t.Fatal("unready SDK accepted mTLS")
	}
	if legacy.accept(authProfileTokenAuth) != nil {
		t.Fatal("legacy SDK rejected bearer")
	}
	for _, tc := range []struct {
		enabled, directory string
		credential         externalWorkerCredential
	}{
		{"true", "", credential}, {"typo", t.TempDir(), credential}, {"true", t.TempDir(), externalWorkerCredential{inline: "secret"}},
	} {
		if _, err := prepareExternalWorkerRollout(tc.enabled, tc.directory, tc.credential); err == nil {
			t.Fatal("invalid readiness accepted")
		}
	}
	dir := t.TempDir()
	ready, err := prepareExternalWorkerRollout("true", dir, credential)
	if err != nil || !ready.ready || ready.pinned {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	if err := ready.accept(authProfileBootstrapMTLS); err != nil {
		t.Fatal(err)
	}
	// A restart with opt-in removed must still offer only mTLS, even before any
	// certificate has been issued. Operator rollback cannot cause a downgrade.
	pinned, err := prepareExternalWorkerRollout("false", dir, credential)
	if err != nil || !pinned.ready || !pinned.pinned || len(pinned.profiles()) != 1 || pinned.currentProfile() != authProfileBootstrapMTLS {
		t.Fatalf("pinned=%+v err=%v", pinned, err)
	}
	if pinned.accept(authProfileTokenAuth) == nil {
		t.Fatal("pinned SDK accepted downgrade")
	}
	info, err := os.Stat(filepath.Join(dir, externalWorkerAuthProfileFile))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("marker permissions: %v %v", info, err)
	}
}

func TestExternalWorkerExistingSessionPinsAndCorruptStateFailsClosed(t *testing.T) {
	credential := externalWorkerCredential{file: "bootstrap-key"}
	dir := t.TempDir()
	path := filepath.Join(dir, externalWorkerSessionFile)
	if err := os.WriteFile(path, []byte("old SDK session"), 0600); err != nil {
		t.Fatal(err)
	}
	pinned, err := prepareExternalWorkerRollout("", dir, credential)
	if err != nil || !pinned.pinned {
		t.Fatalf("existing session=%+v %v", pinned, err)
	}
	if err := os.WriteFile(filepath.Join(dir, externalWorkerAuthProfileFile), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareExternalWorkerRollout("", dir, credential); err == nil {
		t.Fatal("corrupt marker accepted")
	}
	other := t.TempDir()
	if err := os.Symlink(path, filepath.Join(other, externalWorkerAuthProfileFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareExternalWorkerRollout("", other, credential); err == nil {
		t.Fatal("symlink marker accepted")
	}
}

func TestExternalWorkerPinnedDiscoveryResumesAndRejectsDowngrade(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envWorkerSessionDir, dir)
	t.Setenv(envWorkerMTLSEnabled, "false")
	keyPath := filepath.Join(t.TempDir(), "bootstrap-key")
	if err := os.WriteFile(keyPath, []byte("bootstrap"), 0600); err != nil {
		t.Fatal(err)
	}
	authority := externalWorkerConnection{
		ProjectID: "project", EnvironmentID: "environment", DeploymentID: "deployment", WorkerPoolID: "pool", Placement: "customer_docker",
		RuntimeEndpoint: "https://runtime.example.com", Protocol: externalWorkerProtocolPullV1, AuthProfile: authProfileBootstrapMTLS,
	}
	identity := testExternalWorkerIdentity(t, authority)
	if err := writeExternalWorkerIdentity(filepath.Join(dir, externalWorkerSessionFile), identity); err != nil {
		t.Fatal(err)
	}
	var downgrade atomic.Bool
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/v1/worker-discovery" {
			t.Errorf("unexpected issuance request %s", r.URL.Path)
			http.Error(w, "unexpected", 500)
			return
		}
		var input struct {
			MTLSReady bool     `json:"mtls_ready"`
			Current   string   `json:"current_auth_profile"`
			Profiles  []string `json:"supported_auth_profiles"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		if !input.MTLSReady || input.Current != authProfileBootstrapMTLS || len(input.Profiles) != 1 || input.Profiles[0] != authProfileBootstrapMTLS {
			t.Errorf("discovery readiness=%+v", input)
		}
		response := authority
		response.IdentityEndpoint = "http://127.0.0.1:1"
		if downgrade.Load() {
			response.AuthProfile = authProfileTokenAuth
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	config := externalWorkerBootstrapConfig{controlPlaneURL: endpoint, credential: externalWorkerCredential{file: keyPath}, httpClient: server.Client()}
	session, err := connectExternalWorker(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if session.identity == nil || session.identity.SessionID != identity.SessionID {
		t.Fatal("did not resume persisted identity")
	}
	downgrade.Store(true)
	if _, err := connectExternalWorker(context.Background(), config); err == nil {
		t.Fatal("accepted discovery downgrade")
	}
	if calls.Load() != 2 {
		t.Fatalf("unexpected token or enrollment issuance: %d requests", calls.Load())
	}
}
