package agnt5

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestExternalWorkerCSRUsesECDSAP256AndProofOfPossession(t *testing.T) {
	privateKey, encodedCSR, err := newExternalWorkerCSR()
	if err != nil {
		t.Fatalf("newExternalWorkerCSR: %v", err)
	}
	if privateKey == "" {
		t.Fatal("private key is empty")
	}
	csrDER, err := base64.StdEncoding.DecodeString(encodedCSR)
	if err != nil {
		t.Fatalf("decode CSR: %v", err)
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		t.Fatalf("parse CSR: %v", err)
	}
	if err := csr.CheckSignature(); err != nil {
		t.Fatalf("CSR signature: %v", err)
	}
	publicKey, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok || publicKey.Curve.Params().Name != "P-256" {
		t.Fatalf("CSR public key = %T/%v", csr.PublicKey, publicKey)
	}
}

func TestExternalWorkerIdentityFileIsPrivateAtomicAndAuthorityBound(t *testing.T) {
	authority := externalWorkerConnection{
		ProjectID: "project", EnvironmentID: "environment", DeploymentID: "deployment",
		WorkerPoolID: "pool", RuntimeEndpoint: "https://runtime.example.com",
	}
	identity := testExternalWorkerIdentity(t, authority)
	path := filepath.Join(t.TempDir(), "session", externalWorkerSessionFile)
	if err := writeExternalWorkerIdentity(path, identity); err != nil {
		t.Fatalf("writeExternalWorkerIdentity: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat identity: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode = %o, want 600", info.Mode().Perm())
	}
	loaded, err := readExternalWorkerIdentity(path, authority)
	if err != nil || loaded.SessionID != identity.SessionID {
		t.Fatalf("read identity = %#v, %v", loaded, err)
	}
	wrong := authority
	wrong.ProjectID = "other"
	if _, err := readExternalWorkerIdentity(path, wrong); err == nil {
		t.Fatal("cross-project session was accepted")
	}
	if matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), "."+externalWorkerSessionFile+".*.tmp")); err != nil || len(matches) != 0 {
		t.Fatalf("temporary identity files remain: %v, %v", matches, err)
	}
}

func TestExternalWorkerConfigurationDefersAuthenticationToDiscovery(t *testing.T) {
	keyPath := filepath.Join(t.TempDir(), "bootstrap-key")
	if err := os.WriteFile(keyPath, []byte("credential"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envAPIKey, "")
	t.Setenv(envAPIKeyFile, keyPath)
	t.Setenv(envControlPlaneURL, "https://api.example.com")
	t.Setenv(envWorkerSessionDir, t.TempDir())
	config, enabled, err := externalWorkerConfigFromEnv(false)
	if err != nil || !enabled || config.identityMode || config.identityURL != nil || config.sessionPath != "" {
		t.Fatalf("identity config = %#v, enabled=%t, err=%v", config, enabled, err)
	}
}

func testExternalWorkerIdentity(t *testing.T, authority externalWorkerConnection) *externalWorkerIdentity {
	t.Helper()
	privateKeyPEM, _, err := newExternalWorkerCSR()
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode([]byte(privateKeyPEM))
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key := parsed.(*ecdsa.PrivateKey)
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "worker"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	identity := &externalWorkerIdentity{
		SessionID: "session", ProjectID: authority.ProjectID, EnvironmentID: authority.EnvironmentID,
		DeploymentID: authority.DeploymentID, WorkerPoolID: authority.WorkerPoolID, WorkerID: "worker",
		SPIFFEID:        "spiffe://example/workload/project/environment/deployment/worker",
		RuntimeEndpoint: authority.RuntimeEndpoint, CertificateDERBase64: base64.StdEncoding.EncodeToString(der),
		TrustBundleDERBase64: []string{base64.StdEncoding.EncodeToString(der)}, TrustBundleVersion: "v1",
		CertificateExpiresAt: now.Add(time.Hour), RenewAfter: now.Add(40 * time.Minute),
		WorkloadToken: "token", TokenType: "Bearer", TokenExpiresAt: now.Add(10 * time.Minute), PrivateKeyPEM: privateKeyPEM,
	}
	if _, err := json.Marshal(identity); err != nil {
		t.Fatal(err)
	}
	return identity
}

func TestExternalWorkerRenewalResumesSameCSRAndKeyAfterLostResponse(t *testing.T) {
	authority := externalWorkerConnection{ProjectID: "project", EnvironmentID: "environment", DeploymentID: "deployment", WorkerPoolID: "pool", RuntimeEndpoint: "https://runtime.example.com"}
	current := testExternalWorkerIdentity(t, authority)
	current.TokenExpiresAt = time.Now().Add(-time.Minute)
	path := filepath.Join(t.TempDir(), externalWorkerSessionFile)
	if err := writeExternalWorkerIdentity(path, current); err != nil {
		t.Fatal(err)
	}
	var lock sync.Mutex
	var first externalWorkerRenewal
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		calls++
		var request map[string]string
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		stored, err := readExternalWorkerIdentity(path, authority)
		if err != nil || stored.PendingRenewal == nil {
			t.Errorf("intent not persisted before request: %v", err)
			w.WriteHeader(500)
			return
		}
		pending := *stored.PendingRenewal
		if request["request_id"] != pending.RequestID || request["csr_der_base64"] != pending.CSRDERBase64 {
			t.Error("request does not match persisted intent")
		}
		if calls == 1 {
			first = pending
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		if first != pending {
			t.Error("restart replaced request identity or private key")
		}
		block, _ := pem.Decode([]byte(pending.PrivateKeyPEM))
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			t.Error(err)
			return
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		der, err := x509.CreateCertificate(rand.Reader, template, template, &key.(*ecdsa.PrivateKey).PublicKey, key)
		if err != nil {
			t.Error(err)
			return
		}
		next := *current
		next.SessionID = "successor"
		next.CertificateDERBase64 = base64.StdEncoding.EncodeToString(der)
		next.PrivateKeyPEM = ""
		next.PendingRenewal = nil
		next.TokenExpiresAt = time.Now().Add(10 * time.Minute)
		_ = json.NewEncoder(w).Encode(next)
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	config := externalWorkerBootstrapConfig{sessionPath: path, identityURL: endpoint}
	firstProcess := &externalWorkerSession{config: config, connection: authority, identity: current}
	if err := firstProcess.renewIdentityLocked(context.Background()); err == nil {
		t.Fatal("lost response should fail")
	}
	restartedIdentity, err := loadOrOpenExternalWorkerIdentity(context.Background(), config, "unused-bootstrap", authority)
	if err != nil {
		t.Fatal(err)
	}
	restarted := &externalWorkerSession{config: config, connection: authority, identity: restartedIdentity}
	if err := restarted.renewIdentityLocked(context.Background()); err != nil {
		t.Fatal(err)
	}
	final, err := readExternalWorkerIdentity(path, authority)
	if err != nil {
		t.Fatal(err)
	}
	lock.Lock()
	defer lock.Unlock()
	if final.SessionID != "successor" || final.PendingRenewal != nil || final.PrivateKeyPEM != first.PrivateKeyPEM {
		t.Fatal("successor was not durably activated with original replacement key")
	}
	if calls != 2 {
		t.Fatalf("requests = %d, want 2", calls)
	}
}

func TestExternalWorkerServerTrustIsSeparateFromWorkloadTrust(t *testing.T) {
	authority := externalWorkerConnection{ProjectID: "project", EnvironmentID: "environment", DeploymentID: "deployment", WorkerPoolID: "pool", RuntimeEndpoint: "https://runtime.example.com"}
	identity := testExternalWorkerIdentity(t, authority)
	workerDER, _ := base64.StdEncoding.DecodeString(identity.CertificateDERBase64)
	workerLeaf, _ := x509.ParseCertificate(workerDER)
	workloadRoots := x509.NewCertPool()
	workloadRoots.AddCert(workerLeaf)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			t.Error("worker certificate not verified")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: workloadRoots}
	server.StartTLS()
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	session := &externalWorkerSession{identity: identity, config: externalWorkerBootstrapConfig{identityURL: endpoint}}
	t.Setenv("AGNT5_WORKER_SERVER_CA_FILE", "")
	client, err := session.identityHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	if response, err := client.Get(server.URL); err == nil {
		response.Body.Close()
		t.Fatal("untrusted server accepted through workload CA")
	}
	path := filepath.Join(t.TempDir(), "server-ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGNT5_WORKER_SERVER_CA_FILE", path)
	client, err = session.identityHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status=%d", response.StatusCode)
	}
}
