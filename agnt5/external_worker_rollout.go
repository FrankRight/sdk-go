package agnt5

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const envWorkerMTLSEnabled = "AGNT5_WORKER_MTLS_ENABLED"
const externalWorkerAuthProfileFile = "worker-auth-profile"

type externalWorkerRollout struct {
	ready     bool
	pinned    bool
	directory string
}

// A persisted session or selection survives operator rollback and SDK restarts.
// The opt-in flag authorizes first enrollment; clearing it never authorizes downgrade.
func prepareExternalWorkerRollout(enabled, directory string, credential externalWorkerCredential) (externalWorkerRollout, error) {
	r := externalWorkerRollout{directory: strings.TrimSpace(directory)}
	switch strings.TrimSpace(enabled) {
	case "", "false":
	case "true":
		r.ready = true
	default:
		return r, fmt.Errorf("agnt5: %s must be true or false", envWorkerMTLSEnabled)
	}
	if r.directory != "" {
		for _, name := range []string{externalWorkerAuthProfileFile, externalWorkerSessionFile} {
			path := filepath.Join(r.directory, name)
			info, err := os.Lstat(path)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return r, fmt.Errorf("agnt5: inspect worker identity state: %w", err)
			}
			if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
				return r, errors.New("agnt5: worker identity state must be a private regular file")
			}
			if name == externalWorkerAuthProfileFile {
				value, err := os.ReadFile(path)
				if err != nil {
					return r, err
				}
				if string(value) != authProfileBootstrapMTLS {
					return r, errors.New("agnt5: invalid persisted worker auth profile")
				}
			}
			r.pinned = true
		}
	}
	r.ready = r.ready || r.pinned
	if !r.ready {
		return r, nil
	}
	if credential.file == "" || credential.inline != "" {
		return r, fmt.Errorf("agnt5: mTLS readiness requires %s and forbids inline API keys", envAPIKeyFile)
	}
	if r.directory == "" {
		return r, fmt.Errorf("agnt5: mTLS readiness requires %s", envWorkerSessionDir)
	}
	if err := os.MkdirAll(r.directory, 0700); err != nil {
		return r, err
	}
	info, err := os.Lstat(r.directory)
	if err != nil {
		return r, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return r, errors.New("agnt5: worker session directory must be a real directory")
	}
	if err := os.Chmod(r.directory, 0700); err != nil {
		return r, err
	}
	probe, err := os.CreateTemp(r.directory, ".worker-readiness-*")
	if err != nil {
		return r, fmt.Errorf("agnt5: worker session directory must be writable: %w", err)
	}
	closeErr := probe.Close()
	removeErr := os.Remove(probe.Name())
	if closeErr != nil {
		return r, closeErr
	}
	if removeErr != nil {
		return r, removeErr
	}
	return r, nil
}

func (r externalWorkerRollout) profiles() []string {
	if r.pinned {
		return []string{authProfileBootstrapMTLS}
	}
	if r.ready {
		return []string{authProfileBootstrapMTLS, authProfileTokenAuth}
	}
	return []string{authProfileTokenAuth}
}

func (r externalWorkerRollout) currentProfile() string {
	if r.pinned {
		return authProfileBootstrapMTLS
	}
	return ""
}

func (r externalWorkerRollout) accept(profile string) error {
	if profile == authProfileTokenAuth {
		if r.pinned {
			return errors.New("agnt5: refusing to downgrade an enrolled mTLS worker to token-auth")
		}
		return nil
	}
	if profile != authProfileBootstrapMTLS || !r.ready {
		return errors.New("agnt5: discovery selected an authentication profile the worker is not ready to use")
	}
	// Persist the selection before attempting enrollment: even a failed enrollment
	// must not become an implicit fallback on the next process start.
	temporary, err := os.CreateTemp(r.directory, ".worker-auth-profile-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	if _, err := temporary.WriteString(authProfileBootstrapMTLS); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary.Name(), filepath.Join(r.directory, externalWorkerAuthProfileFile)); err != nil {
		return err
	}
	directory, err := os.Open(r.directory)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
