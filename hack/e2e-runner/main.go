/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/argoproj-labs/gitops-promoter/internal/e2ecluster"
)

const fromLiteral = "--from-literal"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		return fmt.Errorf("generate cluster name: %w", err)
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return fmt.Errorf("generate ownership token: %w", err)
	}

	tempDir, err := os.MkdirTemp("", "gitops-promoter-e2e-")
	if err != nil {
		return fmt.Errorf("create E2E directory: %w", err)
	}
	defer os.RemoveAll(tempDir) //nolint:errcheck // best-effort removal after cluster cleanup

	env := e2ecluster.Environment{
		ClusterName: "promoter-e2e-" + hex.EncodeToString(suffix),
		Token:       hex.EncodeToString(tokenBytes),
		Kubeconfig:  filepath.Join(tempDir, "kubeconfig"),
	}

	clusters, err := e2ecluster.RunCommand(ctx, "kind", "get", "clusters")
	if err != nil {
		return fmt.Errorf("list Kind clusters before E2E setup: %w", err)
	}
	for cluster := range strings.SplitSeq(string(clusters), "\n") {
		if strings.TrimSpace(cluster) == env.ClusterName {
			return fmt.Errorf("refusing to reuse existing Kind cluster %q", env.ClusterName)
		}
	}

	createArgs := []string{
		"create", "cluster",
		"--name", env.ClusterName,
		"--kubeconfig", env.Kubeconfig,
		"--wait", "120s",
	}
	if nodeImage := os.Getenv("KIND_NODE_IMAGE"); nodeImage != "" {
		createArgs = append(createArgs, "--image", nodeImage)
	}
	if err := runAttached(ctx, os.Environ(), "kind", createArgs...); err != nil {
		return fmt.Errorf("create owned Kind cluster: %w", err)
	}
	markerVerified := false
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := e2ecluster.VerifyKindIdentity(cleanupCtx, env, e2ecluster.RunCommand); err != nil {
			fmt.Fprintf(os.Stderr, "refusing to delete unidentified Kind cluster %q: %v\n", env.ClusterName, err)
			return
		}
		if markerVerified {
			if err := e2ecluster.VerifyCurrent(cleanupCtx, env, e2ecluster.RunCommand); err != nil {
				fmt.Fprintf(os.Stderr, "refusing to delete unverified Kind cluster %q: %v\n", env.ClusterName, err)
				return
			}
		}
		if err := runAttached(cleanupCtx, os.Environ(), "kind", "delete", "cluster", "--name", env.ClusterName); err != nil {
			fmt.Fprintf(os.Stderr, "delete owned Kind cluster %q: %v\n", env.ClusterName, err)
		}
	}()

	clusterUID, err := e2ecluster.RunCommand(ctx, "kubectl",
		"--kubeconfig", env.Kubeconfig,
		"--context", env.ContextName(),
		"get", "namespace", "kube-system",
		"--output", "jsonpath={.metadata.uid}",
	)
	if err != nil {
		return fmt.Errorf("read owned cluster identity: %w", err)
	}
	markerArgs := []string{
		"--kubeconfig", env.Kubeconfig,
		"--context", env.ContextName(),
		"--namespace", "kube-system",
		"create", "configmap", e2ecluster.MarkerName,
		fromLiteral, "cluster-name=" + env.ClusterName,
		fromLiteral, "cluster-uid=" + string(clusterUID),
		fromLiteral, "token=" + env.Token,
	}
	if err := runAttached(ctx, e2ecluster.SanitizedEnvironment(os.Environ(), env), "kubectl", markerArgs...); err != nil {
		return fmt.Errorf("create ownership marker: %w", err)
	}
	if err := e2ecluster.Verify(ctx, env, e2ecluster.RunCommand, time.Now().UTC()); err != nil {
		return fmt.Errorf("verify owned Kind cluster before testing: %w", err)
	}
	markerVerified = true

	fmt.Printf("Running E2E tests in newly created Kind cluster %q\n", env.ClusterName)
	testEnvironment := e2ecluster.SanitizedEnvironment(os.Environ(), env)
	if err := runAttached(ctx, testEnvironment, "go", "test", "./test/e2e/", "-count=1", "-v"); err != nil {
		return fmt.Errorf("run E2E tests: %w", err)
	}
	return nil
}

func runAttached(ctx context.Context, env []string, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = env
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run %s: %w", name, err)
	}
	return nil
}
