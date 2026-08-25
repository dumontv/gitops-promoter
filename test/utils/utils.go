/*
Copyright 2024.

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

package utils

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	ginkgov2 "github.com/onsi/ginkgo/v2"

	"github.com/argoproj-labs/gitops-promoter/internal/e2ecluster"
)

const (
	certmanagerVersion = "v1.5.3"
	certmanagerURLTmpl = "https://github.com/jetstack/cert-manager/releases/download/%s/cert-manager.yaml"
)

var (
	environmentMu sync.RWMutex
	environment   *e2ecluster.Environment
)

// InitializeOwnedCluster verifies and pins the disposable cluster used by the E2E suite.
func InitializeOwnedCluster() error {
	env, err := e2ecluster.FromEnvironment(os.Getenv)
	if err != nil {
		return fmt.Errorf("load owned cluster environment: %w", err)
	}
	if err := e2ecluster.Verify(context.Background(), env, e2ecluster.RunCommand, time.Now().UTC()); err != nil {
		return fmt.Errorf("refusing to run E2E tests: %w", err)
	}
	environmentMu.Lock()
	environment = &env
	environmentMu.Unlock()
	return nil
}

// Run executes the provided command within this context
func Run(cmd *exec.Cmd) ([]byte, error) {
	environmentMu.RLock()
	env := environment
	environmentMu.RUnlock()
	if env == nil {
		return nil, errors.New("E2E cluster has not been verified")
	}
	if err := e2ecluster.VerifyCurrent(context.Background(), *env, e2ecluster.RunCommand); err != nil {
		return nil, fmt.Errorf("refusing E2E command: %w", err)
	}

	dir, _ := GetProjectDir()
	cmd.Dir = dir
	if err := e2ecluster.ConfigureCommand(cmd, *env); err != nil {
		return nil, fmt.Errorf("configure E2E command: %w", err)
	}
	cmd.Env = append(cmd.Env, "GO111MODULE=on")
	command := strings.Join(cmd.Args, " ")

	//nolint:errcheck // logging to test output, error not critical
	fmt.Fprintf(ginkgov2.GinkgoWriter, "running: %s\n", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%s failed with error: (%w) %s", command, err, string(output))
	}

	return output, nil
}

// InstallCertManager installs the cert manager bundle.
func InstallCertManager() error {
	url := fmt.Sprintf(certmanagerURLTmpl, certmanagerVersion)
	cmd := exec.CommandContext(context.Background(), "kubectl", "apply", "-f", url)
	if _, err := Run(cmd); err != nil {
		return err
	}
	// Wait for cert-manager-webhook to be ready, which can take time if cert-manager
	// was re-installed after uninstalling on a cluster.
	cmd = exec.CommandContext(context.Background(), "kubectl", "wait", "deployment.apps/cert-manager-webhook",
		"--for", "condition=Available",
		"--namespace", "cert-manager",
		"--timeout", "5m",
	)

	_, err := Run(cmd)
	return err
}

// LoadImageToKindClusterWithName loads a local docker image to the kind cluster
func LoadImageToKindClusterWithName(name string) error {
	environmentMu.RLock()
	env := environment
	environmentMu.RUnlock()
	if env == nil {
		return errors.New("E2E cluster has not been verified")
	}
	kindOptions := []string{"load", "docker-image", name, "--name", env.ClusterName}
	cmd := exec.CommandContext(context.Background(), "kind", kindOptions...)
	_, err := Run(cmd)
	return err
}

// GetNonEmptyLines converts given command output string into individual objects
// according to line breakers, and ignores the empty elements in it.
func GetNonEmptyLines(output string) []string {
	var res []string
	elements := strings.SplitSeq(output, "\n")
	for element := range elements {
		if element != "" {
			res = append(res, element)
		}
	}

	return res
}

// GetProjectDir will return the directory where the project is
func GetProjectDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return wd, fmt.Errorf("failed to get current working directory: %w", err)
	}
	wd = strings.ReplaceAll(wd, "/test/e2e", "")
	return wd, nil
}
