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

package e2ecluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

const (
	// ClusterNameEnv carries the unique Kind cluster name from the runner to the suite.
	ClusterNameEnv = "GITOPS_PROMOTER_E2E_CLUSTER"
	// TokenEnv carries the random ownership token from the runner to the suite.
	TokenEnv = "GITOPS_PROMOTER_E2E_TOKEN"
	// MarkerName is the ConfigMap used to bind a test invocation to a cluster.
	MarkerName = "gitops-promoter-e2e-owner"
	// MarkerMaxAge is the maximum age accepted for a cluster and ownership marker.
	MarkerMaxAge = 30 * time.Minute
)

// Environment identifies a Kind cluster created for one E2E invocation.
type Environment struct {
	ClusterName string
	Token       string
	Kubeconfig  string
}

// CommandRunner runs a command and returns its combined output.
type CommandRunner func(ctx context.Context, name string, args ...string) ([]byte, error)

// FromEnvironment loads the evidence supplied by the owned-cluster runner.
func FromEnvironment(getenv func(string) string) (Environment, error) {
	env := Environment{
		ClusterName: getenv(ClusterNameEnv),
		Token:       getenv(TokenEnv),
		Kubeconfig:  getenv("KUBECONFIG"),
	}
	if env.ClusterName == "" || env.Token == "" || env.Kubeconfig == "" {
		return Environment{}, errors.New("E2E tests must be started with make test-e2e; missing owned-cluster evidence")
	}
	if strings.ContainsRune(env.Kubeconfig, os.PathListSeparator) {
		return Environment{}, errors.New("E2E KUBECONFIG must contain exactly one file")
	}
	if !filepath.IsAbs(env.Kubeconfig) {
		return Environment{}, errors.New("E2E KUBECONFIG must be an absolute path")
	}
	if len(env.Token) < 32 {
		return Environment{}, errors.New("E2E ownership token is invalid")
	}
	return env, nil
}

// ContextName returns the context Kind creates for this cluster.
func (e Environment) ContextName() string {
	return "kind-" + e.ClusterName
}

// Verify proves that kubectl is pinned to the newly created Kind cluster.
func Verify(ctx context.Context, env Environment, run CommandRunner, now time.Time) error {
	return verify(ctx, env, run, now, true)
}

// VerifyCurrent proves ownership without requiring the cluster to remain newly created.
func VerifyCurrent(ctx context.Context, env Environment, run CommandRunner) error {
	return verify(ctx, env, run, time.Time{}, false)
}

// VerifyKindIdentity proves that a named Kind cluster still matches the runner's isolated kubeconfig.
func VerifyKindIdentity(ctx context.Context, env Environment, run CommandRunner) error {
	configuredKubeconfig, err := os.ReadFile(env.Kubeconfig)
	if err != nil {
		return fmt.Errorf("read E2E kubeconfig: %w", err)
	}

	clusters, err := run(ctx, "kind", "get", "clusters")
	if err != nil {
		return fmt.Errorf("list Kind clusters: %w", err)
	}
	if !slices.Contains(nonEmptyLines(string(clusters)), env.ClusterName) {
		return fmt.Errorf("owned Kind cluster %q does not exist", env.ClusterName)
	}

	kindKubeconfig, err := run(ctx, "kind", "get", "kubeconfig", "--name", env.ClusterName)
	if err != nil {
		return fmt.Errorf("read kubeconfig from Kind cluster %q: %w", env.ClusterName, err)
	}
	return verifyKubeconfigs(configuredKubeconfig, kindKubeconfig, env.ContextName())
}

func verify(ctx context.Context, env Environment, run CommandRunner, now time.Time, requireRecent bool) error {
	if err := VerifyKindIdentity(ctx, env, run); err != nil {
		return err
	}

	clusterNamespaceJSON, err := run(ctx, "kubectl",
		"--kubeconfig", env.Kubeconfig,
		"--context", env.ContextName(),
		"--request-timeout=10s",
		"get", "namespace", metav1NamespaceSystem,
		"--output", "json",
	)
	if err != nil {
		return fmt.Errorf("read E2E cluster identity: %w", err)
	}
	clusterUID, err := verifyClusterNamespace(clusterNamespaceJSON, now, requireRecent)
	if err != nil {
		return err
	}

	markerJSON, err := run(ctx, "kubectl",
		"--kubeconfig", env.Kubeconfig,
		"--context", env.ContextName(),
		"--request-timeout=10s",
		"--namespace", metav1NamespaceSystem,
		"get", "configmap", MarkerName,
		"--output", "json",
	)
	if err != nil {
		return fmt.Errorf("read E2E ownership marker: %w", err)
	}
	return verifyMarker(markerJSON, env, clusterUID, now, requireRecent)
}

const metav1NamespaceSystem = "kube-system"

func verifyKubeconfigs(configuredData, kindData []byte, expectedContext string) error {
	configured, err := clientcmd.Load(configuredData)
	if err != nil {
		return fmt.Errorf("parse E2E kubeconfig: %w", err)
	}
	kindConfig, err := clientcmd.Load(kindData)
	if err != nil {
		return fmt.Errorf("parse Kind kubeconfig: %w", err)
	}

	if configured.CurrentContext != expectedContext {
		return fmt.Errorf("E2E context is %q, expected %q", configured.CurrentContext, expectedContext)
	}
	if len(configured.Contexts) != 1 || len(configured.Clusters) != 1 || len(configured.AuthInfos) != 1 {
		return errors.New("E2E kubeconfig must contain exactly one context, cluster, and user")
	}

	configuredCluster, configuredAuth, err := activeEntries(configured)
	if err != nil {
		return fmt.Errorf("resolve E2E kubeconfig: %w", err)
	}
	kindCluster, kindAuth, err := activeEntries(kindConfig)
	if err != nil {
		return fmt.Errorf("resolve Kind kubeconfig: %w", err)
	}
	if configuredCluster.Server != kindCluster.Server ||
		configuredCluster.InsecureSkipTLSVerify != kindCluster.InsecureSkipTLSVerify ||
		!bytes.Equal(configuredCluster.CertificateAuthorityData, kindCluster.CertificateAuthorityData) ||
		!bytes.Equal(configuredAuth.ClientCertificateData, kindAuth.ClientCertificateData) ||
		!bytes.Equal(configuredAuth.ClientKeyData, kindAuth.ClientKeyData) {
		return errors.New("E2E kubeconfig does not identify the owned Kind cluster")
	}
	return nil
}

func activeEntries(config *clientcmdapi.Config) (*clientcmdapi.Cluster, *clientcmdapi.AuthInfo, error) {
	contextConfig, ok := config.Contexts[config.CurrentContext]
	if !ok {
		return nil, nil, fmt.Errorf("current context %q is not defined", config.CurrentContext)
	}
	cluster, ok := config.Clusters[contextConfig.Cluster]
	if !ok {
		return nil, nil, fmt.Errorf("cluster %q is not defined", contextConfig.Cluster)
	}
	auth, ok := config.AuthInfos[contextConfig.AuthInfo]
	if !ok {
		return nil, nil, fmt.Errorf("user %q is not defined", contextConfig.AuthInfo)
	}
	return cluster, auth, nil
}

func verifyClusterNamespace(data []byte, now time.Time, requireRecent bool) (string, error) {
	namespace := &corev1.Namespace{}
	if err := json.Unmarshal(data, namespace); err != nil {
		return "", fmt.Errorf("parse E2E cluster identity: %w", err)
	}
	if namespace.Name != metav1NamespaceSystem || namespace.UID == "" {
		return "", errors.New("E2E cluster identity is invalid")
	}
	if requireRecent {
		if err := verifyRecent(namespace.CreationTimestamp.Time, now, "E2E cluster"); err != nil {
			return "", err
		}
	}
	return string(namespace.UID), nil
}

func verifyMarker(data []byte, env Environment, clusterUID string, now time.Time, requireRecent bool) error {
	marker := &corev1.ConfigMap{}
	if err := json.Unmarshal(data, marker); err != nil {
		return fmt.Errorf("parse E2E ownership marker: %w", err)
	}
	if marker.Name != MarkerName || marker.Namespace != metav1NamespaceSystem ||
		marker.Data["cluster-name"] != env.ClusterName || marker.Data["cluster-uid"] != clusterUID ||
		marker.Data["token"] != env.Token {
		return errors.New("E2E ownership marker does not match this test invocation")
	}
	if marker.UID == "" {
		return errors.New("E2E ownership marker has no server-assigned identity")
	}
	if requireRecent {
		return verifyRecent(marker.CreationTimestamp.Time, now, "E2E ownership marker")
	}
	return nil
}

func verifyRecent(createdAt, now time.Time, subject string) error {
	age := now.Sub(createdAt)
	if age < 0 || age > MarkerMaxAge {
		return fmt.Errorf("%s is not recent: age %s", subject, age)
	}
	return nil
}

// ConfigureCommand pins a child command to the verified cluster and removes ambient selectors.
func ConfigureCommand(cmd *exec.Cmd, env Environment) error {
	cmd.Env = sanitizedEnvironment(os.Environ(), env)
	if filepath.Base(cmd.Path) == "kubectl" {
		for _, arg := range cmd.Args[1:] {
			if isClusterSelector(arg) {
				return fmt.Errorf("kubectl cluster selector %q is forbidden in E2E commands", arg)
			}
		}
		cmd.Args = slices.Insert(cmd.Args, 1,
			"--kubeconfig", env.Kubeconfig,
			"--context", env.ContextName(),
		)
	}
	return nil
}

func isClusterSelector(arg string) bool {
	for _, selector := range []string{"--kubeconfig", "--context", "--cluster", "--server", "-s"} {
		if arg == selector || strings.HasPrefix(arg, selector+"=") {
			return true
		}
	}
	return false
}

// SanitizedEnvironment constructs the environment used by the test subprocess.
func SanitizedEnvironment(base []string, env Environment) []string {
	return sanitizedEnvironment(base, env)
}

func sanitizedEnvironment(base []string, env Environment) []string {
	blocked := map[string]struct{}{
		"KUBECONFIG":    {},
		"KUBECTL":       {},
		"KIND_CLUSTER":  {},
		"MAKEFLAGS":     {},
		"MAKEFILES":     {},
		"MAKEOVERRIDES": {},
		"MFLAGS":        {},
		ClusterNameEnv:  {},
		TokenEnv:        {},
	}
	result := make([]string, 0, len(base)+5)
	for _, item := range base {
		key, _, _ := strings.Cut(item, "=")
		if _, found := blocked[key]; !found {
			result = append(result, item)
		}
	}
	return append(result,
		"KUBECONFIG="+env.Kubeconfig,
		"KIND_CLUSTER="+env.ClusterName,
		ClusterNameEnv+"="+env.ClusterName,
		TokenEnv+"="+env.Token,
		"KUBECTL=kubectl --kubeconfig="+shellQuote(env.Kubeconfig)+" --context="+shellQuote(env.ContextName()),
	)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func nonEmptyLines(output string) []string {
	var lines []string
	for line := range strings.SplitSeq(output, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// RunCommand is the production command implementation used by verification.
func RunCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, output)
	}
	return output, nil
}
