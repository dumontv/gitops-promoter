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
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

var _ = Describe("owned E2E clusters", func() {
	const (
		clusterName = "promoter-e2e-123456"
		token       = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	)

	var (
		env Environment
		now time.Time
	)

	BeforeEach(func() {
		now = time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
		env = Environment{
			ClusterName: clusterName,
			Token:       token,
			Kubeconfig:  filepath.Join(GinkgoT().TempDir(), "kubeconfig"),
		}
	})

	Describe("FromEnvironment", func() {
		It("requires all owned-cluster evidence", func() {
			_, err := FromEnvironment(func(string) string { return "" })
			Expect(err).To(MatchError(ContainSubstring("make test-e2e")))
		})

		It("rejects a kubeconfig search path", func() {
			values := map[string]string{
				ClusterNameEnv: clusterName,
				TokenEnv:       token,
				"KUBECONFIG":   "/tmp/first" + string(os.PathListSeparator) + "/tmp/second",
			}
			_, err := FromEnvironment(func(key string) string { return values[key] })
			Expect(err).To(MatchError(ContainSubstring("exactly one file")))
		})
	})

	Describe("Verify", func() {
		It("accepts matching Kind identity and a recent ownership marker", func() {
			kindConfig := kubeconfig(env.ContextName(), "https://127.0.0.1:54321", []byte("ca"), []byte("cert"), []byte("key"))
			writeKubeconfig(env.Kubeconfig, kindConfig)
			runner := fakeRunner(env, kindConfig, clusterNamespace(now.Add(-time.Minute)), marker(env, now.Add(-time.Minute)))

			Expect(Verify(context.Background(), env, runner, now)).To(Succeed())
		})

		It("rejects a cluster that Kind does not list", func() {
			kindConfig := kubeconfig(env.ContextName(), "https://127.0.0.1:54321", []byte("ca"), []byte("cert"), []byte("key"))
			writeKubeconfig(env.Kubeconfig, kindConfig)
			runner := func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name == "kind" && slices.Equal(args, []string{"get", "clusters"}) {
					return []byte("another-cluster\n"), nil
				}
				Fail("unexpected command: " + name + " " + strings.Join(args, " "))
				return nil, nil
			}

			Expect(Verify(context.Background(), env, runner, now)).To(MatchError(ContainSubstring("does not exist")))
		})

		It("rejects a kubeconfig for a different API server", func() {
			kindConfig := kubeconfig(env.ContextName(), "https://127.0.0.1:54321", []byte("ca"), []byte("cert"), []byte("key"))
			configured := kubeconfig(env.ContextName(), "https://production.example.com", []byte("ca"), []byte("cert"), []byte("key"))
			writeKubeconfig(env.Kubeconfig, configured)

			Expect(Verify(context.Background(), env, fakeRunner(env, kindConfig, clusterNamespace(now), marker(env, now)), now)).To(
				MatchError(ContainSubstring("does not identify the owned Kind cluster")),
			)
		})

		It("rejects mismatched ownership", func() {
			kindConfig := kubeconfig(env.ContextName(), "https://127.0.0.1:54321", []byte("ca"), []byte("cert"), []byte("key"))
			writeKubeconfig(env.Kubeconfig, kindConfig)
			wrongMarker := marker(env, now)
			wrongMarker.Data["token"] = strings.Repeat("f", 64)

			Expect(Verify(context.Background(), env, fakeRunner(env, kindConfig, clusterNamespace(now), wrongMarker), now)).To(
				MatchError(ContainSubstring("does not match")),
			)
		})

		It("rejects a cluster that was not newly created", func() {
			kindConfig := kubeconfig(env.ContextName(), "https://127.0.0.1:54321", []byte("ca"), []byte("cert"), []byte("key"))
			writeKubeconfig(env.Kubeconfig, kindConfig)

			Expect(Verify(context.Background(), env, fakeRunner(
				env,
				kindConfig,
				clusterNamespace(now.Add(-MarkerMaxAge-time.Second)),
				marker(env, now),
			), now)).To(
				MatchError(ContainSubstring("cluster is not recent")),
			)
		})

		It("rejects a stale ownership marker", func() {
			kindConfig := kubeconfig(env.ContextName(), "https://127.0.0.1:54321", []byte("ca"), []byte("cert"), []byte("key"))
			writeKubeconfig(env.Kubeconfig, kindConfig)

			Expect(Verify(context.Background(), env, fakeRunner(
				env,
				kindConfig,
				clusterNamespace(now),
				marker(env, now.Add(-MarkerMaxAge-time.Second)),
			), now)).To(
				MatchError(ContainSubstring("not recent")),
			)
		})

		It("keeps accepting the same owned cluster after the startup window", func() {
			kindConfig := kubeconfig(env.ContextName(), "https://127.0.0.1:54321", []byte("ca"), []byte("cert"), []byte("key"))
			writeKubeconfig(env.Kubeconfig, kindConfig)
			old := now.Add(-2 * MarkerMaxAge)

			Expect(VerifyCurrent(
				context.Background(),
				env,
				fakeRunner(env, kindConfig, clusterNamespace(old), marker(env, old)),
			)).To(Succeed())
		})
	})

	Describe("ConfigureCommand", func() {
		It("pins kubectl arguments and replaces ambient selectors", func() {
			cmd := exec.Command("kubectl", "get", "pods")
			Expect(ConfigureCommand(cmd, env)).To(Succeed())

			Expect(cmd.Args).To(Equal([]string{
				"kubectl", "--kubeconfig", env.Kubeconfig, "--context", env.ContextName(), "get", "pods",
			}))
			Expect(environmentValue(cmd.Env, "KUBECONFIG")).To(Equal(env.Kubeconfig))
			Expect(environmentValue(cmd.Env, "KIND_CLUSTER")).To(Equal(env.ClusterName))
		})

		It("rejects kubectl commands with their own cluster selector", func() {
			cmd := exec.Command("kubectl", "--context=unsafe", "get", "pods")
			Expect(ConfigureCommand(cmd, env)).To(MatchError(ContainSubstring("is forbidden")))
		})

		It("removes inherited cluster selectors", func() {
			configured := SanitizedEnvironment([]string{
				"PATH=/bin",
				"KUBECONFIG=/unsafe",
				"KUBECTL=kubectl --context=unsafe",
				"KIND_CLUSTER=unsafe",
				"MAKEFLAGS=KUBECTL=kubectl --context=unsafe",
				"MAKEFILES=/tmp/unsafe.mk",
			}, env)

			Expect(environmentValue(configured, "PATH")).To(Equal("/bin"))
			Expect(environmentValue(configured, "KUBECONFIG")).To(Equal(env.Kubeconfig))
			Expect(environmentValue(configured, "KUBECTL")).To(ContainSubstring(env.ContextName()))
			Expect(configured).To(HaveLen(6))
		})
	})
})

func kubeconfig(contextName, server string, ca, cert, key []byte) *clientcmdapi.Config {
	return &clientcmdapi.Config{
		CurrentContext: contextName,
		Contexts: map[string]*clientcmdapi.Context{
			contextName: {Cluster: contextName, AuthInfo: contextName},
		},
		Clusters: map[string]*clientcmdapi.Cluster{
			contextName: {Server: server, CertificateAuthorityData: ca},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			contextName: {ClientCertificateData: cert, ClientKeyData: key},
		},
	}
}

func writeKubeconfig(path string, config *clientcmdapi.Config) {
	data, err := clientcmd.Write(*config)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	ExpectWithOffset(1, os.WriteFile(path, data, 0o600)).To(Succeed())
}

func marker(env Environment, createdAt time.Time) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:              MarkerName,
			Namespace:         metav1NamespaceSystem,
			UID:               "marker-uid",
			CreationTimestamp: metav1.NewTime(createdAt),
		},
		Data: map[string]string{
			"cluster-name": env.ClusterName,
			"cluster-uid":  "cluster-uid",
			"token":        env.Token,
		},
	}
}

func clusterNamespace(createdAt time.Time) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:              metav1NamespaceSystem,
		UID:               "cluster-uid",
		CreationTimestamp: metav1.NewTime(createdAt),
	}}
}

func fakeRunner(
	env Environment,
	kindConfig *clientcmdapi.Config,
	clusterNamespace *corev1.Namespace,
	ownershipMarker *corev1.ConfigMap,
) CommandRunner {
	return func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch {
		case name == "kind" && slices.Equal(args, []string{"get", "clusters"}):
			return []byte(env.ClusterName + "\n"), nil
		case name == "kind" && slices.Equal(args, []string{"get", "kubeconfig", "--name", env.ClusterName}):
			data, err := clientcmd.Write(*kindConfig)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
			return data, nil
		case name == "kubectl" && slices.Contains(args, "namespace"):
			data, err := json.Marshal(clusterNamespace)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
			return data, nil
		case name == "kubectl" && slices.Contains(args, "configmap"):
			data, err := json.Marshal(ownershipMarker)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
			return data, nil
		default:
			Fail("unexpected command: " + name + " " + strings.Join(args, " "))
			return nil, nil
		}
	}
}

func environmentValue(environment []string, key string) string {
	prefix := key + "="
	for _, item := range environment {
		if strings.HasPrefix(item, prefix) {
			return strings.TrimPrefix(item, prefix)
		}
	}
	return ""
}
