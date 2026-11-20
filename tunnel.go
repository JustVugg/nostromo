package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// The tunnel (Konnectivity) lets the control plane reach its nodes when it
// cannot dial them directly, which is the normal case for a hosted control
// plane: nodes sit in the customer's network. An agent on the nodes opens a
// connection towards nostromo and keeps it open; the apiserver sends through
// it whatever it needs to send to nodes and pods (logs, exec, webhooks).
//
// Per cluster, nostromo runs the upstream tunnel server next to the
// apiserver. The two talk over a local socket; agents connect to the
// cluster's agent port.

const (
	tunnelAudience = "system:konnectivity-server"
	tunnelAgent    = "konnectivity-agent"
)

func (s *server) tunnelBinary() string {
	return filepath.Join(s.tools, "konnectivity", "proxy-server")
}

// tunnelEnabled: nodes are supported, the cluster has an agent port and the
// tunnel server binary is installed.
func (t *tenant) tunnelEnabled() bool {
	if t.srv.nodeAddress == "" || t.AgentPort == 0 {
		return false
	}
	_, err := os.Stat(t.srv.tunnelBinary())
	return err == nil
}

func (t *tenant) socket() string {
	return filepath.Join(t.srv.socketDir, t.Name+".sock")
}

// prepareTunnel writes what the apiserver needs before it starts: the rule
// "traffic towards the cluster goes through the tunnel".
func (t *tenant) prepareTunnel() error {
	if err := os.MkdirAll(t.srv.socketDir, 0o700); err != nil {
		return err
	}
	return writeAtomic(t.file("egress.yaml"), []byte(fmt.Sprintf(`apiVersion: apiserver.k8s.io/v1beta1
kind: EgressSelectorConfiguration
egressSelections:
- name: cluster
  connection:
    proxyProtocol: GRPC
    transport:
      uds:
        udsName: %q
`, t.socket())))
}

// startTunnel starts the tunnel server. With t.mu held, apiserver ready.
func (t *tenant) startTunnel() {
	s := t.srv
	// The tunnel server checks every agent's token with the cluster itself;
	// for that its own identity must be allowed to ask.
	if err := t.apply("/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/nostromo:konnectivity-server", map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
		"metadata": map[string]string{"name": "nostromo:konnectivity-server"},
		"roleRef":  map[string]string{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "system:auth-delegator"},
		"subjects": []any{map[string]string{"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": tunnelAudience}},
	}); err != nil {
		log.Printf("[%s] tunnel permissions: %v", t.Name, err)
	}
	if err := t.internalKubeconfig("konnectivity"); err != nil {
		log.Printf("[%s] tunnel: %v", t.Name, err)
		return
	}
	admin, _ := freePort()
	health, _ := freePort()
	var err error
	if t.tunnelPort, err = freePort(); err != nil {
		log.Printf("[%s] tunnel: %v", t.Name, err)
		return
	}
	t.tunnel, err = start(t.file("konnectivity.log"), s.childEnv, s.tunnelBinary(),
		"--logtostderr=true",
		"--mode=grpc",
		// to reach a node, use the agent running on that very node: agents
		// on other nodes may well be unable to reach it
		"--proxy-strategies=destHost,default",
		// apiserver side: a local socket
		"--uds-name="+t.socket(), "--delete-existing-uds-file", "--server-port=0",
		// agent side: nostromo relays here what arrives at the cluster's agent
		// port; encrypted end to end with the cluster's own certificate
		"--agent-bind-address=127.0.0.1", fmt.Sprintf("--agent-port=%d", t.tunnelPort),
		// with several instances behind one address, an agent keeps calling
		// until it is connected to every one of them
		"--server-id="+s.instance, fmt.Sprintf("--server-count=%d", max(s.instances.Load(), 1)),
		"--cluster-cert="+t.file("apiserver.crt"), "--cluster-key="+t.file("apiserver.key"),
		// agents prove who they are with a token only this cluster can have issued
		"--kubeconfig="+t.file("konnectivity.kubeconfig"),
		"--agent-namespace=kube-system", "--agent-service-account="+tunnelAgent,
		"--authentication-audience="+tunnelAudience,
		"--admin-bind-address=127.0.0.1", fmt.Sprintf("--admin-port=%d", admin),
		"--health-bind-address=127.0.0.1", fmt.Sprintf("--health-port=%d", health),
	)
	if err != nil {
		log.Printf("[%s] tunnel server: %v", t.Name, err)
	}
}

// installTunnelAgent installs, inside the cluster, the agent that every node
// will run. It is the nodes that call nostromo, never the other way round.
func (t *tenant) installTunnelAgent() error {
	s := t.srv
	if err := t.apply("/api/v1/namespaces/kube-system/serviceaccounts/"+tunnelAgent, map[string]any{
		"apiVersion": "v1", "kind": "ServiceAccount",
		"metadata": map[string]string{"name": tunnelAgent, "namespace": "kube-system"},
	}); err != nil {
		return err
	}
	labels := map[string]string{"k8s-app": tunnelAgent}
	return t.apply("/apis/apps/v1/namespaces/kube-system/daemonsets/"+tunnelAgent, map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{"name": tunnelAgent, "namespace": "kube-system", "labels": labels},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": labels},
			"template": map[string]any{
				"metadata": map[string]any{"labels": labels},
				"spec": map[string]any{
					"serviceAccountName": tunnelAgent,
					"priorityClassName":  "system-cluster-critical",
					"hostNetwork":        true,
					"tolerations":        []any{map[string]string{"operator": "Exists"}},
					"nodeSelector":       map[string]string{"kubernetes.io/os": "linux"},
					"containers": []any{map[string]any{
						"name":    tunnelAgent,
						"image":   s.agentImage,
						"command": []string{"/proxy-agent"},
						"args": []string{
							"--logtostderr=true",
							"--agent-identifiers=host=$(HOST_IP)", // lets the server pick this agent for this node
							"--sync-forever=true",                 // notice instances of nostromo added later
							"--ca-cert=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
							"--proxy-server-host=" + s.nodeAddress,
							fmt.Sprintf("--proxy-server-port=%d", t.AgentPort),
							"--service-account-token-path=/var/run/secrets/tokens/" + tunnelAgent + "-token",
							"--admin-server-port=8133", "--health-server-port=8134",
						},
						"env": []any{map[string]any{"name": "HOST_IP",
							"valueFrom": map[string]any{"fieldRef": map[string]string{"fieldPath": "status.hostIP"}}}},
						"livenessProbe": map[string]any{
							"httpGet":             map[string]any{"path": "/healthz", "port": 8134},
							"initialDelaySeconds": 15, "timeoutSeconds": 15,
						},
						"volumeMounts": []any{map[string]string{"name": "token", "mountPath": "/var/run/secrets/tokens"}},
					}},
					"volumes": []any{map[string]any{"name": "token", "projected": map[string]any{
						"sources": []any{map[string]any{"serviceAccountToken": map[string]any{
							"path": tunnelAgent + "-token", "audience": tunnelAudience,
						}}},
					}}},
				},
			},
		},
	})
}
