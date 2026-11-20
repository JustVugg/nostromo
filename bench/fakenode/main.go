// fakenode behaves towards the control plane like a real worker node: it
// registers, keeps a connection open, sends "I am alive" every 10 seconds and
// rewrites its status every now and then. It runs nothing.
// It exists to test nostromo without real machines.
//
// It can authenticate in two ways:
//   - with the cluster admin's certificate (-dir), through the shared port;
//   - like a real kubelet (-token): it joins with a bootstrap token, asks the
//     cluster for its own certificate and then acts under its node identity,
//     through the cluster's dedicated port.
package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func main() {
	dir := flag.String("dir", "", "admin mode: cluster directory inside nostromo's data (holds ca.crt, admin.crt, admin.key)")
	token := flag.String("token", "", "kubelet mode: bootstrap token (from 'nostromo join')")
	caFile := flag.String("ca", "", "kubelet mode: file with the cluster's CA certificate")
	server := flag.String("server", "127.0.0.1:6443", "address to connect to (shared port, or the cluster's dedicated port in kubelet mode)")
	sni := flag.String("sni", "", "cluster name for the shared port, e.g. alpha.nostromo.local")
	name := flag.String("name", "node1", "node name")
	heartbeat := flag.Duration("heartbeat", 10*time.Second, "how often to send the sign of life")
	status := flag.Duration("status", 5*time.Minute, "how often to rewrite the node status")
	ip := flag.String("ip", "", "address the node declares as its own")
	kubeletPort := flag.Int("kubelet-port", 0, "serve a stand-in for the kubelet's HTTPS port here (needs -ip); answers every request with the node name")
	flag.Parse()

	if *caFile == "" {
		*caFile = filepath.Join(*dir, "ca.crt")
	}
	ca, err := os.ReadFile(*caFile)
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	newClient := func(cert *tls.Certificate) *http.Client {
		cfg := &tls.Config{RootCAs: pool, ServerName: *sni}
		if cert != nil {
			cfg.Certificates = []tls.Certificate{*cert}
		}
		return &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{TLSClientConfig: cfg}}
	}

	var cl *http.Client
	bearer := ""
	do := func(method, path, contentType, body string) (int, []byte) {
		r, _ := http.NewRequest(method, "https://"+*server+path, bytes.NewReader([]byte(body)))
		if contentType != "" {
			r.Header.Set("Content-Type", contentType)
		}
		if bearer != "" {
			r.Header.Set("Authorization", "Bearer "+bearer)
		}
		res, err := cl.Do(r)
		if err != nil {
			log.Printf("%s %s: %v", method, path, err)
			return 0, nil
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		if res.StatusCode >= 300 && res.StatusCode != 409 {
			log.Printf("%s %s: %d %s", method, path, res.StatusCode, b)
		}
		return res.StatusCode, b
	}

	if *token != "" {
		// What a kubelet does on its first start: create a key, ask the
		// cluster to certify it, wait for the answer.
		cl, bearer = newClient(nil), *token
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			log.Fatal(err)
		}
		csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
			Subject: pkix.Name{CommonName: "system:node:" + *name, Organization: []string{"system:nodes"}},
		}, key)
		if err != nil {
			log.Fatal(err)
		}
		csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})
		req, _ := json.Marshal(map[string]any{
			"apiVersion": "certificates.k8s.io/v1", "kind": "CertificateSigningRequest",
			"metadata": map[string]string{"generateName": "node-csr-"},
			"spec": map[string]any{"request": base64.StdEncoding.EncodeToString(csrPEM),
				"signerName": "kubernetes.io/kube-apiserver-client-kubelet",
				"usages":     []string{"digital signature", "client auth"}},
		})
		code, b := do("POST", "/apis/certificates.k8s.io/v1/certificatesigningrequests", "application/json", string(req))
		if code != 201 {
			log.Fatalf("certificate request refused (%d)", code)
		}
		var csr struct {
			Metadata struct{ Name string }
			Status   struct{ Certificate string }
		}
		json.Unmarshal(b, &csr)
		deadline := time.Now().Add(2 * time.Minute)
		for csr.Status.Certificate == "" {
			if time.Now().After(deadline) {
				log.Fatal("the cluster did not issue the node certificate in time")
			}
			time.Sleep(time.Second)
			_, b = do("GET", "/apis/certificates.k8s.io/v1/certificatesigningrequests/"+csr.Metadata.Name, "", "")
			json.Unmarshal(b, &csr)
		}
		certPEM, _ := base64.StdEncoding.DecodeString(csr.Status.Certificate)
		keyDER, _ := x509.MarshalECPrivateKey(key)
		cert, err := tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("node %s received its certificate", *name)
		cl, bearer = newClient(&cert), ""
	} else {
		cert, err := tls.LoadX509KeyPair(filepath.Join(*dir, "admin.crt"), filepath.Join(*dir, "admin.key"))
		if err != nil {
			log.Fatal(err)
		}
		cl = newClient(&cert)
	}

	do("POST", "/api/v1/nodes", "application/json",
		fmt.Sprintf(`{"apiVersion":"v1","kind":"Node","metadata":{"name":%q,"labels":{"kubernetes.io/hostname":%q}}}`, *name, *name))

	// what the control plane dials when asked for logs or exec on this node
	where := ""
	if *kubeletPort > 0 && *ip != "" {
		where = fmt.Sprintf(`"addresses":[{"type":"InternalIP","address":%q}],"daemonEndpoints":{"kubeletEndpoint":{"Port":%d}},`, *ip, *kubeletPort)
		go serveKubelet(*ip, *kubeletPort, *name)
	}

	writeStatus := func() {
		now := time.Now().UTC().Format(time.RFC3339)
		do("PATCH", "/api/v1/nodes/"+*name+"/status", "application/merge-patch+json", fmt.Sprintf(
			`{"status":{`+where+`"capacity":{"cpu":"4","memory":"8Gi","pods":"110"},"allocatable":{"cpu":"4","memory":"8Gi","pods":"110"},`+
				`"conditions":[{"type":"Ready","status":"True","reason":"KubeletReady","message":"fake node ready",`+
				`"lastHeartbeatTime":%q,"lastTransitionTime":"2026-01-01T00:00:00Z"}]}}`, now))
	}
	writeHeartbeat := func() {
		now := time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")
		do("PATCH", "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/"+*name+"?fieldManager=fakenode&force=true",
			"application/apply-patch+yaml", fmt.Sprintf(
				`{"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{"name":%q,"namespace":"kube-node-lease"},`+
					`"spec":{"holderIdentity":%q,"leaseDurationSeconds":40,"renewTime":%q}}`, *name, *name, now))
	}

	writeStatus()
	writeHeartbeat()
	log.Printf("node %s registered", *name)
	hb, st := time.NewTicker(*heartbeat), time.NewTicker(*status)
	for {
		select {
		case <-hb.C:
			writeHeartbeat()
		case <-st.C:
			writeStatus()
		}
	}
}

// serveKubelet stands in for the kubelet's own HTTPS port, with a self-signed
// certificate like a freshly installed kubelet has.
func serveKubelet(ip string, port int, name string) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP(ip)}}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{
		Addr:      net.JoinHostPort(ip, fmt.Sprint(port)),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "hello from %s", name)
		}),
	}
	log.Fatal(srv.ListenAndServeTLS("", ""))
}
