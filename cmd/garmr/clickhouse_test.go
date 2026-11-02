package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testCA is a throwaway CA, with the PEM bundle a client would be given written to caFile.
type testCA struct {
	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
	caFile string
}

func newTestCA(t *testing.T, name string) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return testCA{cert: cert, key: key, caFile: caFile}
}

// serve starts a TLS listener presenting a certificate for dnsName signed by ca, and completes the handshake
// of every connection it accepts.
func (ca testCA) serve(t *testing.T, dnsName string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.(*tls.Conn).Handshake()
			_ = conn.Close()
		}
	}()
	return ln.Addr().String()
}

func handshake(t *testing.T, addr string, cfg *tls.Config) error {
	t.Helper()
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, cfg)
	if err != nil {
		return err
	}
	return conn.Close()
}

func TestClickHouseTLSVerifiesTheServer(t *testing.T) {
	ca := newTestCA(t, "platform-ca")
	addr := ca.serve(t, "clickhouse.paas.example")

	cfg, err := clickhouseTLS("true", ca.caFile, "clickhouse.paas.example")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify is set")
	}
	if len(cfg.Certificates) != 0 || cfg.GetClientCertificate != nil {
		t.Fatal("a client certificate is configured")
	}
	if err := handshake(t, addr, cfg); err != nil {
		t.Fatalf("handshake with the right CA and name: %v", err)
	}

	t.Run("refuses a certificate from another CA", func(t *testing.T) {
		other := newTestCA(t, "other-ca")
		cfg, err := clickhouseTLS("true", other.caFile, "clickhouse.paas.example")
		if err != nil {
			t.Fatal(err)
		}
		if err := handshake(t, addr, cfg); err == nil || !strings.Contains(err.Error(), "unknown authority") {
			t.Fatalf("want an unknown authority error, got %v", err)
		}
	})

	t.Run("refuses a certificate for another name", func(t *testing.T) {
		cfg, err := clickhouseTLS("true", ca.caFile, "other.paas.example")
		if err != nil {
			t.Fatal(err)
		}
		if err := handshake(t, addr, cfg); err == nil || !strings.Contains(err.Error(), "other.paas.example") {
			t.Fatalf("want a name mismatch error, got %v", err)
		}
	})

	t.Run("checks the dialled host when no server name is set", func(t *testing.T) {
		cfg, err := clickhouseTLS("true", ca.caFile, "")
		if err != nil {
			t.Fatal(err)
		}
		// The certificate names clickhouse.paas.example, not 127.0.0.1.
		if err := handshake(t, addr, cfg); err == nil {
			t.Fatal("handshake succeeded against a certificate for another host")
		}
	})
}

func TestClickHouseTLSOff(t *testing.T) {
	for _, enabled := range []string{"false", "0"} {
		cfg, err := clickhouseTLS(enabled, "", "")
		if err != nil || cfg != nil {
			t.Fatalf("clickhouseTLS(%q) = %v, %v; want nil, nil", enabled, cfg, err)
		}
	}
}

func TestClickHouseTLSStartupErrors(t *testing.T) {
	notPEM := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(notPEM, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, enabled, caFile, serverName, want string
	}{
		{"enabled is not a boolean", "yes please", "", "", "CLICKHOUSE_TLS_ENABLED"},
		{"a CA file without TLS", "false", "/etc/clickhouse-ca/ca.crt", "", "CLICKHOUSE_TLS_ENABLED is not true"},
		{"a server name without TLS", "false", "", "clickhouse.paas.example", "CLICKHOUSE_TLS_ENABLED is not true"},
		{"a missing CA file", "true", filepath.Join(t.TempDir(), "missing.crt"), "", "CLICKHOUSE_TLS_CA_FILE"},
		{"a CA file with no certificate", "true", notPEM, "", "no PEM certificate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := clickhouseTLS(tc.enabled, tc.caFile, tc.serverName)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestClickHouseOptionsFromEnvironment(t *testing.T) {
	ca := newTestCA(t, "platform-ca")
	t.Setenv("CLICKHOUSE_ADDR", "clickhouse.paas.example:9440")
	t.Setenv("CLICKHOUSE_PASSWORD", "from-env")
	t.Setenv("CLICKHOUSE_TLS_ENABLED", "true")
	t.Setenv("CLICKHOUSE_TLS_CA_FILE", ca.caFile)
	t.Setenv("CLICKHOUSE_TLS_SERVER_NAME", "clickhouse.paas.example")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	f := addClickHouseFlags(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	opts, err := clickhouseOptions(f, "garmr", "otel_garmr", "garmr")
	if err != nil {
		t.Fatal(err)
	}
	if opts.Addr[0] != "clickhouse.paas.example:9440" || opts.Auth.Password != "from-env" {
		t.Fatalf("unexpected options: addr %v", opts.Addr)
	}
	if opts.TLS == nil || opts.TLS.ServerName != "clickhouse.paas.example" || opts.TLS.RootCAs == nil {
		t.Fatalf("TLS is not configured from the environment: %+v", opts.TLS)
	}

	t.Setenv("CLICKHOUSE_TLS_ENABLED", "")
	t.Setenv("CLICKHOUSE_TLS_CA_FILE", "")
	t.Setenv("CLICKHOUSE_TLS_SERVER_NAME", "")
	fs = flag.NewFlagSet("test", flag.ContinueOnError)
	f = addClickHouseFlags(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	opts, err = clickhouseOptions(f, "garmr", "otel_garmr", "garmr")
	if err != nil {
		t.Fatal(err)
	}
	if opts.TLS != nil {
		t.Fatal("empty TLS variables turned TLS on")
	}
}
