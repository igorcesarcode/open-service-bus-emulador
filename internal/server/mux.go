package server

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
	"time"
)

// sniffTimeout bounds how long a new connection may wait before sending
// its first bytes, which tell its protocol apart.
const sniffTimeout = 10 * time.Second

// protocol is what a connection speaks, found from its first bytes.
type protocol int

const (
	protoAMQP protocol = iota
	protoTLS
	protoHTTP
)

// sniff peeks at the first byte of c: AMQP connections start with "AMQP",
// TLS ones with a handshake record (0x16), and anything else is taken as
// HTTP.
func sniff(c net.Conn) (net.Conn, protocol, error) {
	br := bufio.NewReader(c)
	c.SetReadDeadline(time.Now().Add(sniffTimeout))
	b, err := br.Peek(1)
	c.SetReadDeadline(time.Time{})
	if err != nil {
		return nil, 0, err
	}
	pc := &peekedConn{Conn: c, r: br}
	switch b[0] {
	case 'A':
		return pc, protoAMQP, nil
	case 0x16:
		return pc, protoTLS, nil
	}
	return pc, protoHTTP, nil
}

// peekedConn is a connection whose first bytes were buffered by sniff.
type peekedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *peekedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// connListener is a net.Listener fed with connections by the multiplexer,
// for http.Server.
type connListener struct {
	addr  net.Addr
	conns chan net.Conn
	once  sync.Once
	done  chan struct{}
}

func newConnListener(addr net.Addr) *connListener {
	return &connListener{addr: addr, conns: make(chan net.Conn), done: make(chan struct{})}
}

func (l *connListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// push hands c to the listener, or closes it if the listener is closed.
func (l *connListener) push(c net.Conn) {
	select {
	case l.conns <- c:
	case <-l.done:
		c.Close()
	}
}

func (l *connListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *connListener) Addr() net.Addr { return l.addr }

// selfSignedCertificate creates a certificate for localhost and hosts,
// which may be names or IP addresses, valid for a year.
func selfSignedCertificate(hosts []string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return tls.Certificate{}, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		// A unique subject keeps clients that trust the certificates of
		// several emulators from matching one with another's issuer.
		Subject:     pkix.Name{CommonName: "open-service-bus", SerialNumber: serial.Text(16)},
		DNSNames:    []string{"localhost", "*.localhost"},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		IsCA:        true,
		// Clients that trust this certificate as a root can verify it.
		BasicConstraintsValid: true,
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(1, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else if h != "" {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, nil
}
