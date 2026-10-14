//go:build !PSIPHON_DISABLE_QUIC
// +build !PSIPHON_DISABLE_QUIC

/*
 * Copyright (c) 2018, Psiphon Inc.
 * All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package quic

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	tls "github.com/Psiphon-Labs/psiphon-tls"
	ietf_quic "github.com/Psiphon-Labs/quic-go"

	"github.com/Psiphon-Labs/psiphon-tunnel-core/psiphon/common"
	"github.com/Psiphon-Labs/psiphon-tunnel-core/psiphon/common/errors"
	"github.com/Psiphon-Labs/psiphon-tunnel-core/psiphon/common/prng"
	"github.com/Psiphon-Labs/psiphon-tunnel-core/psiphon/common/protocol"
	"golang.org/x/sync/errgroup"
)

func TestQUIC(t *testing.T) {
	for quicVersion := range supportedVersionNumbers {

		t.Run(fmt.Sprintf("%s", quicVersion), func(t *testing.T) {
			if isGQUIC(quicVersion) && !GQUICEnabled() {
				t.Skipf("gQUIC is not enabled")
			}
			runQUIC(t, quicVersion, GQUICEnabled(),
				false,
				false, // obfuscated PSK not supproted in gQUIC
			)
		})

		for _, useObfuscatedPSK := range []bool{false, true} {
			if isIETF(quicVersion) {
				t.Run(fmt.Sprintf("%s (invoke anti-probing)|PSK=%v", quicVersion, useObfuscatedPSK), func(t *testing.T) {
					runQUIC(t, quicVersion, GQUICEnabled(), true, useObfuscatedPSK)
				})
			}
			if isIETF(quicVersion) {
				t.Run(fmt.Sprintf("%s (disable gQUIC)|PSK=%v", quicVersion, useObfuscatedPSK), func(t *testing.T) {
					runQUIC(t, quicVersion, false, false, useObfuscatedPSK)
				})
			}
		}
	}
}

func TestIETFQUICExporterAndPin(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	certificate, privateKey, verifyPin, err :=
		common.GenerateWebServerCertificate("localhost")
	if err != nil {
		t.Fatal(err)
	}
	tlsCertificate, err := tls.X509KeyPair(
		[]byte(certificate), []byte(privateKey))
	if err != nil {
		t.Fatal(err)
	}

	obfuscationKey := prng.HexString(32)
	listener, err := Listen(
		nil,
		func(_ string, err error, _ common.LogFields) {
			t.Errorf("unexpected irregular tunnel event: %v", err)
		},
		"127.0.0.1:0",
		nil,
		false,
		0,
		obfuscationKey,
		&tlsCertificate,
		false,
		false)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	const exporterLabel = "EXPORTER-psiphon-quic-test"
	const exporterSize = 32

	type exporterResult struct {
		exporter []byte
		err      error
	}
	resultChannel := make(chan exporterResult, 2)
	closeServerConn := make(chan struct{})
	defer close(closeServerConn)

	go func() {
		for i := 0; i < 2; i++ {
			conn, err := listener.Accept()
			if err != nil {
				resultChannel <- exporterResult{err: err}
				return
			}
			defer conn.Close()

			exporter, err := conn.(*Conn).ExportKeyingMaterial(
				exporterLabel, nil, exporterSize)
			resultChannel <- exporterResult{
				exporter: exporter,
				err:      err,
			}
		}

		<-closeServerConn
	}()

	sessionCache := &testQUICClientSessionCache{
		ClientSessionCache: tls.NewLRUClientSessionCache(0),
		stored:             make(chan struct{}, 1),
	}

	dial := func(verifyPin string, useObfuscatedPSK bool) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(
			ctx, 5*time.Second)
		defer cancel()

		remoteAddr, err := net.ResolveUDPAddr(
			"udp", listener.Addr().String())
		if err != nil {
			return nil, err
		}
		packetConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}

		return Dial(
			ctx,
			packetConn,
			remoteAddr,
			"localhost",
			protocol.QUIC_VERSION_V1,
			nil,
			obfuscationKey,
			nil,
			nil,
			false,
			0,
			false,
			useObfuscatedPSK,
			verifyPin,
			common.WrapClientSessionCache(sessionCache, "localhost"))
	}

	// Test: exporters match and change when resuming a session without 0-RTT.

	var previousExporter []byte
	for _, expectedDidResume := range []string{"0", "1"} {
		clientConn, err := dial(verifyPin, false)
		if err != nil {
			t.Fatal(err)
		}
		defer clientConn.Close()

		clientExporter, err := clientConn.(*Conn).ExportKeyingMaterial(
			exporterLabel, nil, exporterSize)
		if err != nil {
			t.Fatal(err)
		}

		var result exporterResult
		select {
		case result = <-resultChannel:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if result.err != nil {
			t.Fatal(result.err)
		}
		if !bytes.Equal(clientExporter, result.exporter) {
			t.Fatal("exported key material mismatch")
		}
		if bytes.Equal(clientExporter, previousExporter) {
			t.Fatal("reused QUIC exporter")
		}
		previousExporter = clientExporter
		if clientConn.(*Conn).GetMetrics()["quic_did_resume"] != expectedDidResume {
			t.Fatal("unexpected QUIC resumption")
		}

		// Wait for ticket storage before the next dial.
		select {
		case <-sessionCache.stored:
		case <-ctx.Done():
			t.Fatal("session ticket not received:", ctx.Err())
		}
	}

	// Test: an incorrect pin is rejected even with a cached session.

	_, _, wrongVerifyPin, err :=
		common.GenerateWebServerCertificate("localhost")
	if err != nil {
		t.Fatal(err)
	}

	conn, err := dial(wrongVerifyPin, false)
	if err == nil {
		conn.Close()
		t.Fatal("unexpected dial success with incorrect verify pin")
	}

	if !strings.Contains(err.Error(), "no pin found") {
		t.Fatal(err)
	}

	conn, err = dial(verifyPin, true)
	if err == nil {
		conn.Close()
		t.Fatal("unexpected dial success with verify pin and obfuscated PSK")
	}
	if !strings.Contains(err.Error(), "not supported with obfuscated PSK") {
		t.Fatal(err)
	}
}

func TestIETFQUICEarlyResumptionMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	certificate, privateKey, _, err := common.GenerateWebServerCertificate("localhost")
	if err != nil {
		t.Fatal(err)
	}
	tlsCertificate, err := tls.X509KeyPair([]byte(certificate), []byte(privateKey))
	if err != nil {
		t.Fatal(err)
	}
	packetConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer packetConn.Close()

	// Hold the second handshake before ServerHello so DialEarly must return
	// before the client knows whether TLS resumption succeeded.
	resumeHandshake := make(chan struct{})
	var clientHelloCount atomic.Int32
	listener, err := ietf_quic.Listen(packetConn, &tls.Config{
		Certificates: []tls.Certificate{tlsCertificate},
		NextProtos:   []string{getALPN(ietfQUIC1VersionNumber)},
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			if clientHelloCount.Add(1) > 1 {
				select {
				case <-resumeHandshake:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return nil, nil
		},
	}, &ietf_quic.Config{Allow0RTT: true})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	// Release a blocked TLS callback before closing the listener on failure.
	defer cancel()

	sessionCache := &testQUICClientSessionCache{
		ClientSessionCache: tls.NewLRUClientSessionCache(0),
		stored:             make(chan struct{}, 1),
	}
	transporter, err := NewQUICTransporter(
		ctx, nil,
		func(context.Context) (net.PacketConn, *net.UDPAddr, error) {
			conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
			return conn, listener.Addr().(*net.UDPAddr), err
		},
		"localhost", protocol.QUIC_VERSION_V1, nil, false, true,
		common.WrapClientSessionCache(sessionCache, "localhost"))
	if err != nil {
		t.Fatal(err)
	}
	defer transporter.CloseIdleConnections()

	for _, expectedDidResume := range []string{"0", "1"} {
		connection, err := transporter.dialQUIC()
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		earlyConnection := connection.(*ietfQUICConnection).Connection.(ietf_quic.EarlyConnection)
		if expectedDidResume == "1" {
			if earlyConnection.ConnectionState().TLS.HandshakeComplete {
				t.Fatal("early dial waited for the blocked handshake")
			}
			if transporter.GetMetrics()["quic_did_resume"] != "0" {
				t.Fatal("resumption reported before ServerHello")
			}
			close(resumeHandshake)
		}
		select {
		case <-earlyConnection.HandshakeComplete():
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if earlyConnection.ConnectionState().TLS.DidResume != (expectedDidResume == "1") {
			t.Fatal("unexpected TLS resumption state")
		}
		metrics := transporter.GetMetrics()
		if metrics["quic_dial_early"] != "1" ||
			metrics["quic_sent_ticket"] != expectedDidResume ||
			metrics["quic_did_resume"] != expectedDidResume {
			t.Fatalf("unexpected QUIC metrics after handshake: %v", metrics)
		}

		serverConn, err := listener.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer serverConn.CloseWithError(0, "")
		select {
		case <-sessionCache.stored:
		case <-ctx.Done():
			t.Fatal("session ticket not received:", ctx.Err())
		}
	}
}

type testQUICClientSessionCache struct {
	tls.ClientSessionCache
	stored chan struct{}
}

func (cache *testQUICClientSessionCache) Put(
	key string, state *tls.ClientSessionState) {

	cache.ClientSessionCache.Put(key, state)
	if state != nil {
		select {
		case cache.stored <- struct{}{}:
		default:
		}
	}
}

func runQUIC(
	t *testing.T,
	quicVersion string,
	enableGQUIC bool,
	invokeAntiProbing bool,
	useObfuscatedPSK bool) {

	initGoroutines := getGoroutines()

	clients := 10
	bytesToSend := 1 << 20

	serverReceivedBytes := int64(0)
	clientReceivedBytes := int64(0)

	// Intermittently, on some platforms, the client connection termination
	// packet is not received even when sent/received locally; set a brief
	// idle timeout to ensure the server-side client handler doesn't block too
	// long on Read, causing the test to fail.
	//
	// In realistic network conditions, and especially under adversarial
	// network conditions, we should not expect to regularly receive client
	// connection termination packets.
	serverIdleTimeout = 1 * time.Second

	irregularTunnelLogger := func(_ string, err error, _ common.LogFields) {
		if !invokeAntiProbing {
			t.Errorf("unexpected irregular tunnel event: %v", err)
		}
	}

	obfuscationKey := prng.HexString(32)

	// Disabling MTU discovery is exercised in in-proxy test cases.
	disablePathMTUDiscovery := false

	listener, err := Listen(
		nil,
		irregularTunnelLogger,
		"127.0.0.1:0",
		nil,
		disablePathMTUDiscovery,
		0,
		obfuscationKey,
		nil,
		enableGQUIC,
		true)
	if err != nil {
		t.Fatalf("Listen failed: %s", err)
	}

	serverAddress := listener.Addr().String()

	testGroup, testCtx := errgroup.WithContext(context.Background())

	testGroup.Go(func() error {

		if invokeAntiProbing {
			// The quic-go server can still handshake new sessions even if
			// Accept isn't called.
			return nil
		}

		var serverGroup errgroup.Group

		for i := 0; i < clients; i++ {

			conn, err := listener.Accept()
			if err != nil {
				return errors.Trace(err)
			}

			serverGroup.Go(func() error {
				b := make([]byte, 1024)
				for {
					n, err := conn.Read(b)
					atomic.AddInt64(&serverReceivedBytes, int64(n))
					if err == io.EOF {
						return nil
					} else if err != nil {
						return errors.Trace(err)
					}
					_, err = conn.Write(b[:n])
					if err != nil {
						return errors.Trace(err)
					}
				}
			})
		}

		err := serverGroup.Wait()
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	})

	for i := 0; i < clients; i++ {

		disablePathMTUDiscovery := i%2 == 0

		testGroup.Go(func() error {

			ctx, cancelFunc := context.WithTimeout(
				context.Background(), 1*time.Second)
			defer cancelFunc()

			remoteAddr, err := net.ResolveUDPAddr("udp", serverAddress)
			if err != nil {
				return errors.Trace(err)
			}

			packetConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				return errors.Trace(err)
			}

			clientObfuscationKey := obfuscationKey
			if invokeAntiProbing {
				clientObfuscationKey = prng.HexString(32)
				packetConn = &countReadsConn{PacketConn: packetConn}
			}

			obfuscationPaddingSeed, err := prng.NewSeed()
			if err != nil {
				return errors.Trace(err)
			}

			var clientHelloSeed *prng.Seed
			if isClientHelloRandomized(quicVersion) {
				clientHelloSeed, err = prng.NewSeed()
				if err != nil {
					return errors.Trace(err)
				}
			}

			quicSNIAddress, _, err := net.SplitHostPort(serverAddress)
			if err != nil {
				return errors.Trace(err)
			}

			clientSessionCache := common.WrapClientSessionCache(tls.NewLRUClientSessionCache(0), "localhost")

			conn, err := Dial(
				ctx,
				packetConn,
				remoteAddr,
				quicSNIAddress,
				quicVersion,
				clientHelloSeed,
				clientObfuscationKey,
				obfuscationPaddingSeed,
				nil,
				disablePathMTUDiscovery,
				0,
				true,
				useObfuscatedPSK,
				"",
				clientSessionCache)

			if invokeAntiProbing {

				if err == nil {
					return errors.TraceNew(
						"unexpected dial success with invalid client hello random")
				}

				readCount := packetConn.(*countReadsConn).getReadCount()

				if readCount > 0 {
					return errors.Tracef(
						"unexpected %d read packets with invalid client hello random",
						readCount)
				}

				return nil
			}

			if err != nil {
				return errors.Trace(err)
			}

			// Cancel should interrupt dialing only
			cancelFunc()

			var clientGroup errgroup.Group

			clientGroup.Go(func() error {
				defer conn.Close()
				b := make([]byte, 1024)
				bytesRead := 0
				for bytesRead < bytesToSend {
					n, err := conn.Read(b)
					bytesRead += n
					atomic.AddInt64(&clientReceivedBytes, int64(n))
					if err == io.EOF {
						break
					} else if err != nil {
						return errors.Trace(err)
					}
				}
				return nil
			})

			clientGroup.Go(func() error {
				b := make([]byte, bytesToSend)
				_, err := conn.Write(b)
				if err != nil {
					return errors.Trace(err)
				}
				return nil
			})

			return clientGroup.Wait()
		})

	}

	go func() {
		testGroup.Wait()
	}()

	<-testCtx.Done()
	listener.Close()

	err = testGroup.Wait()
	if err != nil {
		t.Errorf("goroutine failed: %s", err)
	}

	bytes := atomic.LoadInt64(&serverReceivedBytes)
	expectedBytes := int64(clients * bytesToSend)
	if invokeAntiProbing {
		expectedBytes = 0
	}
	if bytes != expectedBytes {
		t.Errorf("unexpected serverReceivedBytes: %d vs. %d", bytes, expectedBytes)
	}

	bytes = atomic.LoadInt64(&clientReceivedBytes)
	if bytes != expectedBytes {
		t.Errorf("unexpected clientReceivedBytes: %d vs. %d", bytes, expectedBytes)
	}

	_, err = listener.Accept()
	if err == nil {
		t.Error("unexpected Accept after Close")
	}

	// Check for unexpected dangling goroutines after shutdown.
	//
	// quic-go.packetHandlerMap.listen shutdown is async and some quic-go
	// goroutines and/or timers dangle so this test makes allowances for these
	// known dangling goroutinees.

	expectedDanglingGoroutines := []string{
		"quic-go.(*packetHandlerMap).Retire.func1",
		"quic-go.(*packetHandlerMap).ReplaceWithClosed.func1",
		"quic-go.(*packetHandlerMap).ReplaceWithClosed.func2",
		"quic-go.(*packetHandlerMap).RetireResetToken.func1",
		"gquic-go.(*packetHandlerMap).removeByConnectionIDAsString.func1",
	}

	sleepTime := 100 * time.Millisecond

	// The longest expected dangling goroutine is in gquic-go and is launched by a timer
	// that fires after ClosedSessionDeleteTimeout, which is 1m. Allow one extra second
	// to ensure this period elapses and the time.AfterFunc runs.
	//
	// To avoid taking 1m to run this test every time, the dangling goroutine check exits
	// early once no dangling goroutines are found. Note that this doesn't account for
	// any timers still pending at the early exit time.
	n := int((61 * time.Second) / sleepTime)

	for i := 0; i < n; i++ {

		// Sleep before making any checks, since quic-go.packetHandlerMap.listen
		// shutdown is asynchronous.
		time.Sleep(100 * time.Millisecond)

		// After the full 61s, no dangling goroutines are expected.
		if i == n-1 {
			expectedDanglingGoroutines = []string{}
		}

		hasDangling, onlyExpectedDangling := checkDanglingGoroutines(
			t, initGoroutines, expectedDanglingGoroutines)
		if !hasDangling {
			break
		} else if !onlyExpectedDangling {
			t.Fatalf("unexpected dangling goroutines")
		}
	}
}

type countReadsConn struct {
	net.PacketConn
	readCount int32
}

func (conn *countReadsConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := conn.PacketConn.ReadFrom(p)
	if n > 0 {
		atomic.AddInt32(&conn.readCount, 1)
	}
	return n, addr, err
}

func (conn *countReadsConn) getReadCount() int {
	return int(atomic.LoadInt32(&conn.readCount))
}

func getGoroutines() []runtime.StackRecord {
	n, _ := runtime.GoroutineProfile(nil)
	for {
		r := make([]runtime.StackRecord, n)
		var ok bool
		n, ok = runtime.GoroutineProfile(r)
		if ok {
			return r[:n]
		}
		// Retry; a goroutine may have spawned between GoroutineProfile calls,
		// so n is now too small.
	}
}

func checkDanglingGoroutines(
	t *testing.T,
	initGoroutines []runtime.StackRecord,
	expectedDanglingGoroutines []string) (bool, bool) {

	hasDangling := false
	onlyExpectedDangling := true
	current := getGoroutines()
	for _, g := range current {
		found := false
		for _, h := range initGoroutines {
			if g == h {
				found = true
				break
			}
		}
		if !found {
			stack := g.Stack()
			funcNames := make([]string, len(stack))
			skip := false
			isExpected := false
			for i := 0; i < len(stack); i++ {
				funcNames[i] = getFunctionName(stack[i])

				// The current goroutine won't have the same stack as in initGoroutines.
				if strings.Contains(funcNames[i], "checkDanglingGoroutines") {
					skip = true
					break
				}

				// testing.T.Run runs the the test function, f, in another goroutine. f is
				// the current goroutine, which captures initGoroutines.
				// https://github.com/golang/go/blob/release-branch.go1.13/src/testing/testing.go#L960-L961:
				//
				//     go tRunner(t, f)
				//     if !<-t.signal {
				//     ...
				//
				// f may capture initGoroutines before or after testing.T.Run advances to
				// the channel receive, so the stack of the testing.T.Run goroutine may or
				// may not match initGoroutines. Skip it.
				if strings.Contains(funcNames[i], "testing.(*T).Run") {
					skip = true
					break
				}

				// This goroutine, created by Listener.clientRandomHistory,
				// terminates nondeterministically, based on garbage
				// collection. Skip it.
				if strings.Contains(funcNames[i], "go-cache-lru.(*janitor).Run") {
					skip = true
					break
				}

				for _, expected := range expectedDanglingGoroutines {
					if strings.Contains(funcNames[i], expected) {
						isExpected = true
						break
					}
				}
				if isExpected {
					break
				}
			}
			if !skip {
				hasDangling = true
				if !isExpected {
					onlyExpectedDangling = false
					s := strings.Join(funcNames, " <- ")
					t.Logf("found unexpected dangling goroutine: %s", s)
				}
			}
		}
	}
	return hasDangling, onlyExpectedDangling
}

func getFunctionName(pc uintptr) string {
	funcName := runtime.FuncForPC(pc).Name()
	index := strings.LastIndex(funcName, "/")
	if index != -1 {
		funcName = funcName[index+1:]
	}
	return funcName
}
