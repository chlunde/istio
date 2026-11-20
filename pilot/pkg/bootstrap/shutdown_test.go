// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bootstrap

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"istio.io/istio/pilot/pkg/model"
	v3 "istio.io/istio/pilot/pkg/xds/v3"
	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/test/util/retry"
)

// TestShutdownKeepsPushingUntilGRPCStopped verifies that istiod keeps watching for changes and
// pushing them to connected proxies while the gRPC servers are gracefully stopping.
//
// Graceful stop does not end long-lived xDS streams, so during the whole shutdownDuration the
// proxies are still connected to this istiod. If the push loop and the informers were stopped as
// soon as the stop signal arrives, those proxies would miss every change in that window (for
// example the removal of endpoints of pods drained together with istiod) until they reconnect
// to another istiod.
func TestShutdownKeepsPushingUntilGRPCStopped(t *testing.T) {
	const shutdownDuration = 3 * time.Second

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcAddr := l.Addr().String()
	_ = l.Close()

	configDir := t.TempDir()
	args := NewPilotArgs(func(p *PilotArgs) {
		p.Namespace = "istio-system"
		p.ServerOptions = DiscoveryServerOptions{
			HTTPAddr:       ":0",
			MonitoringAddr: ":0",
			GRPCAddr:       grpcAddr,
		}
		p.RegistryOptions = RegistryOptions{
			KubeConfig: "config",
			FileDir:    configDir,
		}
		p.ShutdownDuration = shutdownDuration
	})

	s, err := NewServer(args, func(s *Server) {
		s.kubeClient = kube.NewFakeClient()
	})
	if err != nil {
		t.Fatal(err)
	}

	// A component that records when it is told to stop.
	componentStopped := make(chan struct{})
	s.addStartFunc("shutdown test probe", func(stop <-chan struct{}) error {
		go func() {
			<-stop
			close(componentStopped)
		}()
		return nil
	})

	stop := make(chan struct{})
	if err := s.Start(stop); err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			close(stop)
		}
		s.WaitUntilCompletion()
	}()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := discovery.NewAggregatedDiscoveryServiceClient(conn).StreamAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	node := &core.Node{Id: "sidecar~10.0.0.1~app.default~default.svc.cluster.local"}
	if err := stream.Send(&discovery.DiscoveryRequest{Node: node, TypeUrl: v3.ClusterType}); err != nil {
		t.Fatal(err)
	}
	responses := make(chan *discovery.DiscoveryResponse, 100)
	streamErr := make(chan error, 1)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				streamErr <- err
				return
			}
			// ACK, so that istiod keeps pushing to us.
			_ = stream.Send(&discovery.DiscoveryRequest{
				Node:          node,
				TypeUrl:       resp.TypeUrl,
				VersionInfo:   resp.VersionInfo,
				ResponseNonce: resp.Nonce,
			})
			responses <- resp
		}
	}()

	select {
	case <-responses:
	case err := <-streamErr:
		t.Fatalf("stream failed before shutdown: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no initial CDS response")
	}
	retry.UntilSuccessOrFail(t, func() error {
		if n := len(s.XDSServer.AllClients()); n != 1 {
			return fmt.Errorf("expected 1 connected proxy, got %d", n)
		}
		return nil
	})

	// Simulate SIGTERM.
	shutdownStart := time.Now()
	close(stop)
	stopped = true

	// Give the shutdown goroutine time to start the graceful stop.
	time.Sleep(200 * time.Millisecond)

	select {
	case <-componentStopped:
		t.Fatal("components were stopped while proxies were still connected")
	default:
	}

	// A config change during the graceful stop must still reach the connected proxy.
	s.XDSServer.ConfigUpdate(&model.PushRequest{Forced: true, Reason: model.NewReasonStats(model.UnknownTrigger)})
	select {
	case <-responses:
	case err := <-streamErr:
		t.Fatalf("stream closed before the push was delivered: %v", err)
	case <-time.After(shutdownDuration / 2):
		t.Fatal("no push delivered to a connected proxy during graceful shutdown")
	}

	// The stream is closed once shutdownDuration has passed, and only then are the components stopped.
	select {
	case <-streamErr:
	case <-time.After(shutdownDuration + 5*time.Second):
		t.Fatal("xDS stream was not closed after shutdownDuration")
	}
	select {
	case <-componentStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("components were not stopped after the gRPC servers stopped")
	}
	if elapsed := time.Since(shutdownStart); elapsed < shutdownDuration {
		t.Fatalf("components stopped after %v, before shutdownDuration %v", elapsed, shutdownDuration)
	}
}
