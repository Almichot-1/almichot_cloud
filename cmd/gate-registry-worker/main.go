// gate-registry-worker is a minimal standalone binary used ONLY by the G-26 real-infra
// gate test. It is launched as a separate OS process (not a goroutine) to prove that
// registry push and pull genuinely cross a process boundary over a real TCP socket.
//
// Usage:
//
//	gate-registry-worker --mode=push --registry=host:port --tag=img:v1 --digest=sha256:...
//	gate-registry-worker --mode=pull --registry=host:port --tag=img:v1 --expected-digest=sha256:...
//
// Exit codes:
//
//	0 — success
//	1 — failure (message written to stderr)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nebula/nebula/internal/registry"
	"github.com/rs/zerolog"
)

func main() {
	mode := flag.String("mode", "", "push or pull")
	registryAddr := flag.String("registry", "", "host:port of the EmbeddedRegistryServer")
	tag := flag.String("tag", "", "image tag to push or pull")
	digest := flag.String("digest", "", "(push) pre-computed digest of the image data")
	expectedDigest := flag.String("expected-digest", "", "(pull) digest to verify after pull")
	flag.Parse()

	log := zerolog.New(os.Stderr).With().Timestamp().Str("binary", "gate-registry-worker").Logger()

	if *registryAddr == "" || *tag == "" || *mode == "" {
		fmt.Fprintln(os.Stderr, "usage: gate-registry-worker --mode=push|pull --registry=addr --tag=img")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build an independent RemoteRegistryClient from scratch — no shared Go state
	// with any other process.
	client := registry.NewRemoteRegistryClient(registry.RemoteRegistryConfig{
		RegistryHost: *registryAddr,
		Insecure:     true, // gate test uses HTTP loopback
	}, nil, log)

	switch *mode {
	case "push":
		if err := runPush(ctx, client, *tag, *digest, log); err != nil {
			fmt.Fprintf(os.Stderr, "push failed: %v\n", err)
			os.Exit(1)
		}
	case "pull":
		if err := runPull(ctx, client, *tag, *expectedDigest, log); err != nil {
			fmt.Fprintf(os.Stderr, "pull failed: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q; expected push or pull\n", *mode)
		os.Exit(1)
	}
}

func runPush(ctx context.Context, client registry.RegistryClient, tag, expectedDigest string, log zerolog.Logger) error {
	// Synthesise deterministic image payload so the digest is predictable.
	imageData := []byte(fmt.Sprintf("GATE26-IMAGE-DATA-FOR-TAG-%s", tag))
	if expectedDigest == "" {
		expectedDigest = registry.ComputeDigest(imageData)
	}

	gotDigest, err := client.Push(ctx, tag, imageData, expectedDigest)
	if err != nil {
		return fmt.Errorf("Push(%s): %w", tag, err)
	}
	log.Info().Str("tag", tag).Str("digest", gotDigest).Msg("push succeeded over real TCP socket")

	// Write the digest to stdout so the gate test can read it.
	fmt.Println(gotDigest)
	return nil
}

func runPull(ctx context.Context, client registry.RegistryClient, tag, expectedDigest string, log zerolog.Logger) error {
	data, gotDigest, err := client.Pull(ctx, tag)
	if err != nil {
		return fmt.Errorf("Pull(%s): %w", tag, err)
	}
	if len(data) == 0 {
		return fmt.Errorf("Pull(%s): returned empty data", tag)
	}
	if expectedDigest != "" && gotDigest != expectedDigest {
		return fmt.Errorf("Pull(%s): digest mismatch: got %s, expected %s", tag, gotDigest, expectedDigest)
	}
	log.Info().Str("tag", tag).Str("digest", gotDigest).Int("bytes", len(data)).
		Msg("pull succeeded over real TCP socket (independent process, own registry client, empty local cache)")
	return nil
}
