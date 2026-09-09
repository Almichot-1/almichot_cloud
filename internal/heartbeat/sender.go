package heartbeat

import (
	"context"
	"sync"
	"time"

	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
)

// HeartbeatSender emits heartbeats from a worker agent to the Control Plane (FS-02, G-14).
// Key Invariant (G-14, WA-13): Runs completely independently of container operations.
// A failing container or Docker engine error never prevents the worker from emitting heartbeats.
type HeartbeatSender struct {
	workerID string
	client   proto.ControlPlaneServiceClient
	interval time.Duration
	log      zerolog.Logger

	mu       sync.Mutex
	stopCh   chan struct{}
	paused   bool // used for network partition simulation
	sendHook func(success bool)
}

// NewHeartbeatSender creates a new HeartbeatSender.
func NewHeartbeatSender(
	workerID string,
	client proto.ControlPlaneServiceClient,
	interval time.Duration,
	log zerolog.Logger,
) *HeartbeatSender {
	if interval <= 0 {
		interval = 1 * time.Second
	}
	return &HeartbeatSender{
		workerID: workerID,
		client:   client,
		interval: interval,
		log:      log.With().Str("component", "heartbeat-sender").Str("worker_id", workerID).Logger(),
		stopCh:   make(chan struct{}),
	}
}

// SetSendHook registers a callback after each heartbeat attempt (useful for test assertions).
func (s *HeartbeatSender) SetSendHook(hook func(success bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendHook = hook
}

// SetPaused allows pausing heartbeats (e.g. simulating network partition or death) (G-12, G-13).
func (s *HeartbeatSender) SetPaused(paused bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = paused
}

// SendOne emits a single heartbeat RPC.
func (s *HeartbeatSender) SendOne(ctx context.Context) error {
	s.mu.Lock()
	paused := s.paused
	s.mu.Unlock()

	if paused {
		if s.sendHook != nil {
			s.sendHook(false)
		}
		return nil
	}

	req := &proto.HeartbeatRequest{
		WorkerId:  s.workerID,
		Timestamp: time.Now().UTC().Unix(),
	}

	_, err := s.client.Heartbeat(ctx, req)
	success := (err == nil)

	s.mu.Lock()
	hook := s.sendHook
	s.mu.Unlock()
	if hook != nil {
		hook(success)
	}

	if err != nil {
		s.log.Debug().Err(err).Msg("heartbeat delivery failed")
		return err
	}

	s.log.Debug().Msg("heartbeat emitted successfully")
	return nil
}

// Start launches the background heartbeat emission loop.
func (s *HeartbeatSender) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		// Initial heartbeat immediately
		_ = s.SendOne(ctx)

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopCh:
				return
			case <-ticker.C:
				_ = s.SendOne(ctx)
			}
		}
	}()
}

// Stop stops the sender.
func (s *HeartbeatSender) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
}
