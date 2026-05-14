package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"client-demo/internal/sessionrpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_client_http_requests_total",
			Help: "Total HTTP requests received by the client demo service.",
		},
		[]string{"path", "method", "code"},
	)

	httpRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "go_client_http_request_duration_seconds",
			Help:    "HTTP request duration of the client demo service in seconds.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5},
		},
		[]string{"path", "method", "code"},
	)

	processUp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "go_client_process_up",
			Help: "Whether the client demo process is considered up.",
		},
	)

	discoveredGateways = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "go_client_discovered_gateways",
			Help: "Number of gateway instances currently discovered for the client demo.",
		},
		[]string{"service", "target_service"},
	)

	activeStreamsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "go_client_active_streams",
			Help: "Current active gRPC streams held by the client demo.",
		},
	)

	sessionsStartedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_client_sessions_started_total",
			Help: "Total simulated sessions started by the client demo.",
		},
		[]string{"result"},
	)

	eventsSentTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_client_events_sent_total",
			Help: "Total gRPC events sent by the client demo.",
		},
		[]string{"action", "result"},
	)

	acksReceivedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_client_acks_received_total",
			Help: "Total acknowledgements received by the client demo.",
		},
		[]string{"action", "result"},
	)

	streamReconnectTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_client_stream_reconnect_total",
			Help: "Total stream reconnect attempts made by the client demo.",
		},
		[]string{"result"},
	)
)

type config struct {
	serviceName                string
	instanceID                 string
	appPort                    string
	metricsPort                string
	logPath                    string
	consulHTTPAddr             string
	gatewayAddr                string
	targetDiscoveryServiceName string
	peerRefreshInterval        time.Duration
	reconnectDelay             time.Duration
	heartbeatInterval          time.Duration
	sessionGap                 time.Duration
	virtualClients             int
	heartbeatMin               int
	heartbeatMax               int
}

type app struct {
	config     config
	startedAt  time.Time
	logger     *log.Logger
	httpClient *http.Client
	random     *rand.Rand
	randMu     sync.Mutex

	gatewaysMu sync.RWMutex
	gateways   []peer

	requestCount  atomic.Uint64
	activeStreams atomic.Int64
	sessionSeq    atomic.Uint64
}

type peer struct {
	ID      string `json:"id"`
	Service string `json:"service"`
	Address string `json:"address"`
	Port    int    `json:"port"`
}

type consulServiceEntry struct {
	Node struct {
		Address string `json:"Address"`
	} `json:"Node"`
	Service struct {
		ID      string `json:"ID"`
		Service string `json:"Service"`
		Address string `json:"Address"`
		Port    int    `json:"Port"`
	} `json:"Service"`
}

type logEntry struct {
	Level         string `json:"level"`
	Event         string `json:"event"`
	Service       string `json:"service"`
	InstanceID    string `json:"instance_id"`
	TargetService string `json:"target_service,omitempty"`
	PeerID        string `json:"peer_id,omitempty"`
	PeerAddress   string `json:"peer_address,omitempty"`
	Path          string `json:"path,omitempty"`
	Method        string `json:"method,omitempty"`
	Status        int    `json:"status,omitempty"`
	Action        string `json:"action,omitempty"`
	ClientID      string `json:"client_id,omitempty"`
	SessionID     string `json:"session_id,omitempty"`
	EventID       string `json:"event_id,omitempty"`
	Detail        string `json:"detail,omitempty"`
	PeerCount     int    `json:"peer_count,omitempty"`
	ActiveStreams int64  `json:"active_streams,omitempty"`
	Timestamp     string `json:"ts"`
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func main() {
	prometheus.MustRegister(
		httpRequestsTotal,
		httpRequestDurationSeconds,
		processUp,
		discoveredGateways,
		activeStreamsGauge,
		sessionsStartedTotal,
		eventsSentTotal,
		acksReceivedTotal,
		streamReconnectTotal,
	)

	cfg := loadConfig()
	logger, logFile, err := newLogger(cfg.logPath)
	if err != nil {
		log.Fatalf("init logger failed: %v", err)
	}
	defer logFile.Close()

	application := &app{
		config:    cfg,
		startedAt: time.Now(),
		logger:    logger,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	processUp.Set(1)
	activeStreamsGauge.Set(0)
	discoveredGateways.WithLabelValues(cfg.serviceName, cfg.targetDiscoveryServiceName).Set(0)

	appMux := http.NewServeMux()
	appMux.HandleFunc("/", application.handleRoot)
	appMux.HandleFunc("/healthz", application.handleHealth)
	appMux.HandleFunc("/health", application.handleHealth)
	appMux.HandleFunc("/gateways", application.handleGateways)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())

	httpServer := &http.Server{
		Addr:              ":" + cfg.appPort,
		Handler:           application.withMetrics(appMux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	metricsServer := &http.Server{
		Addr:              ":" + cfg.metricsPort,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if cfg.gatewayAddr == "" {
		go application.refreshGatewaysLoop(rootCtx)
	}

	for i := 0; i < cfg.virtualClients; i++ {
		go application.virtualClientLoop(rootCtx, i)
	}

	go func() {
		application.writeLog(logEntry{Level: "info", Event: "http_server_starting"})
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	go func() {
		application.writeLog(logEntry{Level: "info", Event: "metrics_server_starting"})
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("metrics server failed: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	cancel()
	processUp.Set(0)
	application.writeLog(logEntry{Level: "info", Event: "shutdown_signal_received"})

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = metricsServer.Shutdown(shutdownCtx)
}

func (a *app) virtualClientLoop(ctx context.Context, idx int) {
	clientID := fmt.Sprintf("%s-client-%02d", a.config.instanceID, idx)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		target, peerInfo, ok := a.pickGateway()
		if !ok {
			streamReconnectTotal.WithLabelValues("no_gateway").Inc()
			a.writeLog(logEntry{
				Level:         "info",
				Event:         "client_waiting_for_gateway",
				ClientID:      clientID,
				TargetService: a.config.targetDiscoveryServiceName,
			})
			if !sleepWithContext(ctx, a.config.reconnectDelay) {
				return
			}
			continue
		}

		conn, err := sessionrpc.DialContext(ctx, target)
		if err != nil {
			streamReconnectTotal.WithLabelValues("dial_error").Inc()
			a.writeLog(logEntry{
				Level:         "error",
				Event:         "gateway_dial_failed",
				ClientID:      clientID,
				PeerID:        peerInfo.ID,
				PeerAddress:   target,
				TargetService: a.config.targetDiscoveryServiceName,
				Detail:        err.Error(),
			})
			if !sleepWithContext(ctx, a.config.reconnectDelay) {
				return
			}
			continue
		}

		gatewayClient := sessionrpc.NewGatewayServiceClient(conn)
		pingReply, err := gatewayClient.PingText(ctx, &sessionrpc.GatewayPingRequest{
			Message: fmt.Sprintf("hello from %s", clientID),
		})
		if err != nil {
			_ = conn.Close()
			streamReconnectTotal.WithLabelValues("ping_error").Inc()
			a.writeLog(logEntry{
				Level:         "error",
				Event:         "gateway_ping_failed",
				ClientID:      clientID,
				PeerID:        peerInfo.ID,
				PeerAddress:   target,
				TargetService: a.config.targetDiscoveryServiceName,
				Detail:        err.Error(),
			})
			if !sleepWithContext(ctx, a.config.reconnectDelay) {
				return
			}
			continue
		}
		a.writeLog(logEntry{
			Level:         "info",
			Event:         "gateway_ping_succeeded",
			ClientID:      clientID,
			PeerID:        pingReply.GatewayId,
			PeerAddress:   target,
			TargetService: a.config.targetDiscoveryServiceName,
			Detail:        pingReply.Message,
		})

		stream, err := gatewayClient.OpenSession(ctx)
		if err != nil {
			_ = conn.Close()
			streamReconnectTotal.WithLabelValues("open_stream_error").Inc()
			a.writeLog(logEntry{
				Level:         "error",
				Event:         "gateway_stream_open_failed",
				ClientID:      clientID,
				PeerID:        peerInfo.ID,
				PeerAddress:   target,
				TargetService: a.config.targetDiscoveryServiceName,
				Detail:        err.Error(),
			})
			if !sleepWithContext(ctx, a.config.reconnectDelay) {
				return
			}
			continue
		}

		active := a.activeStreams.Add(1)
		activeStreamsGauge.Set(float64(active))
		streamReconnectTotal.WithLabelValues("success").Inc()
		a.writeLog(logEntry{
			Level:         "info",
			Event:         "gateway_stream_opened",
			ClientID:      clientID,
			PeerID:        peerInfo.ID,
			PeerAddress:   target,
			TargetService: a.config.targetDiscoveryServiceName,
			ActiveStreams: active,
		})

		err = a.runClientSessionCycles(ctx, clientID, peerInfo, target, stream)

		_ = stream.CloseSend()
		_ = conn.Close()
		active = a.activeStreams.Add(-1)
		if active < 0 {
			a.activeStreams.Store(0)
			active = 0
		}
		activeStreamsGauge.Set(float64(active))

		if err != nil && !errors.Is(err, context.Canceled) {
			a.writeLog(logEntry{
				Level:         "error",
				Event:         "gateway_stream_closed_with_error",
				ClientID:      clientID,
				PeerID:        peerInfo.ID,
				PeerAddress:   target,
				TargetService: a.config.targetDiscoveryServiceName,
				ActiveStreams: active,
				Detail:        err.Error(),
			})
		}

		if !sleepWithContext(ctx, a.config.reconnectDelay) {
			return
		}
	}
}

func (a *app) runClientSessionCycles(ctx context.Context, clientID string, gateway peer, target string, stream sessionrpc.GatewayService_OpenSessionClient) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		sequence := a.sessionSeq.Add(1)
		sessionID := fmt.Sprintf("%s-session-%06d", clientID, sequence)
		userID := uint64(1000 + sequence)
		deviceID := fmt.Sprintf("device-%02d", sequence%50)

		sessionsStartedTotal.WithLabelValues("started").Inc()
		if err := a.sendAndVerify(stream, clientID, sessionID, userID, deviceID, sessionrpc.ActionLogin, fmt.Sprintf(`{"phase":"login","gateway_target":%q}`, target)); err != nil {
			sessionsStartedTotal.WithLabelValues("error").Inc()
			return err
		}

		heartbeatCount := a.config.heartbeatMin
		if a.config.heartbeatMax > a.config.heartbeatMin {
			heartbeatCount += a.randomInt(a.config.heartbeatMax - a.config.heartbeatMin + 1)
		}

		for i := 0; i < heartbeatCount; i++ {
			if !sleepWithContext(ctx, a.config.heartbeatInterval) {
				return ctx.Err()
			}
			payload := fmt.Sprintf(`{"phase":"heartbeat","index":%d}`, i+1)
			if err := a.sendAndVerify(stream, clientID, sessionID, userID, deviceID, sessionrpc.ActionHeartbeat, payload); err != nil {
				return err
			}
		}

		if err := a.sendAndVerify(stream, clientID, sessionID, userID, deviceID, sessionrpc.ActionLogout, `{"phase":"logout"}`); err != nil {
			return err
		}

		if !sleepWithContext(ctx, a.config.sessionGap) {
			return ctx.Err()
		}

		a.writeLog(logEntry{
			Level:         "info",
			Event:         "session_cycle_finished",
			ClientID:      clientID,
			SessionID:     sessionID,
			PeerID:        gateway.ID,
			PeerAddress:   target,
			TargetService: a.config.targetDiscoveryServiceName,
		})
	}
}

func (a *app) sendAndVerify(stream sessionrpc.GatewayService_OpenSessionClient, clientID, sessionID string, userID uint64, deviceID, action, payload string) error {
	event := &sessionrpc.ClientEvent{
		EventId:   fmt.Sprintf("%s-%s-%d", clientID, action, time.Now().UnixNano()),
		SessionId: sessionID,
		ClientId:  clientID,
		UserId:    userID,
		DeviceId:  deviceID,
		Action:    action,
		Payload:   payload,
		SentAt:    time.Now().Format(time.RFC3339),
	}

	if err := stream.Send(event); err != nil {
		eventsSentTotal.WithLabelValues(action, "error").Inc()
		return err
	}
	eventsSentTotal.WithLabelValues(action, "success").Inc()

	ack, err := stream.Recv()
	if err != nil {
		acksReceivedTotal.WithLabelValues(action, "error").Inc()
		return err
	}

	ackResult := ack.Result
	if ackResult == "" {
		ackResult = "unknown"
	}
	acksReceivedTotal.WithLabelValues(action, ackResult).Inc()

	a.writeLog(logEntry{
		Level:     levelForResult(ackResult),
		Event:     "session_ack_received",
		Action:    action,
		ClientID:  clientID,
		SessionID: sessionID,
		EventID:   event.EventId,
		PeerID:    ack.GatewayId,
		Detail:    fmt.Sprintf("worker_id=%s %s", ack.WorkerId, ack.Message),
	})

	if ackResult != "success" {
		return fmt.Errorf("gateway ack failed: %s", ack.Message)
	}
	return nil
}

func (a *app) handleRoot(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":                a.config.serviceName,
		"instanceId":             a.config.instanceID,
		"targetDiscoveryService": a.config.targetDiscoveryServiceName,
		"gatewayAddr":            a.config.gatewayAddr,
		"virtualClients":         a.config.virtualClients,
		"activeStreams":          a.activeStreams.Load(),
		"gatewayCount":           len(a.snapshotGateways()),
		"requestCount":           a.requestCount.Add(1),
		"uptimeSec":              int64(time.Since(a.startedAt).Seconds()),
		"time":                   time.Now().Format(time.RFC3339),
	})
}

func (a *app) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":                 "ok",
		"service":                a.config.serviceName,
		"instanceId":             a.config.instanceID,
		"targetDiscoveryService": a.config.targetDiscoveryServiceName,
		"activeStreams":          a.activeStreams.Load(),
		"gatewayCount":           len(a.snapshotGateways()),
		"time":                   time.Now().Format(time.RFC3339),
	})
}

func (a *app) handleGateways(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":     a.config.serviceName,
		"instanceId":  a.config.instanceID,
		"gatewayAddr": a.config.gatewayAddr,
		"gateways":    a.snapshotGateways(),
		"time":        time.Now().Format(time.RFC3339),
	})
}

func (a *app) withMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(recorder, r)

		codeLabel := strconv.Itoa(recorder.statusCode)
		httpRequestsTotal.WithLabelValues(r.URL.Path, r.Method, codeLabel).Inc()
		httpRequestDurationSeconds.WithLabelValues(r.URL.Path, r.Method, codeLabel).Observe(time.Since(startedAt).Seconds())
		a.writeLog(logEntry{
			Level:  "info",
			Event:  "http_request_processed",
			Path:   r.URL.Path,
			Method: r.Method,
			Status: recorder.statusCode,
		})
	})
}

func (a *app) refreshGatewaysLoop(ctx context.Context) {
	ticker := time.NewTicker(a.config.peerRefreshInterval)
	defer ticker.Stop()

	a.refreshGateways()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.refreshGateways()
		}
	}
}

func (a *app) refreshGateways() {
	gateways, err := a.fetchPeersFromConsul()
	if err != nil {
		a.writeLog(logEntry{
			Level:         "error",
			Event:         "gateway_refresh_failed",
			TargetService: a.config.targetDiscoveryServiceName,
			Detail:        err.Error(),
		})
		return
	}

	a.gatewaysMu.Lock()
	a.gateways = gateways
	a.gatewaysMu.Unlock()

	discoveredGateways.WithLabelValues(a.config.serviceName, a.config.targetDiscoveryServiceName).Set(float64(len(gateways)))
	a.writeLog(logEntry{
		Level:         "info",
		Event:         "gateway_list_refreshed",
		TargetService: a.config.targetDiscoveryServiceName,
		PeerCount:     len(gateways),
	})
}

func (a *app) fetchPeersFromConsul() ([]peer, error) {
	url := fmt.Sprintf("%s/v1/health/service/%s?passing=true", strings.TrimRight(a.config.consulHTTPAddr, "/"), a.config.targetDiscoveryServiceName)
	response, err := a.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("consul returned status %d", response.StatusCode)
	}

	var entries []consulServiceEntry
	if err := json.NewDecoder(response.Body).Decode(&entries); err != nil {
		return nil, err
	}

	peers := make([]peer, 0, len(entries))
	for _, entry := range entries {
		address := strings.TrimSpace(entry.Service.Address)
		if address == "" {
			address = strings.TrimSpace(entry.Node.Address)
		}
		if address == "" || entry.Service.Port == 0 {
			continue
		}
		peers = append(peers, peer{
			ID:      entry.Service.ID,
			Service: entry.Service.Service,
			Address: address,
			Port:    entry.Service.Port,
		})
	}

	slices.SortFunc(peers, func(a, b peer) int {
		return strings.Compare(peerAddress(a), peerAddress(b))
	})
	return peers, nil
}

func (a *app) pickGateway() (string, peer, bool) {
	if strings.TrimSpace(a.config.gatewayAddr) != "" {
		return a.config.gatewayAddr, peer{
			ID:      "manual-target",
			Service: a.config.targetDiscoveryServiceName,
			Address: a.config.gatewayAddr,
			Port:    0,
		}, true
	}

	gateways := a.snapshotGateways()
	if len(gateways) == 0 {
		return "", peer{}, false
	}
	if len(gateways) == 1 {
		return peerAddress(gateways[0]), gateways[0], true
	}
	selected := gateways[a.randomInt(len(gateways))]
	return peerAddress(selected), selected, true
}

func (a *app) snapshotGateways() []peer {
	a.gatewaysMu.RLock()
	defer a.gatewaysMu.RUnlock()
	result := make([]peer, len(a.gateways))
	copy(result, a.gateways)
	return result
}

func (a *app) randomInt(max int) int {
	a.randMu.Lock()
	defer a.randMu.Unlock()
	return a.random.Intn(max)
}

func loadConfig() config {
	serviceName := envOrDefault("SERVICE_NAME", "client-demo")
	instanceID := envOrDefault("INSTANCE_ID", envOrDefault("NOMAD_ALLOC_ID", hostnameOrDefault()))

	heartbeatMin := envIntOrDefault("HEARTBEAT_MIN", 1)
	heartbeatMax := envIntOrDefault("HEARTBEAT_MAX", 3)
	if heartbeatMax < heartbeatMin {
		heartbeatMax = heartbeatMin
	}

	return config{
		serviceName:                serviceName,
		instanceID:                 instanceID,
		appPort:                    envOrDefault("APP_PORT", "18082"),
		metricsPort:                envOrDefault("METRICS_PORT", "12114"),
		logPath:                    envOrDefault("APP_LOG_PATH", "/app/logs/client-demo.log"),
		consulHTTPAddr:             ensureHTTPPrefix(envOrDefault("CONSUL_HTTP_ADDR", "127.0.0.1:8500")),
		gatewayAddr:                envOrDefault("GATEWAY_ADDR", ""),
		targetDiscoveryServiceName: envOrDefault("TARGET_DISCOVERY_SERVICE_NAME", "gateway-grpc"),
		peerRefreshInterval:        envDurationMillisOrDefault("PEER_REFRESH_INTERVAL_MS", 5000),
		reconnectDelay:             envDurationMillisOrDefault("RECONNECT_DELAY_MS", 3000),
		heartbeatInterval:          envDurationMillisOrDefault("HEARTBEAT_INTERVAL_MS", 2000),
		sessionGap:                 envDurationMillisOrDefault("SESSION_GAP_MS", 3000),
		virtualClients:             envIntOrDefault("VIRTUAL_CLIENTS", 5),
		heartbeatMin:               heartbeatMin,
		heartbeatMax:               heartbeatMax,
	}
}

func newLogger(logPath string) (*log.Logger, *os.File, error) {
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return nil, nil, err
	}
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}
	return log.New(io.MultiWriter(os.Stdout, file), "", 0), file, nil
}

func (a *app) writeLog(entry logEntry) {
	entry.Service = a.config.serviceName
	entry.InstanceID = a.config.instanceID
	entry.Timestamp = time.Now().Format(time.RFC3339)
	body, err := json.Marshal(entry)
	if err != nil {
		a.logger.Printf(`{"level":"error","event":"log_marshal_failed","service":"%s","instance_id":"%s","detail":%q,"ts":"%s"}`,
			a.config.serviceName,
			a.config.instanceID,
			err.Error(),
			time.Now().Format(time.RFC3339),
		)
		return
	}
	a.logger.Println(string(body))
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envDurationMillisOrDefault(key string, fallback int) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return time.Duration(fallback) * time.Millisecond
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return time.Duration(fallback) * time.Millisecond
	}
	return time.Duration(value) * time.Millisecond
}

func envIntOrDefault(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func ensureHTTPPrefix(value string) string {
	if strings.HasPrefix(value, "http://") || strings.HasPrefix(value, "https://") {
		return value
	}
	return "http://" + value
}

func hostnameOrDefault() string {
	name, err := os.Hostname()
	if err != nil || strings.TrimSpace(name) == "" {
		return "unknown-host"
	}
	return name
}

func peerAddress(item peer) string {
	if item.Port == 0 {
		return item.Address
	}
	return fmt.Sprintf("%s:%d", item.Address, item.Port)
}

func levelForResult(result string) string {
	if result == "success" {
		return "info"
	}
	return "error"
}
