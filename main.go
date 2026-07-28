package main

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Session struct {
	code      string
	createdAt time.Time
	expiresAt time.Time
	clients   []*websocket.Conn
}

var (
	sessions   = make(map[string]*Session)
	sessionsMu sync.Mutex
)

const sessionTTL = 30 * time.Minute

// Codes that never get a second client (the caller never connects) shouldn't
// stay valid for the full session TTL. This is a tighter deadline enforced
// only while a session still has fewer than 2 clients.
const unusedCodeTTL = 5 * time.Minute

const maxClientsPerSession = 2

// ---------- Simple per-IP rate limiting ----------
//
// Two separate limiters are used: one for creating sessions (an agent
// action, expected to be infrequent) and one for /ws connection attempts
// (which is also where someone would brute-force guess a 6-digit code).
// Both are plain token buckets keyed by client IP, with no external
// dependencies.

type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
}

type ipRateLimiter struct {
	mu         sync.Mutex
	buckets    map[string]*tokenBucket
	ratePerSec float64 // tokens added per second
	burst      float64 // max tokens a bucket can hold
}

func newIPRateLimiter(ratePerSec, burst float64) *ipRateLimiter {
	rl := &ipRateLimiter{
		buckets:    make(map[string]*tokenBucket),
		ratePerSec: ratePerSec,
		burst:      burst,
	}
	go rl.cleanupLoop()
	return rl
}

// allow reports whether a request from ip should proceed, consuming a
// token if so.
func (rl *ipRateLimiter) allow(ip string) bool {
	now := time.Now()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[ip]
	if !ok {
		b = &tokenBucket{tokens: rl.burst, lastRefill: now}
		rl.buckets[ip] = b
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens += elapsed * rl.ratePerSec
	if b.tokens > rl.burst {
		b.tokens = rl.burst
	}
	b.lastRefill = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// cleanupLoop periodically drops buckets that have been idle long enough
// to have refilled to full, so memory doesn't grow unbounded with the
// number of distinct IPs seen over the server's lifetime.
func (rl *ipRateLimiter) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-10 * time.Minute)
		rl.mu.Lock()
		for ip, b := range rl.buckets {
			if b.lastRefill.Before(cutoff) {
				delete(rl.buckets, ip)
			}
		}
		rl.mu.Unlock()
	}
}

var (
	// 5 session creations per minute per IP, small burst allowance.
	createSessionLimiter = newIPRateLimiter(5.0/60.0, 5)
	// 10 /ws connection attempts per minute per IP. This is the one that
	// matters most for blocking brute-force guessing of 6-digit codes:
	// at 10/min, cycling through even a small fraction of 1,000,000
	// possible codes takes an impractical amount of time.
	wsConnectLimiter = newIPRateLimiter(10.0/60.0, 10)
)

// clientIP extracts the caller's IP, preferring X-Forwarded-For (Render and
// most reverse proxies set this) and falling back to RemoteAddr.
func clientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		// X-Forwarded-For can be a comma-separated chain; the first entry
		// is the original client.
		parts := strings.Split(fwd, ",")
		ip := strings.TrimSpace(parts[0])
		if ip != "" {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func generateSessionCode() string {
	for {
		n := rand.Intn(1000000)
		code := padCode(n)

		sessionsMu.Lock()
		_, exists := sessions[code]
		sessionsMu.Unlock()

		if !exists {
			return code
		}
	}
}

func padCode(n int) string {
	s := ""
	for i := 0; i < 6; i++ {
		digit := n % 10
		s = string(rune('0'+digit)) + s
		n /= 10
	}

	return s
}

func createSessionHandler(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)
	if !createSessionLimiter.allow(ip) {
		log.Println("Rate limit exceeded for session creation from", ip)
		http.Error(w, "too many session requests, please slow down", http.StatusTooManyRequests)
		return
	}

	code := generateSessionCode()
	now := time.Now()

	session := &Session{
		code:      code,
		createdAt: now,
		expiresAt: now.Add(sessionTTL),
		clients:   []*websocket.Conn{},
	}

	sessionsMu.Lock()
	sessions[code] = session
	sessionsMu.Unlock()

	log.Println("Created Session:", code, "expires:", session.expiresAt)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder((w)).Encode(map[string]string{"code": code})
}

// removeSessionLocked deletes a session from the map. Caller must hold sessionsMu.
func removeSessionLocked(code string) {
	if _, ok := sessions[code]; ok {
		delete(sessions, code)
		log.Println("Session removed:", code)
	}
}

// closeSessionClientsLocked closes every websocket still attached to a
// session. Caller must hold sessionsMu.
func closeSessionClientsLocked(session *Session) {
	for _, c := range session.clients {
		c.Close()
	}
	session.clients = nil
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)
	if !wsConnectLimiter.allow(ip) {
		log.Println("Rate limit exceeded for /ws from", ip)
		http.Error(w, "too many connection attempts, please slow down", http.StatusTooManyRequests)
		return
	}

	code := r.URL.Query().Get("code")
	role := r.URL.Query().Get("role")

	sessionsMu.Lock()
	session, exists := sessions[code]
	if exists && time.Now().After(session.expiresAt) {
		// Lazily clean up an expired session the moment anyone touches it.
		closeSessionClientsLocked(session)
		removeSessionLocked(code)
		exists = false
	}
	if exists && len(session.clients) == 0 && time.Now().After(session.createdAt.Add(unusedCodeTTL)) {
		// Nobody ever connected to this code within the short "unused" window
		// (e.g. it was generated, read aloud, but never used, or the caller
		// already finished and the code is now stale). Kill it early rather
		// than leaving it valid for the full session TTL.
		removeSessionLocked(code)
		exists = false
	}
	var full bool
	if exists {
		full = len(session.clients) >= maxClientsPerSession
	}
	sessionsMu.Unlock()

	if !exists {
		http.Error(w, "invalid or expired session code", http.StatusNotFound)
		return
	}
	if full {
		// A code is only ever meant to pair exactly one agent with one
		// caller. Reject anyone trying to join a session that already has
		// both parties connected — this also blocks a third party from
		// piggybacking on a code after the real caller has already joined.
		http.Error(w, "session already has an active agent and caller", http.StatusForbidden)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	log.Println("Client connected to session:", code, "role:", role)

	sessionsMu.Lock()
	// Re-check under lock in case of a race between two simultaneous
	// connection attempts that both passed the check above.
	if len(session.clients) >= maxClientsPerSession {
		sessionsMu.Unlock()
		conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"session full"}`))
		conn.Close()
		return
	}
	session.clients = append(session.clients, conn)
	clientCount := len(session.clients)
	sessionsMu.Unlock()

	log.Println("Session", code, "now has", clientCount, "clients")

	infoMsg, _ := json.Marshal(map[string]string{
		"type":      "session-info",
		"expiresAt": session.expiresAt.Format(time.RFC3339),
	})
	conn.WriteMessage(websocket.TextMessage, infoMsg)

	sessionEnded := false

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read failed, client disconnected:", err)
			break
		}

		// Detect an explicit "stop" so we can tear the whole session down
		// immediately instead of waiting for the TTL to pass.
		var parsed struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(message, &parsed) == nil && parsed.Type == "stop" {
			sessionEnded = true
		}

		sessionsMu.Lock()
		for _, client := range session.clients {
			if client == conn {
				continue
			}

			if writeErr := client.WriteMessage(messageType, message); writeErr != nil {
				log.Println("Write failed:", writeErr)
			}
		}
		sessionsMu.Unlock()

		if sessionEnded {
			break
		}
	}

	sessionsMu.Lock()
	for i, c := range session.clients {
		if c == conn {
			session.clients = append(session.clients[:i], session.clients[i+1:]...)
			break
		}
	}
	remaining := len(session.clients)
	if sessionEnded || remaining == 0 {
		// Either side explicitly ended the call, or everyone has left.
		// Either way, the code should not be usable again.
		closeSessionClientsLocked(session)
		removeSessionLocked(code)
	}
	sessionsMu.Unlock()
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"service": "webrtc-signaling-server",
		"time":    time.Now().Format(time.RFC3339),
	})
}

func turnCredentialsHandler(w http.ResponseWriter, r *http.Request) {
	secretKey := os.Getenv("METERED_SECRET_KEY")
	appDomain := os.Getenv("METERED_APP_DOMAIN")

	if secretKey == "" || appDomain == "" {
		http.Error(w, "TURN credentials not configured", http.StatusInternalServerError)
		return
	}

	// Step 1: create a short-lived credential using the secret key
	createURL := "https://" + appDomain + "/api/v1/turn/credential?secretKey=" + secretKey
	body := strings.NewReader(`{"expiryInSeconds": 600, "label": "screen-assist-session"}`)

	createReq, err := http.NewRequest("POST", createURL, body)
	if err != nil {
		http.Error(w, "failed to build TURN request", http.StatusInternalServerError)
		return
	}
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		log.Println("Failed to create TURN credential:", err)
		http.Error(w, "failed to create TURN credential", http.StatusBadGateway)
		return
	}
	defer createResp.Body.Close()

	var created struct {
		ApiKey string `json:"apiKey"`
	}
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil || created.ApiKey == "" {
		log.Println("Unexpected response creating TURN credential:", err)
		http.Error(w, "failed to parse TURN credential creation response", http.StatusBadGateway)
		return
	}

	// Step 2: fetch the actual ICE servers array using that apiKey
	credsURL := "https://" + appDomain + "/api/v1/turn/credentials?apiKey=" + created.ApiKey

	credsResp, err := http.Get(credsURL)
	if err != nil {
		log.Println("Failed to fetch TURN ICE servers:", err)
		http.Error(w, "failed to fetch TURN credentials", http.StatusBadGateway)
		return
	}
	defer credsResp.Body.Close()

	credsBody, err := io.ReadAll(credsResp.Body)
	if err != nil {
		http.Error(w, "failed to read TURN credentials", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(credsBody)
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}
	fs := http.FileServer(http.Dir("./public"))
	http.Handle("/", fs)

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/create-session", createSessionHandler)
	http.HandleFunc("/api/turn-credentials", turnCredentialsHandler)
	http.HandleFunc("/ws", handleWS)

	log.Println("server listening on :8080")
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // fallback for local dev
	}
	http.ListenAndServe(":"+port, nil)
}