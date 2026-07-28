package main

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
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

func handleWS(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	role := r.URL.Query().Get("role")

	sessionsMu.Lock()
	session, exists := sessions[code]
	sessionsMu.Unlock()

	if !exists {
		http.Error(w, "invalid session code", http.StatusNotFound)
		return
	}

	if time.Now().After(session.expiresAt) {
		http.Error(w, "session expired", http.StatusGone)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	log.Println("Client connected to session:", code, "role:", role)

	sessionsMu.Lock()
	session.clients = append(session.clients, conn)
	clientCount := len(session.clients)
	sessionsMu.Unlock()

	log.Println("Session", code, "now has", clientCount, "clients")

	infoMsg, _ := json.Marshal(map[string]string{
		"type":      "session-info",
		"expiresAt": session.expiresAt.Format(time.RFC3339),
	})
	conn.WriteMessage(websocket.TextMessage, infoMsg)

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read failed, client disconnected:", err)
			break
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
	}

	sessionsMu.Lock()
	for i, c := range session.clients {
		if c == conn {
			session.clients = append(session.clients[:i], session.clients[i+1:]...)
			break
		}
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
