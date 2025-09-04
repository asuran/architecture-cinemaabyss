package main

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

// Configuration
type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

// Global config
var config Config

func main() {
	// Load configuration from environment variables
	loadConfig()

	// Set up HTTP routes
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/movies", handleMovies)
	http.HandleFunc("/api/movies/health", handleMoviesHealth)
	http.HandleFunc("/api/users", handleUsers)
	http.HandleFunc("/api/payments", handlePayments)
	http.HandleFunc("/api/subscriptions", handleSubscriptions)
	http.HandleFunc("/api/events/", handleEvents)

	// Start server
	log.Printf("Starting Strangler Fig Proxy on port %s", config.Port)
	log.Printf("Monolith URL: %s", config.MonolithURL)
	log.Printf("Movies Service URL: %s", config.MoviesServiceURL)
	log.Printf("Events Service URL: %s", config.EventsServiceURL)
	log.Printf("Gradual Migration: %t", config.GradualMigration)
	log.Printf("Movies Migration Percent: %d%%", config.MoviesMigrationPercent)

	log.Fatal(http.ListenAndServe(":"+config.Port, nil))
}

func loadConfig() {
	config.Port = getEnv("PORT", "8000")
	config.MonolithURL = getEnv("MONOLITH_URL", "http://localhost:8080")
	config.MoviesServiceURL = getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")
	config.EventsServiceURL = getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")

	gradualMigrationStr := getEnv("GRADUAL_MIGRATION", "true")
	config.GradualMigration = gradualMigrationStr == "true"

	migrationPercentStr := getEnv("MOVIES_MIGRATION_PERCENT", "50")
	percent, err := strconv.Atoi(migrationPercentStr)
	if err != nil {
		log.Printf("Invalid MOVIES_MIGRATION_PERCENT value: %s, using default 50", migrationPercentStr)
		percent = 50
	}
	config.MoviesMigrationPercent = percent
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Health check handler
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Strangler Fig Proxy is healthy"))
}

// Movies health check handler
func handleMoviesHealth(w http.ResponseWriter, r *http.Request) {
	// Always route to movies service for health check
	proxyRequest(w, r, config.MoviesServiceURL+"/api/movies/health")
}

// Movies handler with Strangler Fig pattern
func handleMovies(w http.ResponseWriter, r *http.Request) {
	// Check if gradual migration is enabled
	if config.GradualMigration {
		// Use random percentage to decide routing
		rand.Seed(time.Now().UnixNano())
		randomPercent := rand.Intn(100)

		if randomPercent < config.MoviesMigrationPercent {
			// Route to new movies service
			log.Printf("Routing movies request to NEW service (random: %d, threshold: %d)", randomPercent, config.MoviesMigrationPercent)
			proxyRequest(w, r, config.MoviesServiceURL+r.URL.Path)
		} else {
			// Route to monolith
			log.Printf("Routing movies request to MONOLITH (random: %d, threshold: %d)", randomPercent, config.MoviesMigrationPercent)
			proxyRequest(w, r, config.MonolithURL+r.URL.Path)
		}
	} else {
		// If gradual migration is disabled, route to monolith
		log.Printf("Gradual migration disabled, routing movies request to MONOLITH")
		proxyRequest(w, r, config.MonolithURL+r.URL.Path)
	}
}

// Users handler - always route to monolith
func handleUsers(w http.ResponseWriter, r *http.Request) {
	log.Printf("Routing users request to MONOLITH")
	proxyRequest(w, r, config.MonolithURL+r.URL.Path)
}

// Payments handler - always route to monolith
func handlePayments(w http.ResponseWriter, r *http.Request) {
	log.Printf("Routing payments request to MONOLITH")
	proxyRequest(w, r, config.MonolithURL+r.URL.Path)
}

// Subscriptions handler - always route to monolith
func handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	log.Printf("Routing subscriptions request to MONOLITH")
	proxyRequest(w, r, config.MonolithURL+r.URL.Path)
}

// Events handler - always route to events service
func handleEvents(w http.ResponseWriter, r *http.Request) {
	log.Printf("Routing events request to EVENTS SERVICE")
	proxyRequest(w, r, config.EventsServiceURL+r.URL.Path)
}

// Generic proxy request function
func proxyRequest(w http.ResponseWriter, r *http.Request, targetURL string) {
	// Create a new request to the target service
	req, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		log.Printf("Error creating request: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Copy headers from original request
	for key, values := range r.Header {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	// Add query parameters
	req.URL.RawQuery = r.URL.RawQuery

	// Make the request
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error making request to %s: %v", targetURL, err)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Set status code
	w.WriteHeader(resp.StatusCode)

	// Copy response body
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Printf("Error copying response body: %v", err)
	}
}

// Helper function to create error response
func createErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	errorResp := map[string]string{"error": message}
	json.NewEncoder(w).Encode(errorResp)
}
