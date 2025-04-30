package explorer

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"pareme/common"
	"sync"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/syndtr/goleveldb/leveldb"
)

// For logging
type responseWriter struct {
	http.ResponseWriter
}

func (rw *responseWriter) WriteHeader(statusCode int) {
	rw.ResponseWriter.WriteHeader(statusCode)
}

func startAPI(ctx context.Context, wg *sync.WaitGroup, db *leveldb.DB, addr string) error {
	// Create router
	r := mux.NewRouter()

	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Log request details
			common.PrintToLog(fmt.Sprintf("Request Method: %v", r.Method))
			common.PrintToLog(fmt.Sprintf("Request URL: %v", r.URL.String()))
			common.PrintToLog(fmt.Sprintf("Request Origin: %v", r.Header.Get("Origin")))
			common.PrintToLog(fmt.Sprintf("Request Access-Control-Request-Method: %v", r.Header.Get("Access-Control-Request-Method")))
			common.PrintToLog(fmt.Sprintf("Request Access-Control-Request-Headers: %v", r.Header.Get("Access-Control-Request-Headers")))

			// Wrap ResponseWriter to log headers
			rw := &responseWriter{ResponseWriter: w}
			next.ServeHTTP(rw, r)

			// Log response headers
			common.PrintToLog(fmt.Sprintf("Response Access-Control-Allow-Origin: %v", rw.Header().Get("Access-Control-Allow-Origin")))
			common.PrintToLog(fmt.Sprintf("Response Access-Control-Allow-Methods: %v", rw.Header().Get("Access-Control-Allow-Methods")))
			common.PrintToLog(fmt.Sprintf("Response Access-Control-Allow-Headers: %v", rw.Header().Get("Access-Control-Allow-Headers")))
		})
	})

	r.HandleFunc("/hash/{hash}", func(w http.ResponseWriter, r *http.Request) {
		common.PrintToLog("Handling requst for /hash/{hash}")

		// Manually set CORS headers (fallback)
		origin := r.Header.Get("Origin")
		if origin == "http://localhost:5173" || origin == "https://pareme.org" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			common.PrintToLog(fmt.Sprintf("Manually set CORS headers for origin: %v", origin))
		}

		// Handle OPTIONS preflight
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			common.PrintToLog("Responded to OPTIONS preflight")
			return
		}

		// Get hash from URL
		vars := mux.Vars(r)
		hashHex := vars["hash"]

		// Decode hex hash (expect 64 chars = 32 bytes)
		hash, err := hex.DecodeString(hashHex)
		if err != nil || len(hash) != 32 {
			http.Error(w, "Invalid hahs: must be 32 bytes (64 hex chars)", http.StatusBadRequest)
			return
		}

		// Check if hash exists and get frequency
		freqBytes, err := db.Get(hash, nil)
		var freq uint32
		if err == leveldb.ErrNotFound {
			freq = 0
		} else if err != nil {
			http.Error(w, "Database error", http.StatusInternalServerError)
			return
		} else {
			freq = binary.BigEndian.Uint32(freqBytes)
		}

		offsetBytes, _ := db.Get(syncKey, nil)
		offset := binary.BigEndian.Uint32(offsetBytes)

		common.PrintToLog(fmt.Sprintf("Explorer is updated to %v", offset))
		common.PrintToLog(fmt.Sprintf("freq is: %v", freq))

		// Respond with JSON
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]uint32{"Frequency": freq}); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		}
	}).Methods("GET", "OPTIONS")

	// Apply CORS middleware
	corsHandler := handlers.CORS(
		handlers.AllowedOrigins([]string{"'http://localhost:5173", "https://pareme.org"}),
		handlers.AllowedMethods([]string{"GET", "OPTIONS"}),
		handlers.AllowedHeaders([]string{"Content-Type"}),
		handlers.OptionStatusCode(http.StatusOK),
	)

	// Create HTTP server
	srv := &http.Server{
		Addr:    addr,
		Handler: corsHandler(r),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		common.PrintToLog("Starting API Server...")

		// Start server
		go func() {
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				common.PrintToLog(fmt.Sprintf("HTTP server error: %v", err.Error()))
			}
		}()

		<-ctx.Done()

		// Initiate graceful shutdown
		if err := srv.Shutdown(context.Background()); err != nil {
			common.PrintToLog(fmt.Sprintf("HTTP shutdown error: %v", err.Error()))
		}
	}()

	return nil
}
