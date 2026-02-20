package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// TODO: Initialize storage (Redis + PostgreSQL)
	// TODO: Initialize queue manager
	// TODO: Initialize HTTP server (Gin)
	// TODO: Register API routes
	// TODO: Start server

	log.Printf("Job Queue API server starting on port %s", port)
	// server.Run(":" + port)
}
