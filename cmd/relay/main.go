package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/beamshare/beam/internal/relay"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := relay.NewServer()

	fmt.Printf("Relay server listening on :%s\n", port)
	if err := http.ListenAndServe(":"+port, srv); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
