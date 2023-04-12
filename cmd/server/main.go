package main

import (
	"log"

	"github.com/noahshpak/proglog/internal/server"
)

func main() {
	log.Println("Starting server...")
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
