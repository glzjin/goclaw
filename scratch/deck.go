package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/nextlevelbuilder/goclaw/pkg/crypt"
	"github.com/jackc/pgx/v5"
	"context"
)

func main() {
	os.Setenv("GOCLAW_ENCRYPTION_KEY", "T3sP9vMwR2kY8cL5vBxQ1tU0gO2xS4zN")
	
	connString := "postgres://goclaw:goclaw@localhost:15432/goclaw?sslmode=disable"
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	var name string
	var credentials []byte
	err = conn.QueryRow(ctx, "SELECT name, credentials FROM channel_instances WHERE name = 'dingtalk'").Scan(&name, &credentials)
	if err != nil {
		log.Fatal(err)
	}

	decrypted, err := crypt.Decrypt(string(credentials), "T3sP9vMwR2kY8cL5vBxQ1tU0gO2xS4zN")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Decrypted: %s\n", string(decrypted))
}
