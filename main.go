package main

import (
	"context"
	"log"

	"github.com/AleZane29/GoQueue/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://postgres:@localhost:5432/GoQueue?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatal(err)
	}

	store := store.NewStore(pool)
	_ = store // per ora, finché non sviluppo dispatcher
}
