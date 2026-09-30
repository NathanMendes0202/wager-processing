package main

import (
	"context"
	"log"

	"go.uber.org/fx"

	"github.com/NathanMendes0202/wager-processing/internal/app"
)

func main() {
	fx.New(
		app.Module(),
	).Run()
	_ = context.Background()
	log.SetFlags(log.LstdFlags | log.LUTC)
}
