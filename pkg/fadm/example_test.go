package fadm_test

import (
	"context"
	"log"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func ExampleNew() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithSeedBrokers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	databases, err := admin.ListDatabases(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, database := range databases {
		log.Printf("%s: %d tables", database.Name, database.TableCount)
	}
}

func ExampleClient_CreateDatabase() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithSeedBrokers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	err = admin.CreateDatabase(ctx, "analytics", fadm.DatabaseDefinition{
		Comment: "application analytics",
		Properties: map[string]string{
			"owner": "data-platform",
		},
	}, true)
	if err != nil {
		log.Fatal(err)
	}
}
