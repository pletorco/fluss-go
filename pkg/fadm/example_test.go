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

func ExampleClient_CreateACLs() {
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
	results, err := admin.CreateACLs(ctx, fadm.ACL{
		ResourceName:  "analytics",
		ResourceType:  fadm.ACLResourceDatabase,
		PrincipalName: "alice",
		PrincipalType: fadm.ACLPrincipalUser,
		Host:          fadm.ACLWildcardHost,
		Operation:     fadm.ACLOperationDescribe,
		Permission:    fadm.ACLPermissionAllow,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		if result.Err != nil {
			log.Printf("create ACL for %s: %v", result.ACL.PrincipalName, result.Err)
		}
	}
}

func ExampleClient_ListACLs() {
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
	acls, err := admin.ListACLs(ctx, fadm.ACLFilter{
		ResourceType: fadm.ACLResourceAny,
		Operation:    fadm.ACLOperationAny,
		Permission:   fadm.ACLPermissionAny,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, acl := range acls {
		log.Printf("%s:%s may perform %d on %s",
			acl.PrincipalType,
			acl.PrincipalName,
			acl.Operation,
			acl.ResourceName,
		)
	}
}

func ExampleClient_DropACLs() {
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
	resourceName := "analytics"
	principalName := "alice"
	principalType := fadm.ACLPrincipalUser
	results, err := admin.DropACLs(ctx, fadm.ACLFilter{
		ResourceName:  &resourceName,
		ResourceType:  fadm.ACLResourceDatabase,
		PrincipalName: &principalName,
		PrincipalType: &principalType,
		Operation:     fadm.ACLOperationAny,
		Permission:    fadm.ACLPermissionAny,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		if result.Err != nil {
			log.Printf("drop ACLs: %v", result.Err)
		}
	}
}
