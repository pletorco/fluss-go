package fadm_test

import (
	"context"
	"encoding/json"
	"fmt"
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
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

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
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

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

func ExampleClient_CreateTable() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithSeedBrokers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	err = admin.CreateTable(
		ctx,
		fgo.TablePath{Database: "production", Table: "customers"},
		fadm.TableDefinition{
			Schema: fgo.Schema{
				Columns: []fgo.Column{
					{Name: "customer_id", Type: fgo.BigIntType},
					{Name: "name", Type: fgo.StringType},
				},
				PrimaryKey: []string{"customer_id"},
			},
			BucketCount: 3,
		},
		true,
	)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_ListOffsets() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithSeedBrokers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	table, err := client.OpenTable(
		ctx,
		fgo.TablePath{Database: "production", Table: "events"},
	)
	if err != nil {
		log.Fatal(err)
	}
	results := admin.ListOffsets(
		ctx,
		table,
		fgo.PhysicalTablePath{TablePath: table.Path},
		-1,
		[]int32{0, 1, 2},
		fgo.Latest(),
	)
	for _, result := range results {
		if result.Err != nil {
			log.Printf("bucket %d: %v", result.Bucket, result.Err)
			continue
		}
		log.Printf("bucket %d offset %d", result.Bucket, result.Offset)
	}
}

func ExampleTableDefinition_JSON() {
	definition := fadm.TableDefinition{
		Schema: fgo.Schema{
			Columns: []fgo.Column{
				{Name: "customer_id", Type: fgo.BigIntType},
				{Name: "name", Type: fgo.StringType},
			},
			PrimaryKey: []string{"customer_id"},
		},
		BucketCount: 3,
	}
	encoded, err := definition.JSON()
	fmt.Println(err == nil, json.Valid(encoded))
	// Output:
	// true true
}
