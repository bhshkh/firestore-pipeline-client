package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/metadata"
)

const (
	projectID  = "my_project"
	databaseID = "my_database"
)

func getCookieManual() string {
	return "my_cookie"
}

// createClient initializes a client connected to a specific database
// using the provided ProjectID and DatabaseID, and adds logging to the gRPC requests.
func createClient(ctx context.Context) *firestore.Client {
	Cookie := getCookieManual()
	if Cookie == "" {
		fmt.Println("Warning: No cookie provided.")
	} else {
		// Add the  cookie to the gRPC metadata.
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		md["cookie"] = []string{Cookie}
		ctx = metadata.NewOutgoingContext(ctx, md)
		fmt.Printf("cookie added to context: %s\n", Cookie)
	}

	client, err := firestore.NewClientWithDatabase(ctx, projectID, databaseID)
	if err != nil {
		fmt.Printf("Failed to create Firestore client: %v\n", err)
		return nil
	}
	return client
}

func main() {
	ctx := context.Background()

	// The client now uses the local variables defined above.
	client := createClient(ctx)
	if client == nil {
		fmt.Println("Failed to create client")
		return
	}
	defer client.Close()

	// deleteCollection(ProjectID, DatabaseID, 100)
	// users_main()
	books_main(client)
}
