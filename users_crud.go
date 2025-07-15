package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

const (
	collUsers  = "users"
	collOrders = "orders"
)

// User represents a document in the 'users' collection.
type User struct {
	FirstName string `firestore:"firstName"`
	LastName  string `firestore:"lastName"`
	Email     string `firestore:"email"`
	Age       int    `firestore:"age"`
}

// Item is a nested struct within an Order.
type Item struct {
	ProductName string  `firestore:"productName"`
	Quantity    int     `firestore:"quantity"`
	Price       float64 `firestore:"price"`
}

// Order represents a document in the 'orders' collection.
type Order struct {
	UserID    string    `firestore:"userID"`
	OrderDate time.Time `firestore:"orderDate"`
	Total     float64   `firestore:"total"`
	Items     []Item    `firestore:"items"`
}

// queryOrdersWithCollectionGroup finds all orders for a specific user ID.
func queryOrdersWithCollectionGroup(ctx context.Context, client *firestore.Client, userID string) {
	fmt.Println("\n--- Querying 'orders' collection group for user ---")

	query := client.CollectionGroup("orders").Where("userID", "==", userID)
	iter := query.Documents(ctx)
	defer iter.Stop()

	fmt.Printf("Found the following orders for user %s:", userID)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Printf("Failed to iterate through query results: %v", err)
			return
		}

		var order Order
		if err := doc.DataTo(&order); err != nil {
			fmt.Printf("Failed to convert order document to struct: %v", err)
			return
		}
		fmt.Printf("  - Order ID: %s, Path: %s", doc.Ref.ID, doc.Ref.Path)
	}
}

// addMultipleUsersAndOrders creates multiple users and orders in separate top-level collections.
func addMultipleUsersAndOrders(ctx context.Context, client *firestore.Client) {
	fmt.Println("--- Writing data to separate top-level collections ---")

	users := []User{
		{FirstName: "Maria", LastName: "Garcia", Email: "maria.garcia@example.com", Age: 30},
		{FirstName: "John", LastName: "Doe", Email: "john.doe@example.com", Age: 40},
		{FirstName: "Alice", LastName: "Smith", Email: "alice.smith@example.com", Age: 50},
	}

	usersCollection := client.Collection(collUsers)
	ordersCollection := client.Collection(collOrders)

	for i, user := range users {
		userRef, _, err := usersCollection.Add(ctx, user)
		if err != nil {
			fmt.Printf("Failed to add user %s %s: %v", user.FirstName, user.LastName, err)
			return
		}
		fmt.Printf("Added user %s %s with ID: %s\n", user.FirstName, user.LastName, userRef.ID)

		// Add a couple of orders for each user
		order1 := Order{
			UserID:    userRef.ID,
			OrderDate: time.Now().Add(time.Duration(i) * -24 * time.Hour), // Vary order dates
			Total:     129.99 + float64(i*10),
			Items: []Item{
				{ProductName: "Wireless Headphones", Quantity: 1, Price: 129.99 + float64(i*5)},
				{ProductName: "Charger", Quantity: 1, Price: float64(i * 5)},
			},
		}
		_, _, err = ordersCollection.Add(ctx, order1)
		if err != nil {
			fmt.Printf("Failed to add order 1 for user %s: %v", userRef.ID, err)
			return
		}
		fmt.Printf("Added order 1 for user %s to top-level '%s' collection\n", userRef.ID, collOrders)

		if i%2 == 0 { // Add a second order for some users
			order2 := Order{
				UserID:    userRef.ID,
				OrderDate: time.Now().Add(time.Duration(i) * -48 * time.Hour),
				Total:     75.50 + float64(i*5),
				Items: []Item{
					{ProductName: "Mousepad", Quantity: 2, Price: 20.00 + float64(i*2)},
					{ProductName: "Keyboard Cleaner", Quantity: 1, Price: 35.50 + float64(i*1)},
				},
			}
			_, _, err = ordersCollection.Add(ctx, order2)
			if err != nil {
				fmt.Printf("Failed to add order 2 for user %s: %v", userRef.ID, err)
				return
			}
			fmt.Printf("Added order 2 for user %s to top-level '%s' collection\n", userRef.ID, collOrders)
		}
	}
}

func readUsersDataUsingPipelines(client *firestore.Client) {
	fmt.Println("\n--- Reading data using pipelines ---")
	itrs := []*firestore.PipelineResultIterator{
		// client.Pipeline().Collection(collUsers).Execute(context.Background()),
		// client.Pipeline().Collection(collUsers).Select("email").Execute(context.Background()),
		client.Pipeline().Collection(collUsers).Select().Execute(context.Background()),
		client.Pipeline().Collection(collUsers).Select(firestore.FieldOf("email").As("contact_email"), firestore.Add("age", 5).As("agePlus5")).Execute(context.Background()),
		client.Pipeline().Collection(collUsers).AddFields(
			firestore.FieldOf("email").As("contact_email"),
			firestore.FieldOf("age").As("orig_age"),
			firestore.Add("age", 5).Add(2).As("agePlus+5*2"),
		).Aggregate(firestore.Sum("age").As("age_sum")).Execute(context.Background()),
		client.Pipeline().Collection(collUsers).
			Aggregate(firestore.Sum("age").As("age_sum")).Execute(context.Background()),
		// client.Pipeline().Database().Execute(context.Background()),
	}

	for i, pri := range itrs {
		fmt.Println("\n\nPipeline #", i)
		for {
			res, err := pri.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				fmt.Printf("Next: %v\n", err)
				return
			}
			data, err := res.Data()
			if err != nil {
				fmt.Printf("Data: %+v\n", err)
			}
			fmt.Printf("Data: %#v\n", data)

			// var c User
			// err = res.DataTo(&c)
			// if err != nil {
			// 	fmt.Printf("DataTo: %v\n", err)
			// 	return
			// }
			// fmt.Printf("DataTo: %+v\n", c)
		}
	}
}

func users_main() {
	ctx := context.Background()

	// The client now uses the global constants defined above.
	client := createClient(ctx)
	defer client.Close()

	// Add multiple users and orders.
	// addMultipleUsersAndOrders(ctx, client)

	readUsersDataUsingPipelines(client)

	// Query the data back using a collection group query.
	// queryOrdersWithCollectionGroup(ctx, client, "someUserID")
}
