package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

const (
	collBooks = "books"
)

type Book struct {
	Title     string          `firestore:"title"`
	Author    string          `firestore:"author"`
	Genre     string          `firestore:"genre"`
	Published int             `firestore:"published"`
	Rating    float64         `firestore:"rating"`
	Tags      []string        `firestore:"tags"`
	Awards    map[string]bool `firestore:"awards"`
}

func newBook(title string,
	author string,
	genre string,
	published int,
	rating float64,
	tags []string,
	awards map[string]bool,
) Book {
	return Book{
		Title:     title,
		Author:    author,
		Genre:     genre,
		Published: published,
		Rating:    rating,
		Tags:      tags,
		Awards:    awards,
	}
}

func addBooks(ctx context.Context, client *firestore.Client) {

	allBooks := map[string]Book{
		"book1": newBook(
			"The Hitchhiker's Guide to the Galaxy",
			"Douglas Adams",
			"Science Fiction",
			1979,
			4.2,
			[]string{"comedy", "space", "adventure"},
			map[string]bool{"hugo": true, "nebula": false},
		),
		"book2": newBook(
			"Pride and Prejudice",
			"Jane Austen",
			"Romance",
			1813,
			4.5,
			[]string{"classic", "social commentary", "love"},
			map[string]bool{"none": true},
		),
		"book3": newBook(
			"One Hundred Years of Solitude",
			"Gabriel García Márquez",
			"Magical Realism",
			1967,
			4.3,
			[]string{"family", "history", "fantasy"},
			map[string]bool{"nobel": true, "nebula": false},
		),
		"book4": newBook(
			"The Lord of the Rings",
			"J.R.R. Tolkien",
			"Fantasy",
			1954,
			4.7,
			[]string{"adventure", "magic", "epic"},
			map[string]bool{"hugo": false, "nebula": false},
		),
		"book5": newBook(
			"The Handmaid's Tale",
			"Margaret Atwood",
			"Dystopian",
			1985,
			4.1,
			[]string{"feminism", "totalitarianism", "resistance"},
			map[string]bool{"arthur c. clarke": true, "booker prize": false},
		),
		"book6": newBook(
			"Crime and Punishment",
			"Fyodor Dostoevsky",
			"Psychological Thriller",
			1866,
			4.3,
			[]string{"philosophy", "crime", "redemption"},
			map[string]bool{"none": true},
		),
		"book7": newBook(
			"To Kill a Mockingbird",
			"Harper Lee",
			"Southern Gothic",
			1960,
			4.2,
			[]string{"racism", "injustice", "coming-of-age"},
			map[string]bool{"pulitzer": true},
		),
		"book8": newBook(
			"1984",
			"George Orwell",
			"Dystopian",
			1949,
			4.2,
			[]string{"surveillance", "totalitarianism", "propaganda"},
			map[string]bool{"prometheus": true}),
		"book9": newBook(
			"The Great Gatsby",
			"F. Scott Fitzgerald",
			"Modernist",
			1925,
			4.0,
			[]string{"wealth", "american dream", "love"},
			map[string]bool{"none": true},
		),
		"book10": newBook(
			"Dune",
			"Frank Herbert",
			"Science Fiction",
			1965,
			4.6,
			[]string{"politics", "desert", "ecology"},
			map[string]bool{"hugo": true, "nebula": true},
		),
	}
	fmt.Println("--- Writing data to Books top-level collections ---")
	booksCollection := client.Collection(collBooks)

	for nameKeyStr, book := range allBooks {
		_, err := booksCollection.Doc(nameKeyStr).Create(ctx, book)
		if err != nil {
			fmt.Printf("Failed to add book %s: %v\n", nameKeyStr, err)
			return
		}
		fmt.Printf("Added book with key: %s\n", nameKeyStr)
	}
}

func readBooksDataUsingPipelines(ctx context.Context, client *firestore.Client) {
	fmt.Println("\n--- Reading data using pipelines ---")
	itrs := []*firestore.PipelineResultIterator{
		// client.Pipeline().Collection(collUsers).Execute(context.Background()),
		// client.Pipeline().Collection(collUsers).SelectFields("email").Execute(context.Background()),
		// client.Pipeline().Collection(collUsers).Select(firestore.FieldOf("email").As("contact_email"), firestore.Add("age", 5).As("agePlus5")).Execute(context.Background()),
		client.Pipeline().
			Collection(collBooks).
			// Where(Lt("published", 1984)).
			AggregateWithSpec(firestore.NewAggregateSpec(firestore.Avg("rating").As("avg_rating")).WithGroups("genre")).
			// Where(Eq("avg_rating", 4.3)).
			Execute(ctx),
		// client.Pipeline().Database().Execute(context.Background()),
	}

	for i, pri := range itrs {
		fmt.Printf("\n\nPipeline #%v\n", i)
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
			//  fmt.Printf("DataTo: %v\n", err)
			//  return
			// }
			// fmt.Printf("DataTo: %+v\n", c)
		}
	}
}

func books_main(client *firestore.Client) {
	ctx := context.Background()

	// Add Books
	// addBooks(ctx, client)

	readBooksDataUsingPipelines(ctx, client)

	// Query the data back using a collection group query.
	// queryOrdersWithCollectionGroup(ctx, client, "someUserID")
}
