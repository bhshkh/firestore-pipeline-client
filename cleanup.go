package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	apiv1 "cloud.google.com/go/firestore/apiv1/admin"
	"cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"google.golang.org/api/iterator"
)

func deleteCollection(projectID, databaseID string, batchSize int) error {
	deleteDocs := true

	// Instantiate a client
	ctx := context.Background()
	client, err := firestore.NewClientWithDatabase(ctx, projectID, databaseID)
	if err != nil {
		return err
	}
	adminC, err := apiv1.NewFirestoreAdminClient(ctx)
	if err != nil {
		fmt.Printf("NewFirestoreAdminClient: %v", err)
	}

	iter := client.Collections(ctx)
	for {
		col, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Printf("\n\nFound collection with id: %s\n", col.ID)
		if col.ID == "cities" {
			fmt.Printf("\n\nSkipping collection with id: %s\n", col.ID)
			continue
		}
		parent := fmt.Sprintf("projects/%s/databases/%s/collectionGroups/%s", projectID, databaseID, col.ID)

		indexItr := adminC.ListIndexes(ctx, &adminpb.ListIndexesRequest{
			Parent: parent,
		})
		for {
			index, err := indexItr.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}
			fmt.Printf("\n\tFound index with id: %s. Index: %+v", index.Name, index)
			err = adminC.DeleteIndex(ctx, &adminpb.DeleteIndexRequest{
				Name: index.Name,
			})
			if err != nil {
				fmt.Printf("\n\t\tFailed to delete index: %+v", err)
			} else {
				fmt.Printf("\n\t\tDeleted index")
			}
		}

		if deleteDocs {
			err := recursiveDeleteDocs(ctx, client, col, batchSize)
			if err != nil {
				fmt.Printf("Failed to delete collection %s: %v", col.ID, err)
				return err
			}
			fmt.Printf("Deleted collection \"%s\"", col.ID)
		}
	}
	return nil
}

func recursiveDeleteDocs(ctx context.Context, client *firestore.Client, col *firestore.CollectionRef, batchSize int) error {
	bulkwriter := client.BulkWriter(ctx)
	addedToBulkWriter := make(map[string]bool)
	for {
		// Get a batch of documents
		// iter := col.Limit(batchSize).Documents(ctx)
		iter2, err := col.DocumentRefs(ctx).GetAll()
		if err != nil {
			fmt.Printf("Error getting all document references %v", err)
		}

		numDeleted := 0

		// Iterate through the documents, adding
		// a delete operation for each one to the BulkWriter.
		for _, docRef := range iter2 {

			// fmt.Printf("Deleting %v\n", docRef.ID)
			// for k, v := range doc.Data() {
			// 	fmt.Printf("k: %s, v: %v\n", k, v)
			// }
			if _, ok := addedToBulkWriter[docRef.ID]; !ok {
				addedToBulkWriter[docRef.ID] = true

				subCollIter := docRef.Collections(ctx)
				for {
					subColl, err := subCollIter.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						fmt.Printf("Error getting next sub coll: %+v", err)
						return err
					}
					err = recursiveDeleteDocs(ctx, client, subColl, batchSize)
					if err != nil {
						fmt.Printf("Error deleting subcollection %v: %+v\n", subColl.ID, err)
						return err
					}
				}

				_, err = bulkwriter.Delete(docRef)
				if err != nil {
					fmt.Printf("Error deleting %v: %+v\n", docRef.ID, err)
				}
			} else {
				break
			}
			numDeleted++
		}

		// If there are no documents to delete,
		// the process is over.
		if numDeleted == 0 {
			bulkwriter.End()
			break
		}

		bulkwriter.Flush()
	}
	return nil
}
