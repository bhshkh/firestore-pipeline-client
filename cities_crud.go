package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
)

// City represents a document in a 'Cities' subcollection.

type City struct {
	Population int64 `firestore:"population"`
}

// addCityDataWithCountryCollection creates documents with a structure like Countries/{CountryName}/Cities/{CityName}.
func addCityDataWithCountryCollection(ctx context.Context, client *firestore.Client) {
	fmt.Println("--- Writing city data with Countries/{CountryName}/Cities/{CityName} structure ---")

	// Define the data structure: map[CountryName]map[CityName]CityData
	countryCitiesData := map[string]map[string]City{
		"France": {
			"Paris":     {Population: 100},
			"Lyon":      {Population: 50},
			"Marseille": {Population: 80},
		},
		"Canada": {
			"Montreal":  {Population: 90},
			"Toronto":   {Population: 120},
			"Vancouver": {Population: 70},
		},
		"Germany": { // Adding another country for more data
			"Berlin":  {Population: 150},
			"Hamburg": {Population: 110},
		},
	}

	for countryName, cities := range countryCitiesData {
		for cityName, cityData := range cities {
			// Path: Countries/{CountryName}/Cities/{CityName}
			_, err := client.Collection("Countries").Doc(countryName).Collection("Cities").Doc(cityName).Set(ctx, cityData)
			if err != nil {
				fmt.Printf("Failed to add city %s in %s: %v", cityName, countryName, err)
				return
			}
			fmt.Printf("Added city %s in %s (Countries/%s/Cities/%s) with population %d\n", cityName, countryName, countryName, cityName, cityData.Population)
		}
	}
}

func cities_main() {
	ctx := context.Background()

	// The client now uses the global constants defined above.
	client := createClient(ctx)
	defer client.Close()

	addCityDataWithCountryCollection(ctx, client)
}
