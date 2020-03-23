package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/tidwall/gjson"
)

func myread(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}

func main() {
	esHost := flag.String("eshost", "http://localhost:9200", "ES Host")
	esUser := flag.String("esuser", "", "ES user")
	esPass := flag.String("espass", "", "ES pass")
	esIndex := flag.String("esindex", "my_index", "Index to dump")
	outFile := flag.String("outfile", "output.json", "Output file")

	flag.Parse()

	log.Printf("Starting %s\n", *esIndex)

	var batchNum int
	var scrollID string
	ctx := context.Background()

	cfg := elasticsearch.Config{
		Addresses: []string{
			*esHost,
		},
		Username: *esUser,
		Password: *esPass,
	}

	f, err := os.Create(*outFile)
	if err != nil {
		fmt.Println(err)
		f.Close()
		return
	}

	defer f.Close()

	client, _ := elasticsearch.NewClient(cfg)
	var buf bytes.Buffer
	// get 1000 per round
	var query = `{"query": {"match_all" : {}},"size": 1000}`
	var b strings.Builder
	b.WriteString(query)
	read := strings.NewReader(b.String())

	if err := json.NewEncoder(&buf).Encode(read); err != nil {
		log.Fatalf("Error encoding query: %s", err)

	// Query is a valid JSON object
	} else {
		// Pass the JSON query to the Golang client's Search() method
		res, err := client.Search(
			client.Search.WithContext(ctx),
			client.Search.WithIndex(*esIndex),
			client.Search.WithBody(read),
			client.Search.WithTrackTotalHits(true),
			client.Search.WithScroll(time.Minute),
		)

		if err != nil {
			log.Fatalf("Error performing search: %s", err)
		}

		json := myread(res.Body)
		res.Body.Close()

		scrollID = gjson.Get(json, "_scroll_id").String()

		hits := gjson.Get(json, "hits.hits")
		hits.ForEach(func(key, value gjson.Result) bool {
			//fmt.Printf("%s\n", value)
			fmt.Fprintln(f, value)
			return true
		})

		for {
			batchNum++

			// Perform the scroll request and pass the scrollID and scroll duration
			//
			res, err := client.Scroll(client.Scroll.WithScrollID(scrollID), client.Scroll.WithScroll(time.Minute))
			if err != nil {
				log.Fatalf("Error: %s", err)
			}
			if res.IsError() {
				log.Fatalf("Error response: %s", res)
			}

			json := myread(res.Body)
			res.Body.Close()

			// Extract the scrollID from response
			scrollID = gjson.Get(json, "_scroll_id").String()

			// Extract the search results
			hits := gjson.Get(json, "hits.hits")

			// Break out of the loop when there are no results
			if len(hits.Array()) < 1 {
				log.Println("Finished scrolling")
				break
			} else {
				hits.ForEach(func(key, value gjson.Result) bool {
					fmt.Fprintln(f, value)
					return true
				})
			}
		}
	}
}
