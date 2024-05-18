package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly"
)

const batchSize = 10

func main() {
	c := colly.NewCollector(
		// Restrict crawling to specific domains
		colly.AllowedDomains("www.boxofficemojo.com"),
		// Allow visiting the same page multiple times
		colly.AllowURLRevisit(),
		// Allow crawling to be done in parallel / async
		colly.Async(true),
	)

	c.Limit(&colly.LimitRule{
		// Filter domains affected by this rule
		DomainGlob: "www.boxofficemojo.com/*",
		// Add an additional random delay
		RandomDelay: 200 * time.Millisecond,
	})

	inputFile, err := os.Open("./to_process_imdb_id.csv")
	if err != nil {
		fmt.Printf("failed to open file: %v\n", err)
		return
	}
	defer inputFile.Close()

	outputFile, err := os.OpenFile("./result.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		fmt.Printf("failed to open file: %v\n", err)
		return
	}
	defer outputFile.Close()

	failFile, err := os.OpenFile("./fail8.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		fmt.Printf("failed to open file: %v\n", err)
		return
	}
	defer failFile.Close()

	result := newResultBatch(batchSize)

	c.OnHTML(".mojo-summary-table.a-section", func(e *colly.HTMLElement) {
		var rec record
		id := e.Request.URL.String()[len("https://www.boxofficemojo.com/title/"):]
		firstSlash := strings.Index(id, "/")
		if firstSlash != -1 {
			id = id[:firstSlash]
		}
		rec.id = id

		defer func() {
			if r := recover(); r != nil {
				result.AddFailed(fail{id, "panic"})
				fmt.Printf("panic: %v, %v\n", id, r)
			}
		}()

		table := e.DOM
		revenues := table.Find(".mojo-performance-summary-table .a-size-small")
		for i := 0; i < len(revenues.Nodes); i++ {
			row := revenues.Eq(i)
			rowTitle := strings.TrimSpace(row.Text())
			if rowTitle == "Worldwide" {
				money := row.Next().Next().Find(".money")
				if len(money.Nodes) == 0 {
					continue
				}
				rev, err := parseMoney(money.Text())
				if err != nil {
					result.AddFailed(fail{id, err.Error()})
					return
				}
				rec.worldwideRevenue = rev
			}
			if strings.HasPrefix(rowTitle, "Domestic") {
				money := row.Next().Next().Find(".money")
				if len(money.Nodes) == 0 {
					continue
				}
				rev, err := parseMoney(money.Text())
				if err != nil {
					result.AddFailed(fail{id, err.Error()})
					return
				}
				rec.domesticRevenue = rev
			}
			if strings.HasPrefix(rowTitle, "International") {
				money := row.Next().Next().Find(".money")
				if len(money.Nodes) == 0 {
					continue
				}
				rev, err := parseMoney(money.Text())
				if err != nil {
					result.AddFailed(fail{id, err.Error()})
					return
				}
				rec.internationalRevenue = rev
			}
		}

		info := table.Find(".mojo-summary-values")
		rows := info.Children()
		for i := 0; i < len(rows.Nodes); i++ {
			row := rows.Eq(i)
			col := row.Children().First()
			switch col.Text() {
			case "Domestic Distributor":
				valCol := col.Next()
				rec.domesticDistributor = valCol.Contents().First().Text()
			case "Domestic Opening":
				do, err := parseMoney(row.Find(".money").Text())
				if err != nil {
					result.AddFailed(fail{id, err.Error()})
					return
				}
				rec.domesticOpening = do
			case "Budget":
				budget, err := parseMoney(row.Find(".money").Text())
				if err != nil {
					result.AddFailed(fail{id, err.Error()})
					return
				}
				rec.budget = budget
			case "MPAA":
				valCol := col.Next()
				rec.mpaa = valCol.Text()
			case "Running Time":
				valCol := col.Next()
				rec.runningTime = valCol.Text()
			case "Genres":
				valCol := col.Next()
				splittedData := strings.Split(valCol.Text(), " \n")
				for i := 0; i < len(splittedData); i++ {
					splittedData[i] = strings.TrimSpace(splittedData[i])
				}
				rec.genres = strings.Join(splittedData, " ")
			}
		}

		result.AddProcessed(rec)
	})

	c.OnError(func(r *colly.Response, err error) {
		id := r.Request.URL.String()[len("https://www.boxofficemojo.com/title/"):]
		id = id[:len(id)-1]
		result.AddFailed(fail{id, err.Error()})
	})

	cnt := 0

	r := csv.NewReader(inputFile)
	w := csv.NewWriter(outputFile)
	fw := csv.NewWriter(failFile)

	// Read CSV header
	var values []string
	_, _ = r.Read()

	// Write CSV header
	// err = w.Write([]string{"imdb_id", "domestic_distributor", "domestic_opening", "budget", "mpaa", "running_time", "genres", "domestic_revenue", "international_revenue", "worldwide_revenue"})
	// if err != nil {
	// 	fmt.Println("writing error: ", err)
	// 	return
	// }
	// w.Flush()

	ids := make([]string, 0, batchSize)
	for values, err = r.Read(); err != io.EOF; values, err = r.Read() {
		cnt++
		id := values[0]
		ids = append(ids, id)

		c.Visit(fmt.Sprintf("https://www.boxofficemojo.com/title/%v", id))

		if cnt%batchSize == 0 {
			c.Wait()

			fmt.Printf("writing batch with ids %v\n", ids)
			ids = ids[:0]

			processed := result.PrepareProcessedCSV()
			err = w.WriteAll(processed)
			if err != nil {
				fmt.Println("failed to write processed batch")
			}

			failed := result.PrepareFailedCSV()
			err = fw.WriteAll(failed)
			if err != nil {
				fmt.Println("failed to write failed batch")
			}

			result.Flush()
			fmt.Println("batch is processed")
		}
	}
	c.Wait()

}

type record struct {
	id                   string
	domesticDistributor  string
	domesticOpening      int64
	budget               int64
	mpaa                 string
	runningTime          string
	genres               string
	domesticRevenue      int64
	internationalRevenue int64
	worldwideRevenue     int64
}

func parseMoney(m string) (int64, error) {
	// Remove $ sign
	m = m[1:]
	m = strings.ReplaceAll(m, ",", "")
	return strconv.ParseInt(m, 10, 64)
}

type resultBatch struct {
	processed []record
	failed    []fail
	m         sync.RWMutex
}

type fail struct {
	id     string
	reason string
}

func newResultBatch(size int) resultBatch {
	return resultBatch{
		processed: make([]record, 0, size),
		failed:    make([]fail, 0, size),
	}
}

func (b *resultBatch) AddProcessed(processed ...record) {
	b.m.Lock()
	defer b.m.Unlock()
	b.processed = append(b.processed, processed...)
}

func (b *resultBatch) AddFailed(failed ...fail) {
	b.m.Lock()
	defer b.m.Unlock()
	b.failed = append(b.failed, failed...)
}

func (b *resultBatch) Flush() {
	b.m.Lock()
	defer b.m.Unlock()

	b.processed = b.processed[:0]
	b.failed = b.failed[:0]
}

func (b *resultBatch) String() string {
	b.m.RLock()
	defer b.m.RUnlock()

	return fmt.Sprintf("processed: %v\nfailed: %v", b.processed, b.failed)
}

func (b *resultBatch) PrepareProcessedCSV() [][]string {
	b.m.RLock()
	defer b.m.RUnlock()

	result := make([][]string, 0, batchSize)
	for _, r := range b.processed {
		result = append(result, []string{
			r.id,
			r.domesticDistributor,
			strconv.FormatInt(r.domesticOpening, 10),
			strconv.FormatInt(r.budget, 10),
			r.mpaa,
			r.runningTime,
			r.genres,
			strconv.FormatInt(r.domesticRevenue, 10),
			strconv.FormatInt(r.internationalRevenue, 10),
			strconv.FormatInt(r.worldwideRevenue, 10),
		})
	}

	return result
}

func (b *resultBatch) PrepareFailedCSV() [][]string {
	b.m.RLock()
	defer b.m.RUnlock()

	result := make([][]string, 0, batchSize)
	for _, r := range b.failed {
		result = append(result, []string{
			r.id,
			r.reason,
		})
	}

	return result
}
