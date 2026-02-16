package main

import (
	"encoding/xml"
	"io"
	"sync"
	"sync/atomic"
)

type locEntry struct {
	Loc string `xml:"loc"`
}

func (a *MirrorApp) DiscoverFromSitemaps(sitemapURLs []string) {
	a.log("--- Step 0: Sitemap Discovery (Pipelined) ---")
	
	var wg sync.WaitGroup
	// Limit concurrent sitemap fetches to avoid connection limits
	sem := make(chan struct{}, 10)

	var crawl func(string)
	crawl = func(u string) {
		defer wg.Done()
		
		sem <- struct{}{}
		defer func() { <-sem }()

		resp, err := a.httpClient.Get(u)
		if err != nil {
			a.log("    Warning: failed to fetch %s: %v", u, err)
			atomic.AddInt32(&a.sitemapDone, 1)
			return
		}
		defer resp.Body.Close()

		decoder := xml.NewDecoder(resp.Body)
		var batch []string
		count := 0
		for {
			t, err := decoder.Token()
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}

			switch se := t.(type) {
			case xml.StartElement:
				if se.Name.Local == "sitemap" {
					var entry locEntry
					if err := decoder.DecodeElement(&entry, &se); err == nil {
						atomic.AddInt32(&a.sitemapTotal, 1)
						wg.Add(1)
						go crawl(entry.Loc)
					}
				} else if se.Name.Local == "url" {
					var entry locEntry
					if err := decoder.DecodeElement(&entry, &se); err == nil {
						// Every <url> tag found is a "raw" scanned URL
						atomic.AddInt32(&a.scannedRawCount, 1)
						
						normalized := a.resolveAndNormalize(entry.Loc, "/")
						if normalized != "" {
							batch = append(batch, normalized)
							if len(batch) >= 100 {
								a.enqueueBatch(batch)
								count += len(batch)
								batch = nil
							}
						}
					}
				}
			}
		}
		if len(batch) > 0 {
			a.enqueueBatch(batch)
			count += len(batch)
		}
		
		atomic.AddInt32(&a.sitemapDone, 1)
		if a.cfg.Verbose {
			a.log("  Finished Sitemap: %s (%d URLs)", u, count)
		}
	}

	for _, u := range sitemapURLs {
		atomic.AddInt32(&a.sitemapTotal, 1)
		wg.Add(1)
		go crawl(u)
	}

	wg.Wait()
	a.log("Sitemap discovery complete.")
}
