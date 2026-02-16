package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/api/iterator"
)

type SpannerStorage struct {
	client  *spanner.Client
	encoder *zstd.Encoder
}

func NewSpannerStorage(ctx context.Context, db string) (*SpannerStorage, error) {
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		return nil, err
	}
	// Match Spanner's Zstd format: Single segment, no CRC.
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderCRC(false), zstd.WithSingleSegment(true))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize zstd: %v", err)
	}
	return &SpannerStorage{client: client, encoder: encoder}, nil
}

func (s *SpannerStorage) Save(docs ...Document) error {
	if len(docs) == 0 {
		return nil
	}

	ctx := context.Background()
	
	// 1. Read existing metadata for the batch using a single snapshot
	existingMetadata := make(map[string]struct {
		StartTime spanner.NullTime
		Hash      spanner.NullString
	})

	var keys []spanner.Key
	for _, doc := range docs {
		keys = append(keys, spanner.Key{doc.Name})
	}

	iter := s.client.Single().Read(ctx, "Documents", spanner.KeySetFromKeys(keys...),
		[]string{"DocumentName", "LatestStartTime", "LatestContentHash"})
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		var name string
		var meta struct {
			StartTime spanner.NullTime
			Hash      spanner.NullString
		}
		if err := row.Columns(&name, &meta.StartTime, &meta.Hash); err != nil {
			return err
		}
		existingMetadata[name] = meta
	}

	// 2. Prepare mutation groups and track the corresponding documents
	var groups []*spanner.MutationGroup
	var mutatingDocs []Document
	for _, doc := range docs {
		content := strings.TrimRight(doc.Content, " \t\r\n") + "\n"
		hashBytes := sha256.Sum256([]byte(content))
		contentHash := hex.EncodeToString(hashBytes[:])

		meta, exists := existingMetadata[doc.Name]

		var mutations []*spanner.Mutation
		if exists && meta.Hash.Valid && meta.Hash.StringVal == contentHash {
			// No change: Only update LastCheckedTime
			mutations = append(mutations, spanner.UpdateMap("Documents", map[string]interface{}{
				"DocumentName":    doc.Name,
				"LastCheckedTime": spanner.CommitTimestamp,
			}))
		} else {
			// New or changed: Update history and parent
			if exists && meta.StartTime.Valid {
				mutations = append(mutations, spanner.UpdateMap("DocumentHistory", map[string]interface{}{
					"DocumentName": doc.Name,
					"StartTime":    meta.StartTime.Time,
					"EndTime":      spanner.CommitTimestamp,
				}))
			}
			
			// Statistics
			contentSize := int64(len(content))
			charCount := int64(utf8.RuneCountInString(content))
			
			// Compression
			compressed := s.encoder.EncodeAll([]byte(content), nil)
			compressedSize := int64(len(compressed))

			mutations = append(mutations, spanner.InsertOrUpdateMap("Documents", map[string]interface{}{
				"DocumentName":      doc.Name,
				"LatestStartTime":   spanner.CommitTimestamp,
				"LatestContentHash": contentHash,
				"LastCheckedTime":   spanner.CommitTimestamp,
			}))
			
			mutations = append(mutations, spanner.InsertMap("DocumentHistory", map[string]interface{}{
				"DocumentName":   doc.Name,
				"StartTime":      spanner.CommitTimestamp,
				"EndTime":        nil,
				"Content":        compressed,
				"ContentHash":    contentHash,
				"ContentSize":    contentSize,
				"CompressedSize": compressedSize,
				"CharacterCount": charCount,
			}))
		}
		groups = append(groups, &spanner.MutationGroup{Mutations: mutations})
		mutatingDocs = append(mutatingDocs, doc)
	}

	if len(groups) == 0 {
		return nil
	}

	// 3. Execute BatchWrite
	var firstErr error
	bwIter := s.client.BatchWrite(ctx, groups)
	err := bwIter.Do(func(r *sppb.BatchWriteResponse) error {
		if r.Status.GetCode() != 0 { // 0 is OK
			fmt.Printf("BatchWrite Response Error: %s (Groups: %v)\n", r.Status.GetMessage(), r.Indexes)
			for _, idx := range r.Indexes {
				doc := mutatingDocs[idx]
				err := fmt.Errorf("MutationGroup %d failed for %s: %s", idx, doc.Name, r.Status.GetMessage())
				fmt.Printf("BatchWrite Error Detail: %v\n", err)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return firstErr
}

func (s *SpannerStorage) LoadProcessedURLs() (map[string]bool, error) {
	ctx := context.Background()
	stmt := spanner.Statement{SQL: `SELECT DocumentName FROM Documents`}
	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	processed := make(map[string]bool)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var name string
		if err := row.Column(0, &name); err != nil {
			return nil, err
		}
		url := "https://" + strings.TrimPrefix(name, "documents/")
		processed[url] = true
	}
	return processed, nil
}
