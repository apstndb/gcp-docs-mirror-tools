# Agent Guide: GCP Docs Mirror Tools

This document provides architectural context and technical guidelines for AI agents working on this repository.

## Project Purpose
A high-performance tool to mirror Google Cloud documentation in Markdown format using the **Developer Knowledge API**. It supports recursive discovery, sitemap parsing, and multi-mode storage (Local Disk or Cloud Spanner).

## Core Architecture

### 1. Orchestration (`MirrorApp` in `main.go`)
- **Pipeline**: Pipelined discovery and mirroring.
- **Queueing**: Uses `queueChan` to decouple discovery from API fetching.
- **Concurrency**: 
  - `apiSem` limits concurrent API calls (default: 2) to strictly respect quota.
  - `numWorkers` (default: 30) handles batch processing and storage.
- **Rate Limiting**: Implements a Token Bucket (`takeToken`) based on `QuotaPerMinute`.

### 2. The Discovery Engine
Discovery happens in multiple parallel phases:
- **Sitemaps (`sitemap.go`)**: Pipelined XML parsing to seed the queue.
- **HTML Navigation (`fetchAndExtractLinks`)**: Scans `devsite-tabs-wrapper` (Islands) and `devsite-nav-list` (Sidebars).
- **Recursive (`discoverLinksFromMirror`)**: Scans local Markdown files for links using Goldmark AST.
- **Refresh**: Re-queues already processed URLs.

### 3. Adaptive Batching (`processBatchRecursive`)
The Developer Knowledge API has a `batchGet` limit (20). If a batch request fails, the tool uses a **recursive binary search** (splitting the batch) to isolate the specific problematic URL (e.g., a 404 or a redirect) without failing the entire batch.

### 4. Storage Providers (`Storage` Interface)
- **DiskStorage**: Saves as `.md` files in a nested directory structure.
- **SpannerStorage (`spanner.go`)**: 
  - **Deduplication**: Uses SHA256 hashes to avoid redundant writes.
  - **History**: Tracks document versions in `DocumentHistory` (interleaved).
  - **Compression**: Content is stored compressed using `zstd`.
  - **Generated Columns**: Uses `ZSTD_DECOMPRESS_TO_STRING` in Spanner for easy querying of history content.
  - **Search Index**: Uses `TOKENLIST` and `TOKENIZE_FULLTEXT` on the `ContentString`. It is confirmed that `SEARCH INDEX` can use `STORING` with virtual generated columns (like `ContentString`).

## Key Data Models
- **`Config`**: TOML/Flag-based configuration.
- **`Document`**: Name (API format `documents/docs.cloud.google.com/...`) and raw Markdown content.
- **URL Normalization**: URLs are always normalized to `https://docs.cloud.google.com/...` with trailing slashes and `.md` extensions removed.

## Technical Findings & Constraints

### Spanner Zstd Compatibility
Spanner's `ZSTD_DECOMPRESS_TO_STRING` has specific requirements for the compressed input:
1. **Single Segment**: The Zstd frame must be a single segment. In Go, use `zstd.WithSingleSegment(true)`.
2. **No CRC**: Spanner's output typically lacks CRC. Use `zstd.WithEncoderCRC(false)` for consistency.
3. **Empty Frames**: Spanner fails to decompress empty Zstd frames (9 bytes). The schema uses an `IF(LENGTH(Content) > 9, ...)` guard to handle this.

### BatchWrite & Indexing
When using `BatchWrite`, always ensure the `mutation_groups` indices align perfectly with the input document slice. Filter skipped documents early to maintain consistent indexing in the response stream.

## Development Workflows

### URL Normalization Rules
Always use `toRootRelative` and `resolveAndNormalize` for consistent URL handling. The API expects `documents/docs.cloud.google.com/PATH`.

### API Key
Requires `DEVELOPERKNOWLEDGE_API_KEY` environment variable.
- All content within the repository, including code comments and documentation, MUST be in English.
