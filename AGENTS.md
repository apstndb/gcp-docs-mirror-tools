# Agent Guide: GCP Docs Mirror Tools

This document provides architectural context and technical guidelines for AI agents working on this repository.

## Project Purpose
A high-performance tool to mirror Google developer documentation in Markdown format using the **Developer Knowledge API**. It supports the full Developer Knowledge corpus (including `cloud.google.com`, `docs.cloud.google.com`, `developers.google.com`, and `firebase.google.com`), recursive discovery, sitemap parsing, and multi-mode storage (Local Disk or Cloud Spanner).

## Core Architecture

### 1. Orchestration (`MirrorApp` in `main.go`)
- **Pipeline**: Pipelined discovery and mirroring.
- **Queueing**: Uses `queueChan` to decouple discovery from API fetching.
- **Concurrency**: 
  - `apiSem` limits concurrent API calls (default: 8).
  - `numWorkers` (default: 30) handles batch processing and storage.
- **Rate Limiting**: Implements a Token Bucket (`takeTokens`) based on `QuotaPerMinute`. Burst is capped at `min(8, qpm)` to stay within quota windows.

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
- **`Config`**: TOML/Flag-based configuration. Includes `default_host` and `extra_hosts` for the multi-host corpus.
- **`Document`**: Name (API format `documents/HOST/PATH`) and raw Markdown content.
- **URL Normalization**: URLs are normalized to `https://HOST/PATH` against the known Developer Knowledge corpus hosts (`defaultKnownHosts()` in `main.go`). Trailing slashes, query strings, fragments, and `.md` extensions are stripped.
- **Google Cloud hosts**: Treat `cloud.google.com` product pages and `docs.cloud.google.com` technical documentation as distinct API documents. Legacy documentation URLs are resolved by the leaf-failure HTTP redirect path rather than a blanket host alias.

## Technical Findings & Constraints

### Spanner Zstd Compatibility
Spanner's `ZSTD_DECOMPRESS_TO_STRING` has specific requirements for the compressed input:
1. **Single Segment**: The Zstd frame must be a single segment. In Go, use `zstd.WithSingleSegment(true)`.
2. **No CRC**: Spanner's output typically lacks CRC. Use `zstd.WithEncoderCRC(false)` for consistency.
3. **Empty Frames**: Spanner fails to decompress empty Zstd frames (9 bytes). The schema uses an `IF(LENGTH(Content) > 9, ...)` guard to handle this.

### BatchWrite & Indexing
When using `BatchWrite`, always ensure the `mutation_groups` indices align perfectly with the input document slice. Every document in the batch is appended to `mutatingDocs`/`groups` (unchanged docs receive a `LastCheckedTime`-only mutation) so response indices stay aligned without filtering skipped documents early.

## Development Workflows

### URL Normalization Rules
Always use `parseCanonical`, `resolveAndNormalize`, `urlPath`, `urlHost`, and `normalizeForAPI` for URL handling. The API expects `documents/HOST/PATH` where HOST is one of the canonical hosts in `defaultKnownHosts()`. Use `apiNameToURL` to reverse-map API document names to canonical URLs.

Prefix matching:
- `/path/` (path-only) matches under any known host. Pair with `default_host` for single-host mirrors.
- `host/path/` (host-scoped) matches only that specific host.
- Explicit seeds bypass prefix filtering; prefixes constrain discovered URLs. Use this to include individual product pages without recursively crawling their host subtree.

### Authentication
Prefer `DEVELOPERKNOWLEDGE_API_KEY` or `GOOGLE_API_KEY`. If neither is set, use ADC. Local `authorized_user` ADC requires a quota project via `GOOGLE_CLOUD_QUOTA_PROJECT` or `gcloud auth application-default set-quota-project`.

### Validation
The validation gate is `go test ./...` (plus `go vet ./...` and `go build ./...` for broader changes). The `Makefile` also provides `make test` (verbose), `make lint` (golangci-lint), and `make build`.

- All content within the repository, including code comments and documentation, MUST be in English.
