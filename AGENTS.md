# Agent Guide: GCP Docs Mirror Tools

This document provides architectural context and technical guidelines for AI agents working on this repository.

## Project Purpose
A high-performance tool to mirror Google developer documentation in Markdown format using the **Developer Knowledge API**. It supports the full Developer Knowledge corpus (15 hosts including `docs.cloud.google.com`, `developers.google.com`, `firebase.google.com`, etc.), recursive discovery, sitemap parsing, and multi-mode storage (Local Disk or Cloud Spanner).

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
- **`Config`**: TOML/Flag-based configuration. Includes `default_host` and `extra_hosts` for the multi-host corpus.
- **`Document`**: Name (API format `documents/HOST/PATH`) and raw Markdown content.
- **URL Normalization**: URLs are normalized to `https://HOST/PATH` against the known Developer Knowledge corpus hosts (`defaultKnownHosts()` in `main.go`). `cloud.google.com` is aliased to `docs.cloud.google.com`. Trailing slashes, query strings, fragments, and `.md` extensions are stripped.

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
Always use `parseCanonical`, `resolveAndNormalize`, `urlPath`, `urlHost`, and `normalizeForAPI` for URL handling. The API expects `documents/HOST/PATH` where HOST is one of the canonical hosts in `defaultKnownHosts()`. Use `apiNameToURL` to reverse-map API document names to canonical URLs.

Prefix matching:
- `/path/` (path-only) matches under any known host. Pair with `default_host` for single-host mirrors.
- `host/path/` (host-scoped) matches only that specific host.

### Authentication
Prefer `DEVELOPERKNOWLEDGE_API_KEY` or `GOOGLE_API_KEY`. If neither is set, use ADC. Local `authorized_user` ADC requires a quota project via `GOOGLE_CLOUD_QUOTA_PROJECT` or `gcloud auth application-default set-quota-project`.
- All content within the repository, including code comments and documentation, MUST be in English.
