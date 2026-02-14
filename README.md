# GCP Docs Mirror Tools (Go)

A high-performance, robust tool to recursively discover and mirror Google Cloud documentation in Markdown format using the Developer Knowledge API.

## Features
- **Intelligent Discovery**: Uses Goldmark (Markdown AST) and HTML parsing to accurately identify links in sidebars and navigation bars.
- **Island Hopping**: Automatically navigates between different documentation "islands" (Overview, Guides, Reference, Samples) by scanning top-level navigation.
- **Adaptive Batching**: Uses a recursive binary-search algorithm to pinpoint 404s/redirects within large batch requests, maximizing API efficiency.
- **Resilient**: Automatically follows redirect chains and caches results to ensure a stable local mirror.

## Usage
```bash
export DEVELOPERKNOWLEDGE_API_KEY=your_api_key
go build -o gcp-docs-mirror .

# Mirror Spanner docs from a few entry points
./gcp-docs-mirror -r \
  https://cloud.google.com/spanner/docs \
  https://cloud.google.com/sdk/gcloud/reference/spanner \
  /spanner/ /sdk/gcloud/reference/spanner/
```

## License
MIT
