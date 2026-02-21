# Firestore Docs Mirror Tools (Go)

A high-performance, robust tool to recursively discover and mirror [Firestore](https://cloud.google.com/firestore/docs) documentation in Markdown format using the Developer Knowledge API.

## Features
- **Intelligent Discovery**: Uses Goldmark (Markdown AST) and HTML parsing to accurately identify links in sidebars and navigation bars.
- **Island Hopping**: Automatically navigates between different documentation "islands" (Overview, Guides, Reference, Samples) by scanning top-level navigation.
- **Adaptive Batching**: Uses a recursive binary-search algorithm to isolate missing pages or redirects within atomic batch requests.
- **Normalization**: Automatically normalizes URLs (strips trailing slashes, fragments, and `.md` extensions) to ensure consistency and prevent redundant downloads.
- **Rate Limiting**: Implements a token-bucket budget management system with adjustable quota wait times to prevent 429 errors.
- **TOML Configuration**: Supports external configuration files for easy management of seeds, prefixes, and performance settings.

## Installation
```bash
go install github.com/apstndb/gcp-docs-mirror-tools@latest
```

## Usage
```bash
export DEVELOPERKNOWLEDGE_API_KEY=your_api_key

# Mirror Firestore documentation using a config file
gcp-docs-mirror -config settings.toml

# Or use command line flags
gcp-docs-mirror -r -qpm 50 -prefix "/firestore/" https://cloud.google.com/firestore/docs
```

### Options
| Flag | Description | Default |
|------|-------------|---------|
| `-config` | Path to TOML configuration file | `""` |
| `-prefix` | Comma-separated path prefixes to mirror | `/firestore/` |
| `-sitemap`| Sitemap URL(s) to discover links | `nil` |
| `-spanner-db`| Spanner database (projects/P/instances/I/databases/D) | `""` |
| `-qpm` | Quota per minute (requests per minute) | `50.0` |
| `-v` | Enable verbose logging | `false` |
| `-r` | Enable recursive discovery from Markdown content | `false` |
| `-f` | Refresh existing documents | `false` |
| `-qw` | Wait duration when quota is exceeded | `1m10s` |
| `-docs` | Output directory for documents | `docs` |
| `-logs` | Directory for log files | `logs` |
| `-metadata` | Path to metadata summary file | `metadata.yaml` |

### Configuration File (`settings.toml`)
```toml
seeds = [
    "https://docs.cloud.google.com/firestore/docs",
    "https://docs.cloud.google.com/firestore/docs/samples/firestore-data-set-id-random-collection-async"
]
prefixes = ["/firestore/"]
recursive = true
qpm = 50.0
qw = "70s"
```

## License
MIT
