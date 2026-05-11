# GCP Docs Mirror Tools (Go)

A high-performance, robust tool to recursively discover and mirror Google Cloud documentation in Markdown format using the Developer Knowledge API.

## Features
- **Intelligent Discovery**: Uses Goldmark (Markdown AST) and HTML parsing to accurately identify links in sidebars and navigation bars.
- **Island Hopping**: Automatically navigates between different documentation "islands" (Overview, Guides, Reference, Samples) by scanning top-level navigation.
- **Adaptive Batching**: Uses a recursive binary-search algorithm to isolate missing pages or redirects within atomic batch requests.
- **Normalization**: Automatically normalizes URLs (strips trailing slashes, fragments, and `.md` extensions) to ensure consistency and prevent redundant downloads.
- **Rate Limiting**: Implements a token-bucket budget management system with adjustable quota wait times to prevent 429 errors.
- **YAML Frontmatter**: Prepends document metadata from the Developer Knowledge API to mirrored Markdown files.
- **TOML Configuration**: Supports external configuration files for easy management of seeds, prefixes, and performance settings.

## Installation
```bash
go install github.com/apstndb/gcp-docs-mirror-tools@latest
```

## Authentication
Use either:

- `DEVELOPERKNOWLEDGE_API_KEY` or `GOOGLE_API_KEY`, or
- Application Default Credentials via `gcloud auth application-default login`

When using local user ADC, set a quota project as well:

```bash
gcloud auth application-default set-quota-project <project-id>
```

## Usage
```bash
# API key authentication
export DEVELOPERKNOWLEDGE_API_KEY=your_api_key

# Or use ADC instead
# gcloud auth application-default login
# gcloud auth application-default set-quota-project <project-id>

# Mirror Spanner documentation using a config file
gcp-docs-mirror -config settings.toml

# Or use command line flags
gcp-docs-mirror -r -qpm 50 -prefix "/spanner/,/sdk/gcloud/" https://cloud.google.com/spanner/docs
```

### Options
| Flag | Description | Default |
|------|-------------|---------|
| `-config` | Path to TOML configuration file | `""` |
| `-prefix` | Comma-separated path prefixes to mirror | `/spanner/docs/` |
| `-sitemap`| Sitemap URL(s) to discover links | `nil` |
| `-spanner-db`| Spanner database (projects/P/instances/I/databases/D) | `""` |
| `-qpm` | Quota per minute (requests per minute) | `50.0` |
| `-v` | Enable verbose logging | `false` |
| `-r` | Enable recursive discovery from Markdown content | `false` |
| `-f` | Refresh existing documents | `false` |
| `-include-update-time` | Include `update_time` in YAML frontmatter | `false` |
| `-qw` | Wait duration when quota is exceeded | `1m10s` |
| `-docs` | Output directory for documents | `docs` |
| `-logs` | Directory for log files | `logs` |
| `-metadata` | Path to metadata summary file | `metadata.yaml` |

### Configuration File (`settings.toml`)
```toml
seeds = [
    "https://docs.cloud.google.com/spanner/docs",
    "https://docs.cloud.google.com/sdk/gcloud/reference/spanner"
]
prefixes = ["/spanner/", "/sdk/gcloud/reference/spanner/"]
recursive = true
qpm = 50.0
qw = "70s"
# include_update_time = true
```

## Output
File-based mirrors are written as Markdown with YAML frontmatter. By default the frontmatter includes:

- `name`
- `uri`
- `title`
- `description`
- `data_source`

Set `-include-update-time` or `include_update_time = true` if you also want `update_time`.

## License
MIT
