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
gcp-docs-mirror -r -qpm 50 -prefix "docs.cloud.google.com/spanner/" https://docs.cloud.google.com/spanner/docs
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
| `-f` | Refresh existing documents (loads prior progress from logs) | `false` |
| `-resume` | Resume from existing progress in logs | `false` |
| `-discovery` | Discover more links from HTML navigation | `true` |
| `-stall-timeout` | Max duration without activity before aborting | `0` (disabled) |
| `-include-update-time` | Include `update_time` in YAML frontmatter | `false` |
| `-qw` | Wait duration when quota is exceeded | `1m10s` |
| `-docs` | Output directory for documents | `docs` |
| `-logs` | Directory for log files | `logs` |
| `-metadata` | Path to metadata summary file | `metadata.yaml` |

### Configuration File (`settings.toml`)
```toml
seeds = [
    "https://docs.cloud.google.com/spanner/docs",
    "https://docs.cloud.google.com/sdk/gcloud/reference/spanner",
    "https://cloud.google.com/spanner",
]
prefixes = [
    "docs.cloud.google.com/spanner/",
    "docs.cloud.google.com/sdk/gcloud/reference/spanner/",
    "cloud.google.com/spanner/docs/",
]
recursive = true
qpm = 50.0
qw = "70s"
# include_update_time = true

# default_host is used for relative links and path-only prefixes.
# Defaults to "docs.cloud.google.com".
# default_host = "developers.google.com"

# extra_hosts adds host(s) on top of the built-in Developer Knowledge corpus.
# Leave unset unless mirroring an undocumented host.
# extra_hosts = ["example.googleapis.com"]
```

### Supported hosts

By default the tool accepts URLs from every domain in the [Developer Knowledge API corpus](https://developers.google.com/knowledge/reference/corpus-reference):

- `adk.dev`, `ai.google.dev`, `antigravity.google`
- `cloud.google.com`
- `dart.dev`, `developer.android.com`, `developer.chrome.com`
- `developers.google.com`, `developers.home.google.com`
- `docs.apigee.com`, `docs.cloud.google.com`, `docs.flutter.dev`
- `firebase.google.com`, `fuchsia.dev`
- `geminicli.com`, `go.dev`, `mapsplatform.google.com`, `web.dev`, `www.tensorflow.org`

`cloud.google.com` and `docs.cloud.google.com` are distinct API data sources. Product and pricing content can exist only under `cloud.google.com`, while technical documentation generally lives under `docs.cloud.google.com`. Preserve both hosts when both are in scope.

Some legacy documentation URLs under `cloud.google.com` redirect to `docs.cloud.google.com`. The tool first tries the original API document name. When a document-level API error isolates a missing URL, it probes the public URL, records an HTTP redirect, and enqueues the destination. This preserves real `cloud.google.com` documents without losing old links. Because redirect records are not yet loaded on later runs, prefer canonical `docs.cloud.google.com` seeds and use explicit `cloud.google.com` seeds only for product pages confirmed to exist in the API.

### Prefix syntax

`prefixes` accepts either:

- a **path-only prefix** like `/spanner/docs/`, which matches any known host (use this with `default_host` when you only mirror one host); or
- a **host-scoped prefix** like `developers.google.com/gemini-code-assist/`, which matches only that host.

Use host-scoped prefixes when mirroring both Google Cloud data sources. Path-only prefixes intentionally match the same path under every known host.

Explicit `seeds` are always fetched, even when they are outside `prefixes`; prefixes constrain URLs found through recursive, HTML, and sitemap discovery. This lets a mirror save a specific `cloud.google.com` product page without recursively crawling product-site links that are not available through the Developer Knowledge API.

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
