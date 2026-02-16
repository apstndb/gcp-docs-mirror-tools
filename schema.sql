CREATE TABLE Documents (
  DocumentName STRING(MAX) NOT NULL,
  LatestStartTime TIMESTAMP OPTIONS (
    allow_commit_timestamp = true
  ),
  LatestContentHash STRING(64),
  LastCheckedTime TIMESTAMP OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY(DocumentName);

CREATE TABLE DocumentHistory (
  DocumentName STRING(MAX) NOT NULL,
  StartTime TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
  EndTime TIMESTAMP OPTIONS (
    allow_commit_timestamp = true
  ),
  Content BYTES(MAX),
  ContentHash STRING(64) NOT NULL,
  ContentSize INT64,
  CompressedSize INT64,
  CharacterCount INT64,
  -- ZSTD_DECOMPRESS_TO_STRING fails on empty Zstd frames (<= 9 bytes).
  -- This IF check prevents "Invalid ZSTD input" errors when Content is empty or too small.
  ContentString STRING(MAX) AS (IF(Content IS NOT NULL AND LENGTH(Content) > 9, ZSTD_DECOMPRESS_TO_STRING(Content), '')),
  ContentTokens TOKENLIST AS (TOKENIZE_FULLTEXT(SUBSTR(ContentString, 1, 100000))) HIDDEN,
) PRIMARY KEY(DocumentName, StartTime DESC),
  INTERLEAVE IN PARENT Documents ON DELETE CASCADE;

CREATE SEARCH INDEX DocumentHistoryIndex ON DocumentHistory(ContentTokens) STORING (ContentString);
