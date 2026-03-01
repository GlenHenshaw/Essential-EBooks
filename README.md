# Essential-EBooks

A reproducible, quality-first public-domain library builder.

This script builds a curated (~700 book by default), English-language EPUB library optimized for:
	•	📖 iPad / general reading
	•	🖥 Raspberry Pi / Kiwix / offline archive use
	•	🧠 High-signal intellectual core (literature, religion, philosophy, history, science)

It prefers Standard Ebooks editions when available and falls back to Project Gutenberg via the Gutendex API.

⸻

Design Goals
	•	Fewer, higher-quality works
	•	Deterministic selection from public catalogs
	•	Prefer professionally typeset Standard Ebooks
	•	Clean directory structure
	•	Deduplicate across sources (Gutenberg vs SE)
	•	Re-runnable yearly as new works enter public domain

⸻

Features

1. Curated Selection (~700 Books Default)

Books are selected from:
	•	Gutenberg curated shelves (e.g., Best Books Ever)
	•	Domain-specific shelves (philosophy, science, religion, etc.)
	•	Subject keyword searches
	•	International literature categories

Selection is quota-based across domains:

```
literature_core
literature_international
history_bio
religion
philosophy
science
mathematics
engineering
economics_social
language_rhetoric_misc
```

Pinned canonical works (e.g., Bible, Augustine, Aquinas, Bhagavad Gita, etc.) are protected from trimming.

⸻

2. Standard Ebooks Preference

When available:
	•	Standard Ebooks EPUB is preferred
	•	Bulk ZIP imports are supported
	•	SE editions can replace Gutenberg copies in shelf folders
	•	Provenance is recorded via:

```
FROM_STANDARD_EBOOKS_BULK.txt
```

If SE rate-limits downloads, the script automatically falls back to Gutenberg and records:

```
FELL_BACK_TO_GUTENBERG.txt
```

3. Bulk Standard Ebooks Import

You can import all Standard Ebooks from:
	•	A Patron bulk ZIP
	•	A directory of .epub files

Imports are placed under:

```
build/library_epub/standardebooks_bulk/
```

Folder format:

```
lastname-firstname Title (SEBULK-<stable_id>)
```

⸻

4. Intelligent Deduplication

Two-pass dedupe:

Pass 1: Exact duplicates
	•	SHA-256 hash match
	•	Deletes entire duplicate directory

Pass 2: Metadata duplicates
	•	Normalized key: (title | author_lastname)
	•	Strips subtitles and leading articles
	•	If SE version exists:
	•	Keep shelf location
	•	Install SE EPUB into shelf folder
	•	Remove duplicate directories

Deletion leaves:

```
DUPLICATE_OF.txt
```

⸻

5. Clean Directory Structure

Books are stored as:

```
build/library_epub/<domain>/
    lastname-firstname Title (<gut_id>)/
        lastname-firstname_title.epub
```

Examples:

```
literature_core/
    austen-jane Pride and Prejudice (1342)/
        austen-jane_pride-and-prejudice.epub
```

⸻

Installation

Requires Python 3.10+

Install dependencies:

```
pip install requests lxml rapidfuzz tqdm
```

⸻

Basic Usage

Build + Download Default Library (~700 books)

```
python3 downloader.py
```

⸻

Build Manifest Only (No Downloads)

```
python3 downloader.py --no-download
```

Produces:

```
build/candidate_pool.csv
build/final_selection.csv
```

⸻

Download Only (Using Existing Manifest)

```
python3 downloader.py --download-only
```

⸻

Standard Ebooks Bulk Import

Import All SE Bulk EPUBs

```
python3 downloader.py \
  --se-bulk-zip ./standardebooks_bulk.zip \
  --import-se-bulk-all
```

Import Only (No Manifest Downloads)

```
python3 downloader.py \
  --se-bulk-dir ./SE_epubs \
  --import-se-bulk-all \
  --import-only
```

⸻

Deduplication

Run dedupe after import/download:

```
python3 downloader.py --dedupe
```

Or combine:

```
python3 downloader.py \
  --se-bulk-dir ./SE_epubs \
  --import-se-bulk-all \
  --dedupe
```

Output example:

```
Dedupe pass2 metadata keys: ok=819 missing=0 groups=776
Deduped: scanned=941 unique=776 removed=165
```

⸻

Customizing the Library

You can modify:
	•	QUOTAS → domain balance
	•	PINS → canonical anchors
	•	SUBJECT_KEYWORDS → thematic supplementation
	•	DEFAULT_TARGET → total library size

The script is deterministic given the same upstream catalogs and configuration.

⸻

Output Structure

```
build/
    candidate_pool.csv
    final_selection.csv
    library_epub/
        <domain>/
            <author-title (id)>/
                *.epub
                FROM_STANDARD_EBOOKS_BULK.txt (optional)
                DUPLICATE_OF.txt (if deduped)
```

⸻

Philosophy

This tool is designed for:
	•	Long-term offline archival
	•	High-value intellectual core libraries
	•	Post-collapse resilience scenarios
	•	Serious reading, not mass scraping

It intentionally favors:
	•	Canonical works
	•	Durable intellectual leverage
	•	International breadth (within English translation)
	•	Standard Ebooks typography when available

⸻

Limitations
	•	English only
	•	Gutenberg metadata quality varies
	•	Standard Ebooks API endpoints may rate-limit
	•	Metadata dedupe is heuristic (though conservative)

⸻

License

Public domain sources only. This script contains no copyrighted content.
