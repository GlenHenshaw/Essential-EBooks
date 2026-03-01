

#!/usr/bin/env python3
"""downloader.py

Reproducible, quality-first public-domain library builder.

Goal
- Build a *high-quality* (~700 book) English-only library for:
  - iPad reading (EPUB master archive)
  - Pi/Kiwix (derive HTML + ZIM later)

Sources
- Standard Ebooks (preferred when available)
- Project Gutenberg (fallback), via Gutendex for clean metadata + URLs

Outputs
- build/candidate_pool.csv    (the big pool we considered)
- build/final_selection.csv   (the final list we downloaded)
- build/library_epub/...      (EPUB downloads)

Notes
- This script intentionally favors *fewer, higher-quality* works.
- It is deterministic given the same upstream catalogs.

Usage
  python3 downloader.py
  python3 downloader.py --target 700 --out build

Dependencies
  python3 -m pip install requests lxml rapidfuzz tqdm

"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import re
import time
import shutil
import zipfile
import hashlib
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests
from lxml import etree, html
from rapidfuzz import fuzz, process
from tqdm import tqdm


# -----------------------------
# Configuration (edit me)
# -----------------------------

LANGUAGE = "en"  # English-only (translations are fine)
DEFAULT_TARGET = 700

# Gutenberg shelves used as *seeds*.
# "Best Books Ever" shelf is 13 (good literature backbone).
# You can add/remove shelf IDs without touching the rest of the code.
GUTENBERG_SHELF_SEEDS: Dict[str, List[int]] = {
    "literature_core": [13, 649],
    "philosophy": [57],
    "mathematics": [102],
    "engineering": [671],
    "science": [667, 668, 669],
    "earth_ag": [670],
    "health_medicine": [681],
}

# Discover additional international literature shelves via Gutenberg shelf search.
INTERNATIONAL_SHELF_SEARCH_QUERY = "language|literature"
INTERNATIONAL_SHELF_MAX = 12

# Fallback international shelves (used if Gutenberg shelf-search is slow/unavailable).
# These are Gutenberg "Books in Category" shelves:
#   649 Classics of Literature, 650 Russian Literature, 651 German Literature, 652 French Literature
INTERNATIONAL_SHELF_FALLBACK: List[int] = [649, 650, 651, 652]

# Domain quotas (sum should be >= target; we trim to target after scoring).
# Tuned for: international breadth + enjoyment + intellectual leverage.
QUOTAS: Dict[str, int] = {
    "literature_core": 320,
    "literature_international": 120,
    "history_bio": 90,
    "religion": 85,
    "philosophy": 75,
    "science": 40,
    "mathematics": 20,
    "engineering": 25,
    "economics_social": 10,
    "language_rhetoric_misc": 10,
}

# Performance tuning
# For each domain, only collect up to (quota * factor) Gutenberg IDs before moving on.
# This keeps huge shelves (e.g., Science/Biology) from generating tens of thousands of IDs we won't use.
MAX_IDS_FACTOR = 12
# Hard cap per domain, even if quota is large or missing.
MAX_IDS_HARD_CAP = 4000

# Gutendex metadata fetching can be parallelized a bit; keep it modest to avoid hammering the API.
GUTENDEX_WORKERS = 10

# Keyword-based (Gutendex subject strings + simple search) supplementation.
SUBJECT_KEYWORDS: Dict[str, List[str]] = {
    "history_bio": [
        "History",
        "Biographies",
        "Biography",
        "Civilization",
        "Ancient",
        "Medieval",
        "Renaissance",
        "Rome",
        "Greece",
        "France",
        "Germany",
        "Russia",
        "China",
        "India",
    ],
    "religion": [
        "Bible",
        "Christianity",
        "Judaism",
        "Islam",
        "Qur'an",
        "Koran",
        "Mythology",
        "Folklore",
        "Hinduism",
        "Buddhism",
        "Confucian",
        "Taoism",
    ],
    "economics_social": [
        "Economics",
        "Economic",
        "Political science",
        "Sociology",
        "Social",
        "Labor",
        "Trade",
    ],
    "language_rhetoric_misc": [
        "Rhetoric",
        "Logic",
        "Grammar",
        "Linguistics",
        "Education",
        "Writing",
        "Oratory",
    ],
}

# Quality filters: drop common low-signal content.
REJECT_SUBJECT_TOKENS = [
    "periodicals",
    "journals",
    "magazines",
    "pamphlets",
    "proceedings",
    "transactions",
    "reports",
    "circulars",
    "bulletins",
]
REJECT_TITLE_PATTERNS = [
    r"\bvol\.?\b",
    r"\bvolume\b",
    r"\bpart\b",
    r"\bno\.?\b",
    r"\bissue\b",
    r"\bjournal\b",
    r"\bmagazine\b",
]

# Pinned canonical anchors: included even if filters would otherwise drop.
# Used to: (a) keep essentials, (b) protect during trimming.
PINS: List[Tuple[str, str]] = [
    # Western-centric comparative religion spine
    ("", "King James Bible"),
    ("", "Bible"),
    ("", "Apocrypha"),
    ("Augustine", "Confessions"),
    ("Augustine", "City of God"),
    ("Calvin", "Institutes of the Christian Religion"),
    ("Thomas Aquinas", "Summa"),
    ("", "Book of Common Prayer"),
    ("Josephus", "Antiquities of the Jews"),
    # Major world texts in English translation
    ("", "Bhagavad Gita"),
    ("", "Upanishads"),
    ("", "Dhammapada"),
    ("", "Analects"),
    ("", "Tao Te Ching"),
    ("", "Zend-Avesta"),
    ("", "Epic of Gilgamesh"),
    ("", "One Thousand and One Nights"),
    ("", "Arabian Nights"),
    ("", "Quran"),
    ("", "Koran"),
]

# NOTE: Standard Ebooks' OPDS endpoints may return 401 to automated clients.
# We therefore prefer the public Atom search feed for per-title lookups.
SE_ATOM_SEARCH = "https://standardebooks.org/feeds/atom/all?query={q}&per-page=50&page=1"

# Gutendex endpoints.
GUTENDEX_BOOK = "https://gutendex.com/books/{id}"
GUTENDEX_LIST = "https://gutendex.com/books"

# Gutenberg shelf pages.
GUTENBERG_SHELF_PAGE = "https://www.gutenberg.org/ebooks/bookshelf/{id}?start_index={start}"
GUTENBERG_SHELF_SEARCH = (
    "https://www.gutenberg.org/ebooks/bookshelves/search/?query={q}&sort_order=quantity"
)


# Polite pacing.
HTTP_TIMEOUT = 45
SLEEP = 0.25

# Standard Ebooks is more likely to rate-limit bulk downloads.
# Use a longer inter-request delay and stronger backoff for 429.
SE_DOWNLOAD_SLEEP = 1.5
SE_DOWNLOAD_MAX_RETRIES = 2

UA = "ebook-downloader/1.0 (personal use; polite requests)"


# -----------------------------
# Data structures
# -----------------------------


@dataclasses.dataclass
class Book:
    gut_id: int
    title: str
    authors: str
    languages: str
    subjects: str
    downloads: int
    gutenberg_epub_url: str

    domain: str = ""
    preferred_source: str = "gutenberg"
    preferred_url: str = ""


# -----------------------------
# Utilities
# -----------------------------


_space_re = re.compile(r"\s+")


def normalize(s: str) -> str:
    s = (s or "").lower()
    s = _space_re.sub(" ", s).strip()
    s = re.sub(r"[^\w\s]", "", s)
    return s


def primary_author(authors: str) -> str:
    """Extract the first author name, preferring 'Last, First' if present."""
    s = (authors or "").strip()
    if not s or s.lower() == "unknown":
        return ""

    if ";" in s:
        s = s.split(";", 1)[0].strip()

    if "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        if len(parts) >= 2:
            return f"{parts[0]}, {parts[1]}"
        return parts[0] if parts else ""

    return s


def author_variants(name: str) -> List[str]:
    """Return plausible variants: 'Last, First' <-> 'First Last'."""
    out: List[str] = []
    n = (name or "").strip()
    if not n:
        return out

    out.append(n)

    if "," in n:
        last, first = [p.strip() for p in n.split(",", 1)]
        if first and last:
            out.append(f"{first} {last}")
    else:
        toks = [t for t in _space_re.split(n) if t]
        if len(toks) >= 2:
            out.append(f"{toks[-1]}, {' '.join(toks[:-1])}")

    # Dedup by normalized form
    dedup: List[str] = []
    seen: Set[str] = set()
    for a in out:
        k = normalize(a)
        if k and k not in seen:
            seen.add(k)
            dedup.append(a)
    return dedup


def safe_component(s: str, limit: int = 100) -> str:
    s = re.sub(r"[^\w\s\-]", "", s or "").strip()
    s = _space_re.sub(" ", s)
    return (s[:limit] or "Unknown").strip()


# --- Helper for domain folder names ---

def domain_folder(b: Book) -> str:
    """Folder name for a book's bucket (domain/quota).

    This corresponds to the manifest "domain" field, which is how we approximate the
    originating Gutenberg shelf/category in this pipeline.
    """
    d = (b.domain or "").strip() or "unknown"
    return safe_component(d, limit=60)


# --- EPUB filename helpers ---

def _slug(s: str, *, limit: int = 140) -> str:
    """Make a stable filesystem-friendly slug (lowercase, hyphens)."""
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = _space_re.sub(" ", s).strip()
    s = s.replace(" ", "-")
    return (s[:limit] or "unknown").strip("-")


def se_style_epub_filename(b: Book) -> str:
    """Return `lastname-firstname_title.epub` based on the first author and title."""
    #a0 = (b.authors.split(",")[0] or b.authors).strip()
    a0 = primary_author(b.authors) or b.authors

    # Prefer Gutendex's common format "Last, First" when available.
    last = ""
    first = ""
    if "," in a0:
        parts = [p.strip() for p in a0.split(",", 1)]
        last = parts[0]
        first = parts[1] if len(parts) > 1 else ""
    else:
        toks = [t for t in _space_re.split(a0) if t]
        if toks:
            last = toks[-1]
            first = "-".join(toks[:-1])

    author_slug = _slug(last) + ("-" + _slug(first) if first else "")
    title_slug = _slug(b.title, limit=160)
    return f"{author_slug}_{title_slug}.epub"


def author_title_dir_prefix(b: Book) -> str:
    """Return a readable directory prefix like 'lastname-firstname Title'."""
    a0 = primary_author(b.authors) or (b.authors or "")

    last = ""
    first = ""
    if "," in a0:
        parts = [p.strip() for p in a0.split(",", 1)]
        last = parts[0]
        first = parts[1] if len(parts) > 1 else ""
    else:
        toks = [t for t in _space_re.split(a0) if t]
        if toks:
            last = toks[-1]
            first = " ".join(toks[:-1])

    last_s = safe_component(last, limit=60).replace(" ", "-")
    first_s = safe_component(first, limit=60).replace(" ", "-") if first else ""
    author_part = last_s + ("-" + first_s if first_s else "")
    title_part = safe_component(b.title, limit=120)

    if (not author_part) or (author_part.lower() == "unknown"):
        return title_part

    return f"{author_part} {title_part}"


def is_pinned(b: Book) -> bool:
    t = normalize(b.title)
    a = normalize(b.authors)
    for auth_sub, title_sub in PINS:
        if title_sub and normalize(title_sub) not in t:
            continue
        if auth_sub and normalize(auth_sub) not in a:
            continue
        return True
    return False


def is_low_quality(b: Book) -> bool:
    if is_pinned(b):
        return False
    subj = normalize(b.subjects)
    for tok in REJECT_SUBJECT_TOKENS:
        if tok in subj:
            return True
    for pat in REJECT_TITLE_PATTERNS:
        if re.search(pat, b.title or "", flags=re.IGNORECASE):
            return True
    return False


def pick_epub(formats: Dict[str, str]) -> Optional[str]:
    if not formats:
        return None
    if "application/epub+zip" in formats:
        return formats["application/epub+zip"]
    for k, v in formats.items():
        if "epub" in (k or "").lower():
            return v
    return None


# -----------------------------
# HTTP client
# -----------------------------


class Client:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA})

    def get(self, url: str) -> requests.Response:
        """HTTP GET with small retry/backoff for transient errors."""
        backoff = 0.6
        last_exc: Optional[Exception] = None
        for attempt in range(6):
            try:
                time.sleep(SLEEP)
                r = self.s.get(url, timeout=HTTP_TIMEOUT)
                # Retry on transient/server/rate-limit errors.
                if r.status_code in (429, 500, 502, 503, 504):
                    # Respect Retry-After if present.
                    ra = r.headers.get("Retry-After")
                    if ra:
                        try:
                            time.sleep(float(ra))
                        except Exception:
                            time.sleep(backoff)
                    else:
                        time.sleep(backoff)
                    backoff = min(backoff * 1.8, 8.0)
                    continue
                r.raise_for_status()
                return r
            except requests.HTTPError as e:
                # If response exists and is transient, retry; otherwise raise.
                resp = getattr(e, "response", None)
                code = getattr(resp, "status_code", None)
                if code in (429, 500, 502, 503, 504):
                    time.sleep(backoff)
                    backoff = min(backoff * 1.8, 8.0)
                    last_exc = e
                    continue
                raise
            except (requests.ConnectionError, requests.Timeout) as e:
                time.sleep(backoff)
                backoff = min(backoff * 1.8, 8.0)
                last_exc = e
                continue

        # Exhausted retries.
        if last_exc:
            raise last_exc
        raise RuntimeError(f"GET failed after retries: {url}")



# -----------------------------
# Standard Ebooks lookup (Atom search feed)
# -----------------------------


def _parse_atom_entries_for_epubs(atom_xml: bytes) -> List[Tuple[str, str, str]]:
    """Return list of (author, title, epub_url) from an Atom feed."""
    out: List[Tuple[str, str, str]] = []
    try:
        root = etree.fromstring(atom_xml)
    except Exception:
        return out

    entries = root.xpath("//*[local-name()='entry']")
    for e in entries:
        title_nodes = e.xpath("./*[local-name()='title']/text()")
        title = title_nodes[0].strip() if title_nodes else ""

        author_nodes = e.xpath("./*[local-name()='author']/*[local-name()='name']/text()")
        author = author_nodes[0].strip() if author_nodes else ""

        epub_url = ""
        for ln in e.xpath("./*[local-name()='link']"):
            href = ln.get("href") or ""
            typ = (ln.get("type") or "").lower()
            rel = (ln.get("rel") or "").lower()
            if href.lower().endswith(".epub"):
                epub_url = href
                break
            if "epub" in typ and href:
                epub_url = href
                break
            if "acquisition" in rel and href.lower().endswith(".epub"):
                epub_url = href
                break

        if author and title and epub_url:
            out.append((author, title, epub_url))

    return out


def se_find_epub_url(cli: Client, author: str, title: str) -> Optional[str]:
    """Best-effort: find a Standard Ebooks EPUB URL for (author, title).

    Uses the public Atom feed search endpoint documented by Standard Ebooks.
    Returns a direct .epub link or None.

    This avoids OPDS endpoints that may respond 401 Unauthorized.
    """
    q = f"{author} {title}".strip()
    if not q:
        return None

    url = SE_ATOM_SEARCH.format(q=requests.utils.quote(q))
    try:
        xml = cli.get(url).content
    except requests.HTTPError as e:
        # If SE blocks or rate-limits this too, treat as unavailable.
        return None
    except Exception:
        return None

    entries = _parse_atom_entries_for_epubs(xml)
    if not entries:
        return None

    # Fuzzy-rank against author|title
    query_key = normalize(author) + "|" + normalize(title)
    cand_keys = [normalize(a) + "|" + normalize(t) for (a, t, _) in entries]
    m = process.extractOne(query_key, cand_keys, scorer=fuzz.token_set_ratio)
    if not m:
        return None
    best_key, score, idx = m
    if score < 92:
        return None
    return entries[idx][2]


# Cache for SE lookups to avoid repeated network hits.
_SE_CACHE: Dict[str, Optional[str]] = {}


def best_se_match(cli: Client, author: str, title: str, min_score: int = 92) -> Optional[str]:
    """Return a Standard Ebooks .epub URL if we can find a good match, else None."""
    cache_key = normalize(author) + "|" + normalize(title)
    if cache_key in _SE_CACHE:
        return _SE_CACHE[cache_key]

    url = se_find_epub_url(cli, author, title)
    _SE_CACHE[cache_key] = url
    return url


# -----------------------------
# Gutenberg: shelves + discovery
# -----------------------------


ebook_id_re = re.compile(r"/ebooks/(\d+)")


def scrape_shelf_ids(cli: Client, shelf_id: int, max_ids: Optional[int] = None) -> List[int]:
    ids: List[int] = []
    seen: Set[int] = set()
    start = 1

    # Gutenberg shelf pagination uses start_index in steps of 25. Some shelves are very large,
    # but Gutenberg will return HTTP 400 once start_index is too high (observed at 5001).
    # Stop cleanly on 400 or any HTTP error.
    for _ in range(800):
        if start > 5000:
            break
        url = GUTENBERG_SHELF_PAGE.format(id=shelf_id, start=start)
        try:
            page = cli.get(url).text
        except requests.HTTPError as e:
            # Stop on Bad Request (e.g., start_index out of range) and on other HTTP errors.
            break
        except Exception:
            break

        found = [int(m.group(1)) for m in ebook_id_re.finditer(page)]
        new = [i for i in found if i not in seen]
        if not new:
            break
        for i in new:
            seen.add(i)
            ids.append(i)
            if max_ids is not None and len(ids) >= max_ids:
                return ids
        if max_ids is not None and len(ids) >= max_ids:
            break
        start += 25

    return ids


def discover_shelves(cli: Client, query: str, max_shelves: int) -> List[int]:
    """Discover Gutenberg bookshelf IDs for a query.

    Gutenberg's shelf-search endpoint can occasionally 504. If it does, fall back to a
    small curated set of international literature shelves.
    """
    url = GUTENBERG_SHELF_SEARCH.format(q=requests.utils.quote(query))
    try:
        content = cli.get(url).content
    except Exception:
        # Fallback (still provides broad international coverage in English translations)
        return INTERNATIONAL_SHELF_FALLBACK[:max_shelves]

    doc = html.fromstring(content)
    shelf_ids: List[int] = []
    for href in doc.xpath("//a/@href"):
        m = re.match(r"^/ebooks/bookshelf/(\d+)$", href)
        if m:
            shelf_ids.append(int(m.group(1)))

    # Dedup, keep order
    out: List[int] = []
    for sid in shelf_ids:
        if sid not in out:
            out.append(sid)
        if len(out) >= max_shelves:
            break

    # If search produced nothing, use fallback.
    if not out:
        return INTERNATIONAL_SHELF_FALLBACK[:max_shelves]

    return out


# -----------------------------
# Gutendex metadata
# -----------------------------

# On-disk cache for Gutendex responses (speeds reruns dramatically).
# We cache by Gutenberg ID: <out>/cache/gutendex/<id>.json

def _gutendex_cache_path(cache_dir: Path, gid: int) -> Path:
    return cache_dir / "gutendex" / f"{gid}.json"

def _load_gutendex_cached_json(cache_dir: Path, gid: int) -> Optional[dict]:
    p = _gutendex_cache_path(cache_dir, gid)
    if not p.exists():
        return None
    try:
        import json
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def _save_gutendex_cached_json(cache_dir: Path, gid: int, data: dict) -> None:
    p = _gutendex_cache_path(cache_dir, gid)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        import json
        p.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass


def gutendex_fetch_book(cli: Client, gid: int, cache_dir: Optional[Path] = None) -> Optional[Book]:
    j = None
    if cache_dir is not None:
        j = _load_gutendex_cached_json(cache_dir, gid)

    if j is None:
        try:
            j = cli.get(GUTENDEX_BOOK.format(id=gid)).json()
            if cache_dir is not None and isinstance(j, dict):
                _save_gutendex_cached_json(cache_dir, gid, j)
        except Exception:
            return None

    title = (j.get("title") or "").strip()
    if not title:
        return None

    langs = j.get("languages") or []
    if LANGUAGE and LANGUAGE not in langs:
        return None

    authors = ", ".join(
        a.get("name", "").strip() for a in (j.get("authors") or []) if a.get("name")
    )
    if not authors:
        authors = "Unknown"

    subjects = "; ".join((j.get("subjects") or [])[:12])
    downloads = int(j.get("download_count") or 0)

    epub = pick_epub(j.get("formats") or {})
    if not epub:
        return None

    return Book(
        gut_id=int(j.get("id") or gid),
        title=title,
        authors=authors,
        languages=",".join(langs),
        subjects=subjects,
        downloads=downloads,
        gutenberg_epub_url=epub,
        preferred_source="gutenberg",
        preferred_url=epub,
    )


def build_pool(cli: Client, ids: Iterable[int], cache_dir: Optional[Path] = None) -> Dict[int, Book]:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    pool: Dict[int, Book] = {}
    id_list = sorted(set(ids))

    def worker(gid: int) -> Optional[Book]:
        return gutendex_fetch_book(cli, gid, cache_dir=cache_dir)

    with ThreadPoolExecutor(max_workers=GUTENDEX_WORKERS) as ex:
        futs = {ex.submit(worker, gid): gid for gid in id_list}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="Gutendex metadata"):
            b = fut.result()
            if b:
                pool[b.gut_id] = b

    return pool


def gutendex_search(cli: Client, query: str, pages: int = 3) -> List[int]:
    ids: List[int] = []
    url = f"{GUTENDEX_LIST}?search={requests.utils.quote(query)}"
    for _ in range(pages):
        try:
            j = cli.get(url).json()
        except Exception:
            break
        for item in (j.get("results") or []):
            try:
                ids.append(int(item.get("id")))
            except Exception:
                pass
        url = j.get("next") or ""
        if not url:
            break
    return ids


# -----------------------------
# Scoring / selection
# -----------------------------


def se_available(cli: Client, b: Book) -> bool:
    author0 = primary_author(b.authors) or b.authors
    return best_se_match(cli, author0, b.title) is not None


def score(domain: str, b: Book, cli: Client) -> float:
    # Hard drop common low-quality classes unless pinned.
    if is_low_quality(b):
        return -1e18

    # Start with a softened popularity proxy.
    base = (max(b.downloads, 1) ** 0.5)

    # Strongly prefer Standard Ebooks when available.
    if se_available(cli, b):
        base *= 3.0

    # Protect pinned anchors.
    if is_pinned(b):
        base *= 5.0

    # Slightly ensure international literature isn't drowned.
    if domain == "literature_international":
        base *= 1.25

    # Penalize extremely generic "collected works" multi-volume indications.
    # (We already reject most Vol/Part titles; this is a soft backstop.)
    if re.search(r"\bcomplete works\b", (b.title or "").lower()):
        base *= 0.9

    return base


def select_domain(
    domain: str,
    pool: Dict[int, Book],
    ids: Sequence[int],
    quota: int,
    cli: Client,
    chosen: Set[int],
) -> List[Book]:
    cands = [pool[i] for i in set(ids) if i in pool and i not in chosen]
    ranked = sorted(cands, key=lambda b: score(domain, b, cli), reverse=True)
    out: List[Book] = []
    for b in ranked:
        if len(out) >= quota:
            break
        if score(domain, b, cli) <= -1e17:
            continue
        b.domain = domain
        out.append(b)
        chosen.add(b.gut_id)
    return out


def trim_to_target(
    books: List[Book], target: int, cli: Client
) -> List[Book]:
    # Keep pinned anchors; trim the rest by global score.
    pinned = [b for b in books if is_pinned(b)]
    others = [b for b in books if not is_pinned(b)]

    # If pins exceed target, keep top-scoring pins.
    if len(pinned) >= target:
        pinned.sort(key=lambda b: score(b.domain, b, cli), reverse=True)
        return pinned[:target]

    # Otherwise fill remaining slots.
    keep_n = target - len(pinned)
    others.sort(key=lambda b: score(b.domain, b, cli), reverse=True)
    return pinned + others[:keep_n]



# -----------------------------
# -----------------------------
# Standard Ebooks bulk ZIP import
# -----------------------------


def _read_epub_opf_metadata(epub_path: Path) -> Tuple[str, str]:
    """Return (title, creator) from an EPUB by reading its OPF metadata.

    Best-effort and robust: handles missing/broken container.xml by scanning for .opf files.
    Returns ("", "") on failure.
    """

    def _extract_from_opf_bytes(opf_xml: bytes) -> Tuple[str, str]:
        try:
            opf_root = etree.fromstring(opf_xml)
            # Try common DC element paths first, then fallback to any title/creator.
            title_nodes = opf_root.xpath(
                "//*[local-name()='metadata']//*[local-name()='title']/text()"
            )
            creator_nodes = opf_root.xpath(
                "//*[local-name()='metadata']//*[local-name()='creator']/text()"
            )
            if not title_nodes:
                title_nodes = opf_root.xpath("//*[local-name()='title']/text()")
            if not creator_nodes:
                creator_nodes = opf_root.xpath("//*[local-name()='creator']/text()")

            title = title_nodes[0].strip() if title_nodes else ""
            creator = creator_nodes[0].strip() if creator_nodes else ""
            return title, creator
        except Exception:
            return "", ""

    try:
        with zipfile.ZipFile(epub_path, "r") as zf:
            names = zf.namelist()

            # 1) Normal path: container.xml -> OPF
            opf_path = ""
            if "META-INF/container.xml" in names:
                try:
                    container_xml = zf.read("META-INF/container.xml")
                    root = etree.fromstring(container_xml)
                    opf_paths = root.xpath("//*[local-name()='rootfile']/@full-path")
                    if opf_paths:
                        opf_path = opf_paths[0]
                except Exception:
                    opf_path = ""

            if opf_path:
                try:
                    opf_xml = zf.read(opf_path)
                    title, creator = _extract_from_opf_bytes(opf_xml)
                    if title:
                        return title, creator
                except Exception:
                    pass

            # 2) Fallback: scan for .opf entries (some EPUBs omit container.xml or have weird paths)
            opf_candidates = [n for n in names if n.lower().endswith(".opf")]
            # Prefer common locations
            def _opf_rank(n: str) -> Tuple[int, int]:
                nl = n.lower()
                pref = 0
                if "content.opf" in nl:
                    pref = 0
                elif nl.endswith(".opf"):
                    pref = 1
                return (pref, len(n))

            for n in sorted(opf_candidates, key=_opf_rank):
                try:
                    opf_xml = zf.read(n)
                    title, creator = _extract_from_opf_bytes(opf_xml)
                    if title:
                        return title, creator
                except Exception:
                    continue

    except Exception:
        return "", ""

    return "", ""


def _se_key(author: str, title: str) -> str:
    return normalize(author) + "|" + normalize(title)


def build_se_bulk_index(
    out_root: Path,
    bulk_zips: Sequence[Path],
    bulk_dirs: Sequence[Path],
) -> Dict[str, Path]:
    """Build an index from (author|title) -> local epub path for Standard Ebooks bulk downloads.

    - Accepts ZIP files downloaded from standardebooks.org bulk downloads (Patrons Circle).
    - Also accepts directories of EPUBs.

    Extracts any ZIPs into: <out_root>/se_bulk_cache/<zip_stem>/...

    Returns dict mapping normalized key to EPUB path.
    """
    idx: Dict[str, Path] = {}
    cache_root = out_root / "se_bulk_cache"
    cache_root.mkdir(parents=True, exist_ok=True)

    # 1) Collect candidate epub paths from directories
    candidates: List[Path] = []
    for d in bulk_dirs:
        if d and d.exists():
            candidates.extend(sorted(d.rglob("*.epub")))

    # 2) Extract zip(s) and collect epubs
    for z in bulk_zips:
        if not z or not z.exists():
            continue
        dest = cache_root / z.stem
        dest.mkdir(parents=True, exist_ok=True)
        try:
            with zipfile.ZipFile(z, "r") as zf:
                # Extract only .epub members
                members = [m for m in zf.namelist() if m.lower().endswith(".epub")]
                for m in members:
                    # Avoid Zip Slip
                    m_path = Path(m)
                    if m_path.is_absolute() or ".." in m_path.parts:
                        continue
                    out_path = dest / m_path
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    if not out_path.exists() or out_path.stat().st_size < 10_000:
                        with zf.open(m) as src, open(out_path, "wb") as dst:
                            shutil.copyfileobj(src, dst)
                    candidates.append(out_path)
        except Exception:
            continue

    # 3) Read metadata and populate index
    for p in candidates:
        try:
            if not p.exists() or p.stat().st_size < 10_000:
                continue
            title, creator = _read_epub_opf_metadata(p)
            if not title:
                continue
            # creator may be blank in some epubs; fall back to filename stem
            # creator may be blank in some epubs; fall back to filename stem
            author = (creator or p.stem).strip()

            # Index multiple author-name variants to improve matching against Gutendex
            for av in author_variants(author) or [author]:
                key = _se_key(av, title)
                if key and key not in idx:
                   idx[key] = p

        except Exception:
            continue

    return idx


def find_se_bulk_match(
    se_bulk_index: Dict[str, Path], author: str, title: str
) -> Optional[Path]:
    """Fuzzy-match a manifest book against the SE bulk index."""
    if not se_bulk_index:
        return None

    keys = list(se_bulk_index.keys())
    best_path: Optional[Path] = None
    best_score = 0

    for a in author_variants(author) or [author]:
        query_key = _se_key(a, title)
        m = process.extractOne(query_key, keys, scorer=fuzz.token_set_ratio)
        if not m:
            continue
        _, score, idx_i = m
        if score > best_score:
            best_score = score
            best_path = se_bulk_index[keys[idx_i]]

    if best_score < 92:
        return None
    return best_path



# -----------------------------
# Bulk import and deduplication helpers
# -----------------------------

def _strip_leading_article(t: str) -> str:
    t = (t or "").strip()

    # Drop common subtitle separators and bracketed/parenthetical suffixes.
    # This helps align Gutenberg vs Standard Ebooks title strings.
    for sep in (" : ", ":", " - ", " — ", " -- ", ";"):
        if sep in t:
            t = t.split(sep, 1)[0].strip()

    # Remove trailing parenthetical/bracketed phrases.
    t = re.sub(r"\s*[\(\[].*?[\)\]]\s*$", "", t).strip()

    tl = t.lower()
    for art in ("the ", "a ", "an "):
        if tl.startswith(art):
            return t[len(art):].strip()
    return t


def _author_last_name(author: str) -> str:
    """Return normalized last name for matching."""
    a0 = primary_author(author) or author
    a0 = (a0 or "").strip()
    if not a0:
        return ""
    if "," in a0:
        return (a0.split(",", 1)[0] or "").strip()
    toks = [t for t in _space_re.split(a0) if t]
    return toks[-1] if toks else a0


def _metadata_key_for_epub(epub_path: Path) -> Optional[str]:
    """Return a normalized key for fuzzy duplicate detection, or None if unavailable."""
    title, creator = _read_epub_opf_metadata(epub_path)
    if not title:
        return None
    title_n = normalize(_strip_leading_article(title))
    last_n = normalize(_author_last_name(creator))
    if not last_n:
        # fallback: sometimes creator empty; use file stem as weak signal
        last_n = normalize(_author_last_name(epub_path.stem))
    if not title_n:
        return None
    return f"{title_n}|{last_n}"


def _keep_score(epub_path: Path) -> Tuple[int, int, int]:
    """Higher is better. Tuple for stable sorting.

    Prefer Standard Ebooks, then larger files, then shorter paths.
    """
    pstr = str(epub_path)
    se_marker = 0
    try:
        if (epub_path.parent / "FROM_STANDARD_EBOOKS_BULK.txt").exists():
            se_marker = 2
    except Exception:
        se_marker = 0

    if "standardebooks" in pstr.lower():
        se_marker = max(se_marker, 1)

    try:
        size = int(epub_path.stat().st_size)
    except Exception:
        size = 0

    return (se_marker, size, -len(pstr))

# -----------------------------
# Dedupe helpers for bucket/shelf/SE detection and SE install
# -----------------------------

def _bucket_name(epub_dir: Path, p: Path) -> str:
    try:
        rel = p.resolve().relative_to(epub_dir.resolve())
        parts = rel.parts
        return parts[0] if parts else ""
    except Exception:
        return ""

def _is_standard_ebooks_epub(p: Path) -> bool:
    try:
        if (p.parent / "FROM_STANDARD_EBOOKS_BULK.txt").exists():
            return True
    except Exception:
        pass
    return "standardebooks" in str(p).lower()

def _install_se_epub_into_dir(se_epub: Path, target_dir: Path, target_epub: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    tmp = target_epub.with_suffix(target_epub.suffix + ".tmp")
    shutil.copyfile(se_epub, tmp)
    tmp.replace(target_epub)
    # provenance marker
    (target_dir / "FROM_STANDARD_EBOOKS_BULK.txt").write_text(
        f"source_path={se_epub}\n",
        encoding="utf-8",
    )

def _sha256_path(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def dedupe_library(epub_dir: Path) -> Tuple[int, int, int]:
    """Deduplicate the library.

    Pass 1: exact duplicates via sha256.
    Pass 2: near-duplicates via EPUB metadata key (title+author-lastname).

    Returns (scanned, removed, kept_unique_groups).
    """
    files = sorted(epub_dir.rglob("*.epub"))

    # Filter to plausible EPUBs
    epub_files: List[Path] = []
    for p in files:
        try:
            if p.exists() and p.stat().st_size >= 10_000:
                epub_files.append(p)
        except Exception:
            continue

    scanned = len(epub_files)
    removed = 0

    # ---- Pass 1: sha256 exact duplicates ----
    by_hash: Dict[str, List[Path]] = {}
    for p in epub_files:
        try:
            hx = _sha256_path(p)
            by_hash.setdefault(hx, []).append(p)
        except Exception:
            continue

    # Track files/directories removed so pass 2 doesn't touch them
    removed_dirs: Set[Path] = set()

    def _remove_duplicate(keep: Path, dup: Path, hx: str) -> bool:
        """Remove dup directory if possible; fallback to deleting only the file."""
        try:
            dup_dir = dup.parent
            keep_dir = keep.parent
            if dup_dir.resolve() == keep_dir.resolve():
                return False

            note = dup_dir / "DUPLICATE_OF.txt"
            note.write_text(
                f"sha256={hx}\nkept={keep}\nremoved={dup}\n",
                encoding="utf-8",
            )

            def _on_rm_error(func, path, exc_info):
                # Try to make the path writable and retry once.
                try:
                    import os, stat
                    os.chmod(path, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
                except Exception:
                    pass
                try:
                    func(path)
                except Exception:
                    pass

            # Prefer deleting the entire duplicate directory.
            try:
                shutil.rmtree(dup_dir, onerror=_on_rm_error)
            except Exception:
                # Fallback: remove the duplicate epub and then attempt to remove the directory.
                try:
                    dup.unlink(missing_ok=True)
                except Exception:
                    pass
                try:
                    # If directory is now empty, remove it.
                    dup_dir.rmdir()
                except Exception:
                    pass

            # If the directory still exists (e.g., had extra files), leave note behind but we still counted a removal.
            removed_dirs.add(dup_dir)
            return True
        except Exception:
            return False

    # Keep count of unique groups kept
    kept_groups = 0

    for hx, paths in by_hash.items():
        if len(paths) == 1:
            kept_groups += 1
            continue

        # Keep the best path by score
        paths2 = [p for p in paths if p.exists()]
        if not paths2:
            continue
        paths_sorted = sorted(paths2, key=_keep_score, reverse=True)
        keep = paths_sorted[0]
        kept_groups += 1

        for dup in paths_sorted[1:]:
            if _remove_duplicate(keep, dup, hx):
                removed += 1

    # Refresh list for pass 2
    epub_files2: List[Path] = []
    for p in sorted(epub_dir.rglob("*.epub")):
        try:
            if p.exists() and p.stat().st_size >= 10_000:
                epub_files2.append(p)
        except Exception:
            continue

    # ---- Pass 2: metadata key duplicates (title + author last name) ----
    by_meta: Dict[str, List[Path]] = {}
    meta_ok = 0
    meta_fail = 0
    for p in epub_files2:
        try:
            key = _metadata_key_for_epub(p)
            if not key:
                meta_fail += 1
                continue
            meta_ok += 1
            by_meta.setdefault(key, []).append(p)
        except Exception:
            meta_fail += 1
            continue

    print(f"Dedupe pass2 metadata keys: ok={meta_ok} missing={meta_fail} groups={len(by_meta)}")

    # In pass 2, each meta key is a group. We'll keep one per group.
    for key, paths in by_meta.items():
        paths = [p for p in paths if p.exists()]
        if len(paths) <= 1:
            continue

        # Partition into shelf vs standardebooks_bulk
        shelf_paths: List[Path] = []
        bulk_paths: List[Path] = []
        for p in paths:
            bname = _bucket_name(epub_dir, p)
            if bname == "standardebooks_bulk":
                bulk_paths.append(p)
            else:
                shelf_paths.append(p)

        # Choose where the final kept directory should live
        if shelf_paths:
            shelf_sorted = sorted(shelf_paths, key=_keep_score, reverse=True)
            keep = shelf_sorted[0]
        else:
            paths_sorted = sorted(paths, key=_keep_score, reverse=True)
            keep = paths_sorted[0]

        # If we have an SE epub anywhere and we are keeping a shelf directory, install the SE epub there.
        if shelf_paths:
            se_candidates = [p for p in paths if _is_standard_ebooks_epub(p)]
            if se_candidates:
                se_sorted = sorted(se_candidates, key=_keep_score, reverse=True)
                se_best = se_sorted[0]
                try:
                    _install_se_epub_into_dir(se_best, keep.parent, keep)
                except Exception:
                    pass

        # Best-effort sha for logging
        try:
            hx_keep = _sha256_path(keep)
        except Exception:
            hx_keep = "meta"

        # Delete all other duplicate dirs
        for dup in paths:
            if dup.resolve() == keep.resolve():
                continue
            if dup.parent.resolve() == keep.parent.resolve():
                continue
            if _remove_duplicate(keep, dup, hx_keep):
                removed += 1

    # Recompute kept groups as number of remaining epub files (approx)
    try:
        remaining = sum(1 for p in epub_dir.rglob("*.epub") if p.exists())
    except Exception:
        remaining = 0

    return scanned, removed, remaining


def _stable_bulk_id(title: str, creator: str) -> str:
    base = (normalize(creator) + "|" + normalize(title)).encode("utf-8")
    return hashlib.sha256(base).hexdigest()[:10]


def import_all_se_bulk_epubs(se_bulk_index: Dict[str, Path], library_root: Path) -> int:
    """Copy all Standard Ebooks bulk EPUBs into per-book folders under library_root/standardebooks_bulk."""
    if not se_bulk_index:
        return 0

    imported = 0
    for key in sorted(se_bulk_index.keys()):
        src = se_bulk_index[key]
        try:
            if (not src.exists()) or src.stat().st_size < 10_000:
                continue

            title, creator = _read_epub_opf_metadata(src)
            if not title:
                continue

            stable = _stable_bulk_id(title, creator or "")

            tmp_book = Book(
                gut_id=0,
                title=title,
                authors=creator or "",
                languages=LANGUAGE,
                subjects="",
                downloads=0,
                gutenberg_epub_url="",
                domain="standardebooks_bulk",
                preferred_source="standardebooks",
                preferred_url="",
            )

            bucket = "standardebooks_bulk"
            prefix = author_title_dir_prefix(tmp_book)
            title_dir = f"{prefix} (SEBULK-{stable})"
            out_dir = library_root / bucket / title_dir

            dst = out_dir / se_style_epub_filename(tmp_book)
            if dst.exists() and dst.stat().st_size > 10_000:
                continue

            shutil.copyfile(src, dst)
            (out_dir / "FROM_STANDARD_EBOOKS_BULK.txt").write_text(
                f"source_path={src}\nsha256={_sha256_path(dst)}\n",
                encoding="utf-8",
            )
            imported += 1
        except Exception:
            continue

    return imported

# -----------------------------
# Downloading
# -----------------------------


# Download with retries/backoff, supporting Standard Ebooks rate limiting.
def _download_with_retries(cli: Client, url: str, dest: Path, *, is_standard_ebooks: bool) -> Optional[str]:
    """Download URL to dest with retry/backoff.

    Returns an error string on failure, or None on success.
    """
    backoff = 2.0 if is_standard_ebooks else 1.0
    max_retries = SE_DOWNLOAD_MAX_RETRIES if is_standard_ebooks else 6

    for attempt in range(1, max_retries + 1):
        try:
            # Extra politeness for Standard Ebooks.
            if is_standard_ebooks:
                time.sleep(SE_DOWNLOAD_SLEEP)
            else:
                time.sleep(SLEEP)

            r = cli.s.get(url, stream=True, timeout=HTTP_TIMEOUT)

            # Handle rate limiting / transient server errors with backoff.
            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        wait = float(ra)
                    except Exception:
                        wait = backoff
                else:
                    wait = backoff

                # Drain response to free connection.
                try:
                    r.close()
                except Exception:
                    pass

                time.sleep(min(wait, 180.0))
                backoff = min(backoff * 1.8, 180.0)
                continue

            r.raise_for_status()

            # Stream to disk.
            tmp = dest.with_suffix(dest.suffix + ".part")
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 128):
                    if chunk:
                        f.write(chunk)
            try:
                r.close()
            except Exception:
                pass
            tmp.replace(dest)
            return None

        except requests.HTTPError as e:
            resp = getattr(e, "response", None)
            code = getattr(resp, "status_code", None)
            if code in (429, 500, 502, 503, 504):
                ra = None
                try:
                    ra = resp.headers.get("Retry-After") if resp is not None else None
                except Exception:
                    ra = None
                if ra:
                    try:
                        wait = float(ra)
                    except Exception:
                        wait = backoff
                else:
                    wait = backoff
                time.sleep(min(wait, 180.0))
                backoff = min(backoff * 1.8, 180.0)
                continue
            return f"HTTPError: {e}"
        except (requests.ConnectionError, requests.Timeout) as e:
            time.sleep(min(backoff, 180.0))
            backoff = min(backoff * 1.8, 180.0)
            continue
        except Exception as e:
            return f"{type(e).__name__}: {e}"

    return f"HTTPError: 429/5xx after {max_retries} retries"


def _is_retry_exhausted_error(err: Optional[str]) -> bool:
    if not err:
        return False
    e = err.lower()
    return "after" in e and "retries" in e and ("429" in e or "5xx" in e)


def decide_preferred_url(cli: Client, b: Book) -> None:
    author0 = primary_author(b.authors) or b.authors
    se_url = best_se_match(cli, author0, b.title)
    if se_url:
        b.preferred_source = "standardebooks"
        b.preferred_url = se_url
    else:
        b.preferred_source = "gutenberg"
        b.preferred_url = b.gutenberg_epub_url



def download_epub(cli: Client, b: Book, root: Path, se_bulk_index: Optional[Dict[str, Path]] = None) -> None:
    bucket = domain_folder(b)
 
    # Include author in the directory name for readability.
    prefix = author_title_dir_prefix(b)

    # Include Gutenberg ID to avoid collisions between same-titled works.
    title_dir = f"{prefix} ({b.gut_id})"

    out_dir = root / bucket / title_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    # Standard Ebooks-style filename
    fname = se_style_epub_filename(b)
    out_path = out_dir / fname

    # Back-compat: if an older run created book.epub, rename it.
    legacy = out_dir / "book.epub"
    if legacy.exists() and (not out_path.exists()):
        try:
            legacy.replace(out_path)
        except Exception:
            pass

    if out_path.exists() and out_path.stat().st_size > 10_000:
        return

    se_bulk_index = se_bulk_index or {}

    # If this book prefers Standard Ebooks, try to copy from local bulk cache first.
    if b.preferred_source == "standardebooks" and se_bulk_index:
        author0 = primary_author(b.authors) or b.authors
        mpath = find_se_bulk_match(se_bulk_index, author0, b.title)
        if mpath and mpath.exists() and mpath.stat().st_size > 10_000:
            shutil.copyfile(mpath, out_path)
            # Record provenance
            with open(out_dir / "FROM_STANDARD_EBOOKS_BULK.txt", "w", encoding="utf-8") as f:
                f.write(f"source_path={mpath}\n")
            return

    # Prefer Standard Ebooks when selected, but fall back to Gutenberg if SE rate-limits too hard.
    primary_url = b.preferred_url or ""
    fallback_url = b.gutenberg_epub_url or ""

    tried: List[Tuple[str, str]] = []  # (url, err)

    # Attempt 1: preferred URL (often Standard Ebooks)
    url1 = primary_url or fallback_url
    is_se1 = (b.preferred_source == "standardebooks") or ("standardebooks.org" in (url1 or ""))
    err1 = _download_with_retries(cli, url1, out_path, is_standard_ebooks=is_se1)
    if err1 is None:
        return
    tried.append((url1, err1))

    # Attempt 2: if SE was preferred and we exhausted retries, try Gutenberg instead.
    if is_se1 and _is_retry_exhausted_error(err1) and fallback_url and fallback_url != url1:
        note = out_dir / "FELL_BACK_TO_GUTENBERG.txt"
        with open(note, "w", encoding="utf-8") as f:
            f.write("Standard Ebooks download was rate-limited; used Gutenberg EPUB instead.\n")
            f.write(f"standardebooks_url={url1}\n")
            f.write(f"standardebooks_error={err1}\n")
            f.write(f"gutenberg_url={fallback_url}\n")

        err2 = _download_with_retries(cli, fallback_url, out_path, is_standard_ebooks=False)
        if err2 is None:
            return
        tried.append((fallback_url, err2))

    # Failed
    with open(out_dir / "DOWNLOAD_FAILED.txt", "w", encoding="utf-8") as f:
        for u, e in tried:
            f.write(f"{u}\n{e}\n\n")



# -----------------------------
# Main
# -----------------------------


def read_final_manifest(path: Path) -> List[Book]:
    """Read build/final_selection.csv into Book objects.

    Expected columns:
      domain,gut_id,title,authors,languages,downloads,subjects,preferred_source,preferred_url,gutenberg_epub_url
    """
    if not path.exists():
        raise FileNotFoundError(f"Final manifest not found: {path}")

    out: List[Book] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        required = {
            "domain",
            "gut_id",
            "title",
            "authors",
            "languages",
            "downloads",
            "subjects",
            "preferred_source",
            "preferred_url",
            "gutenberg_epub_url",
        }
        if r.fieldnames is None or not required.issubset(set(r.fieldnames)):
            raise ValueError(
                f"Manifest {path} missing required columns. Found: {r.fieldnames}"
            )

        for row in r:
            try:
                b = Book(
                    gut_id=int(row["gut_id"]),
                    title=row["title"],
                    authors=row["authors"],
                    languages=row["languages"],
                    subjects=row["subjects"],
                    downloads=int(row["downloads"] or 0),
                    gutenberg_epub_url=row["gutenberg_epub_url"],
                    domain=row.get("domain", ""),
                    preferred_source=row.get("preferred_source", "gutenberg"),
                    preferred_url=row.get("preferred_url", ""),
                )
            except Exception:
                # Skip malformed rows
                continue
            out.append(b)

    return out


def write_csv(path: Path, rows: List[List[object]], header: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Quality-first public domain ebook downloader")
    ap.add_argument("--target", type=int, default=DEFAULT_TARGET, help="Number of books to select")
    ap.add_argument("--out", type=Path, default=Path("build"), help="Output directory")
    ap.add_argument("--no-download", action="store_true", help="Only build manifests; do not download")
    ap.add_argument("--download-only", action="store_true", help="Assumes manifests are already built, and downloads them")
    ap.add_argument(
        "--se-bulk-zip",
        action="append",
        type=Path,
        default=[],
        help="Path to a Standard Ebooks bulk-download ZIP (can be repeated).",
    )
    ap.add_argument(
        "--se-bulk-dir",
        action="append",
        type=Path,
        default=[],
        help="Path to a directory containing Standard Ebooks EPUBs (can be repeated).",
    )
    ap.add_argument(
        "--import-se-bulk-all",
        action="store_true",
        help="Import all Standard Ebooks bulk EPUBs into the output library, regardless of final selection.",
    )
    ap.add_argument(
        "--import-only",
        action="store_true",
        help="After importing (e.g., with --import-se-bulk-all), exit without downloading the final selection.",
    )
    ap.add_argument(
        "--dedupe",
        action="store_true",
        help="After imports/downloads, remove duplicate EPUBs by content hash.",
    )

    args = ap.parse_args(argv)

    out_root: Path = args.out
    cache_dir = out_root / "cache"
    epub_dir = out_root / "library_epub"
    candidates_csv = out_root / "candidate_pool.csv"
    final_csv = out_root / "final_selection.csv"

    cli = Client()

    se_bulk_index: Dict[str, Path] = {}

    final: List[Book] = []

    if args.download_only:
        # Load the previously-built manifest and download exactly those books.
        final = read_final_manifest(final_csv)
        print(f"Loaded final selection: {final_csv} (n={len(final)})")
    else:
        # 1) Standard Ebooks matching
        print("Standard Ebooks matching: using Atom search feed (no full catalog)")

        # 2) Discover international shelves
        print("Discovering international literature shelves…")
        intl_shelves = discover_shelves(cli, INTERNATIONAL_SHELF_SEARCH_QUERY, INTERNATIONAL_SHELF_MAX)

        # 3) Scrape Gutenberg shelf IDs
        print("Scraping Gutenberg shelf seeds…")
        domain_to_ids: Dict[str, List[int]] = {k: [] for k in QUOTAS.keys()}

        for domain, shelf_ids in GUTENBERG_SHELF_SEEDS.items():
            # Cap how many IDs we collect for this domain.
            quota = QUOTAS.get(domain, 0)
            cap = None
            if quota > 0:
                cap = min(MAX_IDS_HARD_CAP, quota * MAX_IDS_FACTOR)
            for sid in shelf_ids:
                ids = scrape_shelf_ids(cli, sid, max_ids=cap)
                domain_to_ids.setdefault(domain, []).extend(ids)
                print(
                    f"  {domain}: shelf {sid} -> {len(ids)} ids"
                    + (f" (cap {cap})" if cap else "")
                )

        domain_to_ids["literature_international"] = []
        intl_cap = min(
            MAX_IDS_HARD_CAP,
            QUOTAS.get("literature_international", 0) * MAX_IDS_FACTOR
            or MAX_IDS_HARD_CAP,
        )
        # IMPORTANT: cap is across all international shelves, not per shelf.
        remaining = intl_cap
        for sid in intl_shelves:
            if remaining <= 0:
                break
            ids = scrape_shelf_ids(cli, sid, max_ids=remaining)
            domain_to_ids["literature_international"].extend(ids)
            remaining = intl_cap - len(domain_to_ids["literature_international"])

        print(
            f"International shelves used: {len(intl_shelves)} (total ids: {len(domain_to_ids['literature_international'])})"
        )

        # 4) Subject-keyword supplementation
        print("Adding subject-search candidates (Gutendex search)…")
        for domain, kws in SUBJECT_KEYWORDS.items():
            accum: List[int] = []
            for kw in kws:
                accum.extend(gutendex_search(cli, kw, pages=2))
            domain_to_ids.setdefault(domain, []).extend(accum)

        # 5) Build metadata pool via Gutendex
        print("Building Gutendex metadata pool…")
        all_ids = [i for ids in domain_to_ids.values() for i in ids]
        pool = build_pool(cli, all_ids, cache_dir=cache_dir)
        print(f"Pool size (language={LANGUAGE}): {len(pool)}")

        # 6) Candidate pool CSV
        cand_rows: List[List[object]] = []
        for b in sorted(pool.values(), key=lambda x: x.downloads, reverse=True):
            cand_rows.append(
                [
                    b.gut_id,
                    b.title,
                    b.authors,
                    b.languages,
                    b.downloads,
                    b.subjects,
                    b.gutenberg_epub_url,
                ]
            )
        write_csv(
            candidates_csv,
            cand_rows,
            header=[
                "gut_id",
                "title",
                "authors",
                "languages",
                "downloads",
                "subjects",
                "gutenberg_epub_url",
            ],
        )
        print(f"Wrote candidates: {candidates_csv}")

        # 7) Domain selection by quotas
        print("Selecting by quotas…")
        chosen_ids: Set[int] = set()
        chosen_books: List[Book] = []

        for domain in QUOTAS.keys():
            ids = domain_to_ids.get(domain, [])
            picked = select_domain(domain, pool, ids, QUOTAS[domain], cli, chosen_ids)
            chosen_books.extend(picked)
            print(f"  {domain}: picked {len(picked)}")

        # 8) Trim globally to target, with pin protection.
        final = trim_to_target(chosen_books, args.target, cli)

        # 9) Decide preferred URLs (SE if matched)
        for b in final:
            decide_preferred_url(cli, b)

        # 10) Write final manifest
        final_rows: List[List[object]] = []
        for b in sorted(final, key=lambda x: (x.domain, -x.downloads, x.title)):
            final_rows.append(
                [
                    b.domain,
                    b.gut_id,
                    b.title,
                    b.authors,
                    b.languages,
                    b.downloads,
                    b.subjects,
                    b.preferred_source,
                    b.preferred_url,
                    b.gutenberg_epub_url,
                ]
            )

        write_csv(
            final_csv,
            final_rows,
            header=[
                "domain",
                "gut_id",
                "title",
                "authors",
                "languages",
                "downloads",
                "subjects",
                "preferred_source",
                "preferred_url",
                "gutenberg_epub_url",
            ],
        )
        print(f"Wrote final selection: {final_csv} (n={len(final)})")

    # If user provided Standard Ebooks bulk downloads, index them locally.
    if args.se_bulk_zip or args.se_bulk_dir:
        print("Indexing Standard Ebooks bulk EPUBs…")
        se_bulk_index = build_se_bulk_index(out_root, args.se_bulk_zip, args.se_bulk_dir)
        print(f"Standard Ebooks bulk index: {len(se_bulk_index)} epubs")

    if args.import_se_bulk_all:
        if not se_bulk_index:
            print("--import-se-bulk-all requires --se-bulk-zip and/or --se-bulk-dir")
        else:
            epub_dir.mkdir(parents=True, exist_ok=True)
            print("Importing all Standard Ebooks bulk EPUBs into library…")
            imported = import_all_se_bulk_epubs(se_bulk_index, epub_dir)
            print(f"Imported {imported} Standard Ebooks bulk EPUBs into: {epub_dir / 'standardebooks_bulk'}")

    if args.import_only:
        if not args.import_se_bulk_all:
            print("--import-only currently requires --import-se-bulk-all")
            return 2
        print("--import-only set; skipping manifest downloads.")
        if args.dedupe:
            print("Deduplicating EPUB library…")
            scanned, removed, kept = dedupe_library(epub_dir)
            print(f"Deduped: scanned={scanned} unique={kept} removed={removed}")
        return 0

    # Download phase
    if args.no_download:
        print("--no-download set; skipping downloads.")
        return 0

    if not final:
        print("Nothing to download (final selection is empty).")
        return 0

    # In download-only mode, rely on manifest URLs; if missing, compute on the fly.
    if args.download_only:
        for b in final:
            if not b.preferred_url:
                decide_preferred_url(cli, b)

    print("Downloading EPUBs…")
    epub_dir.mkdir(parents=True, exist_ok=True)
    for b in tqdm(final, desc="Downloading"):
        download_epub(cli, b, epub_dir, se_bulk_index=se_bulk_index)

    if args.dedupe:
        print("Deduplicating EPUB library…")
        scanned, removed, kept = dedupe_library(epub_dir)
        print(f"Deduped: scanned={scanned} unique={kept} removed={removed}")

    print("Done.")
    print(f"EPUB library at: {epub_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())