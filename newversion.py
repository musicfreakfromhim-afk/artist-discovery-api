# Music Discovery Engine — Full Refactored Implementation

## Directory Structure

```
music_discovery/
├── config.py
├── main.py
├── crawler.py
├── requirements.txt
├── .env.example
├── seeds.txt
├── utils/
│   ├── __init__.py
│   └── names.py
├── core/
│   ├── __init__.py
│   ├── genres.py
│   ├── scoring.py
│   ├── dedup.py
│   ├── filters.py
│   └── discovery.py
├── services/
│   ├── __init__.py
│   ├── cache.py
│   ├── lastfm.py
│   ├── spotify.py
│   └── social.py
├── db/
│   ├── __init__.py
│   ├── database.py
│   └── seeds.py
└── api/
    ├── __init__.py
    ├── models.py
    └── routes/
        ├── __init__.py
        ├── search.py
        └── artists.py
```

---

```python
# config.py
"""
Central configuration.
All tuneable constants live here — never scattered across files.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── API Credentials ───────────────────────────────────────────────────────────
LASTFM_API_KEY        = os.getenv("LASTFM_API_KEY", "")
SPOTIFY_CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")
SUPABASE_URL          = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY          = os.getenv("SUPABASE_KEY", "")

# ── HTTP ──────────────────────────────────────────────────────────────────────
HTTP_TIMEOUT      = int(os.getenv("HTTP_TIMEOUT", "10"))
MAX_RETRIES       = int(os.getenv("MAX_RETRIES", "3"))
RATE_LIMIT_SLEEP  = float(os.getenv("RATE_LIMIT_SLEEP", "0.20"))
LASTFM_BASE_URL   = "http://ws.audioscrobbler.com/2.0/"
MUSICBRAINZ_BASE  = "https://musicbrainz.org/ws/2"

# ── Underground Hard Gates (ZERO exceptions) ──────────────────────────────────
DISCOVERY_TARGET_MIN   = int(os.getenv("DISCOVERY_TARGET_MIN", "5000"))
DISCOVERY_TARGET_MAX   = int(os.getenv("DISCOVERY_TARGET_MAX", "20000"))
DISCOVERY_ABS_MAX      = int(os.getenv("DISCOVERY_ABS_MAX", "50000"))
MAX_SPOTIFY_FOLLOWERS  = int(os.getenv("MAX_SPOTIFY_FOLLOWERS", "50000"))
MAX_SPOTIFY_POPULARITY = int(os.getenv("MAX_SPOTIFY_POPULARITY", "50"))

# ── Graph Expansion ───────────────────────────────────────────────────────────
MAX_DEPTH               = int(os.getenv("MAX_DEPTH", "3"))
MAX_NODES               = int(os.getenv("MAX_NODES", "300"))
MAX_SIMILAR_PER_NODE    = int(os.getenv("MAX_SIMILAR_PER_NODE", "12"))
EXPAND_SCORE_THRESHOLD  = int(os.getenv("EXPAND_SCORE_THRESHOLD", "65"))
EXPAND_TOP_FRACTION     = float(os.getenv("EXPAND_TOP_FRACTION", "0.30"))
SEED_FANOUT             = int(os.getenv("SEED_FANOUT", "8"))

# ── Seeds ─────────────────────────────────────────────────────────────────────
SEED_MODE         = os.getenv("SEED_MODE", "file")   # "file" | "database"
SEEDS_FILE        = os.getenv("SEEDS_FILE", "seeds.txt")
SEEDS_SAMPLE_SIZE = int(os.getenv("SEEDS_SAMPLE_SIZE", "10"))

# ── Cache TTLs (seconds) ──────────────────────────────────────────────────────
CACHE_TTL_LASTFM   = int(os.getenv("CACHE_TTL_LASTFM", "3600"))
CACHE_TTL_SPOTIFY  = int(os.getenv("CACHE_TTL_SPOTIFY", "3600"))
CACHE_TTL_SOCIAL   = int(os.getenv("CACHE_TTL_SOCIAL", "86400"))
CACHE_MAX_SIZE     = int(os.getenv("CACHE_MAX_SIZE", "10000"))

# ── Recommendation Output ─────────────────────────────────────────────────────
MAX_RECOMMENDATIONS = int(os.getenv("MAX_RECOMMENDATIONS", "100"))

# ── Genre Overrides (leave empty [] to auto-derive from seeds) ────────────────
TARGET_GENRES: list[str] = []
```

---

```python
# utils/__init__.py
```

---

```python
# utils/names.py
"""
Name normalization, canonical identity keys, collab splitting, fuzzy similarity.
Preserved exactly from original — these are critical for dedup correctness.
"""
import re
import unicodedata
from difflib import SequenceMatcher

# Splits "Artist A feat. Artist B", "Artist x Artist", "A, B & C", etc.
_COLLAB_SPLIT_RE = re.compile(
    r"\s*(?:,|&|\bft\.?\b|\bfeat\.?\b|\bfeaturing\b|\bwith\b|\bvs\.?\b|\bx\b|/|\+)\s*",
    re.IGNORECASE,
)

# Strips trailing "(Live)", "[Remix]", etc.
_PAREN_NOISE_RE = re.compile(r"\s*[\(\[].*?[\)\]]\s*$")


def primary_artist_name(raw: str) -> str:
    """
    Extract the primary artist name from dirty collab/featured strings.
    'Artist A feat. Artist B' -> 'Artist A'
    'Artist (Live)'           -> 'Artist'
    """
    if not raw:
        return ""
    name = unicodedata.normalize("NFKC", str(raw)).strip()
    name = _PAREN_NOISE_RE.sub("", name)
    name = _COLLAB_SPLIT_RE.split(name)[0].strip()
    name = re.sub(r"\s+", " ", name)
    return name.strip(" -–—_·").strip()


def norm_key(name: str) -> str:
    """
    Canonical identity key: lowercase, diacritics stripped, punctuation removed.
    Used for dedup and graph traversal.
    'Glüme' and 'Glume' -> 'glume'
    'Yeat!' and 'Yeat'  -> 'yeat'
    """
    name = primary_artist_name(name)
    # Strip diacritics: é -> e
    name = "".join(
        c for c in unicodedata.normalize("NFKD", name)
        if not unicodedata.combining(c)
    )
    name = name.lower()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def name_similarity(a: str, b: str) -> float:
    """Fuzzy 0..1 similarity between two normalized names."""
    return SequenceMatcher(None, norm_key(a), norm_key(b)).ratio()
```

---

```python
# core/__init__.py
```



```python
# core/genres.py
"""
Genre taxonomy, family detection, and genre-based query parsing.
Add new genres here — scoring never changes.
"""
from __future__ import annotations

GENRE_TAXONOMY: dict[str, set[str]] = {
    "rap": {
        "rap", "trap", "plug", "pluggnb", "plugg", "rage", "drill",
        "hip hop", "hip-hop", "grime", "cloud rap", "emo rap",
        "boom bap", "gangsta rap",
        "jerk", "jerk rap", "sexyy", "sigilkore", "opium", "dark plugg",
        "underground hip hop", "underground rap",
        "experimental hip hop", "experimental rap",
        "alternative hip hop", "alternative rap", "abstract hip hop",
        "detroit", "detroit rap", "detroit trap", "flint", "michigan rap",
        "dmv", "dmv rap", "memphis", "memphis rap",
        "phonk", "horrorcore", "crunk", "southern hip hop", "atlanta rap",
        "uk drill", "ny drill", "brooklyn drill", "chicago drill",
        "trap metal", "rap rock", "scamcore", "soundcloud rap",
        "melodic rap", "conscious hip hop", "jazz rap", "lo-fi rap",
        "west coast rap", "east coast hip hop", "g funk", "rapper",
    },
    "pop": {
        "pop", "dance pop", "electropop", "art pop", "synthpop",
        "power pop", "indie pop", "bedroom pop",
    },
    "country": {
        "country", "americana", "alt-country", "bluegrass",
        "country pop", "outlaw country", "red dirt",
    },
    "rock": {
        "rock", "classic rock", "hard rock", "garage rock",
        "psychedelic rock", "alt rock", "alternative rock", "shoegaze",
    },
    "punk": {
        "punk", "pop punk", "hardcore punk", "post-punk", "skate punk", "hardcore",
    },
    "emo": {"emo", "midwest emo", "screamo", "emocore", "emo rap"},
    "indie": {
        "indie", "indie rock", "indie pop", "indie folk",
        "bedroom pop", "lo-fi", "lofi",
    },
    "electronic": {
        "electronic", "edm", "house", "techno", "dubstep",
        "drum and bass", "dnb", "trance", "idm", "ambient", "electro",
        "uk garage", "garage", "breakbeat", "future bass", "deep house",
    },
    "hyperpop": {
        "hyperpop", "glitchcore", "digicore", "pc music", "bubblegum bass",
    },
    "rnb": {
        "rnb", "r&b", "alternative rnb", "neo soul", "soul", "contemporary r&b",
    },
    "metal": {
        "metal", "death metal", "black metal", "heavy metal",
        "metalcore", "deathcore", "doom metal", "thrash",
    },
    "folk": {
        "folk", "indie folk", "folk rock", "singer-songwriter", "acoustic",
    },
    "jazz": {"jazz", "smooth jazz", "jazz fusion", "bebop", "nu jazz"},
    "classical": {
        "classical", "orchestral", "baroque", "contemporary classical", "piano",
    },
}


def get_genre_families(
    tags: list[str] | None,
    genres: list[str] | None,
) -> set[str]:
    """
    Map an artist's tags + Spotify genres onto known genre families.
    When a string matches both a specific family AND generic 'pop'
    (e.g. 'pop rap', 'bedroom pop'), the generic 'pop' is dropped for that string.
    This prevents diverse seeds from collapsing into 'pop'.
    """
    items = [t.lower() for t in (tags or [])] + [g.lower() for g in (genres or [])]
    fams: set[str] = set()
    for item in items:
        matched: set[str] = set()
        for fam, kws in GENRE_TAXONOMY.items():
            if any(kw in item for kw in kws):
                matched.add(fam)
        if len(matched) > 1 and "pop" in matched:
            matched.discard("pop")
        fams |= matched
    return fams


def genre_match_score(
    artist_families: set[str],
    target_families: set[str],
) -> float:
    """
    0..1 relevance between an artist and the run's target genre profile.
    Neutral (0.5) when no target is known so all genres pass through.
    """
    if not target_families:
        return 0.5
    if not artist_families:
        return 0.0
    overlap = len(artist_families & target_families)
    if overlap == 0:
        return 0.0
    return min(overlap / len(target_families) + 0.15 * (overlap - 1), 1.0)


def query_to_genre_families(query: str) -> set[str]:
    """
    Map a free-text search query onto known genre families.
    'I need dark trap artists' -> {'rap'}
    'melodic rnb underground'  -> {'rnb'}
    """
    query_lower = query.lower()
    matched: set[str] = set()
    for fam, kws in GENRE_TAXONOMY.items():
        for kw in kws:
            if kw in query_lower:
                matched.add(fam)
                break
    return matched
```

---

```python
# core/scoring.py
"""
Artist scoring (0-100).
Do NOT simplify — recommendation quality is the product's core value.
Scores are used both for expansion decisions during BFS and for final ranking.
"""
from __future__ import annotations
import math

from config import (
    DISCOVERY_TARGET_MIN,
    DISCOVERY_TARGET_MAX,
    DISCOVERY_ABS_MAX,
    MAX_SPOTIFY_POPULARITY,
)
from core.genres import get_genre_families, genre_match_score


def compute_growth_signal(a: dict) -> tuple[int, str]:
    """
    Heuristic 0..100 growth/momentum proxy. No time-series data needed.
    Returns (score, reason_label).
    """
    pop       = a.get("popularity", 0)
    followers = a.get("followers", 0)
    listeners = a.get("listeners", 0)
    playcount = a.get("playcount", 0)
    ratio     = (playcount / listeners) if listeners else 0

    score   = 0
    reasons: list[str] = []

    # Momentum: Spotify popularity higher than audience size implies.
    if followers > 0:
        expected_pop = 10 * math.log10(followers + 10)
        if pop >= expected_pop + 12:
            score += 40
            reasons.append("popularity_above_audience")
        elif pop >= expected_pop:
            score += 20
            reasons.append("mild_momentum")
    elif pop >= 20:
        score += 30
        reasons.append("popularity_without_followers")

    # Last.fm engagement loyalty (plays per listener).
    if ratio >= 30:
        score += 30
        reasons.append("very_high_engagement")
    elif ratio >= 15:
        score += 20
        reasons.append("high_engagement")
    elif ratio >= 8:
        score += 10
        reasons.append("moderate_engagement")

    # Small but real emerging traction.
    if 0 < followers < 25_000 and pop >= 25:
        score += 20
        reasons.append("emerging_traction")

    score = min(score, 100)
    return score, (reasons[0] if reasons else "stable")


def score_artist(
    a: dict,
    target_families: set[str] | None = None,
) -> tuple[int, dict]:
    """
    Score an artist 0-100 across five dimensions.
    Mutates `a` to store derived signals (growth, genre match, families).
    Returns (total_score, breakdown_dict).
    """
    listeners  = a["listeners"]
    playcount  = a["playcount"]
    popularity = a.get("popularity", 0)
    has_spotify = bool(a.get("spotify_id"))

    # ── Derived signals (stored on the artist dict) ───────────────────────────
    growth, growth_reason = compute_growth_signal(a)
    a["growth_signal"]        = growth
    a["growth_signal_reason"] = growth_reason

    fams   = get_genre_families(a.get("tags"), a.get("genres"))
    gmatch = genre_match_score(fams, target_families or set())
    a["genre_match_score"] = round(gmatch, 3)
    a["genre_families"]    = sorted(fams)

    # ── 1) Genre relevance — max 25 (most important for producers) ────────────
    gscore = round(gmatch * 25)

    # ── 2) Growth signal — max 25 ─────────────────────────────────────────────
    gr = round(growth / 100 * 25)

    # ── 3) Size sweet-spot 5k–20k listeners — max 25 ─────────────────────────
    if DISCOVERY_TARGET_MIN <= listeners <= DISCOVERY_TARGET_MAX:
        sz = 25
    elif 0 < listeners < DISCOVERY_TARGET_MIN:
        sz = 18
    elif DISCOVERY_TARGET_MAX < listeners <= DISCOVERY_ABS_MAX:
        sz = 10
    elif listeners == 0:
        sz = 8
    else:
        sz = 0

    # ── 4) Spotify popularity emerging sweet-spot — max 20 ───────────────────
    if not has_spotify:
        p = 0
    elif popularity == 0:
        p = 5
    elif popularity < 15:
        p = 16
    elif popularity < 35:
        p = 20
    elif popularity <= MAX_SPOTIFY_POPULARITY:
        p = 12
    else:
        p = 4

    # ── 5) Last.fm engagement ratio — max 5 ──────────────────────────────────
    ratio = (playcount / listeners) if listeners else 0
    if ratio >= 20:
        e = 5
    elif ratio >= 8:
        e = 3
    else:
        e = 1

    total = min(gscore + gr + sz + p + e, 100)
    return total, {
        "genre_relevance":   gscore,
        "growth":            gr,
        "size":              sz,
        "spotify_popularity": p,
        "engagement":        e,
    }
```

---

```python
# core/dedup.py
"""
Three-pass artist deduplication.
Pass 1: exact normalized key.
Pass 2: Spotify ID (handles alias / casing / diacritic variants).
Pass 3: fuzzy near-duplicate names.
Keeps the record with the most complete data.
"""
from __future__ import annotations
from utils.names import norm_key, name_similarity


def _completeness(a: dict) -> tuple:
    """Higher is better — used to pick the winner when two records conflict."""
    return (
        1 if a.get("spotify_id") else 0,
        a.get("score", 0),
        a.get("listeners", 0),
    )


def dedupe_artists(
    artists: list[dict],
    fuzzy_threshold: float = 0.92,
) -> list[dict]:
    by_key:     dict[str, dict] = {}
    by_spotify: dict[str, dict] = {}

    for a in artists:
        key = norm_key(a["name"])
        if not key:
            continue

        sid = a.get("spotify_id")
        if sid and sid in by_spotify:
            if _completeness(a) > _completeness(by_spotify[sid]):
                by_spotify[sid] = a
            continue

        if key in by_key:
            if _completeness(a) > _completeness(by_key[key]):
                by_key[key] = a
        else:
            by_key[key] = a

        if sid:
            by_spotify[sid] = a

    # Reconcile: Spotify-ID-grouped winner takes precedence.
    merged: dict[str, dict] = {}
    for a in list(by_key.values()) + list(by_spotify.values()):
        key = norm_key(a["name"])
        if key not in merged or _completeness(a) > _completeness(merged[key]):
            merged[key] = a

    # Fuzzy pass: catch near-duplicate names that survived earlier passes.
    result: list[dict] = []
    for a in merged.values():
        dup_of = None
        for kept in result:
            if a.get("spotify_id") and a["spotify_id"] == kept.get("spotify_id"):
                dup_of = kept
                break
            if name_similarity(a["name"], kept["name"]) >= fuzzy_threshold:
                dup_of = kept
                break
        if dup_of is None:
            result.append(a)
        elif _completeness(a) > _completeness(dup_of):
            result[result.index(dup_of)] = a

    return result
```

---

```python
# core/filters.py
"""
Post-discovery filter logic.
Filters run AFTER enough candidates are collected — never during BFS.
All conditions are AND — a None value means 'no filter applied'.
"""
from __future__ import annotations
from typing import Optional


def apply_filters(
    artists: list[dict],
    *,
    max_followers:      Optional[int]       = None,
    min_followers:      Optional[int]       = None,
    max_popularity:     Optional[int]       = None,
    min_popularity:     Optional[int]       = None,
    min_listeners:      Optional[int]       = None,
    max_listeners:      Optional[int]       = None,
    genre_families:     Optional[list[str]] = None,
    has_spotify:        Optional[bool]      = None,
    has_instagram:      Optional[bool]      = None,
    has_tiktok:         Optional[bool]      = None,
    has_youtube:        Optional[bool]      = None,
    has_website:        Optional[bool]      = None,
    min_growth_signal:  Optional[int]       = None,
    tags_include:       Optional[list[str]] = None,
    tags_exclude:       Optional[list[str]] = None,
) -> list[dict]:
    result: list[dict] = []

    for a in artists:
        followers  = a.get("followers", 0)
        popularity = a.get("popularity", 0)
        listeners  = a.get("listeners", 0)

        if max_followers  is not None and followers  > max_followers:
            continue
        if min_followers  is not None and followers  < min_followers:
            continue
        if max_popularity is not None and popularity > max_popularity:
            continue
        if min_popularity is not None and popularity < min_popularity:
            continue
        if max_listeners  is not None and listeners  > max_listeners:
            continue
        if min_listeners  is not None and listeners  < min_listeners:
            continue

        if genre_families is not None:
            a_fams = set(a.get("genre_families", []))
            if not a_fams.intersection(set(genre_families)):
                continue

        if has_spotify is True  and not a.get("spotify_id"):
            continue
        if has_spotify is False and a.get("spotify_id"):
            continue

        socials = a.get("socials") or {}
        if has_instagram is True and not socials.get("instagram"):
            continue
        if has_tiktok    is True and not socials.get("tiktok"):
            continue
        if has_youtube   is True and not socials.get("youtube"):
            continue
        if has_website   is True and not socials.get("website"):
            continue

        if (
            min_growth_signal is not None
            and a.get("growth_signal", 0) < min_growth_signal
        ):
            continue

        artist_tags = {t.lower() for t in (a.get("tags") or [])}
        if tags_include and not any(t.lower() in artist_tags for t in tags_include):
            continue
        if tags_exclude and any(t.lower() in artist_tags for t in tags_exclude):
            continue

        result.append(a)

    return result
```

---

```python
# core/discovery.py
"""
Underground-only BFS crawl over Last.fm 'similar artists'.

Core algorithm — DO NOT simplify.
Level-by-level breadth-first search with:
  - Score-gated expansion (better artists get more API budget)
  - Hard underground gate with zero exceptions
  - Full discovery path recorded per artist
  - Variable fanout based on artist quality
"""
from __future__ import annotations
import logging
import math
from collections import Counter
from typing import TYPE_CHECKING

from config import (
    EXPAND_SCORE_THRESHOLD,
    EXPAND_TOP_FRACTION,
    SEED_FANOUT,
    DISCOVERY_ABS_MAX,
    MAX_SPOTIFY_FOLLOWERS,
    MAX_SPOTIFY_POPULARITY,
)
from utils.names import primary_artist_name, norm_key
from core.scoring import score_artist

if TYPE_CHECKING:
    from services.lastfm import LastFMService
    from services.spotify import SpotifyService

logger = logging.getLogger(__name__)


# ── Underground Gate ──────────────────────────────────────────────────────────

def is_too_big(sp: dict) -> bool:
    """ZERO-exception hard gate. True => never save, never propagate."""
    return (
        sp.get("followers", 0) > MAX_SPOTIFY_FOLLOWERS
        or sp.get("popularity", 0) > MAX_SPOTIFY_POPULARITY
    )


def is_within_discovery_scope(a: dict) -> bool:
    """Final safety net before saving or returning results."""
    return (
        a.get("followers", 0) <= MAX_SPOTIFY_FOLLOWERS
        and a.get("popularity", 0) <= MAX_SPOTIFY_POPULARITY
    )


# ── Variable Fanout ───────────────────────────────────────────────────────────

def fanout_for_score(score: int) -> int:
    """
    API budget per node scales with quality.
    High-scoring artists explore more of the graph.
    """
    if score > 90:
        return 10
    if score >= 80:
        return 6
    if score >= 70:
        return 4
    return 2


# ── Artist Builder ────────────────────────────────────────────────────────────

def build_artist(
    lf: dict,
    sp: dict,
    target_families: set[str] | None = None,
) -> dict:
    """Merge Last.fm + Spotify data into a single scored artist dict."""
    a: dict = {
        "name":                 lf["name"],
        "mbid":                 lf.get("mbid"),
        "listeners":            lf["listeners"],
        "playcount":            lf["playcount"],
        "tags":                 lf["tags"],
        "genres":               sp["genres"],
        "url":                  sp["url"],
        "image":                sp["image"],
        "popularity":           sp["popularity"],
        "followers":            sp["followers"],
        "spotify_id":           sp["spotify_id"],
        "match_score":          sp["match_score"],
        "discovered_from":      lf.get("discovered_from"),
        "score":                0,
        "score_breakdown":      {},
        "growth_signal":        0,
        "growth_signal_reason": "stable",
        "genre_match_score":    0.0,
        "genre_families":       [],
        "socials":              {},
    }
    a["score"], a["score_breakdown"] = score_artist(a, target_families)
    return a


# ── BFS Crawl ─────────────────────────────────────────────────────────────────

def discover(
    seeds: list[str],
    lastfm_svc: "LastFMService",
    spotify_svc: "SpotifyService",
    *,
    depth: int = 2,
    per_artist: int = 12,
    hard_cap: int = 300,
    max_per_origin: int = 40,
    target_families: set[str] | None = None,
    expand_score_threshold: int = EXPAND_SCORE_THRESHOLD,
    expand_top_fraction: float = EXPAND_TOP_FRACTION,
    seed_fanout: int = SEED_FANOUT,
) -> list[dict]:
    """
    Underground-only BFS crawl.

    Strategy:
    - True level-by-level BFS — stays broad, doesn't tunnel deep.
    - Each node is scored first; expansion is score-gated.
    - Better artists receive more API budget (fanout_for_score).
    - Full discovery path is recorded per artist.
    - Hard underground gate: followers > 50k OR popularity > 50 => skip immediately,
      never save, never expand (except level-0 seeds which can start a branch).

    Returns raw Last.fm dicts (with '_spotify' cached on each) for enrichment.
    """
    seen:          set[str]  = set()
    keep:          list[dict] = []
    origin_counts: Counter   = Counter()

    # Build initial frontier from seeds (unique, normalized).
    frontier:      list[tuple[str, str, list[str]]] = []
    seen_frontier: set[str] = set()
    for s in seeds:
        disp = primary_artist_name(s)
        k = norm_key(disp)
        if k and k not in seen_frontier:
            frontier.append((disp, disp, []))
            seen_frontier.add(k)

    for level in range(depth + 1):
        if not frontier or len(seen) >= hard_cap:
            break

        # scored_nodes: (score, lf_dict, display_name, origin_seed, full_path)
        scored_nodes: list[tuple[int, dict, str, str, list[str]]] = []

        for name, origin, path in frontier:
            if len(seen) >= hard_cap:
                break
            key = norm_key(name)
            if not key or key in seen:
                continue
            seen.add(key)

            lf        = lastfm_svc.get_info(name)
            full_path = path + [lf["name"]]
            lf["discovered_from"] = " → ".join(full_path)
            listeners = lf["listeners"]

            # Spotify enrichment is required here for the underground gate check.
            sp         = spotify_svc.get_artist(lf["name"])
            lf["_spotify"] = sp
            followers  = sp.get("followers", 0)
            popularity = sp.get("popularity", 0)

            # ── Hard underground gate ─────────────────────────────────────────
            if is_too_big(sp):
                if level == 0:
                    # Seed entry node: not saved, but may begin a branch.
                    logger.debug(
                        "Seed-node (not saved): %s  followers=%d  pop=%d",
                        lf["name"], followers, popularity,
                    )
                    scored_nodes.append((-1, lf, lf["name"], origin, full_path))
                else:
                    logger.debug(
                        "STOP (too big): %s  followers=%d  pop=%d",
                        lf["name"], followers, popularity,
                    )
                continue

            # ── Underground artist ────────────────────────────────────────────
            if 0 < listeners <= DISCOVERY_ABS_MAX and origin_counts[origin] < max_per_origin:
                keep.append(lf)
                origin_counts[origin] += 1

            # Score now to drive expansion decisions.
            a = build_artist(lf, sp, target_families)
            scored_nodes.append((a["score"], lf, lf["name"], origin, full_path))

            logger.debug(
                "scanned: %s  listeners=%d  followers=%d  pop=%d  score=%d",
                lf["name"], listeners, followers, popularity, a["score"],
            )

        # ── Build next frontier (score-gated, variable fanout) ────────────────
        if level < depth and scored_nodes:
            if level == 0:
                # Seeds always expand regardless of score.
                expandable = list(scored_nodes)
            else:
                by_score = sorted(scored_nodes, key=lambda x: x[0], reverse=True)
                top_n    = max(1, math.ceil(len(by_score) * expand_top_fraction))
                expandable = [
                    node for i, node in enumerate(by_score)
                    if node[0] >= expand_score_threshold or i < top_n
                ]

            next_frontier: list[tuple[str, str, list[str]]] = []
            next_keys:     set[str] = set()

            for score, _lf, pname, origin, full_path in expandable:
                fanout = seed_fanout if level == 0 else fanout_for_score(score)
                sims   = lastfm_svc.get_similar(pname, per_artist)
                added  = 0
                for sim in sims:
                    if added >= fanout:
                        break
                    sk = norm_key(sim)
                    if not sk or sk in seen or sk in next_keys:
                        continue
                    next_frontier.append((sim, origin, full_path))
                    next_keys.add(sk)
                    added += 1

            frontier = next_frontier
        else:
            frontier = []

    return keep
```

---

```python
# services/__init__.py
```

---

```python
# services/cache.py
"""
Thread-safe TTL in-memory cache.
Shared across all requests in the process — the main performance lever.
For multi-process deployments, swap this for a Redis-backed implementation
without changing any call sites.
"""
from __future__ import annotations
import time
import threading
from typing import Any, Optional


class TTLCache:
    """
    Simple dict-backed cache with per-entry TTL and a max-size eviction policy.
    Thread-safe via a single reentrant lock.
    """

    def __init__(self, default_ttl: int = 3600, max_size: int = 10_000):
        self._store:       dict[str, tuple[Any, float]] = {}
        self._lock         = threading.RLock()
        self._default_ttl  = default_ttl
        self._max_size     = max_size

    # ── Public API ────────────────────────────────────────────────────────────

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expires_at = entry
            if time.monotonic() > expires_at:
                del self._store[key]
                return None
            return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        with self._lock:
            if len(self._store) >= self._max_size:
                self._evict()
            expires_at = time.monotonic() + (ttl if ttl is not None else self._default_ttl)
            self._store[key] = (value, expires_at)

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._store)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _evict(self) -> None:
        """Remove all expired entries; if still over limit, drop oldest 10%."""
        now     = time.monotonic()
        expired = [k for k, (_, exp) in self._store.items() if exp < now]
        for k in expired:
            del self._store[k]
        if len(self._store) >= self._max_size:
            oldest = sorted(self._store.items(), key=lambda x: x[1][1])
            for k, _ in oldest[: max(1, self._max_size // 10)]:
                del self._store[k]


# ── Module-level singletons used by all services ─────────────────────────────
from config import CACHE_TTL_LASTFM, CACHE_TTL_SPOTIFY, CACHE_TTL_SOCIAL, CACHE_MAX_SIZE

lastfm_cache  = TTLCache(default_ttl=CACHE_TTL_LASTFM,  max_size=CACHE_MAX_SIZE)
spotify_cache = TTLCache(default_ttl=CACHE_TTL_SPOTIFY, max_size=CACHE_MAX_SIZE)
social_cache  = TTLCache(default_ttl=CACHE_TTL_SOCIAL,  max_size=CACHE_MAX_SIZE // 5)
```

---

```python
# services/lastfm.py
"""
Last.fm API service.
All responses are cached; retries and rate-limiting are handled transparently.
"""
from __future__ import annotations
import logging
import time
from typing import Optional

import requests

from config import (
    LASTFM_API_KEY,
    LASTFM_BASE_URL,
    HTTP_TIMEOUT,
    MAX_RETRIES,
    RATE_LIMIT_SLEEP,
    CACHE_TTL_LASTFM,
)
from services.cache import lastfm_cache
from utils.names import primary_artist_name

logger = logging.getLogger(__name__)

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "music-discovery/2.0"})


# ── Internal helpers ──────────────────────────────────────────────────────────

def _as_list(value) -> list:
    """Last.fm returns a dict for one item, a list for many — normalize both."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _get(params: dict) -> Optional[dict]:
    """GET with retries, timeout, and polite rate limiting."""
    full_params = {**params, "api_key": LASTFM_API_KEY, "format": "json"}
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(RATE_LIMIT_SLEEP)
            r    = _SESSION.get(LASTFM_BASE_URL, params=full_params, timeout=HTTP_TIMEOUT)
            data = r.json()
            if isinstance(data, dict) and "error" in data:
                logger.debug("Last.fm error %s: %s", data.get("error"), data.get("message"))
                return None
            return data
        except (requests.RequestException, ValueError) as exc:
            logger.warning("Last.fm attempt %d failed: %s", attempt + 1, exc)
            time.sleep(0.5 * (attempt + 1))
    return None


# ── Service class ─────────────────────────────────────────────────────────────

class LastFMService:
    """Cached wrapper around the Last.fm REST API."""

    # ── artist.getinfo ────────────────────────────────────────────────────────
    def get_info(self, artist: str) -> dict:
        cache_key = f"lfm:info:{artist.lower().strip()}"
        cached    = lastfm_cache.get(cache_key)
        if cached is not None:
            return cached

        fallback = {
            "name":      primary_artist_name(artist),
            "mbid":      None,
            "listeners": 0,
            "playcount": 0,
            "tags":      [],
        }
        data = _get({"method": "artist.getinfo", "artist": artist})
        if not data or "artist" not in data:
            lastfm_cache.set(cache_key, fallback, ttl=300)   # short TTL on miss
            return fallback

        info  = data["artist"]
        stats = info.get("stats") or {}
        tags  = _as_list((info.get("tags") or {}).get("tag"))

        result = {
            "name":      primary_artist_name(info.get("name", artist)),
            "mbid":      info.get("mbid") or None,
            "listeners": _safe_int(stats.get("listeners")),
            "playcount": _safe_int(stats.get("playcount")),
            "tags":      [t.get("name", "").lower() for t in tags[:10] if t.get("name")],
        }
        lastfm_cache.set(cache_key, result, ttl=CACHE_TTL_LASTFM)
        return result

    # ── artist.getsimilar ─────────────────────────────────────────────────────
    def get_similar(self, artist: str, limit: int = 10) -> list[str]:
        cache_key = f"lfm:sim:{artist.lower().strip()}:{limit}"
        cached    = lastfm_cache.get(cache_key)
        if cached is not None:
            return cached

        data = _get({"method": "artist.getsimilar", "artist": artist, "limit": limit})
        if not data:
            lastfm_cache.set(cache_key, [], ttl=300)
            return []

        raw  = _as_list(data.get("similarartists", {}).get("artist"))
        out  = [primary_artist_name(a.get("name", "")) for a in raw if a.get("name")]
        out  = [n for n in out if n]
        lastfm_cache.set(cache_key, out, ttl=CACHE_TTL_LASTFM)
        return out

    # ── artist.search ─────────────────────────────────────────────────────────
    def search_artists(self, query: str, limit: int = 10) -> list[str]:
        cache_key = f"lfm:search:{query.lower().strip()}:{limit}"
        cached    = lastfm_cache.get(cache_key)
        if cached is not None:
            return cached

        data = _get({"method": "artist.search", "artist": query, "limit": limit})
        if not data:
            return []

        matches = _as_list(
            (data.get("results", {}).get("artistmatches") or {}).get("artist")
        )
        out = [primary_artist_name(a.get("name", "")) for a in matches if a.get("name")]
        lastfm_cache.set(cache_key, out, ttl=CACHE_TTL_LASTFM)
        return out


# ── Module-level singleton ────────────────────────────────────────────────────
lastfm_service = LastFMService()
```

---

```python
# services/spotify.py
"""
Spotify API service.
Uses best-match selection (not blind first-result) and caches all responses.
"""
from __future__ import annotations
import logging
import time
from typing import Optional

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from config import (
    SPOTIFY_CLIENT_ID,
    SPOTIFY_CLIENT_SECRET,
    RATE_LIMIT_SLEEP,
    HTTP_TIMEOUT,
    CACHE_TTL_SPOTIFY,
)
from services.cache import spotify_cache
from utils.names import name_similarity, norm_key

logger = logging.getLogger(__name__)

_EMPTY: dict = {
    "url":          None,
    "image":        None,
    "genres":       [],
    "popularity":   0,
    "followers":    0,
    "spotify_id":   None,
    "matched_name": None,
    "match_score":  0.0,
}


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class SpotifyService:
    """Cached wrapper around the Spotify Web API (via spotipy)."""

    def __init__(self) -> None:
        self._sp: Optional[spotipy.Spotify] = None

    def _client(self) -> spotipy.Spotify:
        """Lazy initialisation — avoids crashing at import time if creds missing."""
        if self._sp is None:
            self._sp = spotipy.Spotify(
                auth_manager=SpotifyClientCredentials(
                    client_id=SPOTIFY_CLIENT_ID,
                    client_secret=SPOTIFY_CLIENT_SECRET,
                ),
                requests_timeout=HTTP_TIMEOUT,
                retries=3,
            )
        return self._sp

    # ── Search by name ────────────────────────────────────────────────────────
    def get_artist(self, artist_name: str) -> dict:
        """
        Search and pick the BEST matching artist.
        Prefers higher name similarity; breaks ties by popularity/followers.
        Rejects matches below 0.60 similarity to avoid false enrichment.
        """
        cache_key = f"sp:name:{norm_key(artist_name)}"
        cached    = spotify_cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            time.sleep(RATE_LIMIT_SLEEP)
            items = self._client().search(
                q=artist_name, type="artist", limit=5
            )["artists"]["items"]
        except Exception as exc:
            logger.warning("Spotify search failed for '%s': %s", artist_name, exc)
            return _EMPTY.copy()

        if not items:
            result = _EMPTY.copy()
            spotify_cache.set(cache_key, result, ttl=300)
            return result

        best, best_sim = None, 0.0
        for a in items:
            sim = name_similarity(artist_name, a.get("name", ""))
            if sim > best_sim + 0.05 or (
                abs(sim - best_sim) <= 0.05
                and best is not None
                and a.get("followers", {}).get("total", 0)
                    > best.get("followers", {}).get("total", 0)
            ):
                best, best_sim = a, sim

        if best is None or best_sim < 0.60:
            result = _EMPTY.copy()
            spotify_cache.set(cache_key, result, ttl=300)
            return result

        result = self._format(best, best_sim)
        spotify_cache.set(cache_key, result, ttl=CACHE_TTL_SPOTIFY)
        return result

    # ── Fetch by Spotify ID ───────────────────────────────────────────────────
    def get_artist_by_id(self, spotify_id: str) -> dict:
        cache_key = f"sp:id:{spotify_id}"
        cached    = spotify_cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            time.sleep(RATE_LIMIT_SLEEP)
            a = self._client().artist(spotify_id)
        except Exception as exc:
            logger.warning("Spotify fetch failed for ID '%s': %s", spotify_id, exc)
            return _EMPTY.copy()

        result = self._format(a, 1.0)
        spotify_cache.set(cache_key, result, ttl=CACHE_TTL_SPOTIFY)
        return result

    # ── Internal ──────────────────────────────────────────────────────────────
    @staticmethod
    def _format(a: dict, match_score: float) -> dict:
        imgs = a.get("images", [])
        return {
            "url":          a.get("external_urls", {}).get("spotify"),
            "image":        imgs[0]["url"] if imgs else None,
            "genres":       [g.lower() for g in a.get("genres", [])],
            "popularity":   _safe_int(a.get("popularity")),
            "followers":    _safe_int(a.get("followers", {}).get("total")),
            "spotify_id":   a.get("id"),
            "matched_name": a.get("name"),
            "match_score":  round(match_score, 3),
        }


# ── Module-level singleton ────────────────────────────────────────────────────
spotify_service = SpotifyService()
```

---

```python
# services/social.py
"""
Social links discovery via MusicBrainz URL relationship data.

Strategy:
  1. If artist has a MusicBrainz ID (from Last.fm), fetch URL rels directly.
  2. Otherwise, search MusicBrainz by name and use the top result.
  3. Never guess — return None for any platform not confidently found.

MusicBrainz rate limit: 1 request/second (we use 1.1s gap to be safe).
"""
from __future__ import annotations
import logging
import time
from typing import Optional

import requests

from config import MUSICBRAINZ_BASE, HTTP_TIMEOUT, MAX_RETRIES, CACHE_TTL_SOCIAL
from services.cache import social_cache

logger = logging.getLogger(__name__)

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": "music-discovery/2.0 (https://github.com/your-repo)",
    "Accept":     "application/json",
})

_MB_RATE_LIMIT = 1.1  # seconds — MusicBrainz policy

_PLATFORM_DOMAINS: dict[str, list[str]] = {
    "instagram": ["instagram.com"],
    "tiktok":    ["tiktok.com"],
    "youtube":   ["youtube.com", "youtu.be"],
    "twitter":   ["twitter.com", "x.com"],
    "facebook":  ["facebook.com"],
}


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _mb_get(url: str, params: Optional[dict] = None) -> Optional[dict]:
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(_MB_RATE_LIMIT)
            r = _SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                time.sleep(15)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            logger.debug("MusicBrainz attempt %d failed: %s", attempt + 1, exc)
            time.sleep(1.5 * (attempt + 1))
    return None


# ── URL classification ────────────────────────────────────────────────────────

def _classify_url(url: str) -> Optional[str]:
    url_lower = url.lower()
    for platform, domains in _PLATFORM_DOMAINS.items():
        if any(d in url_lower for d in domains):
            return platform
    return None


def _extract_socials(data: dict) -> dict:
    socials: dict[str, Optional[str]] = {
        "instagram": None,
        "tiktok":    None,
        "youtube":   None,
        "twitter":   None,
        "facebook":  None,
        "website":   None,
    }
    for rel in data.get("relations", []):
        url      = (rel.get("url") or {}).get("resource", "")
        rel_type = rel.get("type", "").lower()
        if not url:
            continue
        platform = _classify_url(url)
        if platform and not socials[platform]:
            socials[platform] = url
        elif "official homepage" in rel_type and not socials["website"]:
            socials["website"] = url
    return socials


# ── Public API ────────────────────────────────────────────────────────────────

def get_socials_by_mbid(mbid: str) -> dict:
    """Fetch social links by MusicBrainz ID (most reliable path)."""
    if not mbid:
        return {}
    cache_key = f"social:mbid:{mbid}"
    cached    = social_cache.get(cache_key)
    if cached is not None:
        return cached

    data = _mb_get(f"{MUSICBRAINZ_BASE}/artist/{mbid}", params={"inc": "url-rels", "fmt": "json"})
    if not data:
        social_cache.set(cache_key, {}, ttl=3600)
        return {}

    result = _extract_socials(data)
    social_cache.set(cache_key, result, ttl=CACHE_TTL_SOCIAL)
    return result


def get_socials_by_name(name: str) -> dict:
    """Fallback: search MusicBrainz by artist name then fetch URL rels."""
    if not name:
        return {}
    cache_key = f"social:name:{name.lower().strip()}"
    cached    = social_cache.get(cache_key)
    if cached is not None:
        return cached

    data = _mb_get(
        f"{MUSICBRAINZ_BASE}/artist",
        params={"query": name, "limit": 3, "fmt": "json"},
    )
    if not data or not data.get("artists"):
        social_cache.set(cache_key, {}, ttl=3600)
        return {}

    mbid = data["artists"][0].get("id")
    if not mbid:
        social_cache.set(cache_key, {}, ttl=3600)
        return {}

    result = get_socials_by_mbid(mbid)
    social_cache.set(cache_key, result, ttl=CACHE_TTL_SOCIAL)
    return result


def enrich_with_socials(artist: dict) -> dict:
    """
    Add a 'socials' dict to an artist record.
    Uses MBID (from Last.fm) when available — falls back to name search.
    Mutates the artist dict in place and returns it.
    """
    mbid = artist.get("mbid")
    artist["socials"] = get_socials_by_mbid(mbid) if mbid else get_socials_by_name(artist.get("name", ""))
    return artist
```

---

```python
# db/__init__.py
```

---

```python
# db/database.py
"""
Supabase database operations.
Supabase client is lazily initialised — app works without DB if creds are absent.
"""
from __future__ import annotations
import logging
import random
from typing import Optional

from config import SUPABASE_URL, SUPABASE_KEY, MAX_SPOTIFY_FOLLOWERS, MAX_SPOTIFY_POPULARITY

logger = logging.getLogger(__name__)

_client = None


def get_client():
    global _client
    if _client is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_KEY must be set for database operations."
            )
        from supabase import create_client
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client


# ── Scope guard ───────────────────────────────────────────────────────────────

def _within_scope(a: dict) -> bool:
    return (
        a.get("followers", 0) <= MAX_SPOTIFY_FOLLOWERS
        and a.get("popularity", 0) <= MAX_SPOTIFY_POPULARITY
    )


# ── Row builder ───────────────────────────────────────────────────────────────

def _artist_row(a: dict) -> dict:
    return {
        "name":                 a["name"],
        "listeners":            a["listeners"],
        "playcount":            a["playcount"],
        "tags":                 a.get("tags", []),
        "genres":               a.get("genres", []),
        "url":                  a.get("url"),
        "image":                a.get("image"),
        "score":                a.get("score", 0),
        "spotify_id":           a.get("spotify_id"),
        "spotify_followers":    a.get("followers", 0),
        "spotify_popularity":   a.get("popularity", 0),
        "growth_signal":        a.get("growth_signal", 0),
        "growth_signal_reason": a.get("growth_signal_reason", "stable"),
        "genre_match_score":    a.get("genre_match_score", 0.0),
        "match_score":          a.get("match_score", 0.0),
        "discovered_from":      a.get("discovered_from"),
        "socials":              a.get("socials", {}),
    }


# ── Write operations ──────────────────────────────────────────────────────────

def save_artist(a: dict) -> None:
    if not _within_scope(a):
        return
    try:
        get_client().table("artists").upsert(_artist_row(a), on_conflict="name").execute()
    except Exception as exc:
        logger.error("save_artist failed for '%s': %s", a.get("name"), exc)


def save_artists_bulk(artists: list[dict], batch_size: int = 100) -> int:
    rows  = [_artist_row(a) for a in artists if _within_scope(a)]
    saved = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            get_client().table("artists").upsert(batch, on_conflict="name").execute()
            saved += len(batch)
        except Exception as exc:
            logger.error("save_artists_bulk failed at batch %d: %s", i, exc)
    return saved


# ── Seed read (Mode B) ────────────────────────────────────────────────────────

def get_seeds_from_db(
    genre_families: Optional[list[str]] = None,
    sample_size: int = 10,
) -> list[str]:
    """
    Fetch seeds from the 'seeds' table.
    Shuffled before return to prevent bias toward earlier-inserted rows.
    Expected schema: id, name, genre_family, active (bool)
    """
    try:
        query = get_client().table("seeds").select("name").eq("active", True)
        if genre_families:
            query = query.in_("genre_family", genre_families)
        result = query.execute()
        names  = [row["name"] for row in (result.data or [])]
        random.shuffle(names)
        return names[:sample_size]
    except Exception as exc:
        logger.error("get_seeds_from_db failed: %s", exc)
        return []
```

---

```python
# db/seeds.py
"""
Seed management.

Mode A (default): seeds.txt file on disk + in-memory SEED_BANK keyed by genre.
Mode B:           seeds pulled from Supabase 'seeds' table.

Switch via SEED_MODE env var: "file" | "database"
Both modes shuffle results to prevent identical runs.
"""
from __future__ import annotations
import logging
import random
from typing import Optional

from config import SEED_MODE, SEEDS_FILE, SEEDS_SAMPLE_SIZE

logger = logging.getLogger(__name__)


# ── Mode A: Genre-keyed seed bank ─────────────────────────────────────────────
SEED_BANK: dict[str, list[str]] = {
    "rap": [
        "Yeat", "Playboi Carti", "Ken Carson", "Destroy Lonely",
        "Summrs", "Autumn!", "Rylo Rodriguez", "NoCap",
        "Lil Tecca", "Cochise", "9lokknine", "Lil Zay Osama",
        "YSL Shun", "Nardo Wick", "BabyDrill", "Nettspend",
        "Lil Durk", "Polo G", "Rod Wave", "Quando Rondo",
    ],
    "drill": [
        "Fivio Foreign", "Sheff G", "Sleepy Hallow", "Bizzy Banks",
        "Headie One", "Unknown T", "M1llionz", "Central Cee",
        "Digga D", "Potter Payper", "Coi Leray",
    ],
    "electronic": [
        "Burial", "Four Tet", "Bicep", "Mall Grab", "Tirzah",
        "Objekt", "Actress", "Lone", "Com Truise", "Boards of Canada",
    ],
    "rnb": [
        "Giveon", "Lucky Daye", "Snoh Aalegra", "Ari Lennox",
        "Mahalia", "Syd", "VanJess", "Amaarae", "Brent Faiyaz",
    ],
    "hyperpop": [
        "glaive", "ericdoa", "100 gecs", "Charli xcx",
        "Dorian Electra", "Fraxiom", "Alice Gas", "underscores",
    ],
    "indie": [
        "Clairo", "Snail Mail", "Soccer Mommy", "Phoebe Bridgers",
        "beabadoobee", "Men I Trust", "Still Woozy", "Surfaces",
    ],
    "metal": [
        "Spiritbox", "Sleep Token", "Knocked Loose", "Code Orange",
        "Vein.fm", "Body Void", "Loathe",
    ],
    "punk": [
        "Spanish Love Songs", "Militarie Gun", "MSPAINT",
        "Amyl and the Sniffers", "Scowl",
    ],
    "folk": [
        "Adrianne Lenker", "Florist", "Bonny Light Horseman",
        "Watchhouse", "S. Carey",
    ],
}

_FALLBACK_SEEDS = [
    "Yeat", "glaive", "Cochise", "Lil Tecca", "Amaarae",
    "Burial", "Clairo", "Spiritbox", "Brent Faiyaz",
]


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_file_seeds() -> list[str]:
    """Load seed names from SEEDS_FILE (one artist per line)."""
    try:
        with open(SEEDS_FILE, encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logger.warning("Seeds file '%s' not found.", SEEDS_FILE)
        return []


def get_seeds(
    genre_families: Optional[set[str]] = None,
    sample_size: int = SEEDS_SAMPLE_SIZE,
    mode: Optional[str] = None,
) -> list[str]:
    """
    Return a randomized list of seeds for a discovery run.

    Mode B (database): queries Supabase 'seeds' table, shuffled.
    Mode A (file/bank): loads seeds.txt + genre-relevant SEED_BANK entries.

    genre_families: if provided, only seeds matching those families are selected.
    """
    effective_mode = mode or SEED_MODE

    # ── Mode B ───────────────────────────────────────────────────────────────
    if effective_mode == "database":
        from db.database import get_seeds_from_db
        db_seeds = get_seeds_from_db(
            genre_families=list(genre_families) if genre_families else None,
            sample_size=sample_size,
        )
        if db_seeds:
            return db_seeds
        logger.warning("DB seeds empty — falling back to file/bank.")

    # ── Mode A ───────────────────────────────────────────────────────────────
    file_seeds = load_file_seeds()

    bank_seeds: list[str] = []
    if genre_families:
        for fam in genre_families:
            bank_seeds.extend(SEED_BANK.get(fam, []))

    # Merge file + bank, preserve insertion order, deduplicate.
    combined = list(dict.fromkeys(file_seeds + bank_seeds))

    if not combined:
        combined = list(_FALLBACK_SEEDS)

    random.shuffle(combined)
    return combined[:sample_size] if len(combined) > sample_size else combined
```

---

```python
# api/__init__.py
```

---

```python
# api/models.py
"""Pydantic request/response models for the discovery API."""
from __future__ import annotations
from typing import Any, Optional
from pydantic import BaseModel, Field


# ── Request models ────────────────────────────────────────────────────────────

class FilterParams(BaseModel):
    """All filter fields are optional — None means 'no filter applied'."""
    max_followers:     Optional[int]       = Field(None, ge=0)
    min_followers:     Optional[int]       = Field(None, ge=0)
    max_popularity:    Optional[int]       = Field(None, ge=0, le=100)
    min_popularity:    Optional[int]       = Field(None, ge=0, le=100)
    min_listeners:     Optional[int]       = Field(None, ge=0)
    max_listeners:     Optional[int]       = Field(None, ge=0)
    genre_families:    Optional[list[str]] = None
    has_spotify:       Optional[bool]      = None
    has_instagram:     Optional[bool]      = None
    has_tiktok:        Optional[bool]      = None
    has_youtube:       Optional[bool]      = None
    has_website:       Optional[bool]      = None
    min_growth_signal: Optional[int]       = Field(None, ge=0, le=100)
    tags_include:      Optional[list[str]] = None
    tags_exclude:      Optional[list[str]] = None


class SearchRequest(BaseModel):
    query:          str                   = Field(..., min_length=1, max_length=500)
    filters:        FilterParams          = Field(default_factory=FilterParams)
    limit:          int                   = Field(50, ge=1, le=100)
    depth:          int                   = Field(2, ge=1, le=3)
    include_socials: bool                 = False
    # Power-user override: bypass seed selection, use these artists as seeds.
    seed_override:  Optional[list[str]]   = None


# ── Response models ───────────────────────────────────────────────────────────

class ArtistSocials(BaseModel):
    instagram: Optional[str] = None
    tiktok:    Optional[str] = None
    youtube:   Optional[str] = None
    twitter:   Optional[str] = None
    facebook:  Optional[str] = None
    website:   Optional[str] = None


class ArtistResult(BaseModel):
    name:                 str
    score:                int
    score_breakdown:      dict[str, Any]
    listeners:            int
    playcount:            int
    popularity:           int
    followers:            int
    tags:                 list[str]
    genres:               list[str]
    genre_families:       list[str]
    genre_match_score:    float
    growth_signal:        int
    growth_signal_reason: str
    url:                  Optional[str]
    image:                Optional[str]
    spotify_id:           Optional[str]
    match_score:          float
    discovered_from:      Optional[str]
    socials:              Optional[ArtistSocials] = None


class SearchResponse(BaseModel):
    artists:                list[ArtistResult]
    total:                  int
    query:                  str
    genre_families_detected: list[str]
    seeds_used:             list[str]
    cache_stats:            dict[str, int]


class HealthResponse(BaseModel):
    status:      str
    cache_sizes: dict[str, int]
    version:     str = "2.0.0"
```

---

```python
# api/routes/__init__.py
```

---

```python
# api/routes/search.py
"""
Main discovery endpoint.
Orchestrates: seed selection → BFS crawl → enrichment → dedup → filter → rank → serve.
"""
from __future__ import annotations
import logging
import random
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool

from api.models import (
    SearchRequest, SearchResponse,
    ArtistResult, ArtistSocials,
)
from core.discovery import discover, build_artist, is_within_discovery_scope
from core.genres import query_to_genre_families, get_genre_families
from core.dedup import dedupe_artists
from core.filters import apply_filters
from db.seeds import get_seeds
from services.lastfm import lastfm_service
from services.spotify import spotify_service
from services.social import enrich_with_socials
from services.cache import lastfm_cache, spotify_cache, social_cache

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["search"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_target_profile(seeds: list[str]) -> set[str]:
    """
    Auto-derive target genre families from seed artists.
    Results are cached at the service layer so repeat calls are instant.
    """
    fams: set[str] = set()
    for s in seeds:
        sp  = spotify_service.get_artist(s)
        lf  = lastfm_service.get_info(s)
        fams |= get_genre_families(lf.get("tags"), sp.get("genres"))
    return fams


def _diverse_sample(artists: list[dict], n: int) -> list[dict]:
    """
    Returns up to n artists balancing quality and variety.

    - Top 1/3 are always included (quality anchors).
    - Remaining slots are filled by score-weighted random sampling.
    - Final list is re-sorted by score for presentation.

    This makes repeated identical searches feel fresh while keeping
    the highest-quality artists always visible.
    """
    if len(artists) <= n:
        return artists[:]

    artists   = sorted(artists, key=lambda x: x["score"], reverse=True)
    anchor_n  = max(1, n // 3)
    anchors   = artists[:anchor_n]
    pool      = artists[anchor_n:]
    needed    = n - anchor_n

    if not pool or needed <= 0:
        return anchors

    # Weighted sampling without replacement from the pool.
    weights     = [max(1, a["score"]) for a in pool]
    pool_copy   = list(pool)
    wt_copy     = list(weights)
    seen_keys   = {a.get("spotify_id") or a["name"] for a in anchors}
    selected: list[dict] = []

    for _ in range(min(needed, len(pool_copy))):
        total = sum(wt_copy)
        if total <= 0:
            break
        r, cumsum, chosen = random.random() * total, 0.0, 0
        for idx, w in enumerate(wt_copy):
            cumsum += w
            if r <= cumsum:
                chosen = idx
                break
        candidate = pool_copy[chosen]
        key       = candidate.get("spotify_id") or candidate["name"]
        if key not in seen_keys:
            seen_keys.add(key)
            selected.append(candidate)
        pool_copy.pop(chosen)
        wt_copy.pop(chosen)

    result = anchors + selected
    result.sort(key=lambda x: x["score"], reverse=True)
    return result


def _to_artist_result(a: dict) -> ArtistResult:
    socials_raw = a.get("socials") or {}
    return ArtistResult(
        name                 = a["name"],
        score                = a["score"],
        score_breakdown      = a.get("score_breakdown", {}),
        listeners            = a.get("listeners", 0),
        playcount            = a.get("playcount", 0),
        popularity           = a.get("popularity", 0),
        followers            = a.get("followers", 0),
        tags                 = a.get("tags", []),
        genres               = a.get("genres", []),
        genre_families       = a.get("genre_families", []),
        genre_match_score    = a.get("genre_match_score", 0.0),
        growth_signal        = a.get("growth_signal", 0),
        growth_signal_reason = a.get("growth_signal_reason", "stable"),
        url                  = a.get("url"),
        image                = a.get("image"),
        spotify_id           = a.get("spotify_id"),
        match_score          = a.get("match_score", 0.0),
        discovered_from      = a.get("discovered_from"),
        socials              = ArtistSocials(**socials_raw) if socials_raw else None,
    )


# ── Core discovery orchestrator ───────────────────────────────────────────────

def _run_discovery(request: SearchRequest) -> tuple[list[dict], list[str], list[str]]:
    """
    Runs the full pipeline synchronously (called via run_in_threadpool).
    Returns (results, detected_genre_families, seeds_used).
    """
    # 1. Genre detection from free-text query
    query_families = query_to_genre_families(request.query)

    # 2. Seed selection
    if request.seed_override:
        seeds = [s.strip() for s in request.seed_override if s.strip()]
    else:
        seeds = get_seeds(
            genre_families=query_families if query_families else None,
            sample_size=10,
        )
    if not seeds:
        raise ValueError("No seeds available for this query.")

    # 3. Build genre target profile from seeds (cached API calls)
    target_families = _build_target_profile(seeds) | query_families

    # 4. BFS discovery crawl
    candidates = discover(
        seeds          = seeds,
        lastfm_svc     = lastfm_service,
        spotify_svc    = spotify_service,
        depth          = request.depth,
        per_artist     = 12,
        hard_cap       = 300,
        max_per_origin = 40,
        target_families = target_families,
    )

    # 5. Enrich and score all candidates
    enriched: list[dict] = []
    for c in candidates:
        sp = c.get("_spotify") or spotify_service.get_artist(c["name"])
        enriched.append(build_artist(c, sp, target_families))

    # 6. Deduplicate
    results = dedupe_artists(enriched)

    # 7. Hard scope guard
    results = [a for a in results if is_within_discovery_scope(a)]

    # 8. User-defined filters
    f = request.filters
    results = apply_filters(
        results,
        max_followers     = f.max_followers,
        min_followers     = f.min_followers,
        max_popularity    = f.max_popularity,
        min_popularity    = f.min_popularity,
        min_listeners     = f.min_listeners,
        max_listeners     = f.max_listeners,
        genre_families    = f.genre_families,
        has_spotify       = f.has_spotify,
        has_instagram     = f.has_instagram,
        has_tiktok        = f.has_tiktok,
        has_youtube       = f.has_youtube,
        has_website       = f.has_website,
        min_growth_signal = f.min_growth_signal,
        tags_include      = f.tags_include,
        tags_exclude      = f.tags_exclude,
    )

    # 9. Social links enrichment (only for final returned artists)
    if request.include_socials:
        results = [enrich_with_socials(a) for a in results]
        # Re-apply social filters now that we have real data
        results = apply_filters(
            results,
            has_instagram = f.has_instagram,
            has_tiktok    = f.has_tiktok,
            has_youtube   = f.has_youtube,
            has_website   = f.has_website,
        )

    # 10. Diverse sampling with quality guarantee
    results = _diverse_sample(results, request.limit)

    return results, sorted(target_families), seeds


# ── Route ─────────────────────────────────────────────────────────────────────

@router.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest) -> SearchResponse:
    """
    Main music discovery endpoint.

    Runs BFS graph exploration over Last.fm similar artists,
    enriches with Spotify data, scores, deduplicates, filters, and returns
    a ranked list of underground artists matching the query.
    """
    try:
        results, detected_families, seeds_used = await run_in_threadpool(
            _run_discovery, request
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Discovery pipeline failed: %s", exc)
        raise HTTPException(status_code=500, detail="Discovery failed. Please retry.")

    return SearchResponse(
        artists                 = [_to_artist_result(a) for a in results],
        total                   = len(results),
        query                   = request.query,
        genre_families_detected = detected_families,
        seeds_used              = seeds_used,
        cache_stats             = {
            "lastfm":  lastfm_cache.size,
            "spotify": spotify_cache.size,
            "social":  social_cache.size,
        },
    )
```

---

```python
# api/routes/artists.py
"""Single-artist lookup endpoint."""
from __future__ import annotations
import logging

from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool

from api.models import ArtistResult, ArtistSocials
from core.discovery import build_artist
from services.spotify import spotify_service
from services.lastfm import lastfm_service
from services.social import enrich_with_socials

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/artists", tags=["artists"])


def _fetch_artist(spotify_id: str, include_socials: bool) -> dict:
    sp = spotify_service.get_artist_by_id(spotify_id)
    if not sp.get("spotify_id"):
        raise LookupError("Artist not found on Spotify.")
    name = sp.get("matched_name") or ""
    lf   = lastfm_service.get_info(name)
    lf["name"] = name
    a = build_artist(lf, sp)
    if include_socials:
        enrich_with_socials(a)
    return a


@router.get("/{spotify_id}", response_model=ArtistResult)
async def get_artist(spotify_id: str, include_socials: bool = False) -> ArtistResult:
    """Fetch a single artist by Spotify ID, enriched with Last.fm and social data."""
    try:
        a = await run_in_threadpool(_fetch_artist, spotify_id, include_socials)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        logger.exception("Artist fetch failed for '%s': %s", spotify_id, exc)
        raise HTTPException(status_code=500, detail=str(exc))

    socials_raw = a.get("socials") or {}
    return ArtistResult(
        name                 = a["name"],
        score                = a["score"],
        score_breakdown      = a.get("score_breakdown", {}),
        listeners            = a.get("listeners", 0),
        playcount            = a.get("playcount", 0),
        popularity           = a.get("popularity", 0),
        followers            = a.get("followers", 0),
        tags                 = a.get("tags", []),
        genres               = a.get("genres", []),
        genre_families       = a.get("genre_families", []),
        genre_match_score    = a.get("genre_match_score", 0.0),
        growth_signal        = a.get("growth_signal", 0),
        growth_signal_reason = a.get("growth_signal_reason", "stable"),
        url                  = a.get("url"),
        image                = a.get("image"),
        spotify_id           = a.get("spotify_id"),
        match_score          = a.get("match_score", 0.0),
        discovered_from      = a.get("discovered_from"),
        socials              = ArtistSocials(**socials_raw) if socials_raw else None,
    )
```

---

```python
# main.py
"""
FastAPI application entry point.
Run with: uvicorn main:app --reload
"""
import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.models import HealthResponse
from api.routes.search import router as search_router
from api.routes.artists import router as artists_router
from services.cache import lastfm_cache, spotify_cache, social_cache

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title       = "Music Discovery Engine",
    description = "Underground artist discovery for music producers.",
    version     = "2.0.0",
    docs_url    = "/docs",
    redoc_url   = "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins     = os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

app.include_router(search_router)
app.include_router(artists_router)


@app.on_event("startup")
async def startup() -> None:
    """Validate credentials on startup so the error is immediate and clear."""
    from config import LASTFM_API_KEY, SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
    missing = [
        name for name, val in [
            ("LASTFM_API_KEY",        LASTFM_API_KEY),
            ("SPOTIFY_CLIENT_ID",     SPOTIFY_CLIENT_ID),
            ("SPOTIFY_CLIENT_SECRET", SPOTIFY_CLIENT_SECRET),
        ] if not val
    ]
    if missing:
        logger.error("Missing required credentials: %s", ", ".join(missing))
    else:
        logger.info("All API credentials present. Discovery engine ready.")


@app.get("/health", response_model=HealthResponse, tags=["meta"])
async def health() -> HealthResponse:
    return HealthResponse(
        status      = "ok",
        cache_sizes = {
            "lastfm":  lastfm_cache.size,
            "spotify": spotify_cache.size,
            "social":  social_cache.size,
        },
    )


@app.delete("/cache", tags=["meta"])
async def clear_cache() -> dict:
    """Clear all in-memory caches. Useful after bulk crawler runs."""
    lastfm_cache.clear()
    spotify_cache.clear()
    social_cache.clear()
    logger.info("All caches cleared via API.")
    return {"status": "cleared"}
```

---

```python
# crawler.py
"""
Standalone batch crawler.
Run with: python crawler.py

Pre-populates Supabase with discovered artists.
Shares all service/cache infrastructure with the API server.
"""
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

from config import (
    EXPAND_SCORE_THRESHOLD,
    EXPAND_TOP_FRACTION,
    SEED_FANOUT,
    MAX_DEPTH,
    MAX_NODES,
)
from core.discovery import discover, build_artist, is_within_discovery_scope
from core.dedup import dedupe_artists
from core.genres import get_genre_families
from db.database import save_artists_bulk
from db.seeds import get_seeds
from services.lastfm import lastfm_service
from services.spotify import spotify_service
from services.social import enrich_with_socials


def build_target_profile(seeds: list[str]) -> set[str]:
    fams: set[str] = set()
    for s in seeds:
        sp  = spotify_service.get_artist(s)
        lf  = lastfm_service.get_info(s)
        sf  = get_genre_families(lf.get("tags"), sp.get("genres"))
        if sf:
            logger.info("  seed '%s' -> %s", s, sorted(sf))
        fams |= sf
    logger.info("Target genre profile: %s", sorted(fams) or "NEUTRAL (all genres)")
    return fams


if __name__ == "__main__":
    seeds = get_seeds()
    if not seeds:
        raise SystemExit("No seeds available. Add artists to seeds.txt or the database.")

    logger.info("Loaded %d seeds: %s", len(seeds), seeds)
    logger.info("Crawling for UNDERGROUND artists only...\n")

    target_families = build_target_profile(seeds)

    candidates = discover(
        seeds                  = seeds,
        lastfm_svc             = lastfm_service,
        spotify_svc            = spotify_service,
        depth                  = MAX_DEPTH,
        per_artist             = 12,
        hard_cap               = MAX_NODES,
        max_per_origin         = 40,
        target_families        = target_families,
        expand_score_threshold = EXPAND_SCORE_THRESHOLD,
        expand_top_fraction    = EXPAND_TOP_FRACTION,
        seed_fanout            = SEED_FANOUT,
    )

    enriched: list[dict] = []
    for c in candidates:
        sp = c.get("_spotify") or spotify_service.get_artist(c["name"])
        a  = build_artist(c, sp, target_families)
        enrich_with_socials(a)
        enriched.append(a)

    results = dedupe_artists(enriched)
    before  = len(results)
    results = [a for a in results if is_within_discovery_scope(a)]
    logger.info("Scope filter: removed %d artists above size limits.", before - len(results))

    results.sort(key=lambda x: x["score"], reverse=True)
    logger.info("\n=== Found %d underground artists ===\n", len(results))

    for a in results[:30]:
        logger.info(
            "%3d  %-40s  listeners=%-7d  followers=%-6d  pop=%2d  growth=%2d  match=%.2f",
            a["score"], a["name"], a["listeners"],
            a["followers"], a["popularity"],
            a["growth_signal"], a["match_score"],
        )
        if a.get("discovered_from"):
            logger.info("     path: %s", a["discovered_from"])
        socials = {k: v for k, v in (a.get("socials") or {}).items() if v}
        if socials:
            logger.info("     socials: %s", socials)

    saved = save_artists_bulk(results)
    logger.info("\nSaved %d artists to Supabase.", saved)
```

---

```
# requirements.txt
fastapi>=0.110.0
uvicorn[standard]>=0.29.0
pydantic>=2.0.0
spotipy>=2.23.0
requests>=2.31.0
supabase>=2.3.0
python-dotenv>=1.0.0
```

---

```bash
# .env.example
# Copy to .env and fill in your values

LASTFM_API_KEY=your_lastfm_api_key
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret

# Optional — only needed for DB persistence / seed Mode B
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_supabase_anon_or_service_key

# Tuning (all have sensible defaults)
SEED_MODE=file                  # file | database
SEEDS_FILE=seeds.txt
SEEDS_SAMPLE_SIZE=10

MAX_DEPTH=3
MAX_NODES=300
MAX_SIMILAR_PER_NODE=12
EXPAND_SCORE_THRESHOLD=65
EXPAND_TOP_FRACTION=0.30
SEED_FANOUT=8

MAX_SPOTIFY_FOLLOWERS=50000
MAX_SPOTIFY_POPULARITY=50

CACHE_TTL_LASTFM=3600
CACHE_TTL_SPOTIFY=3600
CACHE_TTL_SOCIAL=86400
CACHE_MAX_SIZE=10000

RATE_LIMIT_SLEEP=0.20
HTTP_TIMEOUT=10

CORS_ORIGINS=*
```
