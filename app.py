"""
Music discovery pipeline (Last.fm BFS crawl -> Spotify enrichment -> scoring).

UNDERGROUND-ONLY MODE:
  - Hard Spotify gates DURING the crawl (followers > 50k OR popularity > 50 => skip).
  - No probabilities, no overrides, no "occasionally allow bigger artists".
  - Large artists are never saved AND never propagated (graph stops there).
"""


from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {
        "status": "online",
        "service": "music-discovery-api"
    }


@app.get("/health")
def health():
    return {"ok": True}



import os


import os
from supabase import create_client

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

import os
import re
import time
import math
import random
import unicodedata
from collections import Counter
from difflib import SequenceMatcher

import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from fastapi import FastAPI
import threading



# ===== CONFIG / KEYS =====


LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

if not all([LASTFM_API_KEY, SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET]):
    raise RuntimeError("Missing API credentials. Check your .env file.")

spotify = spotipy.Spotify(
    auth_manager=SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
    ),
    requests_timeout=10,
    retries=3,
)

LASTFM_URL = "http://ws.audioscrobbler.com/2.0/"

# Shared HTTP session w/ sane timeouts & a polite User-Agent.
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "music-discovery/1.0"})
HTTP_TIMEOUT = 10
MAX_RETRIES = 3
RATE_LIMIT_SLEEP = 0.20  # seconds between network calls (be a good citizen)


# ===== IN-MEMORY TTL CACHES =====

_CACHE_TTL = 3600  # seconds (1 hour)


class _TTLCache:
    """Minimal in-process TTL cache (single-threaded use only)."""

    def __init__(self, ttl=_CACHE_TTL):
        self._store = {}
        self._ttl = ttl

    def get(self, key):
        entry = self._store.get(key)
        if entry is None:
            return None
        value, ts = entry
        if time.time() - ts > self._ttl:
            del self._store[key]
            return None
        return value

    def set(self, key, value):
        self._store[key] = (value, time.time())


_spotify_cache = _TTLCache()
_lastfm_cache  = _TTLCache()

# In-run cache for fully enriched artist dicts (norm_key -> artist dict).
# Prevents redundant build_artist / Spotify calls within a single process run.
_artist_discovery_cache = {}


# ===== GENRE SYSTEM (extensible — add genres here, scoring never changes) =====
GENRE_TAXONOMY = {
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
    "pop":        {"pop", "dance pop", "electropop", "art pop", "synthpop",
                   "power pop", "indie pop", "bedroom pop"},
    "country":    {"country", "americana", "alt-country", "bluegrass",
                   "country pop", "outlaw country", "red dirt"},
    "rock":       {"rock", "classic rock", "hard rock", "garage rock",
                   "psychedelic rock", "alt rock", "alternative rock", "shoegaze"},
    "punk":       {"punk", "pop punk", "hardcore punk", "post-punk", "skate punk",
                   "hardcore"},
    "emo":        {"emo", "midwest emo", "screamo", "emocore", "emo rap"},
    "indie":      {"indie", "indie rock", "indie pop", "indie folk",
                   "bedroom pop", "lo-fi", "lofi"},
    "electronic": {"electronic", "edm", "house", "techno", "dubstep",
                   "drum and bass", "dnb", "trance", "idm", "ambient", "electro",
                   "uk garage", "garage", "breakbeat", "future bass", "deep house"},
    "hyperpop":   {"hyperpop", "glitchcore", "digicore", "pc music",
                   "bubblegum bass"},
    "rnb":        {"rnb", "r&b", "alternative rnb", "neo soul", "soul",
                   "contemporary r&b"},
    "metal":      {"metal", "death metal", "black metal", "heavy metal",
                   "metalcore", "deathcore", "doom metal", "thrash"},
    "folk":       {"folk", "indie folk", "folk rock", "singer-songwriter",
                   "acoustic"},
    "jazz":       {"jazz", "smooth jazz", "jazz fusion", "bebop", "nu jazz"},
    "classical":  {"classical", "orchestral", "baroque",
                   "contemporary classical", "piano"},
}

# Optional manual override. Leave empty [] to auto-derive from seeds.
TARGET_GENRES = []

# Kept for backwards-compatibility (no longer used by scoring).
TARGET_KEYWORDS = [
    "rap", "trap", "plug", "hyperpop", "indie", "rnb", "underground",
]


def get_genre_families(tags, genres):
    """
    Map an artist's tags+genres onto known genre families.

    Mapping fix: each tag/genre STRING is classified independently, and when a
    string matches both a specific family and the generic "pop" family
    (e.g. "pop rap", "pop punk", "bedroom pop"), the generic "pop" is dropped
    for that string. This prevents diverse seeds from collapsing into "pop".
    """
    items = [t.lower() for t in (tags or [])] + [g.lower() for g in (genres or [])]
    fams = set()
    for item in items:
        matched = set()
        for fam, kws in GENRE_TAXONOMY.items():
            if any(kw in item for kw in kws):
                matched.add(fam)
        if len(matched) > 1 and "pop" in matched:
            matched.discard("pop")
        fams |= matched
    return fams


def genre_match_score(artist_families, target_families):
    """
    0..1 relevance between an artist and the run's target genre profile.
    If no target profile is known, stay neutral (0.5) so all genres pass.
    """
    if not target_families:
        return 0.5
    if not artist_families:
        return 0.0
    overlap = len(artist_families & target_families)
    if overlap == 0:
        return 0.0
    return min(overlap / len(target_families) + 0.15 * (overlap - 1), 1.0)


_COLLAB_SPLIT_RE = re.compile(
    r"""
    \s*(?:
        ,
        | &
        | \bft\.?\b
        | \bfeat\.?\b
        | \bfeaturing\b
        | \bwith\b
        | \bvs\.?\b
        | \bx\b
        | /
        | \+
    )\s*
    """,
    re.IGNORECASE | re.VERBOSE,
)

_PAREN_NOISE_RE = re.compile(r"\s*[\(\[].*?[\)\]]\s*$")


# ===== HARTE DISCOVERY-LIMITS (UNDERGROUND-ONLY, ZERO exceptions) =====
DISCOVERY_TARGET_MIN = 5000
DISCOVERY_TARGET_MAX = 20000
DISCOVERY_ABS_MAX    = 50000

MAX_SPOTIFY_FOLLOWERS  = 50000
MAX_SPOTIFY_POPULARITY = 50

# ===== EXPANSION POLICY =====
EXPAND_SCORE_THRESHOLD = 65
EXPAND_TOP_FRACTION    = 0.30
SEED_FANOUT            = 8

# ===== SEED SELECTION POLICY =====
SEED_MATCH_FRACTION = 0.70
SEED_MIN_ACTIVE     = 3

# Minimum score for an artist to be promoted into the seed pool for future runs.
SEED_PROMOTION_SCORE = 70

# Minimum combined result count before a shallow live search is triggered.
DB_MIN_RECOMMENDATIONS = 20

# Set True to fetch social links (Instagram/TikTok/YouTube/X/Facebook/website)
# for final recommended artists via MusicBrainz and Wikidata.
INCLUDE_SOCIALS = False


def fanout_for_score(score):
    if score > 90:
        return 10
    if score >= 80:
        return 6
    if score >= 70:
        return 4
    return 2


# ============================================================
# NAME NORMALIZATION / IDENTITY
# ============================================================
def primary_artist_name(raw):
    if not raw:
        return ""
    name = unicodedata.normalize("NFKC", str(raw)).strip()
    name = _PAREN_NOISE_RE.sub("", name)
    name = _COLLAB_SPLIT_RE.split(name)[0].strip()
    name = re.sub(r"\s+", " ", name)
    return name.strip(" -–—_·").strip()


def norm_key(name):
    name = primary_artist_name(name)
    name = "".join(
        c for c in unicodedata.normalize("NFKD", name)
        if not unicodedata.combining(c)
    )
    name = name.lower()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def name_similarity(a, b):
    return SequenceMatcher(None, norm_key(a), norm_key(b)).ratio()


# ============================================================
# LAST.FM
# ============================================================
def _lastfm_get(params):
    """GET with retries, timeout, graceful JSON/error handling, and TTL cache."""
    _cache_key = tuple(sorted(params.items()))
    _cached = _lastfm_cache.get(_cache_key)
    if _cached is not None:
        return _cached

    params = {**params, "api_key": LASTFM_API_KEY, "format": "json"}
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(RATE_LIMIT_SLEEP)
            r = SESSION.get(LASTFM_URL, params=params, timeout=HTTP_TIMEOUT)
            data = r.json()
            if isinstance(data, dict) and "error" in data:
                return None
            _lastfm_cache.set(_cache_key, data)
            return data
        except (requests.RequestException, ValueError):
            time.sleep(0.5 * (attempt + 1))
    return None


def _as_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def get_similar(artist, limit=10):
    data = _lastfm_get({"method": "artist.getsimilar", "artist": artist, "limit": limit})
    if not data:
        return []
    artists = _as_list(data.get("similarartists", {}).get("artist"))
    out = []
    for a in artists:
        name = primary_artist_name(a.get("name", ""))
        if name:
            out.append(name)
    return out


def get_lastfm_info(artist):
    fallback = {"name": primary_artist_name(artist), "mbid": None,
                "listeners": 0, "playcount": 0, "tags": []}
    data = _lastfm_get({"method": "artist.getinfo", "artist": artist})
    if not data or "artist" not in data:
        return fallback

    info = data["artist"]
    stats = info.get("stats", {}) or {}
    tags = _as_list((info.get("tags") or {}).get("tag"))

    return {
        "name": primary_artist_name(info.get("name", artist)),
        "mbid": (info.get("mbid") or None),
        "listeners": _safe_int(stats.get("listeners")),
        "playcount": _safe_int(stats.get("playcount")),
        "tags": [t.get("name", "").lower() for t in tags[:10] if t.get("name")],
    }


# ============================================================
# SPOTIFY
# ============================================================
def get_spotify(artist_name):
    """
    Search Spotify and pick the BEST matching artist instead of blindly taking
    the first hit. Results are stored in a TTL cache to avoid redundant API calls.
    """
    _cache_key = norm_key(artist_name)
    _cached = _spotify_cache.get(_cache_key)
    if _cached is not None:
        return _cached

    empty = {"url": None, "image": None, "genres": [],
             "popularity": 0, "followers": 0, "spotify_id": None,
             "matched_name": None, "match_score": 0.0}
    try:
        time.sleep(RATE_LIMIT_SLEEP)
        items = spotify.search(q=artist_name, type="artist", limit=5)["artists"]["items"]
    except Exception:
        return empty
    if not items:
        _spotify_cache.set(_cache_key, empty)
        return empty

    best, best_sim = None, 0.0
    for a in items:
        sim = name_similarity(artist_name, a.get("name", ""))
        if sim > best_sim + 0.05 or (
            abs(sim - best_sim) <= 0.05 and best
            and a.get("followers", {}).get("total", 0)
                > best.get("followers", {}).get("total", 0)
        ):
            best, best_sim = a, sim

    if best is None or best_sim < 0.60:
        _spotify_cache.set(_cache_key, empty)
        return empty

    imgs = best.get("images", [])
    result = {
        "url": best.get("external_urls", {}).get("spotify"),
        "image": imgs[0]["url"] if imgs else None,
        "genres": [g.lower() for g in best.get("genres", [])],
        "popularity": _safe_int(best.get("popularity")),
        "followers": _safe_int(best.get("followers", {}).get("total")),
        "spotify_id": best.get("id"),
        "matched_name": best.get("name"),
        "match_score": round(best_sim, 3),
    }
    _spotify_cache.set(_cache_key, result)
    return result


# ============================================================
# HARD UNDERGROUND GATE
# ============================================================
def is_too_big(sp):
    if sp.get("followers", 0) > MAX_SPOTIFY_FOLLOWERS:
        return True
    if sp.get("popularity", 0) > MAX_SPOTIFY_POPULARITY:
        return True
    return False


def is_within_discovery_scope(a):
    if a.get("followers", 0) > MAX_SPOTIFY_FOLLOWERS:
        return False
    if a.get("popularity", 0) > MAX_SPOTIFY_POPULARITY:
        return False
    return True


# ============================================================
# GROWTH SIGNAL
# ============================================================
def compute_growth_signal(a):
    pop = a.get("popularity", 0)
    followers = a.get("followers", 0)
    listeners = a.get("listeners", 0)
    playcount = a.get("playcount", 0)
    ratio = (playcount / listeners) if listeners else 0

    score = 0
    reasons = []

    if followers > 0:
        expected_pop = 10 * math.log10(followers + 10)
        if pop >= expected_pop + 12:
            score += 40; reasons.append("popularity_above_audience")
        elif pop >= expected_pop:
            score += 20; reasons.append("mild_momentum")
    elif pop >= 20:
        score += 30; reasons.append("popularity_without_followers")

    if ratio >= 30:
        score += 30; reasons.append("very_high_engagement")
    elif ratio >= 15:
        score += 20; reasons.append("high_engagement")
    elif ratio >= 8:
        score += 10; reasons.append("moderate_engagement")

    if 0 < followers < 25000 and pop >= 25:
        score += 20; reasons.append("emerging_traction")

    score = min(score, 100)
    return score, (reasons[0] if reasons else "stable")


# ============================================================
# SCORING (0–100)
# ============================================================
def score_artist(a, target_families=None):
    listeners = a["listeners"]
    playcount = a["playcount"]
    popularity = a.get("popularity", 0)
    followers = a.get("followers", 0)
    has_spotify = bool(a.get("spotify_id"))

    growth, growth_reason = compute_growth_signal(a)
    a["growth_signal"] = growth
    a["growth_signal_reason"] = growth_reason

    fams = get_genre_families(a.get("tags"), a.get("genres"))
    gmatch = genre_match_score(fams, target_families)
    a["genre_match_score"] = round(gmatch, 3)
    a["genre_families"] = sorted(fams)

    gscore = round(gmatch * 25)
    gr = round(growth / 100 * 25)

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

    ratio = (playcount / listeners) if listeners else 0
    if ratio >= 20:
        e = 5
    elif ratio >= 8:
        e = 3
    else:
        e = 1

    total = min(gscore + gr + sz + p + e, 100)
    return total, {
        "genre_relevance": gscore,
        "growth": gr,
        "size": sz,
        "spotify_popularity": p,
        "engagement": e,
    }


# ============================================================
# BUILD ARTIST OBJECT
# ============================================================
def build_artist(lf, sp, target_families=None):
    a = {
        "name": lf["name"],
        "mbid": lf.get("mbid"),
        "listeners": lf["listeners"],
        "playcount": lf["playcount"],
        "tags": lf["tags"],
        "genres": sp["genres"],
        "url": sp["url"],
        "image": sp["image"],
        "popularity": sp["popularity"],
        "followers": sp["followers"],
        "spotify_id": sp["spotify_id"],
        "match_score": sp["match_score"],
        "discovered_from": lf.get("discovered_from"),
        "score": 0,
        "score_breakdown": {},
        "growth_signal": 0,
        "growth_signal_reason": "stable",
        "genre_match_score": 0.0,
        "genre_families": [],
    }
    a["score"], a["score_breakdown"] = score_artist(a, target_families)
    return a


# ============================================================
# TARGET GENRE PROFILE
# ============================================================
def build_target_profile(seeds):
    if TARGET_GENRES:
        return set(TARGET_GENRES)

    fams = set()

    for s in seeds:
        sp = get_spotify(s)
        lf = get_lastfm_info(s)

        seed_fams = get_genre_families(
            lf.get("tags"),
            sp.get("genres")
        )

        fams.update(seed_fams)

    return fams
# ============================================================
# SEED SELECTION  (less deterministic, genre-aware subset)
# ============================================================
def select_active_seeds(seeds, target_families,
                        match_fraction=SEED_MATCH_FRACTION,
                        min_active=SEED_MIN_ACTIVE):
    """
    Split seeds into genre-matching and fallback groups, shuffle both
    independently, then randomly pick a subset of the matching group.
    """
    matching, fallback = [], []
    for s in seeds:
        sp = get_spotify(s)
        lf = get_lastfm_info(s)
        fams = get_genre_families(lf.get("tags", []), sp.get("genres", []))
        if not target_families or (fams & target_families):
            matching.append(s)
        else:
            fallback.append(s)

    random.shuffle(matching)
    random.shuffle(fallback)

    k = max(min_active, math.ceil(len(matching) * match_fraction))
    active = matching[:k]

    if len(active) < min_active:
        active += fallback[:min_active - len(active)]

    print(f"Seed selection: {len(active)} active / {len(seeds)} total "
          f"(matching={len(matching)}, fallback={len(fallback)}, "
          f"subset_k={k})\n")
    return active


# ============================================================
# DEDUPLICATION
# ============================================================
def dedupe_artists(artists, fuzzy_threshold=0.92):
    def completeness(a):
        return (1 if a.get("spotify_id") else 0, a.get("score", 0), a.get("listeners", 0))

    by_key = {}
    by_spotify = {}

    for a in artists:
        key = norm_key(a["name"])
        if not key:
            continue

        sid = a.get("spotify_id")
        if sid and sid in by_spotify:
            existing = by_spotify[sid]
            if completeness(a) > completeness(existing):
                by_spotify[sid] = a
            continue

        if key in by_key:
            existing = by_key[key]
            if completeness(a) > completeness(existing):
                by_key[key] = a
        else:
            by_key[key] = a

        if sid:
            by_spotify[sid] = a

    merged = {}
    for a in list(by_key.values()) + list(by_spotify.values()):
        key = norm_key(a["name"])
        if key not in merged or completeness(a) > completeness(merged[key]):
            merged[key] = a

    result = []
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
        elif completeness(a) > completeness(dup_of):
            result[result.index(dup_of)] = a

    return result


# ============================================================
# BFS CRAWL (UNDERGROUND-ONLY, breadth-first, score-gated)
# ============================================================
def discover(seeds, depth=2, per_artist=12, sample_size=8, hard_cap=300,
             max_per_origin=40, target_families=None,
             expand_score_threshold=EXPAND_SCORE_THRESHOLD,
             expand_top_fraction=EXPAND_TOP_FRACTION,
             seed_fanout=SEED_FANOUT):
    """
    Underground-only, BREADTH-FIRST, score-gated crawl over Last.fm 'similar'.
    `seeds` should already be the active (shuffled, subsetted) seed list.
    """
    seen = set()
    keep = []
    origin_counts = Counter()

    frontier = []
    seen_frontier = set()
    for s in seeds:
        disp = primary_artist_name(s)
        k = norm_key(disp)
        if k and k not in seen_frontier:
            frontier.append((disp, disp, []))
            seen_frontier.add(k)

    for level in range(depth + 1):
        if not frontier or len(seen) >= hard_cap:
            break

        scored_nodes = []

        for name, origin, path in frontier:
            if len(seen) >= hard_cap:
                break
            key = norm_key(name)
            if not key or key in seen:
                continue
            seen.add(key)

            lf = get_lastfm_info(name)
            full_path = path + [lf["name"]]
            lf["discovered_from"] = " → ".join(full_path)
            listeners = lf["listeners"]

            sp = get_spotify(lf["name"])
            lf["_spotify"] = sp
            followers = sp.get("followers", 0)
            popularity = sp.get("popularity", 0)

            if is_too_big(sp):
                if level == 0:
                    print(f"  seed-node (not saved): {lf['name']} "
                          f"(followers={followers}, pop={popularity})")
                    scored_nodes.append((-1, lf, lf["name"], origin, full_path))
                else:
                    print(f"  STOP (too big): {lf['name']} "
                          f"(followers={followers}, pop={popularity})")
                    continue
            else:
                if 0 < listeners <= DISCOVERY_ABS_MAX and \
                   origin_counts[origin] < max_per_origin:
                    keep.append(lf)
                    origin_counts[origin] += 1

                a = build_artist(lf, sp, target_families)
                scored_nodes.append((a["score"], lf, lf["name"], origin, full_path))

            print(f"  scanned: {lf['name']} ({listeners} listeners, "
                  f"followers={followers}, pop={popularity})")

        if level < depth and scored_nodes:
            if level == 0:
                expandable = list(scored_nodes)
            else:
                by_score = sorted(scored_nodes, key=lambda x: x[0], reverse=True)
                top_n = max(1, math.ceil(len(by_score) * expand_top_fraction))
                expandable = [
                    node for i, node in enumerate(by_score)
                    if node[0] >= expand_score_threshold or i < top_n
                ]

            next_frontier = []
            next_keys = set()
            for score, _lf, pname, origin, full_path in expandable:
                fanout = seed_fanout if level == 0 else fanout_for_score(score)
                sims = get_similar(pname, per_artist)
                added = 0
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


# ============================================================
# SAVE TO SUPABASE
# ============================================================
def _artist_row(a):
    return {
        "name": a["name"]
    }


def save_artist(a):
    supabase.table("artists").upsert({
        "name": a["name"],
        "spotify_id": a.get("spotify_id"),
        "followers": a.get("followers"),
        "popularity": a.get("popularity"),
        "genres": a.get("genres"),
        "tags": a.get("tags"),
        "score": a.get("score"),
        "match_score": a.get("match_score"),
        "genre_families": list(a.get("genre_families", [])),
        "discovered_from": a.get("discovered_from"),
        "growth_signal": a.get("growth_signal"),
        "growth_signal_reason": a.get("growth_signal_reason"),
    }, on_conflict="name").execute()


def save_artists_bulk(artists, batch_size=100):
    rows = [{"name": a["name"]} for a in artists]

    for i in range(0, len(rows), batch_size):
        supabase.table("artists").upsert(
            rows[i:i + batch_size],
            on_conflict="name"
        ).execute()

    return len(rows)

def load_seeds_from_supabase():
    """Pull seed names saved from previous runs so the pool grows automatically."""
    try:
        resp = supabase.table("seeds").select("name").execute()
        return [row["name"] for row in (resp.data or []) if row.get("name")]
    except Exception:
        return []


def save_seeds_bulk(artists, score_threshold=SEED_PROMOTION_SCORE, batch_size=100):
    """
    Promote high-quality underground artists into the seeds table so they
    improve future discovery runs. Upsert on name prevents duplicate rows.
    """
    rows = [
        {"name": a["name"]}
        for a in artists
        if is_within_discovery_scope(a) and a.get("score", 0) >= score_threshold
    ]
    for i in range(0, len(rows), batch_size):
        supabase.table("seeds").upsert(
            rows[i:i + batch_size], on_conflict="name"
        ).execute()
    return len(rows)


# ============================================================
# DB RECOMMENDATION LOADER
# ============================================================
def load_recommendations_from_db(target_families=None, min_score=40, limit=200):
    """
    Load previously discovered artists from the local Supabase database.
    Reconstructs artist dicts compatible with the rest of the pipeline and
    populates _artist_discovery_cache so the live search can reuse them
    without redundant API calls.
    """
    try:
        resp = (
            supabase.table("artists")
            .select("*")
            .gte("score", min_score)
            .order("score", desc=True)
            .limit(limit)
            .execute()
        )
        rows = resp.data or []
    except Exception as e:
        print(f"Warning: DB load failed: {e}")
        return []

    artists = []
    for row in rows:
        a = {
            "name":                 row.get("name", ""),
            "mbid":                 None,
            "listeners":            row.get("listeners", 0),
            "playcount":            row.get("playcount", 0),
            "tags":                 row.get("tags") or [],
            "genres":               row.get("genres") or [],
            "url":                  row.get("url"),
            "image":                row.get("image"),
            "popularity":           row.get("spotify_popularity", 0),
            "followers":            row.get("spotify_followers", 0),
            "spotify_id":           row.get("spotify_id"),
            "match_score":          row.get("match_score", 0.0),
            "score":                row.get("score", 0),
            "score_breakdown":      {},
            "discovered_from":      row.get("discovered_from"),
            "growth_signal":        row.get("growth_signal", 0),
            "growth_signal_reason": row.get("growth_signal_reason", "stable"),
            "genre_match_score":    row.get("genre_match_score", 0.0),
            "genre_families":       [],
        }
        a["genre_families"] = sorted(get_genre_families(a["tags"], a["genres"]))

        # Skip artists that don't match the current run's genre profile.
        if target_families:
            if not (set(a["genre_families"]) & target_families):
                continue

        key = norm_key(a["name"])
        if key and key not in _artist_discovery_cache:
            _artist_discovery_cache[key] = a

        artists.append(a)
    return artists


# ============================================================
# SHALLOW LIVE SEARCH SUPPLEMENT
# ============================================================
def live_search_supplement(seeds, target_families, needed,
                           depth=1, per_artist=6, hard_cap=60):
    """
    Shallow, fast BFS crawl triggered only when the recommendation pool is too
    small.  Uses at most 3 seeds, depth=1, small fanout, and a hard cap of 60
    nodes so it stays quick.  Checks _artist_discovery_cache to avoid
    rebuilding artists already seen this run.  New discoveries are saved to DB.
    """
    sample = seeds[:min(3, len(seeds))]
    print(f"\nLive search supplement: {len(sample)} seed(s), "
          f"depth={depth}, cap={hard_cap} (need ~{needed} more).\n")

    raw = discover(
        sample,
        depth=depth,
        per_artist=per_artist,
        sample_size=per_artist,
        hard_cap=hard_cap,
        max_per_origin=20,
        target_families=target_families,
        expand_score_threshold=EXPAND_SCORE_THRESHOLD,
        expand_top_fraction=0.5,
        seed_fanout=4,
    )

    enriched = []
    for c in raw:
        key = norm_key(c["name"])
        # Reuse already-enriched artist from this run's cache.
        if key in _artist_discovery_cache:
            enriched.append(_artist_discovery_cache[key])
            continue
        sp = c.get("_spotify") or get_spotify(c["name"])
        a = build_artist(c, sp, target_families)
        _artist_discovery_cache[key] = a
        enriched.append(a)

    # Persist new discoveries so future runs benefit immediately.
    valid = [a for a in enriched if is_within_discovery_scope(a)]
    if valid:
        n = save_artists_bulk(valid)
        print(f"Live search supplement saved {n} new artists to DB.")

    return enriched


# ============================================================
# SOCIAL LINK ENRICHMENT  (MusicBrainz + Wikidata)
# ============================================================

MB_API        = "https://musicbrainz.org/ws/2"
MB_USER_AGENT = "music-discovery/1.0"
MB_RATE_SLEEP = 1.0   # MusicBrainz allows ~1 req/sec

# MusicBrainz relationship-type name -> social key.
# Falls back to URL-pattern matching for anything not listed here.
_MB_TYPE_TO_SOCIAL = {
    "official homepage": "website",
    "youtube":           "youtube",
    "instagram":         "instagram",
    "twitter":           "x",
    "facebook":          "facebook",
    "tiktok":            "tiktok",
}


def _url_to_social_key(url):
    """Classify a raw URL into a social key by domain, or None if unrecognised."""
    u = url.lower()
    if "instagram.com"  in u: return "instagram"
    if "tiktok.com"     in u: return "tiktok"
    if "youtube.com"    in u or "youtu.be"  in u: return "youtube"
    if "twitter.com"    in u or "//x.com/"  in u: return "x"
    if "facebook.com"   in u: return "facebook"
    return None


def _mb_url_relations(mbid):
    """Return the URL-relations list from MusicBrainz for the given MBID."""
    if not mbid:
        return []
    try:
        time.sleep(MB_RATE_SLEEP)
        r = SESSION.get(
            f"{MB_API}/artist/{mbid}",
            params={"inc": "url-rels", "fmt": "json"},
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": MB_USER_AGENT},
        )
        if r.status_code == 200:
            return r.json().get("relations", [])
    except Exception:
        pass
    return []


def _wikidata_socials(qid):
    """
    Pull social handles/URLs directly from a Wikidata entity.

    Properties used:
      P856  official website
      P2397 YouTube channel ID
      P2003 Instagram username
      P2002 Twitter / X username
      P2013 Facebook profile ID
      P7085 TikTok username
    """
    if not qid:
        return {}
    try:
        time.sleep(0.5)
        r = SESSION.get(
            f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": MB_USER_AGENT},
        )
        if r.status_code != 200:
            return {}
        claims = (
            r.json()
            .get("entities", {})
            .get(qid, {})
            .get("claims", {})
        )
    except Exception:
        return {}

    def _first_val(prop):
        snaks = claims.get(prop, [])
        if snaks:
            return snaks[0].get("mainsnak", {}).get("datavalue", {}).get("value")
        return None

    out = {}
    website   = _first_val("P856")
    youtube   = _first_val("P2397")
    instagram = _first_val("P2003")
    twitter   = _first_val("P2002")
    facebook  = _first_val("P2013")
    tiktok    = _first_val("P7085")

    if website:   out["website"]   = website
    if youtube:   out["youtube"]   = f"https://www.youtube.com/channel/{youtube}"
    if instagram: out["instagram"] = f"https://www.instagram.com/{instagram}"
    if twitter:   out["x"]         = f"https://x.com/{twitter}"
    if facebook:  out["facebook"]  = f"https://www.facebook.com/{facebook}"
    if tiktok:    out["tiktok"]    = f"https://www.tiktok.com/@{tiktok}"

    return out


def fetch_socials(artist):
    """
    Fetch social links for a single artist.

    Strategy:
      1. Query MusicBrainz URL relations using artist.mbid.
      2. While iterating relations, extract a Wikidata Q-ID if MB links to one.
      3. Fill any remaining gaps from Wikidata property values.

    Returns a dict containing any subset of:
        website, instagram, tiktok, youtube, x, facebook
    """
    socials    = {}
    wikidata_q = None

    for rel in _mb_url_relations(artist.get("mbid")):
        url = (rel.get("url") or {}).get("resource", "")
        if not url:
            continue

        # Capture Wikidata Q-ID for the second-pass lookup.
        if "wikidata.org/wiki/Q" in url and wikidata_q is None:
            m = re.search(r"(Q\d+)", url)
            if m:
                wikidata_q = m.group(1)

        key = _MB_TYPE_TO_SOCIAL.get((rel.get("type") or "").lower())
        if key is None:
            key = _url_to_social_key(url)
        if key and key not in socials:
            socials[key] = url

    # Fill gaps from Wikidata (only makes network call when Q-ID was found above).
    for k, v in _wikidata_socials(wikidata_q).items():
        if k not in socials:
            socials[k] = v

    return socials


# ============================================================
# RUN
# ============================================================
# ============================================================
# RUN
# ============================================================
def run_pipeline():
    SEEDS_FILE = "seeds.txt"

    try:
        with open(SEEDS_FILE, encoding="utf-8") as f:
            seeds = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Seeds file '{SEEDS_FILE}' not found.")
        return

    if not seeds:
        print(f"Seeds file '{SEEDS_FILE}' is empty.")
        return

    print(f"Loaded {len(seeds)} seeds (API mode, no crawling).")

    # Only store seed NAMES in DB (no enrichment data)
    save_seeds_bulk(
    [{"name": s} for s in seeds],
    score_threshold=0
)
    print("Seed names stored. Switching to REQUEST-DRIVEN MODE.")

    # DO NOT crawl, DO NOT expand graph
    # System becomes "on-demand enrichment API"

    app.state.seeds = seeds
    # Shuffle all seeds before anything else so run order is non-deterministic.
    random.shuffle(seeds)
    print(f"Seeds shuffled: {seeds}\n")

    print("Crawling for UNDERGROUND artists only...\n")

    # Build genre profile from ALL seeds so it is always complete.
    target_families = build_target_profile(seeds)

    # Select a shuffled, random subset of genre-matching seeds for this run.
    active_seeds = select_active_seeds(seeds, target_families)

    candidates = discover(
        active_seeds,
        depth=3,
        per_artist=12,
        sample_size=8,
        hard_cap=300,
        max_per_origin=40,
        target_families=target_families,
        expand_score_threshold=EXPAND_SCORE_THRESHOLD,
        expand_top_fraction=EXPAND_TOP_FRACTION,
        seed_fanout=SEED_FANOUT,
    )

    enriched = []
    for c in candidates:
        sp = c.get("_spotify") or get_spotify(c["name"])
        a = build_artist(c, sp, target_families)
        key = norm_key(a["name"])
        if key:
            _artist_discovery_cache[key] = a
        enriched.append(a)

    results = dedupe_artists(enriched)

    before = len(results)
    results = [a for a in results if is_within_discovery_scope(a)]
    print(f"Scope-Filter: {before - len(results)} large artists removed.")

    results.sort(key=lambda x: x["score"], reverse=True)

    print(f"\nFresh crawl produced {len(results)} artists.")
    print("Loading cached recommendations from local DB...")
    db_recs = load_recommendations_from_db(target_families=target_families)
    print(f"DB returned {len(db_recs)} previously discovered artists.")

    if db_recs:
        results = dedupe_artists(results + db_recs)
        results = [a for a in results if is_within_discovery_scope(a)]
        results.sort(key=lambda x: x["score"], reverse=True)
        print(f"After DB merge: {len(results)} artists total.")

    if len(results) < DB_MIN_RECOMMENDATIONS:
        needed = DB_MIN_RECOMMENDATIONS - len(results)
        live = live_search_supplement(active_seeds, target_families, needed)
        live_filtered = [a for a in live if is_within_discovery_scope(a)]
        if live_filtered:
            results = dedupe_artists(results + live_filtered)
            results = [a for a in results if is_within_discovery_scope(a)]
            results.sort(key=lambda x: x["score"], reverse=True)
            print(f"After live supplement: {len(results)} artists total.")

    if INCLUDE_SOCIALS:
        print(f"\nFetching social links for {len(results)} artists "
              f"(MusicBrainz + Wikidata)...\n")
        for a in results:
            a["socials"] = fetch_socials(a)

    print(f"\n=== Found {len(results)} underground artists (deduped) ===\n")
    for a in results[:30]:
        print(f"{a['score']:>3}  {a['name']}  (match={a['match_score']})")
        print(f"     lastfm: listeners={a['listeners']} playcount={a['playcount']}")
        print(f"     spotify: pop={a['popularity']} followers={a['followers']}")
        print(f"     growth={a['growth_signal']} ({a['growth_signal_reason']})"
              f"  genre_match={a['genre_match_score']}  fams={a.get('genre_families')}")
        print(f"     path={a.get('discovered_from')}")
        print(f"     {a['url']}")
        if INCLUDE_SOCIALS and a.get("socials"):
            for platform, link in sorted(a["socials"].items()):
                print(f"     {platform}: {link}")
        print()

    saved = save_artists_bulk(results)
    print(f"Saved {saved} artists to Supabase.")

    seeded = save_seeds_bulk(results)
    print(f"Promoted {seeded} artists to seed pool (score >= {SEED_PROMOTION_SCORE}).")



@app.on_event("startup")
def startup():
    print("API started in SEARCH MODE (no crawling)")


from fastapi import Query
@app.get("/search")
def search_artists(q: str = Query(...)):
    db = (
        supabase.table("artists")
        .select("""
name,
spotify_id,
genres,
followers,
popularity,
images,
external_urls,
socials,
score
""")
        .ilike("name", f"{q}%")
        .limit(50)
        .execute()
    ).data or []

    results = []

    for row in db:
        name = row["name"]

        # STEP 3A — LIVE SPOTIFY ENRICHMENT
        artist = row  # already stored enriched data
        results.append(artist)

    results.sort(key=lambda x: x["score"], reverse=True)

    return {
        "query": q,
        "count": len(results),
        "results": results[:50]
    }


@app.post("/run")
def trigger_pipeline():
    run_pipeline()
    return {"status": "pipeline started"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000))
    )
