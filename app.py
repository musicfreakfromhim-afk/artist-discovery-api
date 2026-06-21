"""
Music discovery pipeline (Last.fm BFS crawl -> Spotify enrichment -> scoring).

UNDERGROUND-ONLY MODE:
  - Hard Spotify gates DURING the crawl (followers > 50k OR popularity > 50 => skip).
  - No probabilities, no overrides, no "occasionally allow bigger artists".
  - Large artists are never saved.
  - Large artists may be used as bridge nodes so the graph can reach smaller related artists.
"""

from fastapi import FastAPI, Query, BackgroundTasks
import os
import re
import time
import math
import random
import logging
import unicodedata
from collections import Counter
from difflib import SequenceMatcher

import requests
import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials
from supabase import create_client


# ===== APP =====
load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("music-discovery-api")

APP_VERSION = "codex-stabilized-2026-06-21-v5"

app = FastAPI()

@app.get("/")
def home():
    return {
        "status": "online",
        "service": "music-discovery-api",
        "version": APP_VERSION,
    }

@app.get("/health")
def health():
    return {
        "ok": True,
        "version": APP_VERSION,
        "configured": {
            "supabase": supabase is not None,
            "lastfm": bool(LASTFM_API_KEY),
            "spotify": spotify is not None,
        },
    }

# ===== SUPABASE =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as exc:
        logger.exception("startup: failed to create Supabase client: %s", exc)
else:
    logger.warning("startup: Supabase is not configured; DB reads/writes will be skipped")


# ===== CONFIG / KEYS =====
LASTFM_API_KEY        = os.getenv("LASTFM_API_KEY")
SPOTIFY_CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

if not LASTFM_API_KEY:
    logger.warning("startup: LASTFM_API_KEY is missing; Last.fm calls will return fallbacks")

spotify = None
if SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET:
    # retries=0 prevents urllib3 from sleeping for Retry-After seconds on 429.
    spotify = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET,
        ),
        requests_timeout=10,
        retries=0,
    )
else:
    logger.warning("startup: Spotify credentials are missing; Spotify enrichment will be skipped")

LASTFM_URL = "http://ws.audioscrobbler.com/2.0/"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "music-discovery/1.0"})
HTTP_TIMEOUT     = 10
MAX_RETRIES      = 3
RATE_LIMIT_SLEEP = 0.20   # seconds between network calls


# ===== IN-MEMORY TTL CACHES =====
_CACHE_TTL = 3600

class _TTLCache:
    """Minimal in-process TTL cache (single-threaded use only)."""
    def __init__(self, ttl=_CACHE_TTL):
        self._store = {}
        self._ttl   = ttl

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

_spotify_cache          = _TTLCache()
_lastfm_cache           = _TTLCache()
# In-run cache for enriched artist dicts (norm_key -> dict).
_artist_discovery_cache = {}


# ===== GENRE SYSTEM =====
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

# Leave empty to auto-derive from seeds.
TARGET_GENRES = []

# Kept for backwards-compatibility (not used by scoring).
TARGET_KEYWORDS = [
    "rap", "trap", "plug", "hyperpop", "indie", "rnb", "underground",
]


# ===== DISCOVERY LIMITS (UNDERGROUND-ONLY) =====
DISCOVERY_TARGET_MIN   = 5_000
DISCOVERY_TARGET_MAX   = 20_000
DISCOVERY_ABS_MAX      = 50_000

MAX_SPOTIFY_FOLLOWERS  = 50_000
MAX_SPOTIFY_POPULARITY = 50
ALLOW_BIG_ARTIST_BRIDGES = True

# ===== EXPANSION POLICY =====
EXPAND_SCORE_THRESHOLD = 65
EXPAND_TOP_FRACTION    = 0.30
SEED_FANOUT            = 8

# ===== SEED SELECTION POLICY =====
SEED_MATCH_FRACTION = 0.70
SEED_MIN_ACTIVE     = 3

SEED_PROMOTION_SCORE   = 50     # minimum score to enter the seeds table
DB_MIN_RECOMMENDATIONS = 20     # minimum results before live supplement fires
INCLUDE_SOCIALS        = False  # fetch MusicBrainz / Wikidata social links

# When the seed pool exceeds this, randomly sample to keep API calls manageable.
MAX_SEEDS_BEFORE_SAMPLING = 50
SEED_SAMPLE_SIZE          = 25

# Minimum DB matches before /search triggers live discovery.
SEARCH_MIN_RESULTS = 5


def fanout_for_score(score):
    if score > 90: return 10
    if score >= 80: return 6
    if score >= 70: return 4
    return 2


# ===== NAME NORMALIZATION =====
_PAREN_NOISE_RE  = re.compile(r"\s*[\(\[].*?[\)\]]\s*$")
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


# ===== GENRE FUNCTIONS =====
def get_genre_families(tags, genres):
    """
    Map tags+genres onto known genre families.
    When a string matches both a specific family and the generic 'pop' family
    the generic 'pop' is dropped, preventing diverse seeds from collapsing into it.
    """
    items = [t.lower() for t in (tags or [])] + [g.lower() for g in (genres or [])]
    fams  = set()
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
    """0..1 relevance. Neutral (0.5) when no target profile is known."""
    if not target_families:
        return 0.5
    if not artist_families:
        return 0.0
    overlap = len(artist_families & target_families)
    if overlap == 0:
        return 0.0
    return min(overlap / len(target_families) + 0.15 * (overlap - 1), 1.0)


# ===== LAST.FM =====
def _lastfm_get(params):
    if not LASTFM_API_KEY:
        logger.warning("_lastfm_get: LASTFM_API_KEY missing; params=%s", params)
        return None
    _cache_key = tuple(sorted(params.items()))
    cached = _lastfm_cache.get(_cache_key)
    if cached is not None:
        return cached
    params = {**params, "api_key": LASTFM_API_KEY, "format": "json"}
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(RATE_LIMIT_SLEEP)
            r    = SESSION.get(LASTFM_URL, params=params, timeout=HTTP_TIMEOUT)
            data = r.json()
            if isinstance(data, dict) and "error" in data:
                logger.warning("_lastfm_get: Last.fm returned error for params=%s response=%s", params, data)
                return None
            _lastfm_cache.set(_cache_key, data)
            return data
        except (requests.RequestException, ValueError) as exc:
            logger.warning(
                "_lastfm_get: request failed attempt=%s/%s params=%s error=%s",
                attempt + 1,
                MAX_RETRIES,
                params,
                exc,
            )
            time.sleep(0.5 * (attempt + 1))
    logger.warning("_lastfm_get: exhausted retries for params=%s", params)
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
    return [primary_artist_name(a.get("name", "")) for a in artists if a.get("name")]

def get_lastfm_info(artist):
    fallback = {"name": primary_artist_name(artist), "mbid": None,
                "listeners": 0, "playcount": 0, "tags": []}
    data = _lastfm_get({"method": "artist.getinfo", "artist": artist})
    if not data or "artist" not in data:
        return fallback
    info  = data["artist"]
    stats = info.get("stats", {}) or {}
    tags  = _as_list((info.get("tags") or {}).get("tag"))
    return {
        "name":      primary_artist_name(info.get("name", artist)),
        "mbid":      info.get("mbid") or None,
        "listeners": _safe_int(stats.get("listeners")),
        "playcount": _safe_int(stats.get("playcount")),
        "tags":      [t.get("name", "").lower() for t in tags[:10] if t.get("name")],
    }


# ===== SPOTIFY =====
SPOTIFY_COOLDOWN_UNTIL = 0
SPOTIFY_COOLDOWN_FALLBACK = 30 * 60
SPOTIFY_RATE_LIMITED = False

def _empty_spotify_result():
    return {"url": None, "image": None, "genres": [],
            "popularity": 0, "followers": 0, "spotify_id": None,
            "matched_name": None, "match_score": 0.0}

def _spotify_retry_after_seconds(exc):
    msg = str(exc)
    match = re.search(r"Retry will occur after:\s*(\d+)\s*s", msg)
    if match:
        return _safe_int(match.group(1), SPOTIFY_COOLDOWN_FALLBACK)
    response = getattr(exc, "http_response", None)
    headers = getattr(response, "headers", {}) or {}
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    return _safe_int(retry_after, 0)

def _is_spotify_rate_limit(exc):
    status = getattr(exc, "http_status", None) or getattr(exc, "status", None)
    return status == 429 or "429" in str(exc)

def get_spotify(artist_name):
    """
    Search Spotify and pick the best-matching artist.
    Results are TTL-cached to avoid redundant API calls.
    Failures are also cached so rate-limited calls are not retried within the TTL window.
    """
    global SPOTIFY_COOLDOWN_UNTIL, SPOTIFY_RATE_LIMITED
    _cache_key = norm_key(artist_name)
    cached = _spotify_cache.get(_cache_key)
    if cached is not None:
        return cached
    empty = _empty_spotify_result()
    if spotify is None:
        logger.warning("get_spotify: Spotify client is not configured; artist=%r", artist_name)
        _spotify_cache.set(_cache_key, empty)
        return empty
    now = time.time()
    if SPOTIFY_RATE_LIMITED or now < SPOTIFY_COOLDOWN_UNTIL:
        logger.warning(
            "get_spotify: Spotify lookup skipped for %r because Spotify is rate-limited",
            artist_name,
        )
        _spotify_cache.set(_cache_key, empty)
        return empty
    try:
        time.sleep(RATE_LIMIT_SLEEP)
        items = spotify.search(q=artist_name, type="artist", limit=5)["artists"]["items"]
    except Exception as exc:
        if _is_spotify_rate_limit(exc):
            retry_after = _spotify_retry_after_seconds(exc) or SPOTIFY_COOLDOWN_FALLBACK
            SPOTIFY_RATE_LIMITED = True
            SPOTIFY_COOLDOWN_UNTIL = max(
                SPOTIFY_COOLDOWN_UNTIL,
                time.time() + min(retry_after, SPOTIFY_COOLDOWN_FALLBACK)
            )
            logger.warning(
                "get_spotify: Spotify 429 for %r; skipping all Spotify enrichment for this process. retry_after=%s error=%s",
                artist_name,
                retry_after,
                exc,
            )
        else:
            logger.warning(
                "get_spotify: Spotify lookup failed for %r; skipping Spotify enrichment for this artist. error=%s",
                artist_name,
                exc,
            )
        _spotify_cache.set(_cache_key, empty)
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
    imgs   = best.get("images", [])
    result = {
        "url":          best.get("external_urls", {}).get("spotify"),
        "image":        imgs[0]["url"] if imgs else None,
        "genres":       [g.lower() for g in best.get("genres", [])],
        "popularity":   _safe_int(best.get("popularity")),
        "followers":    _safe_int(best.get("followers", {}).get("total")),
        "spotify_id":   best.get("id"),
        "matched_name": best.get("name"),
        "match_score":  round(best_sim, 3),
    }
    _spotify_cache.set(_cache_key, result)
    return result


# ===== UNDERGROUND GATES =====
def is_too_big(sp):
    return (sp.get("followers", 0) > MAX_SPOTIFY_FOLLOWERS or
            sp.get("popularity", 0) > MAX_SPOTIFY_POPULARITY)

def is_within_discovery_scope(a):
    """Reuses is_too_big to avoid duplicate threshold logic."""
    return not is_too_big(a)


# ===== GROWTH SIGNAL =====
def compute_growth_signal(a):
    pop       = a.get("popularity", 0)
    followers = a.get("followers", 0)
    listeners = a.get("listeners", 0)
    playcount = a.get("playcount", 0)
    ratio     = (playcount / listeners) if listeners else 0
    score, reasons = 0, []
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
    return min(score, 100), (reasons[0] if reasons else "stable")


# ===== SCORING (0-100) =====
def score_artist(a, target_families=None):
    listeners   = a["listeners"]
    playcount   = a["playcount"]
    popularity  = a.get("popularity", 0)
    has_spotify = bool(a.get("spotify_id"))

    growth, growth_reason = compute_growth_signal(a)
    a["growth_signal"]        = growth
    a["growth_signal_reason"] = growth_reason

    fams   = get_genre_families(a.get("tags"), a.get("genres"))
    gmatch = genre_match_score(fams, target_families)
    a["genre_match_score"] = round(gmatch, 3)
    a["genre_families"]    = sorted(fams)

    gscore = round(gmatch * 25)
    gr     = round(growth / 100 * 25)

    if   DISCOVERY_TARGET_MIN <= listeners <= DISCOVERY_TARGET_MAX: sz = 25
    elif 0 < listeners < DISCOVERY_TARGET_MIN:                      sz = 18
    elif DISCOVERY_TARGET_MAX < listeners <= DISCOVERY_ABS_MAX:     sz = 10
    elif listeners == 0:                                             sz = 8
    else:                                                            sz = 0

    if   not has_spotify:                       p = 0
    elif popularity == 0:                       p = 5
    elif popularity < 15:                       p = 16
    elif popularity < 35:                       p = 20
    elif popularity <= MAX_SPOTIFY_POPULARITY:  p = 12
    else:                                       p = 4

    ratio = (playcount / listeners) if listeners else 0
    e = 5 if ratio >= 20 else (3 if ratio >= 8 else 1)

    total = min(gscore + gr + sz + p + e, 100)
    return total, {"genre_relevance": gscore, "growth": gr,
                   "size": sz, "spotify_popularity": p, "engagement": e}


# ===== BUILD ARTIST OBJECT =====
def build_artist(lf, sp, target_families=None):
    a = {
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
    }
    a["score"], a["score_breakdown"] = score_artist(a, target_families)
    return a


# ===== TARGET GENRE PROFILE =====
def build_target_profile(seeds):
    if TARGET_GENRES:
        return set(TARGET_GENRES)
    # Sample if pool is large to avoid excessive API calls.
    profile_seeds = (
        seeds if len(seeds) <= MAX_SEEDS_BEFORE_SAMPLING
        else random.sample(seeds, SEED_SAMPLE_SIZE)
    )
    fams = set()
    for s in profile_seeds:
        sp = get_spotify(s)
        lf = get_lastfm_info(s)
        fams.update(get_genre_families(lf.get("tags"), sp.get("genres")))
    return fams


# ===== SEED SELECTION =====
def select_active_seeds(seeds, target_families,
                        match_fraction=SEED_MATCH_FRACTION,
                        min_active=SEED_MIN_ACTIVE):
    """Shuffled, genre-aware subset of seeds for a single run."""
    # Sample if pool is large to avoid excessive API calls.
    if len(seeds) > MAX_SEEDS_BEFORE_SAMPLING:
        seeds = random.sample(seeds, SEED_SAMPLE_SIZE)
        print(f"Seed pool sampled to {len(seeds)} before selection.")

    matching, fallback = [], []
    for s in seeds:
        sp   = get_spotify(s)
        lf   = get_lastfm_info(s)
        fams = get_genre_families(lf.get("tags", []), sp.get("genres", []))
        if not target_families or (fams & target_families):
            matching.append(s)
        else:
            fallback.append(s)
    random.shuffle(matching)
    random.shuffle(fallback)
    k      = max(min_active, math.ceil(len(matching) * match_fraction))
    active = matching[:k]
    if len(active) < min_active:
        active += fallback[:min_active - len(active)]
    print(f"Seed selection: {len(active)} active / {len(seeds)} total "
          f"(matching={len(matching)}, fallback={len(fallback)}, k={k})\n")
    return active


# ===== DEDUPLICATION =====
def dedupe_artists(artists, fuzzy_threshold=0.92):
    def completeness(a):
        return (1 if a.get("spotify_id") else 0,
                a.get("score", 0),
                a.get("listeners", 0))

    by_key, by_spotify = {}, {}
    for a in artists:
        key = norm_key(a["name"])
        if not key:
            continue
        sid = a.get("spotify_id")
        if sid and sid in by_spotify:
            if completeness(a) > completeness(by_spotify[sid]):
                by_spotify[sid] = a
            continue
        if key in by_key:
            if completeness(a) > completeness(by_key[key]):
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
                dup_of = kept; break
            if name_similarity(a["name"], kept["name"]) >= fuzzy_threshold:
                dup_of = kept; break
        if dup_of is None:
            result.append(a)
        elif completeness(a) > completeness(dup_of):
            result[result.index(dup_of)] = a
    return result


# ===== BFS CRAWL (UNDERGROUND-ONLY) =====
def discover(seeds, depth=2, per_artist=12, sample_size=8, hard_cap=300,
             max_per_origin=40, target_families=None,
             expand_score_threshold=EXPAND_SCORE_THRESHOLD,
             expand_top_fraction=EXPAND_TOP_FRACTION,
             seed_fanout=SEED_FANOUT):
    seen          = set()
    keep          = []
    origin_counts = Counter()
    frontier, seen_frontier = [], set()
    stats = {
        "scanned": 0,
        "kept": 0,
        "bridge_too_big": 0,
        "skipped_too_big": 0,
        "skipped_out_of_scope": 0,
    }

    for s in seeds:
        disp = primary_artist_name(s)
        k    = norm_key(disp)
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

            lf              = get_lastfm_info(name)
            full_path       = path + [lf["name"]]
            lf["discovered_from"] = " → ".join(full_path)
            listeners       = lf["listeners"]
            sp              = get_spotify(lf["name"])
            lf["_spotify"]  = sp
            followers       = sp.get("followers", 0)
            popularity      = sp.get("popularity", 0)
            stats["scanned"] += 1

            if is_too_big(sp):
                stats["skipped_too_big"] += 1
                if ALLOW_BIG_ARTIST_BRIDGES:
                    stats["bridge_too_big"] += 1
                    print(f"  BRIDGE (too big, not saved): {lf['name']} "
                          f"(followers={followers}, pop={popularity})")
                    scored_nodes.append((0, lf, lf["name"], origin, full_path))
                else:
                    print(f"  STOP (too big): {lf['name']} "
                          f"(followers={followers}, pop={popularity})")
                    continue
            else:
                if 0 < listeners <= DISCOVERY_ABS_MAX and \
                   origin_counts[origin] < max_per_origin:
                    keep.append(lf)
                    origin_counts[origin] += 1
                    stats["kept"] += 1
                else:
                    stats["skipped_out_of_scope"] += 1
                a = build_artist(lf, sp, target_families)
                scored_nodes.append((a["score"], lf, lf["name"], origin, full_path))

            print(f"  scanned: {lf['name']} ({listeners} listeners, "
                  f"followers={followers}, pop={popularity})")

        if level < depth and scored_nodes:
            if level == 0:
                expandable = list(scored_nodes)
            else:
                by_score = sorted(scored_nodes, key=lambda x: x[0], reverse=True)
                top_n    = max(1, math.ceil(len(by_score) * expand_top_fraction))
                expandable = [
                    n for i, n in enumerate(by_score)
                    if n[0] >= expand_score_threshold or i < top_n
                ]
            next_frontier, next_keys = [], set()
            for score, _lf, pname, origin, full_path in expandable:
                fanout = seed_fanout if level == 0 else fanout_for_score(score)
                sims   = get_similar(pname, per_artist)
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

    logger.info(
        "discover: scanned=%s kept=%s bridge_too_big=%s skipped_too_big=%s skipped_out_of_scope=%s",
        stats["scanned"],
        stats["kept"],
        stats["bridge_too_big"],
        stats["skipped_too_big"],
        stats["skipped_out_of_scope"],
    )
    return keep


# ===== SAVE TO SUPABASE =====
ARTIST_SELECT_COLUMNS = (
    "name,mbid,spotify_id,genres,followers,popularity,"
    "listeners,playcount,url,image,score,tags,genre_families,"
    "growth_signal,growth_signal_reason,match_score,genre_match_score,score_breakdown,"
    "discovered_from"
)

REQUIRED_ARTIST_COLUMNS = {"name"}
OPTIONAL_ARTIST_COLUMNS = {
    "mbid", "spotify_id", "followers", "popularity", "listeners", "playcount",
    "genres", "tags", "score", "score_breakdown", "match_score",
    "genre_match_score", "genre_families", "discovered_from", "growth_signal",
    "growth_signal_reason", "url", "image",
}

def _missing_db_columns(exc):
    msg = str(exc)
    return {
        col for col in (REQUIRED_ARTIST_COLUMNS | OPTIONAL_ARTIST_COLUMNS)
        if col in msg
    }

def _drop_columns(rows, columns):
    if not columns:
        return rows
    return [
        {k: v for k, v in row.items() if k not in columns}
        for row in rows
    ]

def _artist_row(a):
    """Single source of truth for the artists table row format."""
    return {
        "name":                 a["name"],
        "mbid":                 a.get("mbid"),
        "spotify_id":           a.get("spotify_id"),
        "followers":            a.get("followers"),
        "popularity":           a.get("popularity"),
        "listeners":            a.get("listeners"),
        "playcount":            a.get("playcount"),
        "genres":               a.get("genres"),
        "tags":                 a.get("tags"),
        "score":                a.get("score"),
        "score_breakdown":      a.get("score_breakdown") or {},
        "match_score":          a.get("match_score"),
        "genre_match_score":    a.get("genre_match_score"),
        "genre_families":       list(a.get("genre_families", [])),
        "discovered_from":      a.get("discovered_from"),
        "growth_signal":        a.get("growth_signal"),
        "growth_signal_reason": a.get("growth_signal_reason"),
        "url":                  a.get("url"),
        "image":                a.get("image"),
    }

def save_artist(a):
    """Save a single artist. Delegates to save_artists_bulk for consistency."""
    save_artists_bulk([a])

def save_artists_bulk(artists, batch_size=100):
    """Save all fields for a list of artists. Uses _artist_row as the single source of truth."""
    if not artists:
        logger.info("save_artists_bulk: Saving 0 artists")
        return 0
    if supabase is None:
        logger.error("save_artists_bulk: Supabase is not configured; skipped saving %s artists", len(artists))
        return 0
    rows = [_artist_row(a) for a in artists]
    logger.info("save_artists_bulk: Saving %s artists", len(rows))
    saved = 0
    disabled_optional_columns = set()
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            resp = supabase.table("artists").upsert(
                _drop_columns(batch, disabled_optional_columns),
                on_conflict="name"
            ).execute()
            saved += len(batch)
            logger.info(
                "save_artists_bulk: Saved batch %s-%s (%s rows, response_rows=%s)",
                i + 1,
                i + len(batch),
                len(batch),
                len(resp.data or []),
            )
        except Exception as exc:
            missing = _missing_db_columns(exc)
            missing_required = missing & REQUIRED_ARTIST_COLUMNS
            missing_optional = missing & OPTIONAL_ARTIST_COLUMNS
            if missing_optional and not missing_required:
                disabled_optional_columns |= missing_optional
                logger.exception(
                    "save_artists_bulk: artists table missing optional column(s) %s; retrying batch without them. error=%s",
                    sorted(missing_optional),
                    exc,
                )
                try:
                    resp = supabase.table("artists").upsert(
                        _drop_columns(batch, disabled_optional_columns),
                        on_conflict="name"
                    ).execute()
                    saved += len(batch)
                    logger.info(
                        "save_artists_bulk: Saved batch %s-%s after dropping optional columns %s (response_rows=%s)",
                        i + 1,
                        i + len(batch),
                        sorted(disabled_optional_columns),
                        len(resp.data or []),
                    )
                    continue
                except Exception as retry_exc:
                    logger.exception(
                        "save_artists_bulk: retry failed for batch %s-%s after dropping optional columns %s: %s",
                        i + 1,
                        i + len(batch),
                        sorted(disabled_optional_columns),
                        retry_exc,
                    )
                    continue
            logger.exception(
                "save_artists_bulk: failed saving artists batch %s-%s. Check artists table columns. error=%s",
                i + 1,
                i + len(batch),
                exc,
            )
            continue
    logger.info("save_artists_bulk: Saved %s artists", saved)
    return saved

def load_seeds_from_supabase():
    """Pull seed names from previous runs so the pool grows automatically."""
    if supabase is None:
        logger.error("load_seeds_from_supabase: Supabase is not configured")
        return []
    try:
        resp = supabase.table("seeds").select("name").execute()
        return [row["name"] for row in (resp.data or []) if row.get("name")]
    except Exception as exc:
        logger.exception("load_seeds_from_supabase: failed loading seeds: %s", exc)
        return []

def save_seeds_bulk(artists, score_threshold=SEED_PROMOTION_SCORE, batch_size=100):
    """Promote high-quality underground artists into the seeds table."""
    if supabase is None:
        logger.error("save_seeds_bulk: Supabase is not configured; skipped seed promotion")
        return 0
    in_scope = [a for a in artists if is_within_discovery_scope(a)]
    qualified = [a for a in in_scope if a.get("score", 0) >= score_threshold]
    rows = [
        {"name": a["name"]}
        for a in qualified
    ]
    if artists:
        print(
            f"Seed promotion check: {len(in_scope)}/{len(artists)} in scope, "
            f"{len(qualified)} score >= {score_threshold}."
        )
    saved = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            supabase.table("seeds").upsert(
                batch, on_conflict="name"
            ).execute()
            saved += len(batch)
        except Exception as exc:
            logger.exception(
                "save_seeds_bulk: failed saving seeds batch %s-%s: %s",
                i + 1,
                i + len(batch),
                exc,
            )
            continue
    return saved


def select_artist_rows_for_search(q):
    if supabase is None:
        logger.error("select_artist_rows_for_search: Supabase is not configured; query=%r", q)
        return []
    try:
        rows = (
            supabase.table("artists")
            .select(ARTIST_SELECT_COLUMNS)
            .ilike("name", f"%{q}%")
            .order("score", desc=True)
            .limit(50)
            .execute()
        ).data or []
        logger.info("select_artist_rows_for_search: Database returned %s artists for query=%r", len(rows), q)
        return rows
    except Exception as exc:
        logger.exception("select_artist_rows_for_search: search query failed for query=%r: %s", q, exc)
        return []


# ===== DB RECOMMENDATION LOADER =====
def load_recommendations_from_db(target_families=None, min_score=40, limit=200):
    """Load previously discovered artists from Supabase."""
    if supabase is None:
        logger.error("load_recommendations_from_db: Supabase is not configured")
        return []
    try:
        rows = (
            supabase.table("artists")
            .select("*")
            .gte("score", min_score)
            .order("score", desc=True)
            .limit(limit)
            .execute()
        ).data or []
    except Exception as exc:
        logger.exception("load_recommendations_from_db: DB load failed: %s", exc)
        return []

    artists = []
    for row in rows:
        a = {
            "name":                 row.get("name", ""),
            "mbid":                 row.get("mbid"),
            "listeners":            row.get("listeners", 0),
            "playcount":            row.get("playcount", 0),
            "tags":                 row.get("tags") or [],
            "genres":               row.get("genres") or [],
            "url":                  row.get("url"),
            "image":                row.get("image"),
            "popularity":           row.get("popularity", 0),
            "followers":            row.get("followers", 0),
            "spotify_id":           row.get("spotify_id"),
            "match_score":          row.get("match_score", 0.0),
            "score":                row.get("score", 0),
            "score_breakdown":      row.get("score_breakdown") or {},
            "discovered_from":      row.get("discovered_from"),
            "growth_signal":        row.get("growth_signal", 0),
            "growth_signal_reason": row.get("growth_signal_reason", "stable"),
            "genre_match_score":    row.get("genre_match_score", 0.0),
            "genre_families":       [],
        }
        a["genre_families"] = sorted(get_genre_families(a["tags"], a["genres"]))
        if target_families and not (set(a["genre_families"]) & target_families):
            continue
        key = norm_key(a["name"])
        if key and key not in _artist_discovery_cache:
            _artist_discovery_cache[key] = a
        artists.append(a)
    return artists


# ===== SHALLOW LIVE SEARCH SUPPLEMENT =====
def live_search_supplement(seeds, target_families, needed,
                           depth=1, per_artist=6, hard_cap=60):
    """
    Shallow BFS crawl when the recommendation pool is too small.
    Saves new artists to both the artists table and the seeds table
    so every discovery pass makes future runs smarter.
    """
    sample = seeds[:min(3, len(seeds))]
    logger.info(
        "live_search_supplement: Starting discovery with %s seed(s), depth=%s, cap=%s, need~%s",
        len(sample),
        depth,
        hard_cap,
        needed,
    )
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
    logger.info("live_search_supplement: Discovered %s raw artists", len(raw))
    enriched = []
    for c in raw:
        key = norm_key(c["name"])
        if key in _artist_discovery_cache:
            enriched.append(_artist_discovery_cache[key])
            continue
        # FIX 4: an empty dict {"spotify_id": None, ...} is truthy in Python,
        # so the old `or` idiom never retried failed lookups.
        # Check spotify_id explicitly instead.
        sp = c.get("_spotify")
        if not (sp and sp.get("spotify_id")):
            sp = get_spotify(c["name"])
        a  = build_artist(c, sp, target_families)
        _artist_discovery_cache[key] = a
        enriched.append(a)
    logger.info("live_search_supplement: Enriched %s artists", len(enriched))
    valid = [a for a in enriched if is_within_discovery_scope(a)]
    logger.info(
        "live_search_supplement: %s/%s artists are within discovery scope",
        len(valid),
        len(enriched),
    )
    if valid:
        logger.info("live_search_supplement: Saving %s artists", len(valid))
        n = save_artists_bulk(valid)
        save_seeds_bulk(valid)   # promote high-scorers to seeds
        logger.info("live_search_supplement: Saved %s artists", n)
    else:
        logger.info("live_search_supplement: Saving 0 artists")
    return enriched


# ===== SOCIAL LINK ENRICHMENT (MusicBrainz + Wikidata) =====
MB_API        = "https://musicbrainz.org/ws/2"
MB_USER_AGENT = "music-discovery/1.0"
MB_RATE_SLEEP = 1.0

_MB_TYPE_TO_SOCIAL = {
    "official homepage": "website",
    "youtube":           "youtube",
    "instagram":         "instagram",
    "twitter":           "x",
    "facebook":          "facebook",
    "tiktok":            "tiktok",
}

def _url_to_social_key(url):
    u = url.lower()
    if "instagram.com"  in u: return "instagram"
    if "tiktok.com"     in u: return "tiktok"
    if "youtube.com"    in u or "youtu.be" in u: return "youtube"
    if "twitter.com"    in u or "//x.com/" in u: return "x"
    if "facebook.com"   in u: return "facebook"
    return None

def _mb_url_relations(mbid):
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
    except Exception as exc:
        logger.warning("_mb_url_relations: failed for mbid=%r error=%s", mbid, exc)
    return []

def _wikidata_socials(qid):
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
        claims = (r.json().get("entities", {}).get(qid, {}).get("claims", {}))
    except Exception as exc:
        logger.warning("_wikidata_socials: failed for qid=%r error=%s", qid, exc)
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
    socials, wikidata_q = {}, None
    for rel in _mb_url_relations(artist.get("mbid")):
        url = (rel.get("url") or {}).get("resource", "")
        if not url:
            continue
        if "wikidata.org/wiki/Q" in url and wikidata_q is None:
            m = re.search(r"(Q\d+)", url)
            if m:
                wikidata_q = m.group(1)
        key = _MB_TYPE_TO_SOCIAL.get((rel.get("type") or "").lower())
        if key is None:
            key = _url_to_social_key(url)
        if key and key not in socials:
            socials[key] = url
    for k, v in _wikidata_socials(wikidata_q).items():
        if k not in socials:
            socials[k] = v
    return socials


# ===== DISCOVERY PIPELINE =====
def run_pipeline():
    SEEDS_FILE = "seeds.txt"

    # Load seeds from file (optional — works without it if DB seeds exist).
    file_seeds = []
    try:
        with open(SEEDS_FILE, encoding="utf-8") as f:
            file_seeds = [line.strip() for line in f if line.strip()]
        print(f"Loaded {len(file_seeds)} seeds from {SEEDS_FILE}.")
    except FileNotFoundError:
        print(f"'{SEEDS_FILE}' not found — continuing with DB seeds only.")

    # Merge with previously promoted DB seeds so the pool grows over time.
    db_seeds     = load_seeds_from_supabase()
    seed_key_set = {norm_key(s) for s in file_seeds}
    extra_db     = [s for s in db_seeds if norm_key(s) not in seed_key_set]
    seeds        = file_seeds + extra_db

    if not seeds:
        print("No seeds available (no seeds.txt and no DB seeds). Aborting.")
        return

    print(f"Seed pool: {len(file_seeds)} from file + {len(extra_db)} from DB "
          f"= {len(seeds)} total.")

    # Persist file seeds to DB so they survive restarts (score_threshold=0 = all pass).
    if file_seeds:
        save_seeds_bulk([{"name": s} for s in file_seeds], score_threshold=0)

    app.state.seeds = seeds
    random.shuffle(seeds)

    # Build genre profile (auto-samples if pool is large).
    target_families = build_target_profile(seeds)

    # Pick a shuffled, genre-matching subset for this run.
    active_seeds = select_active_seeds(seeds, target_families)

    print("Crawling for underground artists...\n")
    candidates = discover(
        active_seeds,
        depth=3,
        per_artist=5,
        sample_size=8,
        hard_cap=100,
        max_per_origin=40,
        target_families=target_families,
        expand_score_threshold=EXPAND_SCORE_THRESHOLD,
        expand_top_fraction=EXPAND_TOP_FRACTION,
        seed_fanout=SEED_FANOUT,
    )

    enriched = []
    for c in candidates:
        # FIX 3: an empty dict {"spotify_id": None, ...} is truthy in Python,
        # so the old `or` idiom never retried failed Spotify lookups from the
        # crawl phase.  Check spotify_id explicitly instead.
        sp = c.get("_spotify")
        if not (sp and sp.get("spotify_id")):
            sp = get_spotify(c["name"])
        a   = build_artist(c, sp, target_families)
        key = norm_key(a["name"])
        if key:
            _artist_discovery_cache[key] = a
        enriched.append(a)

    results = dedupe_artists(enriched)
    before  = len(results)
    results = [a for a in results if is_within_discovery_scope(a)]
    print(f"Scope filter: removed {before - len(results)} large artists.")
    results.sort(key=lambda x: x["score"], reverse=True)

    print(f"\nFresh crawl: {len(results)} artists.")
    db_recs = load_recommendations_from_db(target_families=target_families)
    print(f"DB cache: {len(db_recs)} previously discovered artists.")

    if db_recs:
        results = dedupe_artists(results + db_recs)
        results = [a for a in results if is_within_discovery_scope(a)]
        results.sort(key=lambda x: x["score"], reverse=True)
        print(f"After DB merge: {len(results)} total.")

    if len(results) < DB_MIN_RECOMMENDATIONS:
        needed = DB_MIN_RECOMMENDATIONS - len(results)
        live   = live_search_supplement(active_seeds, target_families, needed)
        live_f = [a for a in live if is_within_discovery_scope(a)]
        if live_f:
            results = dedupe_artists(results + live_f)
            results = [a for a in results if is_within_discovery_scope(a)]
            results.sort(key=lambda x: x["score"], reverse=True)
            print(f"After live supplement: {len(results)} total.")

    if INCLUDE_SOCIALS:
        print(f"\nFetching socials for {len(results)} artists...\n")
        for a in results:
            a["socials"] = fetch_socials(a)

    print(f"\n=== {len(results)} underground artists (deduped) ===\n")
    for a in results[:30]:
        print(f"{a['score']:>3}  {a['name']}  (match={a['match_score']})")
        print(f"     lastfm:  listeners={a['listeners']} playcount={a['playcount']}")
        print(f"     spotify: pop={a['popularity']} followers={a['followers']}")
        print(f"     growth={a['growth_signal']} ({a['growth_signal_reason']})"
              f"  genre_match={a['genre_match_score']}  fams={a.get('genre_families')}")
        print(f"     path={a.get('discovered_from')}")
        print(f"     {a['url']}")
        if INCLUDE_SOCIALS and a.get("socials"):
            for platform, link in sorted(a["socials"].items()):
                print(f"     {platform}: {link}")
        print()

    saved  = save_artists_bulk(results)
    print(f"Saved {saved} artists to Supabase.")

    seeded = save_seeds_bulk(results)
    print(f"Promoted {seeded} artists to seed pool (score >= {SEED_PROMOTION_SCORE}).")


# ===== STARTUP =====
@app.on_event("startup")
def startup():
    app.state.pipeline_running = False
    app.state.search_discovery_running = set()
    logger.info(
        "startup: API started version=%s; search is live, use GET/POST /run to trigger full discovery",
        APP_VERSION,
    )


# ===== ENDPOINTS =====
def run_pipeline_guarded():
    try:
        run_pipeline()
    except Exception as exc:
        logger.exception("run_pipeline_guarded: Pipeline failed: %s", exc)
        raise
    finally:
        app.state.pipeline_running = False

def run_search_discovery_guarded(query):
    key = norm_key(query)
    try:
        logger.info("run_search_discovery_guarded: Starting discovery for query=%r", query)
        live_search_supplement(
            [query],
            target_families=set(),
            needed=SEARCH_MIN_RESULTS,
            depth=1,
            per_artist=8,
            hard_cap=40,
        )
        logger.info("run_search_discovery_guarded: Finished discovery for query=%r", query)
    except Exception as exc:
        logger.exception("run_search_discovery_guarded: discovery failed for query=%r: %s", query, exc)
    finally:
        running = getattr(app.state, "search_discovery_running", set())
        running.discard(key)

@app.get("/search")
def search_artists(background_tasks: BackgroundTasks, q: str = Query(...)):
    """
    Search artists by name.
    Returns DB results immediately. If fewer than SEARCH_MIN_RESULTS exist, a
    small discovery pass is queued in the background so future searches improve
    without blocking the current request on Spotify or Last.fm.
    """
    logger.info("search_artists: request received query=%r", q)
    db_rows = select_artist_rows_for_search(q)
    results = list(db_rows)
    logger.info("search_artists: Database returned %s artists", len(results))

    discovery_queued = False
    discovery_already_running = False

    if len(results) < SEARCH_MIN_RESULTS:
        running = getattr(app.state, "search_discovery_running", set())
        key = norm_key(q)
        if key and key not in running:
            running.add(key)
            app.state.search_discovery_running = running
            discovery_queued = True
            logger.info(
                "search_artists: Starting discovery in background for query=%r because DB returned %s/%s artists",
                q,
                len(results),
                SEARCH_MIN_RESULTS,
            )
            background_tasks.add_task(run_search_discovery_guarded, q)
        else:
            discovery_already_running = True
            logger.info("search_artists: Discovery already running for query=%r", q)
    else:
        logger.info("search_artists: DB has enough results; no discovery queued")

    results.sort(key=lambda x: x.get("score") or 0, reverse=True)
    logger.info("search_artists: Returning %s artists", len(results[:50]))
    return {
        "query": q,
        "count": len(results),
        "results": results[:50],
        "discovery_queued": discovery_queued,
        "discovery_already_running": discovery_already_running,
    }


@app.get("/run")
def trigger_pipeline_from_browser(background_tasks: BackgroundTasks):
    """Browser-friendly alias for manual testing."""
    return _trigger_pipeline(background_tasks, source="GET /run")


@app.post("/run")
def trigger_pipeline(background_tasks: BackgroundTasks):
    """Trigger the full discovery pipeline in the background (non-blocking)."""
    return _trigger_pipeline(background_tasks, source="POST /run")


@app.get("/status")
def status():
    """Quick operational status for Railway/manual testing."""
    return {
        "ok": True,
        "version": APP_VERSION,
        "configured": {
            "supabase": supabase is not None,
            "lastfm": bool(LASTFM_API_KEY),
            "spotify": spotify is not None,
        },
        "pipeline_running": getattr(app.state, "pipeline_running", False),
        "search_discovery_running": sorted(
            getattr(app.state, "search_discovery_running", set())
        ),
        "spotify_rate_limited": SPOTIFY_RATE_LIMITED,
        "spotify_cooldown_remaining_seconds": max(
            0,
            int(SPOTIFY_COOLDOWN_UNTIL - time.time()),
        ),
    }


def _trigger_pipeline(background_tasks, source):
    if getattr(app.state, "pipeline_running", False):
        logger.info("trigger_pipeline: pipeline already running source=%s", source)
        return {"status": "pipeline already running"}
    app.state.pipeline_running = True
    logger.info("trigger_pipeline: pipeline started source=%s", source)
    background_tasks.add_task(run_pipeline_guarded)
    return {"status": "pipeline started"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080))
    )
