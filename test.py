import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id="727bc1f7bf47498792f8397c0a8636fa",
    client_secret="ad2d7871734f4325bb6e745320f352a3",
))

a = sp.artist("2hlmm7s2ICUX0LVIhVFlZQ")  # Gunna

print("KEYS RETURNED:", list(a.keys()))
print("---- FULL RESPONSE ----")
print(json.dumps(a, indent=2))