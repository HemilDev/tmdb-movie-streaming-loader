import os
from dotenv import load_dotenv
import requests
import mysql.connector
import time
from datetime import datetime

load_dotenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

# -------------------------
# Config
# -------------------------
API_KEY = os.getenv("TMDB_API_KEY")
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "ssl_ca": "certs/ca.pem"   # put your Aiven ca.pem here if using Aiven
}

YEARS = range(2015, 2026)
LANGUAGES = ["en", "hi", "ta", "te", "ml", "kn"]  # Hollywood + major Indian languages
ITEMS_PER_PAGE = 20
SLEEP_TIME = 0.35  # 3 req/sec safe for TMDb rate limits

# -------------------------
# Database Connection
# -------------------------
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# -------------------------
# Helper Functions
# -------------------------
def fetch_movies(year, language, page=1):
    """Fetch a page of movies from TMDB discover API"""
    url = "https://api.themoviedb.org/3/discover/movie"
    params = {
        "api_key": API_KEY,
        "language": "en-US",  # keep results readable
        "region": "IN" if language != "en" else "US",
        "with_original_language": language,
        "sort_by": "popularity.desc",
        "primary_release_year": year,
        "page": page
    }
    r = requests.get(url, params=params)
    if r.status_code == 429:
        print("Rate limit hit. Sleeping 10 sec...")
        time.sleep(10)
        return fetch_movies(year, language, page)
    return r.json()


def fetch_movie_details(movie_id):
    """Fetch detailed movie info (runtime, imdb_id, etc.)"""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {"api_key": API_KEY}
    r = requests.get(url, params=params)
    if r.status_code == 429:
        print("Rate limit hit. Sleeping 10 sec...")
        time.sleep(10)
        return fetch_movie_details(movie_id)
    if r.status_code != 200:
        print(f"Failed to fetch details for {movie_id}: {r.text}")
        return {}
    return r.json()


def fetch_movie_credits(movie_id):
    """Fetch credits (director + cast)"""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits"
    params = {"api_key": API_KEY}
    r = requests.get(url, params=params)
    if r.status_code == 429:
        print("Rate limit hit. Sleeping 10 sec...")
        time.sleep(10)
        return fetch_movie_credits(movie_id)
    if r.status_code != 200:
        print(f"Failed to fetch credits for {movie_id}: {r.text}")
        return {}
    return r.json()


def save_movie(movie):
    details = fetch_movie_details(movie["id"])
    credits = fetch_movie_credits(movie["id"])

    release_year = int(movie["release_date"].split("-")[0]) if movie.get("release_date") else None
    thumbnail = f"https://image.tmdb.org/t/p/w500{movie['backdrop_path']}" if movie.get("backdrop_path") else None
    poster = f"https://image.tmdb.org/t/p/w500{movie['poster_path']}" if movie.get("poster_path") else None
    
    # Extract director
    director = None
    for crew in credits.get("crew", []):
        if crew.get("job") == "Director":
            director = crew.get("name")
            break
    
    # Extract top 5 cast
    cast = [c.get("name") for c in credits.get("cast", [])[:5]]
    cast_str = ", ".join(cast)

    cursor.execute("""
        INSERT INTO movie_series 
        (content_id, title, type, release_year, description, language, duration, thumbnail, Poster_img, imdb_id, director, cast)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            title=VALUES(title), description=VALUES(description), language=VALUES(language),
            duration=VALUES(duration), thumbnail=VALUES(thumbnail), Poster_img=VALUES(Poster_img),
            imdb_id=VALUES(imdb_id), director=VALUES(director), cast=VALUES(cast)
    """, (
        movie["id"],
        movie["title"],
        "Movie",
        release_year,
        movie["overview"],
        movie["original_language"],
        details.get("runtime"),
        thumbnail,
        poster,
        details.get("imdb_id"),
        director,
        cast_str
    ))
    conn.commit()


# -------------------------
# Main Loop
# -------------------------
for year in YEARS:
    for lang in LANGUAGES:
        page = 1
        while True:
            print(f"Fetching {year}-{lang}, page {page}")
            data = fetch_movies(year, lang, page)
            results = data.get("results", [])
            if not results:
                break
            for movie in results:
                save_movie(movie)
                time.sleep(SLEEP_TIME)
            total_pages = data.get("total_pages", 1)
            if page >= total_pages:
                break
            page += 1

cursor.close()
conn.close()
print("All movies inserted successfully!")
