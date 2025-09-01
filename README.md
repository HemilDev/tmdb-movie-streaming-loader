# tmdb-movie-streaming-loader
data ingestion pipeline that fetches Indian &amp; Hollywood movies (2015–2025) from TMDb API and stores them in MySQL (Aiven) using a custom streaming schema. Powers a movie website with metadata, genres, cast, and recommendations.


# TMDb → MySQL Data Loader

This script fetches movies from TMDb (2015–2025, Hollywood + Indian languages) and inserts them into a MySQL database.

## Setup

1. Clone repo
   ```bash
   git clone https://github.com/yourname/tmdb-to-mysql.git
   cd tmdb-to-mysql

2. Install dependencies
    ```bash
    pip install -r requirements.txt


3. .env
    ```bash
    TMDB_API_KEY=your_tmdb_api_key
    DB_HOST=your_mysql_host
    DB_USER=your_mysql_user
    DB_PASS=your_mysql_password
    DB_NAME=your_mysql_db

4. Run script
    ```bash
    python tmdb_to_mysql.py