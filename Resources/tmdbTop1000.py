#!/usr/bin/env python3
import datetime
import time
import requests
import pymysql
import logging
from math import ceil

# ====== LOGGING ======
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("tmdb-import")

# ====== CONFIG ======
TMDB_API_KEY = "bdeddfef8a400a117d24a8c6c6b99351"
MYSQL_HOST = "localhost"
MYSQL_USER = "dimme"
MYSQL_PASS = "Telenet00"
MYSQL_PORT = 3306
DB_NAME    = "thegame"

PAGES = 60                       # 25 * 20 = 500
REGION = "BE"                    # bias movie popularity to Belgium
SLEEP_BETWEEN_PAGES = 0.25
SLEEP_BETWEEN_EXTERNAL = 0.05
HTTP_TIMEOUT = 30
RETRIES = 3
BACKOFF = 1.5

# ====== TMDb helpers ======
SESSION = requests.Session()
BASE = "https://api.themoviedb.org/3"

def tmdb_get(path, params=None):
    p = {"api_key": TMDB_API_KEY, "language": "en-US"}
    if params:
        p.update(params)
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = SESSION.get(f"{BASE}{path}", params=p, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            wait = BACKOFF ** (attempt - 1)
            log.warning("TMDb %s failed (attempt %d/%d): %s; retrying in %.1fs",
                        path, attempt, RETRIES, e, wait)
            time.sleep(wait)
    raise last_err

def fetch_popular_movies(page: int):
    params = {"page": page}
    if REGION:
        params["region"] = REGION
    return tmdb_get("/movie/popular", params)

def fetch_popular_tv(page: int):
    return tmdb_get("/tv/popular", {"page": page})

def external_ids_movie(tmdb_id: int):
    return tmdb_get(f"/movie/{tmdb_id}/external_ids")

def external_ids_tv(tmdb_id: int):
    return tmdb_get(f"/tv/{tmdb_id}/external_ids")

# ====== DDL ======
DDL_DB = f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"

DDL_MOVIES = """
CREATE TABLE IF NOT EXISTS `popular_movies` (
  `rank` INT NOT NULL,
  `tmdb_id` INT NOT NULL,
  `imdb_id` VARCHAR(20) NULL,
  `title` VARCHAR(255) NOT NULL,
  `release_date` DATE NULL,
  `popularity` DOUBLE NULL,
  `vote_average` DOUBLE NULL,
  `vote_count` INT NULL,
  `checked_at` DATE NOT NULL,
  PRIMARY KEY (`tmdb_id`),
  KEY `idx_rank` (`rank`),
  KEY `idx_popularity` (`popularity`),
  KEY `idx_checked_at` (`checked_at`),
  KEY `idx_imdb_id` (`imdb_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

DDL_TV = """
CREATE TABLE IF NOT EXISTS `popular_tv` (
  `rank` INT NOT NULL,
  `tmdb_id` INT NOT NULL,
  `imdb_id` VARCHAR(20) NULL,
  `name` VARCHAR(255) NOT NULL,
  `first_air_date` DATE NULL,
  `popularity` DOUBLE NULL,
  `vote_average` DOUBLE NULL,
  `vote_count` INT NULL,
  `checked_at` DATE NOT NULL,
  PRIMARY KEY (`tmdb_id`),
  KEY `idx_rank` (`rank`),
  KEY `idx_popularity` (`popularity`),
  KEY `idx_checked_at` (`checked_at`),
  KEY `idx_imdb_id` (`imdb_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

UPSERT_MOVIE = """
INSERT INTO `popular_movies`
(`rank`, `tmdb_id`, `imdb_id`, `title`, `release_date`, `popularity`, `vote_average`, `vote_count`, `checked_at`)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  `rank`=VALUES(`rank`),
  `imdb_id`=VALUES(`imdb_id`),
  `title`=VALUES(`title`),
  `release_date`=VALUES(`release_date`),
  `popularity`=VALUES(`popularity`),
  `vote_average`=VALUES(`vote_average`),
  `vote_count`=VALUES(`vote_count`),
  `checked_at`=VALUES(`checked_at`);
"""

UPSERT_TV = """
INSERT INTO `popular_tv`
(`rank`, `tmdb_id`, `imdb_id`, `name`, `first_air_date`, `popularity`, `vote_average`, `vote_count`, `checked_at`)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  `rank`=VALUES(`rank`),
  `imdb_id`=VALUES(`imdb_id`),
  `name`=VALUES(`name`),
  `first_air_date`=VALUES(`first_air_date`),
  `popularity`=VALUES(`popularity`),
  `vote_average`=VALUES(`vote_average`),
  `vote_count`=VALUES(`vote_count`),
  `checked_at`=VALUES(`checked_at`);
"""

def connect(db=None, autocommit=True):
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        port=MYSQL_PORT,
        database=db,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
        autocommit=autocommit,
    )

def ensure_schema():
    log.info("Connecting to MySQL on %s:%d as %s …", MYSQL_HOST, MYSQL_PORT, MYSQL_USER)
    with connect(db=None) as cnx, cnx.cursor() as cur:
        cur.execute(DDL_DB)
        log.info("Database ensured: %s", DB_NAME)
    with connect(db=DB_NAME) as cnx, cnx.cursor() as cur:
        cur.execute(DDL_MOVIES)
        cur.execute(DDL_TV)
        log.info("Tables ensured: popular_movies, popular_tv")

def upsert_movies():
    checked_at = datetime.date.today().isoformat()
    rank = 1
    with connect(db=DB_NAME, autocommit=False) as cnx, cnx.cursor() as cur:
        for page in range(1, PAGES + 1):
            data = fetch_popular_movies(page)
            results = data.get("results", []) or []
            if not results:
                log.info("Movies page %d returned 0 results, stopping.", page)
                break
            for m in results:
                tmdb_id = m.get("id")
                title = (m.get("title") or m.get("original_title") or "")[:255]
                rel = m.get("release_date") or None
                pop = m.get("popularity")
                va = m.get("vote_average")
                vc = m.get("vote_count")

                imdb_id = None
                try:
                    ext = external_ids_movie(tmdb_id)
                    imdb_id = ext.get("imdb_id")
                except Exception as e:
                    log.warning("movie %s external_ids failed: %s", tmdb_id, e)

                cur.execute(UPSERT_MOVIE, (
                    rank, tmdb_id, imdb_id, title, rel, pop, va, vc, checked_at
                ))
                rank += 1
                time.sleep(SLEEP_BETWEEN_EXTERNAL)
            cnx.commit()
            log.info("Movies page %d committed (+%d rows), rank now %d.",
                     page, len(results), rank - 1)
            time.sleep(SLEEP_BETWEEN_PAGES)

def upsert_tv():
    checked_at = datetime.date.today().isoformat()
    rank = 1
    with connect(db=DB_NAME, autocommit=False) as cnx, cnx.cursor() as cur:
        for page in range(1, PAGES + 1):
            data = fetch_popular_tv(page)
            results = data.get("results", []) or []
            if not results:
                log.info("TV page %d returned 0 results, stopping.", page)
                break
            for t in results:
                tmdb_id = t.get("id")
                name = (t.get("name") or t.get("original_name") or "")[:255]
                fad = t.get("first_air_date") or None
                pop = t.get("popularity")
                va = t.get("vote_average")
                vc = t.get("vote_count")

                imdb_id = None
                try:
                    ext = external_ids_tv(tmdb_id)
                    imdb_id = ext.get("imdb_id")
                except Exception as e:
                    log.warning("tv %s external_ids failed: %s", tmdb_id, e)

                cur.execute(UPSERT_TV, (
                    rank, tmdb_id, imdb_id, name, fad, pop, va, vc, checked_at
                ))
                rank += 1
                time.sleep(SLEEP_BETWEEN_EXTERNAL)
            cnx.commit()
            log.info("TV page %d committed (+%d rows), rank now %d.",
                     page, len(results), rank - 1)
            time.sleep(SLEEP_BETWEEN_PAGES)

def main():
    log.info("Starting import (movies region=%s)…", REGION or "global")
    ensure_schema()
    log.info("Upserting movies …")
    upsert_movies()
    log.info("Upserting TV …")
    upsert_tv()
    log.info("Done: thegame.popular_movies and thegame.popular_tv updated.")

if __name__ == "__main__":
    main()
