#!/usr/bin/env python3
import time, datetime, logging, pymysql, requests

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tmdb-enrich")

# --- CONFIG ---
TMDB_API_KEY = "bdeddfef8a400a117d24a8c6c6b99351"
MYSQL_HOST, MYSQL_USER, MYSQL_PASS, MYSQL_PORT = "localhost", "dimme", "Telenet00", 3306
DB_NAME = "thegame"

HTTP_TIMEOUT = 30
RETRIES = 3
BACKOFF = 1.5

# Limit how many people per title we enrich with extra headshot variants (keeps calls sane)
MAX_CAST_PER_TITLE = 20
MAX_DIRECTORS_PER_TITLE = 10

# --- HTTP helpers ---
SESSION = requests.Session()
BASE = "https://api.themoviedb.org/3"

def tmdb_get(path, params=None):
    p = {"api_key": TMDB_API_KEY}
    if params: p.update(params)
    err = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = SESSION.get(f"{BASE}{path}", params=p, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            err = e
            wait = BACKOFF ** (attempt - 1)
            log.warning("GET %s failed (attempt %d/%d): %s; retry in %.1fs", path, attempt, RETRIES, e, wait)
            time.sleep(wait)
    raise err

# --- DB helpers ---
def connect(db=None, autocommit=True):
    return pymysql.connect(
        host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS, port=MYSQL_PORT,
        database=db, charset="utf8mb4", cursorclass=pymysql.cursors.Cursor, autocommit=autocommit
    )

DDL = f"""
CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
"""

# People & headshots (shared)
DDL_PEOPLE = """
CREATE TABLE IF NOT EXISTS people (
  person_id INT NOT NULL,                -- TMDb person id
  imdb_id VARCHAR(20) NULL,
  name VARCHAR(255) NOT NULL,
  known_for_department VARCHAR(64) NULL,
  gender TINYINT NULL,
  popularity DOUBLE NULL,
  profile_path VARCHAR(255) NULL,        -- default headshot
  checked_at DATE NOT NULL,
  PRIMARY KEY (person_id),
  KEY idx_name (name),
  KEY idx_pop (popularity)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
DDL_PERSON_IMAGES = """
CREATE TABLE IF NOT EXISTS person_images (
  person_id INT NOT NULL,
  file_path VARCHAR(255) NOT NULL,
  width INT NULL,
  height INT NULL,
  vote_average DOUBLE NULL,
  vote_count INT NULL,
  aspect_ratio DOUBLE NULL,
  PRIMARY KEY (person_id, file_path),
  KEY idx_dims (width, height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

# Movies
DDL_MOVIE_DETAILS = """
CREATE TABLE IF NOT EXISTS movie_details (
  tmdb_id INT NOT NULL,
  imdb_id VARCHAR(20) NULL,
  title VARCHAR(255) NOT NULL,
  original_title VARCHAR(255) NULL,
  release_date DATE NULL,
  runtime INT NULL,
  original_language VARCHAR(8) NULL,
  homepage VARCHAR(255) NULL,
  status VARCHAR(64) NULL,
  overview TEXT NULL,
  popularity DOUBLE NULL,
  vote_average DOUBLE NULL,
  vote_count INT NULL,
  revenue BIGINT NULL,
  budget BIGINT NULL,
  checked_at DATE NOT NULL,
  PRIMARY KEY (tmdb_id),
  KEY idx_imdb (imdb_id),
  KEY idx_rel (release_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
DDL_MOVIE_GENRES = """
CREATE TABLE IF NOT EXISTS movie_genres (
  tmdb_id INT NOT NULL,
  genre_id INT NOT NULL,
  genre_name VARCHAR(64) NOT NULL,
  PRIMARY KEY (tmdb_id, genre_id),
  KEY idx_genre (genre_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
DDL_MOVIE_COUNTRIES = """
CREATE TABLE IF NOT EXISTS movie_countries (
  tmdb_id INT NOT NULL,
  iso_3166_1 VARCHAR(2) NOT NULL,
  country_name VARCHAR(80) NOT NULL,
  PRIMARY KEY (tmdb_id, iso_3166_1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
DDL_MOVIE_IMAGES = """
CREATE TABLE IF NOT EXISTS movie_images (
  tmdb_id INT NOT NULL,
  img_type ENUM('backdrop','poster','logo') NOT NULL,
  file_path VARCHAR(255) NOT NULL,
  width INT NULL,
  height INT NULL,
  iso_639_1 VARCHAR(10) NULL,
  aspect_ratio DOUBLE NULL,
  vote_average DOUBLE NULL,
  vote_count INT NULL,
  PRIMARY KEY (tmdb_id, img_type, file_path),
  KEY idx_size (width, height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
DDL_MOVIE_CAST = """
CREATE TABLE IF NOT EXISTS movie_cast (
  tmdb_id INT NOT NULL,
  person_id INT NOT NULL,
  `order_in_cast` INT NULL,
  character_name VARCHAR(255) NULL,
  popularity DOUBLE NULL,
  PRIMARY KEY (tmdb_id, person_id),
  KEY idx_order (`order_in_cast`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
DDL_MOVIE_DIRECTORS = """
CREATE TABLE IF NOT EXISTS movie_directors (
  tmdb_id INT NOT NULL,
  person_id INT NOT NULL,
  PRIMARY KEY (tmdb_id, person_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

# TV (aggregate credits recommended)
DDL_TV_DETAILS = """
CREATE TABLE IF NOT EXISTS tv_details (
  tmdb_id INT NOT NULL,
  imdb_id VARCHAR(20) NULL,
  name VARCHAR(255) NOT NULL,
  original_name VARCHAR(255) NULL,
  first_air_date DATE NULL,
  last_air_date DATE NULL,
  number_of_seasons INT NULL,
  number_of_episodes INT NULL,
  original_language VARCHAR(8) NULL,
  homepage VARCHAR(255) NULL,
  status VARCHAR(64) NULL,
  overview TEXT NULL,
  popularity DOUBLE NULL,
  vote_average DOUBLE NULL,
  vote_count INT NULL,
  checked_at DATE NOT NULL,
  PRIMARY KEY (tmdb_id),
  KEY idx_imdb (imdb_id),
  KEY idx_first (first_air_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
DDL_TV_GENRES = """
CREATE TABLE IF NOT EXISTS tv_genres (
  tmdb_id INT NOT NULL,
  genre_id INT NOT NULL,
  genre_name VARCHAR(64) NOT NULL,
  PRIMARY KEY (tmdb_id, genre_id),
  KEY idx_genre (genre_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
DDL_TV_COUNTRIES = """
CREATE TABLE IF NOT EXISTS tv_countries (
  tmdb_id INT NOT NULL,
  iso_3166_1 VARCHAR(2) NOT NULL,
  country_name VARCHAR(80) NOT NULL,
  PRIMARY KEY (tmdb_id, iso_3166_1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
DDL_TV_IMAGES = """
CREATE TABLE IF NOT EXISTS tv_images (
  tmdb_id INT NOT NULL,
  img_type ENUM('backdrop','poster','logo') NOT NULL,
  file_path VARCHAR(255) NOT NULL,
  width INT NULL,
  height INT NULL,
  iso_639_1 VARCHAR(10) NULL,
  aspect_ratio DOUBLE NULL,
  vote_average DOUBLE NULL,
  vote_count INT NULL,
  PRIMARY KEY (tmdb_id, img_type, file_path),
  KEY idx_size (width, height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
DDL_TV_CAST = """
CREATE TABLE IF NOT EXISTS tv_cast (
  tmdb_id INT NOT NULL,
  person_id INT NOT NULL,
  total_episode_count INT NULL,
  popularity DOUBLE NULL,
  PRIMARY KEY (tmdb_id, person_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
DDL_TV_DIRECTORS = """
CREATE TABLE IF NOT EXISTS tv_directors (
  tmdb_id INT NOT NULL,
  person_id INT NOT NULL,
  total_episode_count INT NULL,
  PRIMARY KEY (tmdb_id, person_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

# --- UPSERT statements ---
UPSERT_PEOPLE = """
INSERT INTO people (person_id, imdb_id, name, known_for_department, gender, popularity, profile_path, checked_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  imdb_id=VALUES(imdb_id), name=VALUES(name), known_for_department=VALUES(known_for_department),
  gender=VALUES(gender), popularity=VALUES(popularity), profile_path=VALUES(profile_path), checked_at=VALUES(checked_at);
"""
UPSERT_PERSON_IMAGE = """
INSERT IGNORE INTO person_images (person_id, file_path, width, height, vote_average, vote_count, aspect_ratio)
VALUES (%s,%s,%s,%s,%s,%s,%s);
"""

UPSERT_MOVIE_DETAILS = """
INSERT INTO movie_details
(tmdb_id, imdb_id, title, original_title, release_date, runtime, original_language, homepage, status, overview,
 popularity, vote_average, vote_count, revenue, budget, checked_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
 imdb_id=VALUES(imdb_id), title=VALUES(title), original_title=VALUES(original_title),
 release_date=VALUES(release_date), runtime=VALUES(runtime), original_language=VALUES(original_language),
 homepage=VALUES(homepage), status=VALUES(status), overview=VALUES(overview),
 popularity=VALUES(popularity), vote_average=VALUES(vote_average), vote_count=VALUES(vote_count),
 revenue=VALUES(revenue), budget=VALUES(budget), checked_at=VALUES(checked_at);
"""
UPSERT_MOVIE_GENRE = "INSERT IGNORE INTO movie_genres (tmdb_id, genre_id, genre_name) VALUES (%s,%s,%s);"
UPSERT_MOVIE_COUNTRY = "INSERT IGNORE INTO movie_countries (tmdb_id, iso_3166_1, country_name) VALUES (%s,%s,%s);"
UPSERT_MOVIE_IMAGE = """
INSERT IGNORE INTO movie_images (tmdb_id, img_type, file_path, width, height, iso_639_1, aspect_ratio, vote_average, vote_count)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""
UPSERT_MOVIE_CAST = """
INSERT INTO movie_cast (tmdb_id, person_id, `order_in_cast`, character_name, popularity)
VALUES (%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE `order_in_cast`=VALUES(`order_in_cast`), character_name=VALUES(character_name), popularity=VALUES(popularity);
"""
UPSERT_MOVIE_DIRECTOR = "INSERT IGNORE INTO movie_directors (tmdb_id, person_id) VALUES (%s,%s);"

UPSERT_TV_DETAILS = """
INSERT INTO tv_details
(tmdb_id, imdb_id, name, original_name, first_air_date, last_air_date, number_of_seasons, number_of_episodes,
 original_language, homepage, status, overview, popularity, vote_average, vote_count, checked_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
 imdb_id=VALUES(imdb_id), name=VALUES(name), original_name=VALUES(original_name),
 first_air_date=VALUES(first_air_date), last_air_date=VALUES(last_air_date),
 number_of_seasons=VALUES(number_of_seasons), number_of_episodes=VALUES(number_of_episodes),
 original_language=VALUES(original_language), homepage=VALUES(homepage), status=VALUES(status), overview=VALUES(overview),
 popularity=VALUES(popularity), vote_average=VALUES(vote_average), vote_count=VALUES(vote_count), checked_at=VALUES(checked_at);
"""
UPSERT_TV_GENRE = "INSERT IGNORE INTO tv_genres (tmdb_id, genre_id, genre_name) VALUES (%s,%s,%s);"
UPSERT_TV_COUNTRY = "INSERT IGNORE INTO tv_countries (tmdb_id, iso_3166_1, country_name) VALUES (%s,%s,%s);"
UPSERT_TV_IMAGE = """
INSERT IGNORE INTO tv_images (tmdb_id, img_type, file_path, width, height, iso_639_1, aspect_ratio, vote_average, vote_count)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""
UPSERT_TV_CAST = """
INSERT INTO tv_cast (tmdb_id, person_id, total_episode_count, popularity)
VALUES (%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE total_episode_count=VALUES(total_episode_count), popularity=VALUES(popularity);
"""
UPSERT_TV_DIRECTOR = """
INSERT INTO tv_directors (tmdb_id, person_id, total_episode_count)
VALUES (%s,%s,%s)
ON DUPLICATE KEY UPDATE total_episode_count=VALUES(total_episode_count);
"""

def ensure_schema():
    with connect(db=None) as cnx, cnx.cursor() as cur:
        cur.execute(DDL)
    with connect(db=DB_NAME) as cnx, cnx.cursor() as cur:
        for ddl in (
            DDL_PEOPLE, DDL_PERSON_IMAGES,
            DDL_MOVIE_DETAILS, DDL_MOVIE_GENRES, DDL_MOVIE_COUNTRIES, DDL_MOVIE_IMAGES, DDL_MOVIE_CAST, DDL_MOVIE_DIRECTORS,
            DDL_TV_DETAILS, DDL_TV_GENRES, DDL_TV_COUNTRIES, DDL_TV_IMAGES, DDL_TV_CAST, DDL_TV_DIRECTORS
        ):
            cur.execute(ddl)
    log.info("Schema ensured.")

def upsert_person(cur, person_id, name, profile_path, known_for_department, gender, popularity):
    checked_at = datetime.date.today().isoformat()
    imdb_id = None
    try:
        ext = tmdb_get(f"/person/{person_id}/external_ids")
        imdb_id = ext.get("imdb_id")
    except Exception:
        pass
    cur.execute(UPSERT_PEOPLE, (person_id, imdb_id, (name or "")[:255], known_for_department, gender, popularity, profile_path, checked_at))

def upsert_person_images(cur, person_id):
    try:
        imgs = tmdb_get(f"/person/{person_id}/images")
        for p in imgs.get("profiles", []) or []:
            cur.execute(UPSERT_PERSON_IMAGE, (
                person_id, p.get("file_path"), p.get("width"), p.get("height"),
                p.get("vote_average"), p.get("vote_count"), p.get("aspect_ratio")
            ))
    except Exception as e:
        log.debug("person %s images failed: %s", person_id, e)

def process_movies():
    with connect(db=DB_NAME, autocommit=False) as cnx, cnx.cursor() as cur:
        # Only movies with vote_count >= 100 that do NOT have details yet
        cur.execute("""
            SELECT pm.tmdb_id
            FROM popular_movies pm
            LEFT JOIN movie_details md ON md.tmdb_id = pm.tmdb_id
            WHERE COALESCE(pm.vote_count,0) >= 100
              AND md.tmdb_id IS NULL
            ORDER BY pm.rank
        """)
        kept = [row[0] for row in cur.fetchall()]
        log.info("Movies needing enrichment (no details yet, vote_count>=100): %d", len(kept))

        for i, tmdb_id in enumerate(kept, start=1):
            data = tmdb_get(f"/movie/{tmdb_id}", {
                "append_to_response": "images,credits,external_ids",
                "include_image_language": "en,null"
            })

            checked_at = datetime.date.today().isoformat()
            cur.execute(UPSERT_MOVIE_DETAILS, (
                data.get("id"),
                (data.get("external_ids") or {}).get("imdb_id"),
                (data.get("title") or "")[:255],
                (data.get("original_title") or "")[:255],
                data.get("release_date") or None,
                data.get("runtime"),
                data.get("original_language"),
                data.get("homepage"),
                data.get("status"),
                data.get("overview"),
                data.get("popularity"),
                data.get("vote_average"),
                data.get("vote_count"),
                data.get("revenue"),
                data.get("budget"),
                checked_at
            ))

            # genres
            cur.execute("DELETE FROM movie_genres WHERE tmdb_id=%s", (tmdb_id,))
            for g in data.get("genres", []) or []:
                cur.execute(UPSERT_MOVIE_GENRE, (tmdb_id, g.get("id"), g.get("name")))

            # production countries
            cur.execute("DELETE FROM movie_countries WHERE tmdb_id=%s", (tmdb_id,))
            for c in data.get("production_countries", []) or []:
                cur.execute(UPSERT_MOVIE_COUNTRY, (tmdb_id, c.get("iso_3166_1"), c.get("name")))

            # images
            cur.execute("DELETE FROM movie_images WHERE tmdb_id=%s", (tmdb_id,))
            images = data.get("images") or {}
            for img in images.get("backdrops", []) or []:
                cur.execute(UPSERT_MOVIE_IMAGE, (tmdb_id, "backdrop", img.get("file_path"),
                                                 img.get("width"), img.get("height"), img.get("iso_639_1"),
                                                 img.get("aspect_ratio"), img.get("vote_average"), img.get("vote_count")))
            for img in images.get("posters", []) or []:
                cur.execute(UPSERT_MOVIE_IMAGE, (tmdb_id, "poster", img.get("file_path"),
                                                 img.get("width"), img.get("height"), img.get("iso_639_1"),
                                                 img.get("aspect_ratio"), img.get("vote_average"), img.get("vote_count")))
            for img in images.get("logos", []) or []:
                cur.execute(UPSERT_MOVIE_IMAGE, (tmdb_id, "logo", img.get("file_path"),
                                                 img.get("width"), img.get("height"), img.get("iso_639_1"),
                                                 img.get("aspect_ratio"), img.get("vote_average"), img.get("vote_count")))

            # credits (actors & directors only)
            credits = data.get("credits") or {}
            cast = credits.get("cast") or []
            crew = credits.get("crew") or []

            cast_sorted = sorted(cast, key=lambda x: (x.get("order", 9999), -(x.get("popularity") or 0)))[:MAX_CAST_PER_TITLE]
            cur.execute("DELETE FROM movie_cast WHERE tmdb_id=%s", (tmdb_id,))
            for c in cast_sorted:
                pid = c.get("id")
                upsert_person(cur, pid, c.get("name"), c.get("profile_path"), "Acting", c.get("gender"), c.get("popularity"))
                cur.execute(UPSERT_MOVIE_CAST, (tmdb_id, pid, c.get("order"), c.get("character"), c.get("popularity")))

            directors = [m for m in crew if (m.get("job") == "Director")][:MAX_DIRECTORS_PER_TITLE]
            cur.execute("DELETE FROM movie_directors WHERE tmdb_id=%s", (tmdb_id,))
            for d in directors:
                pid = d.get("id")
                upsert_person(cur, pid, d.get("name"), d.get("profile_path"), d.get("known_for_department"), d.get("gender"), d.get("popularity"))
                cur.execute(UPSERT_MOVIE_DIRECTOR, (tmdb_id, pid))

            for p in cast_sorted[:10] + directors:
                upsert_person_images(cur, p.get("id"))

            cnx.commit()
            if i % 20 == 0 or i == len(kept):
                log.info("Movies processed %d/%d", i, len(kept))
            time.sleep(0.05)

def process_tv():
    with connect(db=DB_NAME, autocommit=False) as cnx, cnx.cursor() as cur:
        # Only TV with vote_count >= 100 that do NOT have details yet
        cur.execute("""
            SELECT pt.tmdb_id
            FROM popular_tv pt
            LEFT JOIN tv_details td ON td.tmdb_id = pt.tmdb_id
            WHERE COALESCE(pt.vote_count,0) >= 100
              AND td.tmdb_id IS NULL
            ORDER BY pt.rank
        """)
        kept = [row[0] for row in cur.fetchall()]
        log.info("TV needing enrichment (no details yet, vote_count>=100): %d", len(kept))

        for i, tmdb_id in enumerate(kept, start=1):
            data = tmdb_get(f"/tv/{tmdb_id}", {
                "append_to_response": "images,external_ids,aggregate_credits",
                "include_image_language": "en,null"
            })
            checked_at = datetime.date.today().isoformat()

            cur.execute(UPSERT_TV_DETAILS, (
                data.get("id"),
                (data.get("external_ids") or {}).get("imdb_id"),
                (data.get("name") or "")[:255],
                (data.get("original_name") or "")[:255],
                data.get("first_air_date") or None,
                data.get("last_air_date") or None,
                data.get("number_of_seasons"),
                data.get("number_of_episodes"),
                data.get("original_language"),
                data.get("homepage"),
                data.get("status"),
                data.get("overview"),
                data.get("popularity"),
                data.get("vote_average"),
                data.get("vote_count"),
                checked_at
            ))

            cur.execute("DELETE FROM tv_genres WHERE tmdb_id=%s", (tmdb_id,))
            for g in data.get("genres", []) or []:
                cur.execute(UPSERT_TV_GENRE, (tmdb_id, g.get("id"), g.get("name")))

            cur.execute("DELETE FROM tv_countries WHERE tmdb_id=%s", (tmdb_id,))
            for iso in data.get("origin_country", []) or []:
                cur.execute(UPSERT_TV_COUNTRY, (tmdb_id, iso, iso))

            cur.execute("DELETE FROM tv_images WHERE tmdb_id=%s", (tmdb_id,))
            images = data.get("images") or {}
            for img in images.get("backdrops", []) or []:
                cur.execute(UPSERT_TV_IMAGE, (tmdb_id, "backdrop", img.get("file_path"),
                                              img.get("width"), img.get("height"), img.get("iso_639_1"),
                                              img.get("aspect_ratio"), img.get("vote_average"), img.get("vote_count")))
            for img in images.get("posters", []) or []:
                cur.execute(UPSERT_TV_IMAGE, (tmdb_id, "poster", img.get("file_path"),
                                              img.get("width"), img.get("height"), img.get("iso_639_1"),
                                              img.get("aspect_ratio"), img.get("vote_average"), img.get("vote_count")))
            for img in images.get("logos", []) or []:
                cur.execute(UPSERT_TV_IMAGE, (tmdb_id, "logo", img.get("file_path"),
                                              img.get("width"), img.get("height"), img.get("iso_639_1"),
                                              img.get("aspect_ratio"), img.get("vote_average"), img.get("vote_count")))

            agg = data.get("aggregate_credits") or {}
            cast = agg.get("cast") or []
            crew = agg.get("crew") or []

            cast_sorted = sorted(
                cast, key=lambda x: (-(x.get("total_episode_count") or 0), -(x.get("popularity") or 0))
            )[:MAX_CAST_PER_TITLE]
            cur.execute("DELETE FROM tv_cast WHERE tmdb_id=%s", (tmdb_id,))
            for c in cast_sorted:
                pid = c.get("id")
                upsert_person(cur, pid, c.get("name"), c.get("profile_path"), "Acting", c.get("gender"), c.get("popularity"))
                cur.execute(UPSERT_TV_CAST, (tmdb_id, pid, c.get("total_episode_count"), c.get("popularity")))

            dirs = []
            for member in crew:
                jobs = member.get("jobs") or []
                total_eps = 0
                for j in jobs:
                    if (j.get("job") == "Director"):
                        total_eps += (j.get("episode_count") or 0)
                if total_eps > 0:
                    m = dict(member)
                    m["_dir_episode_count"] = total_eps
                    dirs.append(m)
            dirs = sorted(dirs, key=lambda m: -m["_dir_episode_count"])[:MAX_DIRECTORS_PER_TITLE]

            cur.execute("DELETE FROM tv_directors WHERE tmdb_id=%s", (tmdb_id,))
            for d in dirs:
                pid = d.get("id")
                upsert_person(cur, pid, d.get("name"), d.get("profile_path"), d.get("known_for_department"), d.get("gender"), d.get("popularity"))
                cur.execute(UPSERT_TV_DIRECTOR, (tmdb_id, pid, d["_dir_episode_count"]))

            for p in cast_sorted[:10] + dirs:
                upsert_person_images(cur, p.get("id"))

            cnx.commit()
            if i % 20 == 0 or i == len(kept):
                log.info("TV processed %d/%d", i, len(kept))
            time.sleep(0.05)

def main():
    ensure_schema()
    log.info("Processing movies…")
    process_movies()
    log.info("Processing TV…")
    process_tv()
    log.info("Done.")

if __name__ == "__main__":
    main()
