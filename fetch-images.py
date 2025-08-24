#!/usr/bin/env python3
# fetch_question_images.py
# - Reads unique URLs from thegame.questions
# - If images/<filename> exists (non-empty) -> skip download, but still set filename in DB
# - Else download, resize to height 720px with ImageMagick, save to images/<filename>
# - Update questions.filename (no directory)

import os
import sys
import logging
import time
import shutil
import subprocess
import urllib.parse

import requests
import pymysql
from typing import Optional

# ---------- CONFIG ----------
MYSQL = dict(host="localhost", user="dimme", password="Telenet00", port=3306, database="thegame")
IMAGES_DIR = "static\images"   # will be created if missing
HTTP_TIMEOUT = 60
RETRIES = 3
BACKOFF = 1.5  # exponential backoff base (1.0, 1.5, 2.25, ...)

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fetch-images")

# ---------- DB ----------
def connect():
    return pymysql.connect(
        host=MYSQL["host"],
        user=MYSQL["user"],
        password=MYSQL["password"],
        port=MYSQL["port"],
        database=MYSQL["database"],
        autocommit=False,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
    )

# ---------- ImageMagick detection ----------
def detect_imagemagick() -> Optional[str]:
    # Prefer 'magick' (Windows/newer IM), else 'convert' (Linux/macOS)
    for exe in ("magick", "convert"):
        path = shutil.which(exe)
        if path:
            return exe
    return None

IM_BIN = detect_imagemagick()
if not IM_BIN:
    log.error("ImageMagick not found. Install it or add it to PATH. "
              "On Windows, install from imagemagick.org and ensure 'magick.exe' is in PATH.")
    sys.exit(1)

# ---------- HTTP ----------
SESSION = requests.Session()

def http_get_with_retries(url: str):
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = SESSION.get(url, stream=True, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            wait = BACKOFF ** (attempt - 1)
            log.warning("GET failed (attempt %d/%d) %s ; retry in %.1fs", attempt, RETRIES, e, wait)
            time.sleep(wait)
    raise last_err

# ---------- Helpers ----------
def filename_from_url(url: str) -> Optional[str]:
    # last segment of the path, e.g. https://.../original/abc123.jpg -> abc123.jpg
    path = urllib.parse.urlparse(url).path
    if not path:
        return None
    name = os.path.basename(path)
    return name or None

def make_src_name(final_path: str) -> str:
    # Keep the real extension last so IM reads the correct format.
    # e.g. images\foo.jpg -> images\foo.src.jpg
    base, ext = os.path.splitext(final_path)
    return f"{base}.src{ext}"

def resize_to_height_720(src_path: str, dst_path: str):
    # Use ImageMagick to resize by height, keep aspect ratio, auto-orient and strip metadata
    # Command pattern works for both 'magick' and 'convert'
    cmd = [IM_BIN, src_path, "-auto-orient", "-resize", "x720", "-strip", "-quality", "90", dst_path]
    subprocess.run(cmd, check=True)

def file_exists_nonempty(path: str) -> bool:
    try:
        return os.path.isfile(path) and os.path.getsize(path) > 0
    except OSError:
        return False

def main():
    os.makedirs(IMAGES_DIR, exist_ok=True)

    with connect() as cnx, cnx.cursor() as cur:
        # Get unique URLs (you can add "AND (filename IS NULL OR filename='')" to process only missing files)
        cur.execute("SELECT DISTINCT url FROM questions WHERE url IS NOT NULL AND url <> ''")
        urls = [row[0] for row in cur.fetchall()]
        total = len(urls)
        log.info("Found %d unique URLs to evaluate.", total)

        processed = 0
        for url in urls:
            fname = filename_from_url(url)
            if not fname:
                log.warning("Skipping URL without filename: %s", url)
                continue

            out_path = os.path.join(IMAGES_DIR, fname)
            need_download = not file_exists_nonempty(out_path)

            if need_download:
                tmp_part = out_path + ".part"
                src_path = make_src_name(out_path)
                tmp_resized = out_path + ".resized"

                # Clean any stale temp files
                for p in (tmp_part, src_path, tmp_resized):
                    try:
                        if os.path.exists(p):
                            os.remove(p)
                    except OSError:
                        pass

                try:
                    # Download to .part
                    r = http_get_with_retries(url)
                    with open(tmp_part, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 256):
                            if chunk:
                                f.write(chunk)

                    # Rename .part -> .src.<ext> so IM sees the correct format (prevents PART decoder issue)
                    os.replace(tmp_part, src_path)

                    # Resize into .resized, then atomically move to final
                    resize_to_height_720(src_path, tmp_resized)
                    os.replace(tmp_resized, out_path)

                    log.info("Downloaded & resized -> %s", out_path)
                except subprocess.CalledProcessError as e:
                    log.error("ImageMagick failed for %s : %s", url, e)
                    # Cleanup temps
                    for p in (tmp_part, src_path, tmp_resized):
                        try:
                            if os.path.exists(p):
                                os.remove(p)
                        except OSError:
                            pass
                    # Do not update DB on failure; continue
                    continue
                except Exception as e:
                    log.error("Failed processing %s : %s", url, e)
                    # Cleanup temps
                    for p in (tmp_part, src_path, tmp_resized):
                        try:
                            if os.path.exists(p):
                                os.remove(p)
                        except OSError:
                            pass
                    continue
                finally:
                    # Remove the source copy if it remains
                    try:
                        if os.path.exists(src_path):
                            os.remove(src_path)
                    except OSError:
                        pass
            else:
                log.info("Exists, skipping download: %s", out_path)

            # Update DB: set filename for every row with this URL (even if file already existed)
            try:
                cur.execute("UPDATE questions SET filename=%s WHERE url=%s", (fname, url))
                cnx.commit()
            except Exception as e:
                cnx.rollback()
                log.error("DB update failed for %s : %s", url, e)
                continue

            processed += 1
            if processed % 25 == 0 or processed == total:
                log.info("Progress: %d/%d URLs processed", processed, total)

    log.info("Done.")

if __name__ == "__main__":
    main()
