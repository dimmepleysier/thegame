-- Create table
CREATE TABLE IF NOT EXISTS `questions` (
  `tmdbid`   INT NOT NULL,
  `type`     ENUM('movie','tv') NOT NULL,
  `title`    VARCHAR(255) NOT NULL,
  `url`      VARCHAR(512) NOT NULL,
  `filename` VARCHAR(255) NULL,
  PRIMARY KEY (`tmdbid`,`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Populate from MOVIES: best NULL-language backdrop per title
INSERT IGNORE INTO `questions` (tmdbid, `type`, title, url, filename)
SELECT
  md.tmdb_id                                           AS tmdbid,
  'movie'                                              AS `type`,
  md.title                                             AS title,
  CONCAT('https://image.tmdb.org/t/p/original', mi.file_path) AS url,
  NULL                                                 AS filename
FROM `movie_details` md
JOIN (
  SELECT tmdb_id, file_path
  FROM (
    SELECT
      tmdb_id, file_path, vote_count, vote_average, width,
      ROW_NUMBER() OVER (
        PARTITION BY tmdb_id
        ORDER BY vote_count DESC, vote_average DESC, width DESC
      ) AS rn
    FROM `movie_images`
    WHERE img_type = 'backdrop' AND (iso_639_1 IS NULL OR iso_639_1 = '')
  ) ranked
  WHERE rn = 1
) mi ON mi.tmdb_id = md.tmdb_id
-- if you only want to include titles you previously imported to popular_movies (optional):
-- JOIN popular_movies pm ON pm.tmdb_id = md.tmdb_id
;

-- Populate from TV: best NULL-language backdrop per title
INSERT IGNORE INTO `questions` (tmdbid, `type`, title, url, filename)
SELECT
  td.tmdb_id                                           AS tmdbid,
  'tv'                                                 AS `type`,
  td.name                                              As title,
  CONCAT('https://image.tmdb.org/t/p/original', ti.file_path) AS url,
  NULL                                                 AS filename
FROM `tv_details` td
JOIN (
  SELECT tmdb_id, file_path
  FROM (
    SELECT
      tmdb_id, file_path, vote_count, vote_average, width,
      ROW_NUMBER() OVER (
        PARTITION BY tmdb_id
        ORDER BY vote_count DESC, vote_average DESC, width DESC
      ) AS rn
    FROM `tv_images`
    WHERE img_type = 'backdrop' AND (iso_639_1 IS NULL OR iso_639_1 = '')
  ) ranked
  WHERE rn = 1
) ti ON ti.tmdb_id = td.tmdb_id
-- optional: only include titles from popular_tv
-- JOIN popular_tv pt ON pt.tmdb_id = td.tmdb_id
;
