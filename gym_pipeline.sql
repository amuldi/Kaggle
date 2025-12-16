/* ============================================================
   Gym Progress SQL Data Pipeline
   - Dataset: Kaggle Gym Progress Tracking Dataset (200 Days)
   - Purpose: Raw → Clean → Mart → Analysis
   - DB: MySQL
   ============================================================ */


/* ============================================================
   1. Database Setup
   ============================================================ */

CREATE DATABASE IF NOT EXISTS gym_pipeline;     -- DB 없으면 생성
USE gym_pipeline;                               -- 사용할 DB 선택
SELECT DATABASE();                              -- 현재 DB 확인


/* ============================================================
   2. Raw Table Inspection
   (Executed after CSV import via DBeaver)
   ============================================================ */

DESCRIBE Gym_Progress_Dataset;                  -- 원본 컬럼/타입 확인


/* ============================================================
   3. Clean Table Creation (Transform)
   - Date parsing
   - Type casting
   - Invalid row filtering
   ============================================================ */

DROP TABLE IF EXISTS gym_progress_clean;        -- 기존 clean 테이블 제거

CREATE TABLE gym_progress_clean AS
SELECT
  STR_TO_DATE(Day, '%Y-%m-%d')              AS log_date,                -- 날짜 문자열 → DATE
  CAST(Weight_kg AS DECIMAL(5,2))           AS weight_kg,               -- 체중(소수 2자리)
  CAST(Calories_Intake AS UNSIGNED)          AS calories_intake,         -- 칼로리
  CAST(Protein_Intake_g AS UNSIGNED)         AS protein_intake_g,        -- 단백질
  CAST(Workout_Duration_min AS UNSIGNED)     AS workout_duration_min,    -- 운동 시간
  CAST(Steps_Walked AS UNSIGNED)             AS steps_walked             -- 걸음 수
FROM Gym_Progress_Dataset
WHERE Day IS NOT NULL
  AND STR_TO_DATE(Day, '%Y-%m-%d') IS NOT NULL; -- 날짜 파싱 실패 제거


/* ============================================================
   4. Data Validation
   ============================================================ */

-- Row count comparison
SELECT COUNT(*) AS raw_cnt
FROM Gym_Progress_Dataset;

SELECT COUNT(*) AS clean_cnt
FROM gym_progress_clean;

-- Sample check
SELECT *
FROM gym_progress_clean
ORDER BY log_date
LIMIT 10;

-- Date parsing failure inspection
SELECT Day
FROM Gym_Progress_Dataset
WHERE Day IS NOT NULL
  AND STR_TO_DATE(Day, '%Y-%m-%d') IS NULL
LIMIT 50;

-- NULL value check
SELECT
  SUM(weight_kg IS NULL)            AS null_weight,
  SUM(calories_intake IS NULL)      AS null_calories,
  SUM(protein_intake_g IS NULL)     AS null_protein,
  SUM(workout_duration_min IS NULL) AS null_workout,
  SUM(steps_walked IS NULL)         AS null_steps
FROM gym_progress_clean;

-- Range check
SELECT
  MIN(weight_kg)       AS min_weight,
  MAX(weight_kg)       AS max_weight,
  MIN(calories_intake) AS min_calories,
  MAX(calories_intake) AS max_calories,
  MIN(steps_walked)    AS min_steps,
  MAX(steps_walked)    AS max_steps
FROM gym_progress_clean;


/* ============================================================
   5. Indexing
   ============================================================ */

ALTER TABLE gym_progress_clean
  ADD INDEX idx_log_date (log_date);


/* ============================================================
   6. Data Mart Tables
   ============================================================ */

-- 6-1. Daily weight change
DROP TABLE IF EXISTS mart_weight_trend;

CREATE TABLE mart_weight_trend AS
SELECT
  log_date,
  weight_kg,
  weight_kg - LAG(weight_kg) OVER (ORDER BY log_date) AS daily_change
FROM gym_progress_clean;

ALTER TABLE mart_weight_trend
  ADD INDEX idx_mart_log_date (log_date);


-- 6-2. 7-day moving average of weight
DROP TABLE IF EXISTS mart_weight_trend_7d;

CREATE TABLE mart_weight_trend_7d AS
SELECT
  log_date,
  weight_kg,
  AVG(weight_kg) OVER (
    ORDER BY log_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS ma_7d
FROM gym_progress_clean;

ALTER TABLE mart_weight_trend_7d
  ADD INDEX idx_mart7_log_date (log_date);


-- 6-3. Weekly summary mart
DROP TABLE IF EXISTS mart_weekly_summary;

CREATE TABLE mart_weekly_summary AS
SELECT
  YEARWEEK(log_date, 1)        AS yearweek,       -- ISO week (Mon start)
  MIN(log_date)                AS week_start,
  MAX(log_date)                AS week_end,
  AVG(weight_kg)               AS avg_weight,
  AVG(calories_intake)         AS avg_calories,
  AVG(protein_intake_g)        AS avg_protein,
  AVG(workout_duration_min)    AS avg_workout_min,
  AVG(steps_walked)            AS avg_steps
FROM gym_progress_clean
GROUP BY YEARWEEK(log_date, 1);


/* ============================================================
   7. Analysis Queries
   ============================================================ */

-- Top 10 weight loss days
SELECT *
FROM mart_weight_trend
WHERE daily_change IS NOT NULL
ORDER BY daily_change ASC
LIMIT 10;

-- Top 10 weight gain days
SELECT *
FROM mart_weight_trend
WHERE daily_change IS NOT NULL
ORDER BY daily_change DESC
LIMIT 10;

-- Overall averages
SELECT
  ROUND(AVG(calories_intake), 1)      AS avg_calories,
  ROUND(AVG(protein_intake_g), 1)     AS avg_protein,
  ROUND(AVG(workout_duration_min), 1) AS avg_workout_min,
  ROUND(AVG(steps_walked), 1)         AS avg_steps,
  ROUND(AVG(weight_kg), 2)            AS avg_weight
FROM gym_progress_clean;

-- High calorie days
SELECT
  log_date,
  calories_intake,
  weight_kg
FROM gym_progress_clean
ORDER BY calories_intake DESC
LIMIT 20;

-- High activity days
SELECT
  log_date,
  workout_duration_min,
  steps_walked,
  weight_kg
FROM gym_progress_clean
ORDER BY (workout_duration_min + steps_walked / 1000) DESC
LIMIT 20;

-- Weekly summary check
SELECT *
FROM mart_weekly_summary
ORDER BY yearweek
LIMIT 30;
