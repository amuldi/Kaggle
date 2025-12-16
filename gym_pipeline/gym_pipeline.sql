-- DB 선택
CREATE DATABASE IF NOT EXISTS gym_pipeline;                       -- DB 없으면 생성
USE gym_pipeline;                                                 -- 사용할 DB 선택
SELECT DATABASE();                                                -- 현재 DB 확인

--  원본 테이블 구조 확인 (CSV import 후에 실행)
DESCRIBE Gym_Progress_Dataset;                                    -- 컬럼/타입 확인

-- clean 테이블 재생성 (중복 생성 방지용)
DROP TABLE IF EXISTS gym_progress_clean;                          -- 기존 clean 제거

CREATE TABLE gym_progress_clean AS                                -- clean 테이블 생성
SELECT 
  STR_TO_DATE(Day, '%Y-%m-%d')        AS log_date,                -- 날짜 문자열 -> DATE
  CAST(Weight_kg AS DECIMAL(5,2))     AS weight_kg,               -- 몸무게 소수 2자리
  CAST(Calories_Intake AS UNSIGNED)   AS calories_intake,         -- 칼로리 정수
  CAST(Protein_Intake_g AS UNSIGNED)  AS protein_intake_g,        -- 단백질 정수
  CAST(Workout_Duration_min AS UNSIGNED) AS workout_duration_min, -- 운동시간 정수
  CAST(Steps_Walked AS UNSIGNED)      AS steps_walked             -- 걸음수 정수
FROM Gym_Progress_Dataset                                         -- 원본에서
WHERE Day IS NOT NULL                                             -- 날짜 NULL 제거
  AND STR_TO_DATE(Day, '%Y-%m-%d') IS NOT NULL;                   -- 날짜 파싱 실패 제거

-- clean 검증
SELECT COUNT(*) AS raw_cnt   FROM Gym_Progress_Dataset;           -- 원본 행 수
SELECT COUNT(*) AS clean_cnt FROM gym_progress_clean;             -- clean 행 수
SELECT * FROM gym_progress_clean ORDER BY log_date LIMIT 10;      -- 샘플 확인

-- 날짜 파싱 실패 케이스 확인(있으면 Day 포맷이 섞인 것)
SELECT Day
FROM Gym_Progress_Dataset
WHERE Day IS NOT NULL
  AND STR_TO_DATE(Day, '%Y-%m-%d') IS NULL
LIMIT 50;

-- 결측/범위 점검
SELECT
  SUM(weight_kg IS NULL)            AS null_weight,
  SUM(calories_intake IS NULL)      AS null_calories,
  SUM(protein_intake_g IS NULL)     AS null_protein,
  SUM(workout_duration_min IS NULL) AS null_workout,
  SUM(steps_walked IS NULL)         AS null_steps
FROM gym_progress_clean;

SELECT
  MIN(weight_kg)       AS min_weight,
  MAX(weight_kg)       AS max_weight,
  MIN(calories_intake) AS min_cal,
  MAX(calories_intake) AS max_cal,
  MIN(steps_walked)    AS min_steps,
  MAX(steps_walked)    AS max_steps
FROM gym_progress_clean;

-- 인덱스(날짜 기반 분석/정렬/조인 성능)
ALTER TABLE gym_progress_clean
  ADD INDEX idx_log_date (log_date);


-- mart: 분석용 요약 테이블들
-- 체중 변화(mart_weight_trend)
DROP TABLE IF EXISTS mart_weight_trend;

CREATE TABLE mart_weight_trend AS
SELECT
  log_date,
  weight_kg,
  weight_kg - LAG(weight_kg) OVER (ORDER BY log_date) AS daily_change -- 전일 대비 변화
FROM gym_progress_clean;

ALTER TABLE mart_weight_trend
  ADD INDEX idx_mart_log_date (log_date);

-- 7일 이동평균(mart_weight_trend_7d)
DROP TABLE IF EXISTS mart_weight_trend_7d;

CREATE TABLE mart_weight_trend_7d AS
SELECT
  log_date,
  weight_kg,
  AVG(weight_kg) OVER (
    ORDER BY log_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS ma_7d                                             -- 7일 이동평균
FROM gym_progress_clean;

ALTER TABLE mart_weight_trend_7d
  ADD INDEX idx_mart7_log_date (log_date);

-- 주간 요약(mart_weekly_summary)
DROP TABLE IF EXISTS mart_weekly_summary;

CREATE TABLE mart_weekly_summary AS
SELECT
  YEARWEEK(log_date, 1) AS yearweek,                      -- ISO 주차(월요일 시작)
  MIN(log_date)         AS week_start,
  MAX(log_date)         AS week_end,
  AVG(weight_kg)        AS avg_weight,
  AVG(calories_intake)  AS avg_calories,
  AVG(protein_intake_g) AS avg_protein,
  AVG(workout_duration_min) AS avg_workout_min,
  AVG(steps_walked)     AS avg_steps
FROM gym_progress_clean
GROUP BY YEARWEEK(log_date, 1);


-- 분석 쿼리 예시(바로 실행해서 결과 확인)
-- 체중 감소가 컸던 날 TOP 10
SELECT *
FROM mart_weight_trend
WHERE daily_change IS NOT NULL
ORDER BY daily_change ASC
LIMIT 10;

-- 체중 증가가 컸던 날 TOP 10
SELECT *
FROM mart_weight_trend
WHERE daily_change IS NOT NULL
ORDER BY daily_change DESC
LIMIT 10;

-- 칼로리/단백질/운동시간/걸음수 요약
SELECT
  ROUND(AVG(calories_intake), 1)      AS avg_calories,
  ROUND(AVG(protein_intake_g), 1)     AS avg_protein,
  ROUND(AVG(workout_duration_min), 1) AS avg_workout_min,
  ROUND(AVG(steps_walked), 1)         AS avg_steps,
  ROUND(AVG(weight_kg), 2)            AS avg_weight
FROM gym_progress_clean;

-- 고칼로리 상위 20일의 체중
SELECT log_date, calories_intake, weight_kg
FROM gym_progress_clean
ORDER BY calories_intake DESC
LIMIT 20;

-- 운동 많이 한 날 상위 20일의 체중
SELECT log_date, workout_duration_min, steps_walked, weight_kg
FROM gym_progress_clean
ORDER BY (workout_duration_min + steps_walked/1000) DESC
LIMIT 20;

-- 주간 요약 확인
SELECT *
FROM mart_weekly_summary
ORDER BY yearweek
LIMIT 30;
