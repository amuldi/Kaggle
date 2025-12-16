# Gym Progress SQL Data Pipeline

## 프로젝트 개요
본 프로젝트는 Kaggle의 **Gym Progress Tracking Dataset (200 Days)**를 활용하여  
**MySQL 기반 데이터 파이프라인**을 구성한 개인 프로젝트입니다.

CSV 형태의 원본 데이터를 데이터베이스에 적재한 뒤,  
데이터를 정제하고 검증하여 **분석에 적합한 구조로 재가공**하는 과정을  
SQL 중심으로 설계하고 구현하는 것을 목표로 하고 있습니다.

---

## 데이터셋
- 출처: Kaggle
- 이름: Gym Progress Tracking Dataset (200 Days)
- 링크: https://www.kaggle.com/datasets/rishabhagarwal997889/gym-progress-tracking-dataset-200-days

※ 원본 CSV 파일은 저장소에 포함하지 않았으며,  
DBeaver를 이용해 MySQL에 직접 Import하여 사용합니다.

---

## 사용 기술
- MySQL
- SQL
- DBeaver
- GitHub

---

## 파이프라인 구성
- Kaggle CSV
- Raw 데이터 테이블
- 정제된 Clean 테이블
- 분석용 요약 테이블(Data Mart)
- 분석 결과 확인
