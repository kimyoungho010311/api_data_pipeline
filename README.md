# API 데이터 주기적 수집 및 DB 저장

외부 API에서 JSON 데이터를 주기적으로 수집하고 PostgreSQL에 저장하는 Airflow 기반 파이프라인입니다.  
자세한 구현 과정과 설정, 로그 확인 방법 등은 [Notion 문서](https://harsh-cabbage-818.notion.site/API-24904fe2aebc80bab85ed541d5eec9bb?source=copy_link)에서 확인하세요.

## 기능
- 외부 API 호출 및 JSON 데이터 수집
- 데이터 파싱 및 형변환
- PostgreSQL, 파일, 클라우드 저장
- DAG 스케줄링 및 Task별 로그 확인
- 민감 정보 안전 저장 (Airflow Variable 사용)

## 초기 설정

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.4/docker-compose.yaml'
docker-compose up airflow-init
