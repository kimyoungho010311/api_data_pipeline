from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin
from psycopg2.extras import execute_batch
from datetime import datetime
import requests, json

log = LoggingMixin().log
now = datetime.now()
today_str = now.strftime("%Y%m%d")
formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")

with DAG(
    dag_id="api_to_db",
    schedule="0 8 * * *",
    start_date=datetime(2025,8,14),
    catchup=False
) as dag:

    @task()
    def remove_table():
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
        remove_table_sql = "DROP TABLE IF EXISTS exchange_rate"
        pg_hook.run(remove_table_sql)

    @task()
    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS exchange_rate (
                rate_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                date TIMESTAMP,
                "result" INTEGER,
                cur_unit TEXT,
                ttb NUMERIC,
                tts NUMERIC,
                deal_bas_r NUMERIC,
                bkpr NUMERIC,
                yy_efee_r NUMERIC,
                ten_dd_efee_r NUMERIC,
                kftc_bkpr NUMERIC,
                kftc_deal_bas_r NUMERIC,
                cur_nm TEXT
            );
        """
        pg_hook.run(create_table_sql)

    @task()
    def load_exchange_rate_data():
        try:
            service_key = Variable.get("CURRENT_RATE_API_KEY")
            log.info(f"SERVICE_KEY: {service_key}")
        except Exception as e:
            log.error(f"FAILED TO LOAD SERVICE KEY : {e}")
            return []

        url = f'https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={service_key}&searchdate={today_str}&data=AP01'
        response = requests.get(url)
        log.info(f"Response status: {response.status_code}")

        try:
            data = response.json()  # JSON 문자열을 Python 객체로 변환
        except Exception as e:
            log.error(f"JSON 파싱 실패: {e}")
            data = []

        return data
    @task()
    def save_exchange_rate(data):

        if not data:
            log.info("저장할 데이터 없음")
            return

        def parse_numeric(value):
            try:
                return float(str(value).replace(',', ''))
            except:
                return None

        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        insert_sql = """
            INSERT INTO exchange_rate (
                date, "result", cur_unit, ttb, tts, deal_bas_r,
                bkpr, yy_efee_r, ten_dd_efee_r, kftc_bkpr, kftc_deal_bas_r, cur_nm
            ) VALUES (%s ,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (   
                formatted_time,
                item.get("result"),
                item.get("cur_unit"),
                parse_numeric(item.get("ttb")),
                parse_numeric(item.get("tts")),
                parse_numeric(item.get("deal_bas_r")),
                parse_numeric(item.get("bkpr")),
                parse_numeric(item.get("yy_efee_r")),
                parse_numeric(item.get("ten_dd_efee_r")),
                parse_numeric(item.get("kftc_bkpr")),
                parse_numeric(item.get("kftc_deal_bas_r")),
                item.get("cur_nm")
            )
            for item in data
        ]

        execute_batch(cursor, insert_sql, rows)
        conn.commit()
        conn.close()

    # DAG 순서 정의
    create_table() >> save_exchange_rate(load_exchange_rate_data())