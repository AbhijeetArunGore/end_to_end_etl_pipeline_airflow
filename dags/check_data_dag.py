"""
Check Weather Data DAG
======================
A lightweight utility DAG that queries the ``weather_data`` PostgreSQL table
and prints the results to the Airflow task log.

**Usage:** Trigger manually from the Airflow UI and inspect the task logs
under ``query_weather_data → Logs`` to view all stored records.

This DAG has no schedule and will never run automatically.
"""

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime


POSTGRES_CONN_ID = "postgres_default"

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="check_weather_data",
    start_date=datetime(2026, 6, 1),
    default_args=default_args,
    schedule=None,       # Manual trigger only — never runs on a schedule
    catchup=False,
    tags=["utility", "weather"],
    doc_md=__doc__,
) as dag:

    @task
    def query_weather_data() -> dict:
        """
        Query the ``weather_data`` table and print all rows to the task log.

        Returns a summary dict ``{"row_count": int, "columns": list[str]}``
        so the result is also accessible via XCom.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg_hook.get_conn()
        cursor = conn.cursor()

        # Confirm the table exists before querying
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE  table_name = 'weather_data'
            );
        """)
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            print("⚠️  Table 'weather_data' does not exist yet.")
            print("   Run the 'weather_etl_pipeline' DAG first to create it.")
            cursor.close()
            conn.close()
            return {"row_count": 0, "columns": []}

        cursor.execute("SELECT COUNT(*) FROM weather_data;")
        count = cursor.fetchone()[0]

        cursor.execute("SELECT * FROM weather_data ORDER BY timestamp DESC;")
        rows      = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        separator = "=" * 72
        print(f"\n{separator}")
        print("  WEATHER DATA — QUERY RESULTS")
        print(separator)
        print(f"  Total rows  : {count}")
        print(f"  Columns     : {', '.join(col_names)}")
        print("-" * 72)
        for row in rows:
            print("  " + " | ".join(str(v) for v in row))
        print(f"{separator}\n")

        cursor.close()
        conn.close()

        return {"row_count": count, "columns": col_names}

    query_weather_data()
