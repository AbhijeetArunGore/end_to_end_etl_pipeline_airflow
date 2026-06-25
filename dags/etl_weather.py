"""
Weather ETL Pipeline DAG
========================
An end-to-end Apache Airflow ETL pipeline that:

  1. **Extracts**   — Fetches current weather data for London (lat 51.5074, lon -0.1278)
                      from the Open-Meteo API via an Airflow HTTP Connection.
  2. **Transforms** — Parses and validates the raw JSON payload into a clean,
                      typed Python dictionary ready for persistence.
  3. **Loads**      — Upserts the transformed record into a PostgreSQL table
                      (`weather_data`) using an Airflow Postgres Connection.

Schedule : Daily  (@daily)
Catchup  : Disabled
Tags     : weather, etl
"""

import json
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------
LATITUDE       = "51.5074"   # London
LONGITUDE      = "-0.1278"
API_CONN_ID    = "open_meteo_api"
POSTGRES_CONN_ID = "postgres_default"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 6, 1),
    "retries": 2,
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["weather", "etl"],
    doc_md=__doc__,
) as dag:

    # -----------------------------------------------------------------------
    # EXTRACT
    # -----------------------------------------------------------------------
    @task
    def extract_weather_data() -> dict:
        """
        Fetch current-weather JSON from Open-Meteo for London.

        Uses the Airflow HTTP Hook tied to the ``open_meteo_api`` connection
        so that credentials and base-URL are managed centrally in the UI.
        Returns the raw deserialized API payload as a Python dict.
        """
        http_hook = HttpHook(method="GET", http_conn_id=API_CONN_ID)
        endpoint = (
            f"v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}"
            "&current_weather=true&timezone=UTC"
        )
        response = http_hook.run(endpoint, extra_options={"timeout": 30})
        response.raise_for_status()
        return json.loads(response.text)

    # -----------------------------------------------------------------------
    # TRANSFORM
    # -----------------------------------------------------------------------
    @task
    def transform_weather_data(weather_data: dict) -> dict:
        """
        Validate and reshape the raw API payload into a flat, typed record.

        Raises
        ------
        ValueError
            If the upstream task returned ``None``.
        KeyError
            If the ``current_weather`` key is absent from the payload.
        """
        if weather_data is None:
            raise ValueError(
                "No data received from extract_weather_data — "
                "check that the HTTP connection is configured correctly."
            )
        if isinstance(weather_data, str):
            weather_data = json.loads(weather_data)

        current_weather = weather_data.get("current_weather", {})
        if not current_weather:
            raise KeyError(
                "Key 'current_weather' not found in the API response. "
                f"Available keys: {list(weather_data.keys())}"
            )

        return {
            "latitude":     weather_data["latitude"],
            "longitude":    weather_data["longitude"],
            "temperature":  current_weather["temperature"],
            "windspeed":    current_weather["windspeed"],
            "winddirection": current_weather["winddirection"],
            "weathercode":  current_weather["weathercode"],
            "timestamp":    current_weather["time"],
        }

    # -----------------------------------------------------------------------
    # LOAD
    # -----------------------------------------------------------------------
    @task
    def load_weather_data(transformed_data: dict) -> None:
        """
        Upsert a single weather record into the ``weather_data`` PostgreSQL table.

        The table is created on first run (idempotent DDL).
        A unique index on ``timestamp`` prevents duplicate records; an
        ``ON CONFLICT … DO UPDATE`` clause refreshes stale rows in-place
        rather than raising an error on re-runs.
        """
        if transformed_data is None:
            raise ValueError(
                "No transformed data received — ensure both extract and "
                "transform tasks completed successfully before this task."
            )

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg_hook.get_conn()
        cursor = conn.cursor()

        # 1. Create table (idempotent)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude       DOUBLE PRECISION NOT NULL,
                longitude      DOUBLE PRECISION NOT NULL,
                temperature    DOUBLE PRECISION NOT NULL,
                windspeed      DOUBLE PRECISION NOT NULL,
                winddirection  DOUBLE PRECISION NOT NULL,
                weathercode    INTEGER          NOT NULL,
                timestamp      TIMESTAMP        NOT NULL,
                ingested_at    TIMESTAMPTZ      NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 2. Ensure unique index on timestamp (idempotent)
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS weather_data_timestamp_uidx
            ON weather_data (timestamp);
        """)

        # 3. Upsert — insert or refresh on timestamp conflict
        cursor.execute("""
            INSERT INTO weather_data
                (latitude, longitude, temperature, windspeed,
                 winddirection, weathercode, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO UPDATE SET
                latitude      = EXCLUDED.latitude,
                longitude     = EXCLUDED.longitude,
                temperature   = EXCLUDED.temperature,
                windspeed     = EXCLUDED.windspeed,
                winddirection = EXCLUDED.winddirection,
                weathercode   = EXCLUDED.weathercode,
                ingested_at   = CURRENT_TIMESTAMP;
        """, (
            transformed_data["latitude"],
            transformed_data["longitude"],
            transformed_data["temperature"],
            transformed_data["windspeed"],
            transformed_data["winddirection"],
            transformed_data["weathercode"],
            transformed_data["timestamp"],
        ))

        conn.commit()
        cursor.close()
        conn.close()

    # -----------------------------------------------------------------------
    # Pipeline wiring  (Extract → Transform → Load)
    # -----------------------------------------------------------------------
    raw_data   = extract_weather_data()
    clean_data = transform_weather_data(raw_data)
    load_weather_data(clean_data)
