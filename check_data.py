"""
Weather Data Verification Script
=================================
A standalone helper that connects directly to PostgreSQL and prints the
contents of the ``weather_data`` table.

Run Options
-----------
Inside the Airflow container (via Astro CLI)::

    astro dev bash
    python /usr/local/airflow/check_data.py

Locally (requires ``psycopg2-binary`` installed)::

    pip install psycopg2-binary
    python check_data.py --host localhost

For a full list of arguments::

    python check_data.py --help
"""

import argparse
import sys

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 is not installed.")
    print("  Inside container : it is pre-installed.")
    print("  Locally          : pip install psycopg2-binary")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Core function
# ---------------------------------------------------------------------------

def check_weather_data(
    host: str = "******",
    port: int = XXXX,
    user: str = "*****",
    password: str = "*******",
    database: str = "******",
) -> None:
    """Connect to PostgreSQL and display all rows in the weather_data table."""
    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
        )
        cursor = connection.cursor()

        # Confirm the table exists before querying
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'weather_data'
            );
        """)
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            print("\n⚠️  Table 'weather_data' does not exist yet.")
            print("   Run the 'weather_etl_pipeline' DAG first to create it.\n")
            cursor.close()
            connection.close()
            return

        # Row count
        cursor.execute("SELECT COUNT(*) FROM weather_data;")
        count = cursor.fetchone()[0]

        # All data ordered newest first
        cursor.execute("SELECT * FROM weather_data ORDER BY timestamp DESC;")
        rows      = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        separator = "=" * 72
        print(f"\n{separator}")
        print("  WEATHER DATA FROM POSTGRESQL DATABASE")
        print(separator)
        print(f"  Total rows : {count}")
        print(f"  Columns    : {', '.join(col_names)}")
        print("-" * 72)
        for row in rows:
            print("  " + " | ".join(str(v) for v in row))
        print(f"{separator}\n")

        cursor.close()
        connection.close()

    except psycopg2.OperationalError as exc:
        print(f"\n❌ Could not connect to PostgreSQL at {host}:{port}.")
        print(f"   Error: {exc}\n")
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        print(f"\n❌ Unexpected error: {exc}\n")
        sys.exit(1)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Display weather_data table contents from PostgreSQL.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--host", default="postgres",
        help="DB hostname ('postgres' inside container, 'localhost' for local run)",
    )
    parser.add_argument("--port",     type=int, default=5432)
    parser.add_argument("--user",     default="postgres")
    parser.add_argument("--password", default="postgres")
    parser.add_argument("--database", default="postgres")

    args = parser.parse_args()
    check_weather_data(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )
