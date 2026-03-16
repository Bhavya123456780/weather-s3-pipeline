import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host     = os.getenv("DB_HOST"),
        port     = os.getenv("DB_PORT"),
        dbname   = os.getenv("DB_NAME"),
        user     = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD")
    )

def insert_dim_city(cursor, row):
    cursor.execute("""
        INSERT INTO dim_city (city_name, country, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING city_id
    """, (row["city_name"], row["country"],
          row["latitude"], row["longitude"]))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(
        "SELECT city_id FROM dim_city WHERE city_name = %s",
        (row["city_name"],)
    )
    return cursor.fetchone()[0]

def insert_dim_date(cursor, row):
    cursor.execute("""
        INSERT INTO dim_date (full_date, day, month, year, hour)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING date_id
    """, (row["date"], row["day"],
          row["month"], row["year"], row["hour"]))
    return cursor.fetchone()[0]

def insert_fact_weather(cursor, city_id, date_id, row):
    cursor.execute("""
        INSERT INTO fact_weather
            (city_id, date_id, temperature, feels_like,
             humidity, wind_speed, weather_desc, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (city_id, date_id,
          row["temperature"], row["feels_like"],
          row["humidity"],    row["wind_speed"],
          row["weather_desc"],row["recorded_at"]))

def load(df):
    conn   = get_connection()
    cursor = conn.cursor()
    count  = 0

    try:
        for _, row in df.iterrows():
            city_id = insert_dim_city(cursor, row)
            date_id = insert_dim_date(cursor, row)
            insert_fact_weather(cursor, city_id, date_id, row)
            count += 1

        conn.commit()
        print(f"✓ Loaded {count} records into PostgreSQL")

    except Exception as e:
        conn.rollback()
        print(f"✗ Error loading data: {e}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("Load module ready — run pipeline.py to execute")