# weather-s3-pipelin
Live weather API → AWS S3 → PostgreSQL pipeline orchestrated with Apache Airflow
# Weather Data Pipeline 🌤️

An automated data engineering pipeline that collects live weather data
from the OpenWeatherMap API, stores raw data in AWS S3, transforms it,
and loads it into a PostgreSQL data warehouse — orchestrated with Apache Airflow.

## Architecture
```
OpenWeatherMap API → AWS S3 (raw JSON) → Transform (Pandas) → PostgreSQL (Star Schema)
                                                ↑
                                          Apache Airflow (hourly schedule)
```

## Tech Stack

- **Python** — pipeline logic
- **Apache Airflow** — orchestration and scheduling
- **AWS S3** — raw data lake storage
- **PostgreSQL** — data warehouse
- **Pandas** — data transformation
- **Docker** — containerized database

## Project Structure
```
weather-s3-pipeline/
├── dags/
│   └── weather_pipeline.py   # Airflow DAG
├── src/
│   ├── extract.py            # API extraction + S3 upload
│   ├── transform.py          # Data cleaning + transformation
│   └── load.py               # PostgreSQL loading
├── sql/
│   └── create_tables.sql     # Star Schema DDL
├── docker-compose.yml        # PostgreSQL container
├── requirements.txt          # Python dependencies
└── .env                      # API keys (not committed)
```

## Data Model (Star Schema)
```
         dim_city
            |
dim_date — fact_weather
```

- **fact_weather** — temperature, humidity, wind speed, recorded_at
- **dim_city** — city name, country, latitude, longitude
- **dim_date** — date, day, month, year, hour

## Setup & Run

### 1. Clone the repo
```bash
git clone https://github.com/Bhavya123456780/weather-s3-pipeline.git
cd weather-s3-pipeline
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Add your credentials to .env
```
OPENWEATHER_API_KEY=your_key_here
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_BUCKET_NAME=your_bucket
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weatherdb
DB_USER=postgres
DB_PASSWORD=postgres
```

### 4. Start PostgreSQL
```bash
docker-compose up -d
```

### 5. Create tables
```bash
psql -h localhost -U postgres -d weatherdb -f sql/create_tables.sql
```

### 6. Run the pipeline
```bash
python src/extract.py
python src/transform.py
python src/load.py
```

## Author
Bhavya Sri Muddana — [github.com/Bhavya123456780](https://github.com/Bhavya123456780)