import pandas as pd
from datetime import datetime

def transform(raw_data):
    records = []

    for data in raw_data:
        record = {
            "city_name"    : data["name"],
            "country"      : data["sys"]["country"],
            "latitude"     : data["coord"]["lat"],
            "longitude"    : data["coord"]["lon"],
            "temperature"  : data["main"]["temp"],
            "feels_like"   : data["main"]["feels_like"],
            "humidity"     : data["main"]["humidity"],
            "wind_speed"   : data["wind"]["speed"],
            "weather_desc" : data["weather"][0]["description"],
            "recorded_at"  : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "date"         : datetime.now().strftime("%Y-%m-%d"),
            "hour"         : datetime.now().hour,
            "day"          : datetime.now().day,
            "month"        : datetime.now().month,
            "year"         : datetime.now().year,
        }
        records.append(record)

    df = pd.DataFrame(records)

    # data quality checks
    df = df.dropna(subset=["temperature", "city_name"])
    df = df.drop_duplicates(subset=["city_name", "recorded_at"])
    df["temperature"] = df["temperature"].astype(float)
    df["humidity"]    = df["humidity"].astype(int)

    print(f"✓ Transformed {len(df)} records")
    print(df[["city_name", "temperature", "humidity"]].to_string())
    return df

if __name__ == "__main__":
    # test with dummy data
    sample = [{
        "name"    : "Seattle",
        "sys"     : {"country": "US"},
        "coord"   : {"lat": 47.6, "lon": -122.3},
        "main"    : {"temp": 12.5, "feels_like": 10.2, "humidity": 80},
        "wind"    : {"speed": 3.5},
        "weather" : [{"description": "light rain"}]
    }]
    transform(sample)