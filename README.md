# ETL Процесс: Spotify Tracks Dataset

## Описание проекта

Проект демонстрирует процесс обработки и нормализации данных из датасета Spotify Tracks, предоставленного на платформе Kaggle. Основной задачей было преобразование исходного датасета в нормализованные таблицы, связанные между собой уникальными идентификаторами, с последующей загрузкой данных в базу данных PostgreSQL. Процесс выполнен с использованием PySpark и SQL.



---
| track_id                           | track_name                      | artist_name          | year | popularity | artwork_url                        | album_name                      | acousticness | danceability | duration_ms | energy | instrumentalness | key | liveness | loudness | mode | speechiness | tempo   | time_signature | valence | track_url                        | language |
|------------------------------------|---------------------------------|----------------------|------|------------|------------------------------------|---------------------------------|--------------|--------------|-------------|--------|------------------|-----|----------|----------|------|-------------|---------|----------------|---------|----------------------------------|----------|
| 2r0ROhr7pRN4MXDMT...               | Leo Das Entry (From "Leo")      | Anirudh Ravichander  | 2024 | 59         | https://i.scdn.co...               | Leo Das Entry (From "Leo")      | 0.0241       | 0.753        | 97297.0     | 0.97   | 0.0553           | 8.0 | 0.1      | -5.994   | 0.0  | 0.103       | 110.997 | 4.0            | 0.459   | https://open.spot...             | Tamil    |
| 4I38e6Dg52a2o2a8i...               | AAO KILLELLE                    | Anirudh Ravichand... | 2024 | 47         | https://i.scdn.co...               | AAO KILLELLE                    | 0.0851       | 0.78         | 207369.0    | 0.793  | 0.0              | 10.0| 0.0951   | -5.674   | 0.0  | 0.0952      | 164.995 | 3.0            | 0.821   | https://open.spot...             | Tamil    |
| 59NoiRhnom3lTeRFa...               | Mayakiriye Sirikithe (From "Leo")| Anirudh Ravichand... | 2024 | 35         | https://i.scdn.co...               | Mayakiriye Sirikithe (From "Leo")| 0.0311       | 0.457        | 82551.0     | 0.491  | 0.0              | 2.0 | 0.0831   | -8.937   | 0.0  | 0.153       | 169.996 | 4.0            | 0.598   | https://open.spot...             | Tamil    |
| 5uUqRQd385pvLxC8J...               | Scene Ah Scene Ah (From "Leo")   | Anirudh Ravichand... | 2024 | 24         | https://i.scdn.co...               | Scene Ah Scene Ah (From "Leo")   | 0.227        | 0.718        | 115831.0    | 0.63   | 7.27E-4          | 7.0 | 0.124    | -11.104  | 1.0  | 0.445       | 169.996 | 4.0            | 0.362   | https://open.spot...             | Tamil    |
| 1KaBRg2xgNeCljmyx...               | Gundellonaa X I Am Leo (From "Leo")| Anirudh Ravichand... | 2024 | 22         | https://i.scdn.co...               | Gundellonaa X I Am Leo (From "Leo")| 0.0153       | 0.689        | 129621.0    | 0.748  | 1.35E-6          | 7.0 | 0.345    | -9.637   | 1.0  | 0.158       | 128.961 | 4.0            | 0.593   | https://open.spot...             | Tamil    |
| 1vpppvz6ihHvKFIaU...               | Villain Yevadu Raa (From "Leo")  | Anirudh Ravichand... | 2024 | 26         | https://i.scdn.co...               | Villain Yevadu Raa (From "Leo")  | 0.199        | 0.699        | 185802.0    | 0.789  | 0.0353           | 7.0 | 0.215    | -8.139   | 0.0  | 0.0395      | 106.048 | 4.0            | 0.411   | https://open.spot...             | Telugu   |
| 6FSIepMOKFf9p4KVQ...               | Gundellonaa - Pop Version (From "Leo")| Anirudh Ravichand... | 2024 | 18         | https://i.scdn.co...               | Gundellonaa (Pop Version) (From "Leo")| 0.085        | 0.559        | 74579.0     | 0.467  | 1.35E-5          | 9.0 | 0.178    | -9.585   | 0.0  | 0.0802      | 132.038 | 4.0            | 0.518   | https://open.spot...             | Tamil    |
## Полный процесс и код

### 1. Установка необходимых библиотек
```bash
!pip install pyspark kagglehub psycopg2-binary python-dotenv
```

### 2. Инициализация сессии PySpark и подключение к базе данных
```python
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import psycopg2
from pyspark.sql.functions import split, explode
import kagglehub

spark = SparkSession.builder \
    .appName("Spotify ETL") \
    .config("spark.master", "local[*]") \
    .getOrCreate()
print("Spark Session Created")

load_dotenv()
db_password = os.getenv("DB_PASSWORD")

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password=db_password
)
print("Подключение успешно установлено!")

cursor = conn.cursor()
```

### 3. Загрузка датасета с Kaggle
```python
path = kagglehub.dataset_download("gauthamvijayaraj/spotify-tracks-dataset-updated-every-week")
print("Path to dataset files:", path)

file_path = f"{path}/spotify_tracks.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.show(20)
df.printSchema()
```

### 4. Предварительная обработка данных
```python
# Разделение списка артистов
from pyspark.sql.functions import split, explode

df = df.withColumn("artist_name", split(df["artist_name"], ", "))
df = df.withColumn("artist_name", explode(df["artist_name"]))

# Приведение типов данных
from pyspark.sql.types import IntegerType, DoubleType

df = df.withColumn("year", df["year"].cast(IntegerType())) \
       .withColumn("popularity", df["popularity"].cast(IntegerType())) \
       .withColumn("duration_ms", df["duration_ms"].cast(IntegerType())) \
       .withColumn("key", df["key"].cast(IntegerType())) \
       .withColumn("loudness", df["loudness"].cast(DoubleType())) \
       .withColumn("mode", df["mode"].cast(IntegerType())) \
       .withColumn("tempo", df["tempo"].cast(DoubleType())) \
       .withColumn("time_signature", df["time_signature"].cast(DoubleType())) \
       .withColumn("valence", df["valence"].cast(DoubleType())) \
       .withColumn("acousticness", df["acousticness"].cast(DoubleType())) \
       .withColumn("energy", df["energy"].cast(DoubleType())) \
       .withColumn("danceability", df["danceability"].cast(DoubleType())) \
       .withColumn("liveness", df["liveness"].cast(DoubleType())) \
       .withColumn("speechiness", df["speechiness"].cast(DoubleType()))
```

### 5. Нормализация данных и создание таблиц
```python
from pyspark.sql.functions import monotonically_increasing_id

# Artists Table
df_artists = df.select("artist_name").distinct() \
              .withColumn("artist_id", monotonically_increasing_id())

# Albums Table
df_albums = df.select("album_name", "artwork_url").distinct() \
             .withColumn("album_id", monotonically_increasing_id())

# Languages Table
df_languages = df.select("language").distinct() \
               .withColumn("language_id", monotonically_increasing_id())

# Tracks Table
df_tracks = df.select("track_id", "track_name", "duration_ms", "track_url", "language", "album_name") \
             .join(df_languages, on="language", how="left") \
             .join(df_albums, on="album_name", how="left") \
             .select("track_id", "track_name", "album_id", "duration_ms", "track_url", "language_id")

# Track Features Table
df_track_features = df.select(
    "track_id", "acousticness", "danceability", "energy", "instrumentalness", 
    "key", "liveness", "loudness", "mode", "speechiness", "tempo",
    "time_signature", "valence"
)

# Track Artists Table
df_track_artists = df.select("track_id", "artist_name") \
                   .join(df_artists, on="artist_name", how="left") \
                   .select("track_id", "artist_id")
```

### 6. Создание таблиц в PostgreSQL
```python
cursor.execute("""
CREATE TABLE IF NOT EXISTS Artists (
    artist_id SERIAL PRIMARY KEY,
    artist_name TEXT NOT NULL
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Albums (
    album_id SERIAL PRIMARY KEY,
    album_name TEXT NOT NULL,
    artwork_url TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Languages (
    language_id SERIAL PRIMARY KEY,
    language_name TEXT NOT NULL
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Tracks (
    track_id TEXT PRIMARY KEY,
    track_name TEXT NOT NULL,
    album_id INT REFERENCES Albums(album_id),
    duration_ms INT,
    track_url TEXT,
    language_id INT REFERENCES Languages(language_id)
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS TrackFeatures (
    track_id TEXT PRIMARY KEY REFERENCES Tracks(track_id),
    acousticness DOUBLE PRECISION,
    danceability DOUBLE PRECISION,
    energy DOUBLE PRECISION,
    instrumentalness DOUBLE PRECISION,
    key INT,
    liveness DOUBLE PRECISION,
    loudness DOUBLE PRECISION,
    mode INT,
    speechiness DOUBLE PRECISION,
    tempo DOUBLE PRECISION,
    time_signature INT,
    valence DOUBLE PRECISION
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS TrackArtists (
    track_id TEXT REFERENCES Tracks(track_id),
    artist_id INT REFERENCES Artists(artist_id),
    PRIMARY KEY (track_id, artist_id)
);
""")
conn.commit()
```

### 7. Загрузка данных в PostgreSQL
```python
from psycopg2.extras import execute_values

# Загрузка данных для каждой таблицы
for df_data, table_name in [
    (df_artists, "Artists"),
    (df_albums, "Albums"),
    (df_languages, "Languages"),
    (df_tracks, "Tracks"),
    (df_track_features, "TrackFeatures"),
    (df_track_artists, "TrackArtists")
]:
    data = df_data.collect()
    columns = df_data.columns
    execute_values(
        cursor,
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s",
        [[getattr(row, col) for col in columns] for row in data]
    )
    conn.commit()
```

### 8. Анализ данных с помощью SQL
```python

cursor.execute("""
SELECT distinct * 
FROM tracks t
LEFT JOIN albums ON albums.album_id = t.album_id
left join languages l on l.language_id = t.language_id 
left join trackfeatures t2 on t2.track_id =t.track_id 	
left join trackartists t3 on t3.track_id = t.track_id 
left join artists a on a.artist_id = t3.artist_id; 
""")

# Получение результатов
results = cursor.fetchall()

# Вывод результатов
for row in results:
    print(row)
```
| track_id                           | track_name                      | album_id | duration_ms | track_url                                      | language_id | album_name                                      | artwork_url                                      | language_name | acousticness | danceability | energy | instrumentalness | key | liveness | loudness | mode | speechiness | tempo   | time_signature | valence | artist_name          |
|------------------------------------|---------------------------------|----------|-------------|------------------------------------------------|-------------|------------------------------------------------|-------------------------------------------------|---------------|--------------|--------------|--------|------------------|-----|----------|----------|------|-------------|---------|----------------|---------|----------------------|
| 0003ilHWJH7c5UtjKLPiC9             | Yamuna Nathi Karaiyil           | 12423    | 304170      | https://open.spotify.com/track/0003ilHWJH7c5UtjKLPiC9 | 9           | Rojavai Killathe (Original Motion Picture Soundtrack) | https://i.scdn.co/image/ab67616d0000b2736d2a3a6b1e23a9fee921401e | Tamil          | 0.43         | 0.765        | 0.676  | 0.00000191       | 1   | 0.272    | -9.427   | 1    | 0.045       | 133.554 | 4              | 0.895   | Yugendran            |
| 0003ilHWJH7c5UtjKLPiC9             | Yamuna Nathi Karaiyil           | 12423    | 304170      | https://open.spotify.com/track/0003ilHWJH7c5UtjKLPiC9 | 9           | Rojavai Killathe (Original Motion Picture Soundtrack) | https://i.scdn.co/image/ab67616d0000b2736d2a3a6b1e23a9fee921401e | Tamil          | 0.43         | 0.765        | 0.676  | 0.00000191       | 1   | 0.272    | -9.427   | 1    | 0.045       | 133.554 | 4              | 0.895   | Harris Jayaraj       |
| 0003ilHWJH7c5UtjKLPiC9             | Yamuna Nathi Karaiyil           | 12423    | 304170      | https://open.spotify.com/track/0003ilHWJH7c5UtjKLPiC9 | 9           | Rojavai Killathe (Original Motion Picture Soundtrack) | https://i.scdn.co/image/ab67616d0000b2736d2a3a6b1e23a9fee921401e | Tamil          | 0.43         | 0.765        | 0.676  | 0.00000191       | 1   | 0.272    | -9.427   | 1    | 0.045       | 133.554 | 4              | 0.895   | Poojan Sahil         |
| 000MGC6RIiDr9psFHMmX0x             | Ujla Sa Jo Chand Pe             | 1254     | 405535      | https://open.spotify.com/track/000MGC6RIiDr9psFHMmX0x | 12          | Bas Ek Tamanna                                 | https://i.scdn.co/image/ab67616d0000b273ee82bdd8e18bd7c4f68883d8 | Hindi          | 0.245        | 0.352        | 0.591  | 0.0              | 3   | 0.0806   | -8.473   | 1    | 0.0811      | 70.549  | 1              | 0.296   | Raghu Ram            |
| 001VMKfkHZrlyj7JlQbQFL             | Await The King's Justice        | 20485    | 120840      | https://open.spotify.com/track/001VMKfkHZrlyj7JlQbQFL | 5           | Game Of Thrones - Music From The HBO Series    | https://i.scdn.co/image/ab67616d0000b273239a1395e4d595efc28af924 | English        | 0.275        | 0.168        | 0.0354 | 0.929            | 9   | 0.205    | -25.34   | 0    | 0.0498      | 113.659 | 3              | 0.0301  | Unknown Artist       |
| 002dozOGSO8AhuXoz7kDZT             | It's An Abstract                | 18590    | 147627      | https://open.spotify.com/track/002dozOGSO8AhuXoz7kDZT | 5           | Steve Jobs (Original Motion Picture Soundtrack)| https://i.scdn.co/image/ab67616d0000b27320ef46811d3c05195f7bff8e | English        | 0.937        | 0.559        | 0.0538 | 0.953            | 5   | 0.0983   | -26.238  | 1    | 0.0528      | 130.017 | 4              | 0.0344  | Unknown Artist       |
| 003bJaa09EH1JkdJvudAVo             | You Are Next                    | 54       | 176412      | https://open.spotify.com/track/003bJaa09EH1JkdJvudAVo | 6           | 2.0 (Original Sound Track)                     | https://i.scdn.co/image/ab67616d0000b2733948e87d399c0ed5c0bc2326 | Unknown        | 0.0965       | 0.238        | 0.461  | 0.831            | 7   | 0.318    | -15.5    | 1    | 0.0826      | 117.961 | 3              | 0.0377  | V2 Vijay Vicky       |
| 003G9lHqhSVntZmg5j5Eju             | Jeeva Nanna Jeeva               | 6078     | 283873      | https://open.spotify.com/track/003G9lHqhSVntZmg5j5Eju | 6           | Naayaka                                        | https://i.scdn.co/image/ab67616d0000b2736017cfff88bf2f56887578d8 | Unknown        | 0.447        | 0.722        | 0.553  | 0.000222         | 2   | 0.214    | -8.713    | 0    | 0.0274      | 116.004 | 4              | 0.44    | Abhay Jodhpurkar     |
| 004HT1OpMxRDBnsD2Ro4gU             | Enna Aacho                      | 5915     | 296067      | https://open.spotify.com/track/004HT1OpMxRDBnsD2Ro4gU | 6           | Nee Ingu Sugame (Original Motion Picture Soundtrack) | https://i.scdn.co/image/ab67616d0000b273dc0f8b547581d4b040fab1b6 | Unknown        | 0.268        | 0.626        | 0.525  | 0.0              | 0   | 0.166    | -9.577    | 1    | 0.0609      | 84.729  | 4              | 0.439   | Vedanth              |
| 004HT1OpMxRDBnsD2Ro4gU             | Enna Aacho                      | 5915     | 296067      | https://open.spotify.com/track/004HT1OpMxRDBnsD2Ro4gU | 6           | Nee Ingu Sugame (Original Motion Picture Soundtrack) | https://i.scdn.co/image/ab67616d0000b273dc0f8b547581d4b040fab1b6 | Unknown        | 0.268        | 0.626        | 0.525  | 0.0              | 0   | 0.166    | -9.577    | 1    | 0.0609      | 84.729  | 4              | 0.439   | Waterbed             |
---
![Mindmap](https://i.ibb.co/PNfWpB5/photo-2024-12-30-10-34-52.jpg)
## Заключение

Этот проект продемонстрировал полный процесс ETL для нормализации данных, их анализа и загрузки в реляционную базу данных. Пример использования PySpark в сочетании с PostgreSQL подчёркивает возможности эффективной обработки и структурирования больших объёмов данных. В процессе работы применялись техники работы с данными: от предварительной очистки до нормализации и загрузки в базу данных. Проект также иллюстрирует возможность интеграции нескольких технологий для создания надежных решений для аналитики данных.
