# ETL Процесс: Spotify Tracks Dataset

## Описание проекта

Проект демонстрирует процесс обработки и нормализации данных из датасета Spotify Tracks, предоставленного на платформе Kaggle. Основной задачей было преобразование исходного датасета в нормализованные таблицы, связанные между собой уникальными идентификаторами, с последующей загрузкой данных в базу данных PostgreSQL. Процесс выполнен с использованием PySpark и SQL.

---

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
# Пример аналитического запроса: треки с наибольшей энергией
cursor.execute("""
SELECT 
    t.track_name, 
    a.artist_name, 
    tf.energy
FROM 
    Tracks t
JOIN 
    TrackArtists ta ON t.track_id = ta.track_id
JOIN 
    Artists a ON ta.artist_id = a.artist_id
JOIN 
    TrackFeatures tf ON t.track_id = tf.track_id
ORDER BY 
    tf.energy DESC
LIMIT 10;
""")

results = cursor.fetchall()
for row in results:
    print(row)
```

---
![Mindmap](https://i.ibb.co/PNfWpB5/photo-2024-12-30-10-34-52.jpg)
## Заключение

Этот проект продемонстрировал полный процесс ETL для нормализации данных, их анализа и загрузки в реляционную базу данных. Пример использования PySpark в сочетании с PostgreSQL подчёркивает возможности эффективной обработки и структурирования больших объёмов данных. В процессе работы применялись техники работы с данными: от предварительной очистки до нормализации и загрузки в базу данных. Проект также иллюстрирует возможность интеграции нескольких технологий для создания надежных решений для аналитики данных.
