# ETL Процесс: Spotify Tracks Dataset

## Описание проекта

Проект демонстрирует процесс обработки и нормализации данных из датасета Spotify Tracks, предоставленного на платформе Kaggle. Основной задачей было преобразование исходного датасета в нормализованные таблицы, связанные между собой уникальными идентификаторами, с последующей загрузкой данных в базу данных PostgreSQL. Процесс выполнен с использованием PySpark и SQL.



---
| track_id         | track_name               | artist_name          | year | pop | artwork_url | album_name               | acous | dance | dur_ms  | energy | instrum | key | live | loud | mode | speech | tempo  | t_sig | valence | track_url         | lang     |
|------------------|--------------------------|----------------------|------|-----|-------------|--------------------------|-------|-------|---------|--------|---------|-----|------|------|------|--------|--------|-------|---------|-------------------|----------|
| 2r0ROhr7pRN4MXDMT | Leo Das Entry (F...)     | Anirudh Ravichander  | 2024 | 59  | [URL]       | Leo Das Entry (F...)     | 0.024 | 0.753 | 97297   | 0.97   | 0.055   | 8   | 0.1  | -5.99| 0    | 0.103  | 111.0  | 4     | 0.459   | [URL]             | Tamil    |
| 4I38e6Dg52a2o2a8i | AAO KILLELLE             | Anirudh Ravichand... | 2024 | 47  | [URL]       | AAO KILLELLE             | 0.085 | 0.78  | 207369  | 0.793  | 0.0     | 10  | 0.095| -5.67| 0    | 0.095  | 165.0  | 3     | 0.821   | [URL]             | Tamil    |
| 59NoiRhnom3lTeRFa | Mayakiriye Siriki...     | Anirudh Ravichand... | 2024 | 35  | [URL]       | Mayakiriye Siriki...     | 0.031 | 0.457 | 82551   | 0.491  | 0.0     | 2   | 0.083| -8.94| 0    | 0.153  | 170.0  | 4     | 0.598   | [URL]             | Tamil    |
| 5uUqRQd385pvLxC8J | Scene Ah Scene Ah...     | Anirudh Ravichand... | 2024 | 24  | [URL]       | Scene Ah Scene Ah...     | 0.227 | 0.718 | 115831  | 0.63   | 0.0007  | 7   | 0.124| -11.1| 1    | 0.445  | 170.0  | 4     | 0.362   | [URL]             | Tamil    |
| 1KaBRg2xgNeCljmyx | Gundellonaa X I A...     | Anirudh Ravichand... | 2024 | 22  | [URL]       | Gundellonaa X I A...     | 0.015 | 0.689 | 129621  | 0.748  | 0.000001| 7   | 0.345| -9.64| 1    | 0.158  | 129.0  | 4     | 0.593   | [URL]             | Tamil    |
| 1vpppvz6ihHvKFIaU | Villain Yevadu R...      | Anirudh Ravichand... | 2024 | 26  | [URL]       | Villain Yevadu R...      | 0.199 | 0.699 | 185802  | 0.789  | 0.035   | 7   | 0.215| -8.14| 0    | 0.039  | 106.0  | 4     | 0.411   | [URL]             | Telugu   |
| 6FSIepMOKFf9p4KVQ | Gundellonaa - Pop...     | Anirudh Ravichand... | 2024 | 18  | [URL]       | Gundellonaa (Pop ...     | 0.085 | 0.559 | 74579   | 0.467  | 0.00001 | 9   | 0.178| -9.59| 0    | 0.080  | 132.0  | 4     | 0.518   | [URL]             | Tamil    |
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
