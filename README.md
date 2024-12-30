# ETL Процесс: Spotify Tracks Dataset

## Описание проекта

Проект демонстрирует процесс обработки и нормализации данных из датасета Spotify Tracks, предоставленного на платформе Kaggle. Основной задачей было преобразование исходного датасета в нормализованные таблицы, связанные между собой уникальными идентификаторами, с последующей загрузкой данных в базу данных PostgreSQL. Процесс выполнен с использованием PySpark и SQL.

---

## Этапы работы с данными

### 1. Загрузка и подготовка данных

1. **Загрузка датасета:**
   - Данные были скачаны с Kaggle с помощью библиотеки `kagglehub`.
   - Исходный датасет включал информацию о треках, альбомах, артистах, характеристиках треков и языках.

2. **Инициализация PySpark:**
   - Настроена сессия PySpark для обработки больших объемов данных.
   - Файл был загружен в PySpark DataFrame с автоматическим определением схемы.

```python
!pip install pyspark kagglehub psycopg2-binary python-dotenv

from pyspark.sql import SparkSession
import kagglehub

spark = SparkSession.builder \
    .appName("Spotify ETL") \
    .config("spark.master", "local[*]") \
    .getOrCreate()
print("Spark Session Created")

path = kagglehub.dataset_download("gauthamvijayaraj/spotify-tracks-dataset-updated-every-week")
file_path = f"{path}/spotify_tracks.csv"

# Загрузка данных
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.show(20)
df.printSchema()
```

### 2. Обработка данных

1. **Разделение данных об артистах:**
   - В исходных данных некоторые строки содержали несколько имен артистов, разделенных запятыми.
   - С помощью функций `split` и `explode` каждая запись была преобразована в отдельную строку.

```python
from pyspark.sql.functions import split, explode

df = df.withColumn("artist_name", split(df["artist_name"], ", "))
df = df.withColumn("artist_name", explode(df["artist_name"]))
```

2. **Приведение типов данных:**
   - Поля, такие как `year`, `popularity`, `duration_ms`, были приведены к числовым типам для обеспечения корректности обработки данных.

```python
from pyspark.sql.types import IntegerType, DoubleType

df = df.withColumn("year", df["year"].cast(IntegerType())) \
       .withColumn("popularity", df["popularity"].cast(IntegerType())) \
       .withColumn("duration_ms", df["duration_ms"].cast(IntegerType()))
```

### 3. Нормализация данных

1. **Создание таблицы артистов:**
   - Уникальные имена артистов были выделены в отдельный DataFrame с добавлением уникального идентификатора `artist_id`.

```python
from pyspark.sql.functions import monotonically_increasing_id

df_artists = df.select("artist_name").distinct() \
               .withColumn("artist_id", monotonically_increasing_id())
```

2. **Создание таблицы альбомов:**
   - Для каждого альбома были сохранены его название, обложка и связь с артистом.

```python
df_albums = df.select("album_name", "artwork_url").distinct() \
              .withColumn("album_id", monotonically_increasing_id())
```

3. **Создание других таблиц:**
   - **Languages:** Таблица уникальных языков с идентификаторами.
   - **Tracks:** Основная информация о треках с указанием альбома и языка.
   - **TrackFeatures:** Характеристики треков, такие как танцевальность, энергия, акустичность и т.д.
   - **TrackArtists:** Связь треков с артистами.

```python
df_languages = df.select("language").distinct() \
                 .withColumn("language_id", monotonically_increasing_id())

df_tracks = df.select("track_id", "track_name", "duration_ms", "track_url", "album_name", "language") \
              .join(df_languages, on="language", how="left") \
              .join(df_albums, on="album_name", how="left") \
              .select("track_id", "track_name", "album_id", "duration_ms", "track_url", "language_id")

track_features_cols = [
    "track_id", "acousticness", "danceability", "energy", "instrumentalness",
    "key", "liveness", "loudness", "mode", "speechiness", "tempo", "time_signature", "valence"
]
df_track_features = df.select(*track_features_cols)

df_track_artists = df.select("track_id", "artist_name") \
                    .join(df_artists, on="artist_name", how="left") \
                    .select("track_id", "artist_id")
```

### 4. Загрузка данных в PostgreSQL

1. **Создание таблиц в базе данных:**
   - Схема базы данных была спроектирована для нормализованного хранения данных.
   - Пример создания таблицы артистов:

   ```sql
   CREATE TABLE IF NOT EXISTS Artists (
       artist_id SERIAL PRIMARY KEY,
       artist_name TEXT NOT NULL
   );
   ```

2. **Загрузка данных:**
   - С использованием библиотеки `psycopg2` данные из PySpark DataFrame были загружены в соответствующие таблицы PostgreSQL.

```python
from psycopg2.extras import execute_values

artists_data = df_artists.select("artist_id", "artist_name").collect()

execute_values(
    cursor,
    "INSERT INTO Artists (artist_id, artist_name) VALUES %s",
    [(row.artist_id, row.artist_name) for row in artists_data]
)
conn.commit()
```

### 5. Анализ данных

1. **SQL-запросы:**
   - Были выполнены аналитические запросы для получения полезной информации, например:
     - Треки с наибольшей энергией.
     - Альбомы с наибольшим количеством треков.

   ```sql
   SELECT t.track_name, a.artist_name, tf.energy
   FROM Tracks t
   JOIN TrackArtists ta ON t.track_id = ta.track_id
   JOIN Artists a ON ta.artist_id = a.artist_id
   JOIN TrackFeatures tf ON t.track_id = tf.track_id
   ORDER BY tf.energy DESC
   LIMIT 10;
   ```

2. **Результаты анализа:**
   - Полученные результаты позволили выявить наиболее популярные треки, их артистов и ключевые характеристики.

---

## Заключение

Данный проект демонстрирует навыки обработки данных с использованием PySpark, нормализации данных, проектирования базы данных и выполнения аналитических запросов. Процесс ETL был успешно реализован, данные преобразованы в удобный для анализа формат и загружены в базу данных PostgreSQL.
