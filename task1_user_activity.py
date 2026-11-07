import os
import sys

# Жестко привязываем локальный IP и текущий интерпретатор Python
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from datetime import date
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DateType,
    MapType, StringType, ArrayType, DoubleType,
)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("user_activity_task1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------------------------
# 1. Создание и проверка структуры DataFrame
# ---------------------------------------------------------------------------
# 1.1. Создаём df_user_activity
data = [
    (101, date(2025, 1, 1), {"mobile": 3, "desktop": 1}, ["/home", "/products", "/cart"], 4.5),
    (102, date(2025, 1, 1), {"desktop": 2}, ["/home", "/about"], 3.0),
    (101, date(2025, 1, 2), {"mobile": 2}, ["/products", "/checkout"], None),
    (103, date(2025, 1, 2), {"tablet": 1, "mobile": 1}, ["/blog", "/contact"], 5.0),
    (104, date(2025, 1, 3), {"desktop": 4}, ["/dashboard"], 3.5),
    (101, date(2025, 1, 3), {"mobile": 1, "desktop": 1}, ["/home", "/products"], 4.0),
    (105, date(2025, 1, 4), {"mobile": 5}, ["/faq"], None),
    (102, date(2025, 1, 4), {"desktop": 1, "mobile": 1}, ["/settings"], 3.8),
    (103, date(2025, 1, 5), {"tablet": 2}, ["/products"], 4.2),
    (106, date(2025, 1, 5), {"desktop": 3, "mobile": 2}, ["/login", "/profile", "/home"], 4.7),
    (101, date(2025, 1, 6), {"mobile": 1}, ["/cart", "/checkout"], 4.0),
    (104, date(2025, 1, 6), {"desktop": 2, "tablet": 1}, ["/contact"], None),
    (105, date(2025, 1, 7), {"mobile": 3, "desktop": 1}, ["/pricing"], 4.1),
    (106, date(2025, 1, 7), {"desktop": 1}, ["/home", "/about"], 3.9),
    (107, date(2025, 1, 8), {"mobile": 4, "tablet": 2}, ["/products", "/blog"], 4.9),
]

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("activity_date", DateType(), True),
    StructField("sessions_by_device", MapType(StringType(), IntegerType()), True),
    StructField("visited_pages", ArrayType(StringType()), True),
    StructField("usability_rating", DoubleType(), True),
])

df_user_activity = spark.createDataFrame(data, schema)

# 1.2. Схема DataFrame
print("=== Результат задания 1.2: схема df_user_activity ===")
df_user_activity.printSchema()

# ---------------------------------------------------------------------------
# 2. Расчёт построчных показателей
# ---------------------------------------------------------------------------
# 2.1. total_sessions_count: сумма значений map по всем устройствам в строке
df_with_total = df_user_activity.withColumn(
    "total_sessions_count",
    F.aggregate(
        F.map_values(F.col("sessions_by_device")),
        F.lit(0),
        lambda acc, x: acc + x,
    ),
)
print("=== Результат задания 2.1: total_sessions_count ===")
df_with_total.show()

# 2.2. mobile_sessions: значение по ключу "mobile", а если ключа нет - 0
df_with_mobile = df_with_total.withColumn(
    "mobile_sessions",
    F.coalesce(F.col("sessions_by_device").getItem("mobile"), F.lit(0)),
)
print("=== Результат задания 2.2: mobile_sessions ===")
df_with_mobile.show()

# ---------------------------------------------------------------------------
# 3. Агрегация данных по пользователям
# ---------------------------------------------------------------------------
# 3.1. total_sessions_all_time: сумма сессий пользователя за весь период,
#      сортировка по убыванию
df_user_total = (
    df_with_total
    .groupBy("user_id")
    .agg(F.sum("total_sessions_count").alias("total_sessions_all_time"))
    .orderBy(F.col("total_sessions_all_time").desc())
)
print("=== Результат задания 3.1: total_sessions_all_time ===")
df_user_total.show()

# 3.2. unique_visited_pages_all_time: уникальные страницы пользователя
#      explode_outer разворачивает массив в строки, collect_set собирает уникальные значения
df_user_pages = (
    df_user_activity
    .select("user_id", F.explode_outer("visited_pages").alias("page"))
    .groupBy("user_id")
    .agg(F.collect_set("page").alias("unique_visited_pages_all_time"))
    .orderBy("user_id")
)
print("=== Результат задания 3.2: unique_visited_pages_all_time ===")
df_user_pages.show(truncate=False)

# ---------------------------------------------------------------------------
# 4. Фильтрация данных
# ---------------------------------------------------------------------------
# 4.1. Записи с usability_rating > 3.5, сортировка по рейтингу по убыванию
#      (записи с NULL-рейтингом условием > 3.5 отбрасываются автоматически)
df_filtered = (
    df_with_total
    .filter(F.col("usability_rating") > 3.5)
    .orderBy(F.col("usability_rating").desc())
)
print("=== Результат задания 4.1: usability_rating > 3.5 ===")
df_filtered.show()

spark.stop()
