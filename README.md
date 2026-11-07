# Самостоятельное задание №1: Анализ активности онлайн-пользователей (PySpark)

**Выполнила:** Кайсина П.Д., студентка группы ИТБ(б)-О-23/1

## Цель работы

Освоить создание DataFrame в PySpark со сложными типами данных (Map и Array) и выполнить над ним расчёт построчных показателей, агрегации по пользователям и фильтрацию.

## Исходные данные

Синтетический набор данных об онлайн-активности пользователей (15 записей). Структура `df_user_activity`:

| Колонка | Тип | Описание |
|---|---|---|
| `user_id` | IntegerType | идентификатор пользователя |
| `activity_date` | DateType | дата активности |
| `sessions_by_device` | MapType(StringType, IntegerType) | количество сессий по типам устройств |
| `visited_pages` | ArrayType(StringType) | список посещённых страниц |
| `usability_rating` | DoubleType | оценка удобства (может быть NULL) |

Внешние файлы не требуются: данные создаются в коде.

## Запуск

```bash
pip install pyspark
python task1_user_activity.py
```

Требуется установленная Java (JDK 8, 11 или 17). Работа выполнена на Python 3.12, PySpark запускается в локальном режиме (`local[*]`).

## Что было сделано

### 1. Создание DataFrame и проверка структуры
Создан `df_user_activity` по заданным данным с явной схемой `StructType`. Схема выведена методом `printSchema()`.

<img width="572" height="242" alt="image_2026-11-07_22-18-02" src="https://github.com/user-attachments/assets/a8f6dd50-7572-43b8-b918-54a6b0122a99" />


### 2. Расчёт построчных показателей
**2.1.** Колонка `total_sessions_count` равна сумме значений словаря `sessions_by_device` по всем устройствам (`map_values` + `aggregate`).

```python
F.aggregate(F.map_values("sessions_by_device"), F.lit(0), lambda a, x: a + x)
```

<img width="1052" height="463" alt="image_2026-11-07_22-25-57" src="https://github.com/user-attachments/assets/e885d13f-73b3-41e3-a747-7b9856827450" />


**2.2.** Колонка `mobile_sessions` содержит число сессий с устройства `mobile`. Если ключа нет, подставляется 0 (`getItem` + `coalesce`).

```python
F.coalesce(F.col("sessions_by_device").getItem("mobile"), F.lit(0))
```

<img width="1238" height="463" alt="image_2026-11-07_22-26-16" src="https://github.com/user-attachments/assets/a0afcdc2-ae7c-4b7d-9720-5089842e9526" />


### 3. Агрегация по пользователям
**3.1.** `total_sessions_all_time` — общее число сессий пользователя за весь период, сортировка по убыванию (`groupBy` + `sum` + `orderBy`).

<img width="643" height="290" alt="image_2026-11-07_22-26-25" src="https://github.com/user-attachments/assets/72c31c5d-aae0-4fa6-a0ed-f98892cc98d9" />


**3.2.** `unique_visited_pages_all_time` — список уникальных страниц пользователя: массив разворачивается через `explode_outer`, затем значения собираются через `collect_set`.

<img width="657" height="282" alt="image_2026-11-07_22-26-48" src="https://github.com/user-attachments/assets/f8cc4524-643c-4478-af85-536af2fd066d" />


### 4. Фильтрация
**4.1.** Отобраны записи с `usability_rating > 3.5`, результат отсортирован по рейтингу по убыванию. Записи с `NULL` в рейтинге исключаются автоматически.

<img width="1056" height="362" alt="image_2026-11-07_22-27-06" src="https://github.com/user-attachments/assets/4c696ba0-f4fb-4769-aa8c-ffeed7559115" />


## Вывод

В ходе работы создан DataFrame со сложными типами данных Map и Array, рассчитаны построчные показатели (общее и мобильное число сессий), выполнены агрегации по пользователям и фильтрация по оценке удобства. Наибольшее число сессий за период у пользователей 101 и 105 (по 9).
