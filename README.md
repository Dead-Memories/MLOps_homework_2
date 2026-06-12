# MLOps HW2 — Real-time Fraud Detection Service

Сервис потокового обнаружения мошеннических транзакций на базе Kafka, CatBoost, PostgreSQL, Streamlit, Prometheus и Grafana.

Модель и препроцессинг — из соревнования [TETA ML 1 2025](https://www.kaggle.com/competitions/teta-ml-1-2025).

---

## Архитектура

| Сервис           | Описание |
|------------------|----------|
| **Kafka**        | Шина сообщений для потоковой передачи транзакций |
| **Zookeeper**    | Координация кластера Kafka |
| **Kafka UI**     | Веб-интерфейс мониторинга Kafka (http://localhost:8080) |
| **fraud_detector** | Чтение из `transactions`, препроцессинг (MLOps 1), инференс CatBoost, запись в `scoring` |
| **scoring_writer** | Чтение `scoring`, сохранение результатов в PostgreSQL, экспорт Prometheus-метрик |
| **PostgreSQL**   | Хранилище результатов скоринга (таблица `scores`) |
| **interface**    | Streamlit UI для загрузки CSV и просмотра результатов (http://localhost:8501) |
| **Prometheus**   | Сбор метрик (http://localhost:9090) |
| **Grafana**      | Дашборды с фильтрами по `us_state`, `merch` и barplot по `cat_id` (http://localhost:3000) |
| **Node Exporter**| Системные метрики (CPU, RAM, диск, сеть) |

---

## Структура проекта

```
├── docker-compose.yaml
├── .env.example
├── README.md
├── fraud_detector/           # Сервис инференса
│   ├── app/app.py            # Kafka-consumer + пайплайн
│   ├── src/preprocessing.py  # Препроцессинг (train.csv + маппинги)
│   ├── src/scorer.py         # Инференс CatBoost
│   ├── models/fraud_model.cbm
│   ├── train_data/train.csv
│   ├── Dockerfile
│   └── requirements.txt
├── scoring_writer/           # Запись результатов в PostgreSQL
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── interface/                # Streamlit UI
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── prometheus/
│   └── prometheus.yml
└── grafana/
    ├── provisioning/
    │   ├── dashboards/dashboards.yaml
    │   └── datasources/
    │       ├── prometheus.yaml
    │       └── postgres.yaml
    └── dashboards/
        ├── fraud_detector.json
        ├── scoring.json
        └── node_exporter.json
```

---

## Как запустить

### 1. Скопируйте .env

```bash
cp .env.example .env
```

### 2. Скачайте train.csv

Скачайте `train.csv` из [соревнования TETA ML 1 2025](https://www.kaggle.com/competitions/teta-ml-1-2025/data) и поместите в `fraud_detector/train_data/train.csv`.

### 3. Запустите контейнеры

```bash
docker-compose up --build
```

Первый запуск займёт несколько минут на сборку образов и скачивание.

### 4. Откройте интерфейсы

| Сервис         | URL                     | Логин/пароль   |
|----------------|-------------------------|----------------|
| Streamlit UI   | http://localhost:8501   | —              |
| Kafka UI       | http://localhost:8080   | —              |
| Grafana        | http://localhost:3000   | admin / admin  |
| Prometheus     | http://localhost:9090   | —              |

---

## Как использовать

1. Перейдите в **Streamlit UI** (http://localhost:8501).
2. Загрузите CSV-файл с транзакциями (формат test.csv соревнования).
3. Нажмите **«Отправить»** — данные уйдут в Kafka.
4. Нажмите **«Посмотреть результаты»**:
   - Таблица последних 10 фродовых транзакций (`fraud_flag == 1`).
   - Гистограмма распределения скоров последних 100 транзакций.
5. Откройте **Grafana** (http://localhost:3000) → **Fraud Detection Dashboard**:
   - **Плотность распределения скоров** — фильтры по штату (`us_state`) и мерчендайзеру (`merch`).
   - **TPS обработки транзакций** — с теми же фильтрами.
   - **Barplot средней доли фрода по категории продукта (`cat_id`)** за последние 1000 транзакций.
   - **Gauge доли мошеннических транзакций**.

---

## Таблица `scores` (PostgreSQL)

| Поле           | Тип       | Описание                           |
|----------------|-----------|------------------------------------|
| id             | SERIAL    | PK                                |
| transaction_id | TEXT      | UUID транзакции                    |
| score          | FLOAT     | Вероятность фрода (0..1)           |
| fraud_flag     | INT       | 1 — фрод, 0 — нормально (порог 0.5)|
| us_state       | TEXT      | Штат                               |
| merch          | TEXT      | Мерчендайзер                       |
| cat_id         | TEXT      | Категория продукта                 |
| created_at     | TIMESTAMP | Время записи                       |

---

## Остановка и очистка

```bash
docker-compose down -v
```
