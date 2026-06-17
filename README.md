# MLOps HW2 — Realtime Fraud Detection

Сервис для потокового детекта фродовых транзакций. Данные идут через Kafka, модель — CatBoost из соревнования [TETA ML 1 2025](https://www.kaggle.com/competitions/teta-ml-1-2025). Визуализация в Streamlit и Grafana, метрики в Prometheus, результаты в PostgreSQL.

## Что внутри

Поднимается 10 контейнеров:
- **Zookeeper** и **Kafka** — брокер сообщений, два топика: `transactions` (вход) и `scoring` (выход)
- **Kafka UI** на порту 8080 — посмотреть, что в топиках
- **fraud_detector** — читает транзакции из Kafka, делает препроцессинг и инференс CatBoost, пишет скоры обратно в Kafka. Отдаёт метрики на порт 8000
- **scoring_writer** — читает топик `scoring`, складывает результаты в PostgreSQL. Отдаёт метрики на порт 8001
- **PostgreSQL** — хранит таблицу `scores`
- **interface** — Streamlit на порту 8501. Загружаешь CSV, отправляешь в Kafka, смотришь результаты из базы
- **Prometheus** на порту 9090 — собирает метрики с fraud_detector, scoring_writer и node-exporter
- **Grafana** на порту 3000 (логин/пароль: admin/admin) — дашборды с фильтрами
- **Node Exporter** — системные метрики (CPU, RAM)

## Структура

```
fraud_detector/
  app/app.py            - читает Kafka, запускает пайплайн
  src/preprocessing.py  - препроцессинг на основе train.csv
  src/scorer.py         - CatBoost инференс
  models/               - сохранённая модель
  train_data/           - сюда класть train.csv
scoring_writer/
  app.py                - читает scoring-топик, пишет в БД, экспортит метрики
interface/
  app.py                - Streamlit UI
prometheus/
  prometheus.yml        - конфиг сбора метрик
grafana/
  provisioning/         - автонастройка датасорсов и дашбордов
  dashboards/           - JSON-дашборды
docker-compose.yaml
.env.example
```

## Запуск

Скопировать .env:

```
cp .env.example .env
```

Скачать `train.csv` из [соревнования](https://www.kaggle.com/competitions/teta-ml-1-2025/data) и положить в `fraud_detector/train_data/train.csv`.

Запуск:

```
docker-compose up --build
```

## Как пользоваться

1. Открыть http://localhost:8501
2. Загрузить CSV (формат как test.csv из соревнования)
3. Нажать «Отправить» — транзакции уходят в Kafka
4. Нажать «Посмотреть результаты» — сервис показывает последние 10 фродовых транзакций и гистограмму скоров за последние 100 записей

В Grafana (http://localhost:3000, логин/пароль admin/admin) дашборд **Fraud Detection Dashboard**:
- Плотность распределения скоров
- TPS обработки транзакций
- Средняя доля фрода по категории продукта (последние 1000)
- Gauge доли фрода

На двух первых графиках работают фильтры по штату и мерчендайзеру — выпадающие списки сверху дашборда.

## Таблица scores

Поля: id, transaction_id, score, fraud_flag, us_state, merch, cat_id, created_at.

fraud_flag считается так: если score > 0.5, то 1 (фрод), иначе 0.

## Остановка

```
docker-compose down -v
```
