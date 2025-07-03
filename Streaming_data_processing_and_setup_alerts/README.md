# Пайплайн миграции данных: PostgreSQL → Kafka → ClickHouse

Устойчивое решение для миграции данных из PostgreSQL в ClickHouse через Apache Kafka с защитой от дубликатов.

## Описание

Данный пайплайн реализует ETL процесс для переноса данных логов пользователей из PostgreSQL в ClickHouse с использованием Apache Kafka в качестве промежуточного брокера сообщений.

### Архитектура

```
PostgreSQL → Kafka Producer → Apache Kafka → Kafka Consumer → ClickHouse
```

**Компоненты:**
- **Producer** (`producer.py`) - извлекает данные из PostgreSQL и отправляет в Kafka
- **Consumer** (`consumer.py`) - читает данные из Kafka и записывает в ClickHouse
- **Kafka** - обеспечивает надежную доставку сообщений
- **Защита от дубликатов** - флаг `sent_to_kafka` предотвращает повторную отправку

## Требования

### Системные требования
- Python 3.7+
- Docker и Docker Compose (для запуска сервисов)
- PostgreSQL 12+
- Apache Kafka 2.8+
- ClickHouse 21.8+

### Python зависимости

```bash
pip install kafka-python==2.0.2
pip install psycopg2-binary==2.9.7
pip install clickhouse-connect==0.6.8
```

## Установка и настройка

### 1. Клонирование репозитория

```bash
git clone <repository-url>
cd repository_name
```

### 2. Создание виртуального окружения

```cmd
python -m venv .venv
```

### 3. Установка зависимостей

```bash
pip install kafka-python==2.0.2
pip install psycopg2-binary==2.9.7
pip install clickhouse-connect==0.6.8
```

### 4. Настройка PostgreSQL

#### Создание таблицы user_logins

```sql
CREATE TABLE user_logins (
    id SERIAL PRIMARY KEY,
    username TEXT,
    event_type TEXT,
    event_time TIMESTAMP,
    sent_to_kafka BOOLEAN DEFAULT FALSE
);
```

#### Добавление тестовых данных

```sql
INSERT INTO user_logins (username, event_type, event_time, sent_to_kafka) VALUES
('user1', 'login', NOW(), FALSE),
('user2', 'logout', NOW(), FALSE),
('user3', 'login', NOW() - INTERVAL '1 hour', FALSE);
```

## Запуск системы

### 1. Запуск инфраструктуры

```bash
# Запуск всех сервисов
docker-compose up -d

# Проверка статуса контейнеров
docker-compose ps
```

### 2. Создание топика Kafka

```bash
# Создание топика user_events
docker exec -it kafka kafka-topics --create \
  --topic user_events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 3. Запуск Producer

```bash
python producer.py
```

**Ожидаемый вывод:**
```
Sent: {'user': 'user1', 'event': 'login', 'timestamp': 1672531200.0}
Sent: {'user': 'user2', 'event': 'logout', 'timestamp': 1672531201.0}
```

### 4. Запуск Consumer

```bash
python consumer.py
```

**Ожидаемый вывод:**
```
Received: {'user': 'user1', 'event': 'login', 'timestamp': 1672531200.0}
Received: {'user': 'user2', 'event': 'logout', 'timestamp': 1672531201.0}
```

## Описание компонентов

### Producer (producer.py)

**Функции:**
- Подключается к PostgreSQL
- Извлекает записи с `sent_to_kafka = FALSE`
- Отправляет данные в Kafka топик `user_events`
- Обновляет флаг `sent_to_kafka = TRUE`
- Предотвращает дубликаты

**Ключевые особенности:**
- Использует timestamp в формате epoch для совместимости
- Транзакционная безопасность с commit после каждой записи
- Задержка 0.5 секунды между отправками

### Consumer (consumer.py)

**Функции:**
- Подключается к Kafka топику `user_events`
- Читает сообщения с начала топика (`earliest`)
- Автоматически создает таблицу в ClickHouse
- Записывает данные в ClickHouse

**Ключевые особенности:**
- Использует consumer group `user_logins_consumer`
- Автоматический commit offset'ов
- Преобразование timestamp из epoch в DateTime

## Защита от дубликатов

### Механизм защиты

1. **Флаг sent_to_kafka** - предотвращает повторную отправку из PostgreSQL
2. **Consumer Group** - гарантирует обработку каждого сообщения только один раз
3. **Transactional safety** - commit в PostgreSQL после успешной отправки в Kafka


## Мониторинг и логирование

### Проверка данных в ClickHouse

```bash
# Подключение к ClickHouse
docker exec -it clickhouse clickhouse-client --user user --password password

# Проверка данных
SELECT * FROM user_logins ORDER BY event_time;
```

### Проверка статуса в PostgreSQL
```sql
-- Количество необработанных записей
docker-compose exec postgres \
  psql -U admin -d test_db -c "SELECT COUNT(*) FROM user_logins WHERE sent_to_kafka = FALSE;"

-- Количество обработанных записей
docker-compose exec postgres \
  psql -U admin -d test_db -c "SELECT COUNT(*) FROM user_logins WHERE sent_to_kafka = TRUE;"
```
