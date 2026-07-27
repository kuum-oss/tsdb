# TSDB (Time Series Database)

Высокопроизводительная распределенная база данных временных рядов (Time Series Database) на C++20 с поддержкой асинхронного ввода-вывода (`io_uring`), асинхронной репликации с гарантией сохранности лога (WAL), оптимизированным in-memory columnar-хранилищем и встроенным языком запросов.

---

## Архитектура и ключевые компоненты

Проект состоит из следующих основных модулей:
1. **In-Memory Storage Engine (`storage.hpp`/`storage.cpp`):**
   * Данные организованы по колоночному принципу (отдельно `timestamps` и `values`) для максимальной локальности кэша и эффективной агрегации.
   * Память для векторов выделяется с помощью `AlignedAllocator` (выравнивание по 64 байта) для поддержки SIMD-инструкций.
2. **Write-Ahead Log (WAL):**
   * Гарантирует надежность хранения (Durability). Каждая запись последовательно пишется на диск через mmap-буфер, снабжается уникальным LSN и контрольной суммой CRC32-IEEE для верификации целостности при восстановлении.
3. **Асинхронные Снимки (Snapshots):**
   * Фоновое сохранение снимка БД в `snapshot.bin` без блокирования пишущих потоков.
4. **Сетевой движок (`network.hpp`/`network.cpp`):**
   * Поддерживает `io_uring` (Linux), `epoll` (Linux) и стандартный блокирующий I/O (macOS/Unix).
5. **Асинхронная Репликация (`replication.hpp`):**
   * Передача записей от Primary к Replica в виде непрерывного потока транзакций с отслеживанием подтвержденных LSN.
6. **Концепты C++20 (`concepts.hpp` / `query.hpp`):**
   * Проверка типов на этапе компиляции (например, концепты `NumericRange` и `Serializable`).

---

## Решенные проблемы и оптимизации

В рамках стабилизации были устранены следующие критические уязвимости и баги:

### 1. Исправление раскладки WAL и переполнения размера метрик
* Разрез payload переведен на 4-байтовое представление (`uint32_t`). 
* Исправлены смещения: LSN (0-7B), CRC (8-11B), Len (12-15B), Sentinel `0xAB` (16B), Payload (с 17B). Расчет CRC теперь охватывает диапазон от поля Len до конца Payload.

### 2. Устранение взаимных блокировок (Deadlocks) в корутинах
* Переход на Symmetric Transfer через coroutine handles для `Task<void>`.
* Для ожидания `std::future` спроектирован единый фоновый планировщик `FutureWaiter`, исключающий thread explosion.

### 3. Фоновые снимки
* Запись снимков перенесена на фоновый `std::thread`, который детачится (`.detach()`), обеспечивая асинхронность.

### 4. Ограничение потребления памяти репликацией (OOM Prevention)
* Добавлен метод `trim_log(uint64_t ack_lsn)`, удаляющий подтвержденные репликой транзакции из памяти Primary.

### 5. Безопасность retention очистки
* Добавлено автоматическое выравнивание векторов `timestamps` и `values` при обнаружении рассогласования.

---

## Сборка и запуск

### Вариант 1: Запуск через Docker (Рекомендуемый)

1. **Сборка Docker-образа:**
   ```bash
   docker build -t tsdb .
   ```

2. **Запуск Primary-ноды:**
   ```bash
   docker run -d --name tsdb-primary -p 7700:7700 tsdb ./tsdb --role primary --port 7700 --wal-dir /data/wal --ttl 3600
   ```

3. **Запуск Replica-ноды:**
   ```bash
   docker run -d --name tsdb-replica -p 7701:7701 tsdb ./tsdb --role replica --port 7701 --wal-dir /data/wal_replica --primary localhost:7700
   ```

---

## Тестирование

### Запуск интерактивного CLI в Docker-контейнере

```bash
docker exec -it tsdb-primary ./tsdb-cli --port 7700
```

### Запись метрик (WRITE)

```sql
WRITE cpu.usage host="server-1" value=42.5
WRITE cpu.usage host="server-1" value=58.2
```

### Чтение и агрегация метрик (QUERY)

```sql
SELECT avg(cpu.usage) WHERE host="server-1" OVER last 5m
SELECT percentile(95)(cpu.usage) WHERE host="*" OVER last 1h
```
