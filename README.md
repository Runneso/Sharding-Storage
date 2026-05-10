# Sharding Key-Value Storage with Golang

---

## Как собрать и запустить Router/Shard/CLI

Проект реализует распределённое in-memory key-value хранилище с шардированием. В текущей домашней работе система разделена на три основных компонента:

- **Router** — принимает клиентские и cluster-запросы, хранит конфигурацию кластера и маршрутизирует операции на нужный shard.
- **Shard** — хранит локальные данные и выполняет операции `put`, `get`, `delete`, `dump`.
- **CLI** — командная строка для управления кластером, настройки стратегии шардирования и выполнения пользовательских/debug-команд.

Сборка выполняется из директорий соответствующих команд:

```bash
# Router
go build -o router.exe ./cmd/router

# Shard
go build -o shard.exe ./cmd/shard

# CLI
go build -o cli.exe ./cmd/cli
```

Пример запуска:

```bash
# Запуск Router
./router.exe -id=router -host="127.0.0.1" -port=8081

# Запуск shard-узлов
./shard.exe -id=B -host="127.0.0.1" -port=8082
./shard.exe -id=C -host="127.0.0.1" -port=8083
./shard.exe -id=D -host="127.0.0.1" -port=8084

# Запуск CLI
./cli.exe
```

После запуска CLI нужно указать адрес Router:

```bash
router 127.0.0.1 8081
```

---

## Краткая архитектура

### Router

Router является центральной точкой входа для клиента. Он принимает команды от CLI и пользовательские запросы, после чего определяет shard-владельца ключа по текущей стратегии шардирования.

Router хранит конфигурацию кластера:

```text
nodes: nodeId -> NodeInfo
strategy: range | hash | consistent
ranges: список границ для range-based sharding
vnodes: количество виртуальных узлов для consistent hashing
weights: nodeId -> weight
```

Основные обязанности Router:

- принимать `CLIENT_PUT_REQUEST`, `CLIENT_GET_REQUEST`, `CLIENT_DELETE_REQUEST`, `CLIENT_DUMP_REQUEST`, `CLIENT_RANGE_GET_REQUEST`;
- принимать cluster-команды: `addNode`, `removeNode`, `setStrategy`, `setRanges`, `setVnodes`, `setWeight`, `migrateData`;
- выбирать shard по ключу;
- пересылать `SHARD_*_REQUEST` на shard-узлы;
- ждать `SHARD_*_RESPONSE` и возвращать клиенту ответ;
- выполнять миграцию данных при изменении конфигурации.

### Shard

Shard хранит локальные данные в in-memory storage. Он не принимает решения о маршрутизации, а только выполняет команды, которые присылает Router.

Shard поддерживает:

- `SHARD_PUT_REQUEST` — записать ключ;
- `SHARD_GET_REQUEST` — получить ключ;
- `SHARD_DELETE_REQUEST` — удалить ключ;
- `SHARD_DUMP_REQUEST` — вернуть полный локальный dump.

Для защиты от повторных `PUT` и `DELETE` запросов используется дедупликация по `request_id`.

### CLI

CLI является тонким клиентом к Router. Он не хранит полную cluster state и не выполняет маршрутизацию самостоятельно. Все команды отправляются в Router через TCP JSON Lines.

Основные команды CLI:

```bash
router <host> <port>

addNode <nodeId> <host> <port>
removeNode <nodeId>
listNodes
setStrategy range|hash_mod_n|consistent_hashing
setRanges <boundary1> <boundary2> ...
setVnodes <count>
setWeight <nodeId> <weight>
migrateData

put <key> <value>
get <key>
delete <key>
rangeGet <leftKey> <rightKey>

dump --target <nodeId>
clusterDump
stats
ringInfo
```

---

## Спецификация протокола

Взаимодействие между CLI, Router и Shard выполняется поверх **TCP**.

Формат сообщений — **JSON Lines**:

- каждое сообщение является одним JSON-объектом;
- объект сериализуется в одну строку;
- строка завершается символом `\n`.

Каждое сообщение содержит поля:

```json
{
  "request_id": "uuid",
  "type": "MESSAGE_TYPE"
}
```

---

## Client -> Router

### CLIENT_PUT_REQUEST

```json
{
  "type": "CLIENT_PUT_REQUEST",
  "request_id": "uuid",
  "key": "string",
  "value": "string"
}
```

### CLIENT_GET_REQUEST

```json
{
  "type": "CLIENT_GET_REQUEST",
  "request_id": "uuid",
  "key": "string"
}
```

### CLIENT_DELETE_REQUEST

```json
{
  "type": "CLIENT_DELETE_REQUEST",
  "request_id": "uuid",
  "key": "string"
}
```

### CLIENT_DUMP_REQUEST

```json
{
  "type": "CLIENT_DUMP_REQUEST",
  "request_id": "uuid",
  "node_id": "string"
}
```

### CLIENT_RANGE_GET_REQUEST

```json
{
  "type": "CLIENT_RANGE_GET_REQUEST",
  "request_id": "uuid",
  "left_key": "string",
  "right_key": "string"
}
```

---

## Router -> Client

### CLIENT_PUT_RESPONSE

```json
{
  "type": "CLIENT_PUT_RESPONSE",
  "request_id": "uuid",
  "node": {
    "node_id": "string",
    "hostname": "string",
    "port": 8081
  },
  "status": "OK | ERROR",
  "error_code": "Optional[string]",
  "error_msg": "Optional[string]"
}
```

### CLIENT_GET_RESPONSE

```json
{
  "type": "CLIENT_GET_RESPONSE",
  "request_id": "uuid",
  "node": {
    "node_id": "string",
    "hostname": "string",
    "port": 8081
  },
  "status": "OK | ERROR",
  "found": true,
  "value": "string",
  "error_code": "Optional[string]",
  "error_msg": "Optional[string]"
}
```

### CLIENT_DELETE_RESPONSE

```json
{
  "type": "CLIENT_DELETE_RESPONSE",
  "request_id": "uuid",
  "node": {
    "node_id": "string",
    "hostname": "string",
    "port": 8081
  },
  "status": "OK | ERROR",
  "error_code": "Optional[string]",
  "error_msg": "Optional[string]"
}
```

### CLIENT_DUMP_RESPONSE

```json
{
  "type": "CLIENT_DUMP_RESPONSE",
  "request_id": "uuid",
  "node": {
    "node_id": "string",
    "hostname": "string",
    "port": 8081
  },
  "status": "OK | ERROR",
  "dump": {
    "key1": "value1",
    "key2": "value2"
  },
  "error_code": "Optional[string]",
  "error_msg": "Optional[string]"
}
```

### CLIENT_RANGE_GET_RESPONSE

```json
{
  "type": "CLIENT_RANGE_GET_RESPONSE",
  "request_id": "uuid",
  "node": {
    "node_id": "string",
    "hostname": "string",
    "port": 8081
  },
  "status": "OK | ERROR",
  "dump": {
    "key1": "value1",
    "key2": "value2"
  },
  "error_code": "Optional[string]",
  "error_msg": "Optional[string]"
}
```

---

## Router -> Shard

### SHARD_PUT_REQUEST

```json
{
  "type": "SHARD_PUT_REQUEST",
  "request_id": "uuid",
  "key": "string",
  "value": "string"
}
```

### SHARD_GET_REQUEST

```json
{
  "type": "SHARD_GET_REQUEST",
  "request_id": "uuid",
  "key": "string"
}
```

### SHARD_DELETE_REQUEST

```json
{
  "type": "SHARD_DELETE_REQUEST",
  "request_id": "uuid",
  "key": "string"
}
```

### SHARD_DUMP_REQUEST

```json
{
  "type": "SHARD_DUMP_REQUEST",
  "request_id": "uuid"
}
```

---

## Shard -> Router

### SHARD_PUT_RESPONSE

```json
{
  "type": "SHARD_PUT_RESPONSE",
  "request_id": "uuid",
  "node": {
    "node_id": "string",
    "hostname": "string",
    "port": 8082
  }
}
```

### SHARD_GET_RESPONSE

```json
{
  "type": "SHARD_GET_RESPONSE",
  "request_id": "uuid",
  "node": {
    "node_id": "string",
    "hostname": "string",
    "port": 8082
  },
  "found": true,
  "value": "string"
}
```

### SHARD_DELETE_RESPONSE

```json
{
  "type": "SHARD_DELETE_RESPONSE",
  "request_id": "uuid",
  "node": {
    "node_id": "string",
    "hostname": "string",
    "port": 8082
  }
}
```

### SHARD_DUMP_RESPONSE

```json
{
  "type": "SHARD_DUMP_RESPONSE",
  "request_id": "uuid",
  "node": {
    "node_id": "string",
    "hostname": "string",
    "port": 8082
  },
  "dump": {
    "key1": "value1",
    "key2": "value2"
  }
}
```

---

## Cluster commands

### CLUSTER_ADD_NODE

```json
{
  "type": "CLUSTER_ADD_NODE",
  "request_id": "uuid",
  "node": {
    "node_id": "B",
    "hostname": "127.0.0.1",
    "port": 8082
  }
}
```

### CLUSTER_REMOVE_NODE

```json
{
  "type": "CLUSTER_REMOVE_NODE",
  "request_id": "uuid",
  "node": {
    "node_id": "B",
    "hostname": "127.0.0.1",
    "port": 8082
  }
}
```

### CLUSTER_SET_STRATEGY

```json
{
  "type": "CLUSTER_SET_STRATEGY",
  "request_id": "uuid",
  "strategy": "range | hash | consistent"
}
```

В CLI допускаются алиасы:

```text
range
hash_mod_n
consistent_hashing
```

### CLUSTER_SET_RANGES

```json
{
  "type": "CLUSTER_SET_RANGES",
  "request_id": "uuid",
  "boundaries": ["user:02001", "user:04001", "user:06001", "user:08001"]
}
```

### CLUSTER_SET_VNODES

```json
{
  "type": "CLUSTER_SET_VNODES",
  "request_id": "uuid",
  "count": 150
}
```

### CLUSTER_SET_WEIGHT

```json
{
  "type": "CLUSTER_SET_WEIGHT",
  "request_id": "uuid",
  "node_id": "B",
  "weight": 2
}
```

### CLUSTER_MIGRATE_DATA

```json
{
  "type": "CLUSTER_MIGRATE_DATA",
  "request_id": "uuid"
}
```

### CLUSTER_INFO

```json
{
  "type": "CLUSTER_INFO",
  "request_id": "uuid"
}
```

### RING_INFO_REQUEST

```json
{
  "type": "RING_INFO_REQUEST",
  "request_id": "uuid"
}
```

---

## Стратегии шардирования

В проекте реализованы три стратегии шардирования:

- `range`
- `hash`
- `consistent`

CLI использует более человекочитаемые названия:

```text
range
hash_mod_n
consistent_hashing
```

---

## Range-based sharding

В range-based sharding ключевое пространство делится на непрерывные диапазоны по лексикографическому порядку ключей.

Для `N` shard-узлов нужно задать `N - 1` границу:

```bash
setRanges <boundary1> <boundary2> ... <boundaryN-1>
```

Например, для трёх узлов:

```bash
setRanges m z
```

Если ноды отсортированы по `nodeId` как `B, C, D`, то распределение будет таким:

```text
B: key < "m"
C: "m" <= key < "z"
D: key >= "z"
```

Для ключей вида `user:001`, `user:002`, ..., `user:100` границы `m z` плохие, потому что все ключи `user:*` попадают в диапазон между `m` и `z`.

Более удачные границы для 100 пользователей и трёх узлов:

```bash
setRanges user:034 user:067
```

После изменения границ нужно вручную вызвать миграцию:

```bash
migrateData
```

---

## Hash-mod-N sharding

В hash-mod-N каждый ключ хешируется, после чего выбирается shard по формуле:

```text
index = hash(key) % N
```

где `N` — количество shard-узлов в кластере.

Преимущество этой стратегии — простота и обычно хорошее распределение ключей.

Главный недостаток — при изменении числа узлов меняется `N`, поэтому для большинства ключей меняется результат `hash(key) % N`. Из-за этого при добавлении или удалении узла происходит катастрофическая ребалансировка: может переехать значительная часть данных.

В демонстрации при переходе с 4 на 5 узлов получилось около `80%` мигрированных ключей.

---

## Consistent hashing

В consistent hashing ключи и узлы отображаются на логическое hash-кольцо.

Алгоритм выбора shard:

1. Для каждого shard-узла создаётся несколько виртуальных точек на кольце.
2. Ключ хешируется в число.
3. На кольце ищется первая виртуальная точка, чей hash больше или равен hash ключа.
4. Если такой точки нет, выбирается первая точка кольца.
5. Физический узел этой виртуальной точки считается владельцем ключа.

Преимущество consistent hashing — минимальная миграция при добавлении или удалении узлов. При добавлении 5-го узла ожидаемо переезжает около `1/5 = 20%` ключей.

---

## Virtual nodes

Virtual nodes нужны для улучшения равномерности распределения ключей в consistent hashing.

Если у каждой физической ноды только одна точка на кольце (`V=1`), распределение может быть сильно неравномерным: одна нода получит большой сектор кольца, другая — маленький.

При увеличении `V` каждая нода получает больше виртуальных точек, сектора становятся меньше и лучше перемешиваются. Поэтому распределение ключей становится более равномерным.

Краткая закономерность:

```text
чем больше V, тем больше виртуальных точек у каждой ноды на кольце,
тем равномернее распределяются ключи,
но тем тяжелее хранить и пересчитывать кольцо.
```

---

## Миграция данных

Миграция нужна, когда после изменения конфигурации кластера меняется соответствие:

```text
key -> shard node
```

В текущей реализации миграция устроена так:

1. Router собирает `dump` с shard-узлов.
2. Для каждого ключа заново вычисляет владельца по текущей стратегии.
3. Если новый владелец отличается от старого, Router отправляет `SHARD_PUT_REQUEST` на новый shard.
4. После успешного переноса Router отправляет `SHARD_DELETE_REQUEST` на старый shard.
5. В лог выводятся `totalKeys`, `movedKeys`, `movedPercent`, `duration`.

### Когда миграция запускается автоматически

```text
hash_mod_n:
  addNode
  removeNode

consistent_hashing:
  addNode
  removeNode
  setVnodes
  setWeight
```

### Когда миграция запускается вручную

Для `range` миграция выполняется вручную:

```bash
setRanges <new boundaries>
migrateData
```

---

## Ограничения реализации

- Хранилище является in-memory, поэтому данные теряются при перезапуске shard-процесса.
- Router хранит конфигурацию кластера в памяти.
- Для `range` границы задаются вручную и не пересчитываются автоматически.
- Shard-level ошибки в текущем протоколе не имеют отдельного `status/error` поля, поэтому ошибки уровня shard обычно проявляются на Router как timeout.
- Повторные `PUT` и `DELETE` запросы защищены дедупликацией по `request_id`.

---

## Итог

В текущей домашней работе реализовано распределённое key-value хранилище с тремя стратегиями шардирования:

- range-based sharding;
- hash-mod-N;
- consistent hashing с virtual nodes и weights.

Демонстрационные сценарии показывают:

- зависимость range-based sharding от выбранных границ;
- катастрофическую ребалансировку в hash-mod-N;
- минимальную миграцию в consistent hashing;
- улучшение равномерности при увеличении числа virtual nodes;
- поведение системы при падении shard-узла.