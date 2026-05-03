# Demo scenarios

---

## Сценарий 1 — Range-based: неравномерность при плохих границах

```bash
# запускаем роутер
./router.exe

# Запускаем 3 шарда 
./shard.exe -id=B -host="127.0.0.1" -port=8082
./shard.exe -id=C -host="127.0.0.1" -port=8083
./shard.exe -id=D -host="127.0.0.1" -port=8084

# Запускаем CLI
./cli.exe 

# Настраиваем кластер
router 127.0.0.1 8081
addNode B 127.0.0.1 8082
addNode C 127.0.0.1 8083
addNode D 127.0.0.1 8084
setStrategy range
setRanges m z

# Запускаем скрипт (1000 пользователей)
python .\scripts\users.py
>> 127.0.0.1
>> 8081

# Видим, что все ключи user:<int> попали на один и тот же шард
stats

# Меняем границы шардирования и запускаем миграцию
setRanges user:334 user:667
migrateData

# Видим, что распределение ключей стало равномерным
stats
```

---

## Сценарий 2 — Hash-mod-N: катастрофическая ребалансировка

```bash
# запускаем роутер
./router.exe

# Запускаем 4 шарда 
./shard.exe -id=B -host="127.0.0.1" -port=8082
./shard.exe -id=C -host="127.0.0.1" -port=8083
./shard.exe -id=D -host="127.0.0.1" -port=8084
./shard.exe -id=E -host="127.0.0.1" -port=8085

# Запускаем CLI
./cli.exe 

# Настраиваем кластер
router 127.0.0.1 8081
addNode B 127.0.0.1 8082
addNode C 127.0.0.1 8083
addNode D 127.0.0.1 8084
setStrategy hash_mod_n

# Запускаем скрипт
python .\scripts\uniform.py
>> 127.0.0.1
>> 8081
>> 1000

# Видим, что распределение равномерное
stats

# Добавляем новый шард и смотрим на movedKeys (я получил 75.57%)
addNode E 127.0.0.1 8085

# Проверяем работоспособность кластера после миграции
clusterDump
```

---

## Сценарий 3 — Consistent hashing: минимальная миграция

```bash
# запускаем роутер
./router.exe

# Запускаем 4 шарда 
./shard.exe -id=B -host="127.0.0.1" -port=8082
./shard.exe -id=C -host="127.0.0.1" -port=8083
./shard.exe -id=D -host="127.0.0.1" -port=8084
./shard.exe -id=E -host="127.0.0.1" -port=8085

# Запускаем CLI
./cli.exe 

# Настраиваем кластер
router 127.0.0.1 8081
addNode B 127.0.0.1 8082
addNode C 127.0.0.1 8083
addNode D 127.0.0.1 8084
setStrategy consistent_hashing
setVnodes 150

# Запускаем скрипт
python .\scripts\uniform.py
>> 127.0.0.1
>> 8081
>> 1000

# Видим, что распределение равномерное
stats

# Добавляем новый шард и смотрим на movedKeys (я получил 26.75%)
addNode E 127.0.0.1 8085

# Проверяем работоспособность кластера после миграции
clusterDump
```

## Сценарий 4 — Влияние числа виртуальных узлов на равномерность
```bash
# запускаем роутер
./router.exe

# Запускаем 5 шарда 
./shard.exe -id=B -host="127.0.0.1" -port=8082
./shard.exe -id=C -host="127.0.0.1" -port=8083
./shard.exe -id=D -host="127.0.0.1" -port=8084
./shard.exe -id=E -host="127.0.0.1" -port=8085
./shard.exe -id=F -host="127.0.0.1" -port=8086

# Запускаем CLI
./cli.exe 

# Настраиваем кластер
router 127.0.0.1 8081
addNode B 127.0.0.1 8082
addNode C 127.0.0.1 8083
addNode D 127.0.0.1 8084
addNode E 127.0.0.1 8085
addNode F 127.0.0.1 8086
setStrategy consistent_hashing
setVnodes 1|5|150

# Запускаем скрипт
python .\scripts\uniform.py
>> 127.0.0.1
>> 8081
>> 10000

# Видим закономерность, что чем больше V, 
# тем больше виртуальных точек у каждой ноды на кольце,
# поэтому ключи распределяются всё равномернее, 
# но кольцо становится тяжелее хранить и пересчитывать
stats
```

## Сценарий 5 — Отказоустойчивость: падение узла
```bash
# запускаем роутер
./router.exe

# Запускаем 3 шарда 
./shard.exe -id=B -host="127.0.0.1" -port=8082
./shard.exe -id=C -host="127.0.0.1" -port=8083
./shard.exe -id=D -host="127.0.0.1" -port=8084

# Запускаем CLI
./cli.exe 

# Настраиваем кластер
router 127.0.0.1 8081
addNode B 127.0.0.1 8082
addNode C 127.0.0.1 8083
addNode D 127.0.0.1 8084
setStrategy consistent_hashing
setVnodes 1

# Запускаем скрипт
python .\scripts\uniform.py
>> 127.0.0.1
>> 8081
>> 10000

# Останавливаем какой-то шард
Ctrl+C
# Видим, что ключи, которые затрагивают упавший шард получают TIMEOUT, а остальные корректно работают
get <key>
put <key> <value>
```