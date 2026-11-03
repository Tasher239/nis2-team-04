# Проверка восстановления

Flink/PyFlink 2.3.0, Python 3.12.12, OpenJDK 17, локальный MiniCluster.
Вход: 100 000 покупок, 1000 пользователей, MD5 `131147124f169c698e8875326b094381`.
файл не менялся между фазами. Checkpoint каждые 1000 мс. Остановка через 10 с
после статуса RUNNING.

```sh
export JAVA_HOME=/path/to/jdk-17
export INPUT_PATH=data/generated/purchases.jsonl USER_COUNT=1000 PURCHASES_PER_USER=100
uv run flink-recovery generate
RUN_NAME=kt1-rescale-2to4-v3 PARALLELISM=2 uv run flink-recovery recovery-smoke --restore-parallelism 4
RUN_NAME=kt1-restart-2to2-v3 PARALLELISM=2 uv run flink-recovery recovery-smoke --restore-parallelism 2
```

Для повтора нужны новые имена каталогов: существующий выход не перезаписывается.

| Сценарий | Снимок, байт | Пауза, сек | Восстановление, сек | Записей до / после | check.ok |
|---|---:|---:|---:|---:|---|
| Увеличение числа воркеров с 2 до 4 | 73 798 | 3.558304 | 2.912447 | 41 815 / 58 185 | true |
| Рестарт 2 -> 2 | 73 798 | 3.110907 | 2.459894 | 42 741 / 57 259 | true |

До остановки в обоих экспериментах обработаны все 1000 пользователей. После увеличения числа воркеров
работали подзадачи 0, 1, 2 и 3. Это по одному запуску каждого сценария.

Пропускная способность после восстановления: для рестарта 2 -> 2 окно полное, 3322.5 событий/сек.
Для увеличения числа воркеров с 2 до 4 `null` - восстановленное задание закончилось через 9.5 сек, раньше конца
10-секундного окна.

Файлы экспериментов:

| Сценарий | Отчет | Команды | Снимок | Запуск | Граница восстановления | Контрольные суммы |
|---|---|---|---|---|---|---|
| Увеличение числа воркеров с 2 до 4 | [отчет](rescale-2to4/recovery-smoke.json) | [команды](rescale-2to4/commands.jsonl) | [снимок](rescale-2to4/savepoint.json) | [запуск](rescale-2to4/run.json) | [образец](rescale-2to4/boundary-sample.jsonl) | [файлы](rescale-2to4/files.json) |
| Рестарт 2 -> 2 | [отчет](restart-2to2/recovery-smoke.json) | [команды](restart-2to2/commands.jsonl) | [снимок](restart-2to2/savepoint.json) | [запуск](restart-2to2/run.json) | [образец](restart-2to2/boundary-sample.jsonl) | [файлы](restart-2to2/files.json) |

Что лежит в файлах:

- `recovery-smoke.json` - полный отчет проверки и метрик.
- `commands.jsonl` - исходные отметки команд.
- `savepoint.json` - путь к canonical savepoint, размеры каждого файла и их сумма.
- `run.json` - идентификатор и длительность восстановленного задания.
- `boundary-sample.jsonl` - последние три записи до восстановления и первые три после.
  Этого достаточно для пересчета паузы и времени восстановления.
- `files.json` - размеры и SHA-256 входа и всех завершенных part-файлов.

Пути относительные. Полный выход и бинарные savepoint лежат в `output/` и не входят
в Git. Образец границы нужен только для проверки времени. Корректность и пропускная способность
считаются по полному выходу.

Первый прогон тех же сценариев тоже прошел `check`: снимки 73 741 и 73 798 байт,
паузы 2.98 и 3.52 с, восстановление 2.43 и 2.84 с. Его заменил повтор выше после
правки формата артефактов.

[Диагностика ошибки обертки](pyflink-stop-wrapper-failure.txt)
и [журнал команд](pyflink-stop-wrapper-commands.jsonl) фиксируют первую
попытку: стандартная Python-обертка вызывает удаленную Java-перегрузку
`stopWithSavepoint(boolean, String)`. Обход в коде использует
[Java JobClient](https://nightlies.apache.org/flink/flink-docs-release-2.3/api/java/org/apache/flink/core/execution/JobClient.html)
с третьим аргументом `SavepointFormatType.CANONICAL` через существующий gateway.
После этого оба сценария прошли успешно.
