# Flink: rescaling и восстановление

**Проектное задание:** [P14-B. Flink: rescaling и восстановление](https://distsys.ru/osukhoroslov/nis2/src/branch/main/project-tasks/p14-b-flink-rescaling-recovery.md)

**Участники:**

- Асадуллин Айдар
- Костин Даниил
- Поляков Иван

## Сокращенный запуск

Нужны Python 3.12, JDK 17 и [uv](https://docs.astral.sh/uv/).

```text
uv sync
cp .env.example .env
export JAVA_HOME=/path/to/jdk-17

uv run flink-recovery generate
uv run flink-recovery run
uv run flink-recovery check
uv run flink-recovery metrics
uv run pytest
```

Параметры находятся в `.env`; их можно переопределить переменными окружения.
`generate` создает вход, `run` запускает Flink, `check` сверяет выход с входом,
а `metrics` считает паузу, время восстановления и throughput. Результаты лежат
в `output/<RUN_NAME>/`: выходные файлы, `commands.jsonl` и `run.json`.

Один эксперимент сохранения и восстановления на сгенерированном входе:

```sh
RUN_NAME=smoke-rescale PARALLELISM=2 uv run flink-recovery recovery-smoke --restore-parallelism 4
RUN_NAME=smoke-restart PARALLELISM=2 uv run flink-recovery recovery-smoke --restore-parallelism 2
```

Команда ждет статус `RUNNING`, через 10 секунд останавливает задание с canonical
savepoint, измеряет снимок, восстанавливает задание и проверяет полный выход.
`--stop-after-s` меняет задержку. Вход должен содержать события до и после
восстановления. Для каждого эксперимента нужен новый `RUN_NAME`.

В каталоге запуска появятся `savepoint.json`, `recovery-smoke.json` и, при ошибке,
`failure.txt`. Снимок лежит в `output/savepoints/<RUN_NAME>/`.

Можно восстановить остановленное задание отдельно: `run --savepoint /путь/к/savepoint`.
Для этого сохраняются исходные `INPUT_PATH`, `RUN_NAME`, выходной каталог и нужный
`PARALLELISM`. На один каталог допускается одно восстановление.

Разбор проверок, ожидаемые и полученные результаты:
[контрольная точка 1](nis/checkpoints/01.md).

## Навигация

- [Описание проекта](nis/project.md)
- [План эксперимента](nis/experiment-plan.md)
- [Контрольная точка 1](nis/checkpoints/01.md)
- Код: [команды CLI](src/flink_recovery/cli/main.py), [запуск Flink](src/flink_recovery/streaming/runner.py)
- Тесты: [проверка результата](tests/domain/test_validation.py), [расчет метрик](tests/domain/test_metrics.py)
- Данные: [data/purchases.jsonl](data/purchases.jsonl), крупные входы генерируются командой `generate`
