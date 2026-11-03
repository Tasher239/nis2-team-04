# Flink: rescaling и восстановление

**Проектное задание:** [P14-B. Flink: rescaling и восстановление](https://distsys.ru/osukhoroslov/nis2/src/branch/main/project-tasks/p14-b-flink-rescaling-recovery.md)

**Участники:**

- Костин Даниил
- Асадуллин Айдар
- Поляков Иван

## Сокращенный запуск

Нужны Python 3.12, JDK 17 и [uv](https://docs.astral.sh/uv/).

```text
uv sync
cp .env.example .env
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

uv run flink-recovery generate
uv run flink-recovery run
uv run flink-recovery check
uv run flink-recovery metrics
uv run pytest
```

Параметры запуска читаются из `.env` (образец - `.env.example`) и при необходимости
переопределяются переменными окружения. `generate` записывает воспроизводимый вход
в `INPUT_PATH`, `run` выполняет задание Flink для `RUN_NAME`, `check` сверяет выход
приемника с исходными событиями и завершается с кодом 1 при расхождениях,
`metrics` считает паузу, время восстановления и пропускную способность.
Каталог запуска `output/<RUN_NAME>/` содержит выходные файлы приемника, журнал
команд `commands.jsonl` и параметры запуска `run.json`.

Разбор проверок, ожидаемые и полученные результаты:
[контрольная точка 1](nis/checkpoints/01.md).

## Навигация

- [Описание проекта](nis/project.md)
- [План эксперимента](nis/experiment-plan.md)
- [Контрольная точка 1](nis/checkpoints/01.md)
- Код: [src/flink_recovery](src/flink_recovery)
- Тесты: [tests](tests)
- Данные: [data/purchases.jsonl](data/purchases.jsonl), крупные входы генерируются командой `generate`
- Архитектура кода: [docs/architecture.md](docs/architecture.md)
- Что делает код: [docs/code-overview.md](docs/code-overview.md)
