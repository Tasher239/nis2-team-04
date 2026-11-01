# Flink: rescaling и восстановление

Задание [P14-B](https://distsys.ru/osukhoroslov/nis2/src/branch/main/project-tasks/p14-b-flink-rescaling-recovery.md).
Команда 4: Асадуллин Айдар, Костин Даниил, Поляков Иван.

## Окружение

Python 3.12, Java 17. Зависимости - в `requirements.txt`.

```sh
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

На macOS выбрать установленный JDK 17:

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

## Контрольный запуск

Flink запускается локально вместе с `job.py`.
[generate.py](generate.py) записывает вход в `INPUT_PATH`: `USER_COUNT` покупателей
по `PURCHASES_PER_USER` покупок. Параметры задаются в `.env`.

```sh
python generate.py
python job.py
python check.py
```

Результаты сохраняются в `output/<RUN_NAME>/`. Перед повторным запуском измените `RUN_NAME` в [.env](.env).

## Материалы

1. [Описание проекта](nis/project.md).
2. [План эксперимента](nis/experiment-plan.md).
3. [КТ1: контрольный пример](nis/checkpoints/01.md).
