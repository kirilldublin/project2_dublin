# Primitive DB

Консольное приложение на Python, имитирующее простую базу данных.
Проект поддерживает управление таблицами, CRUD-операции, валидацию типов,
декораторы для обработки ошибок/подтверждения действий/замера времени и
кэширование `select` через замыкание.

## Установка

```bash
make install
# или
poetry install
```

## Запуск

```bash
make run
# или
poetry run database
```

## Команды

### Управление таблицами

- `create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> ...`
- `list_tables`
- `drop_table <имя_таблицы>`

Поддерживаемые типы данных: `int`, `str`, `bool`.

### CRUD-операции

- `insert into <имя_таблицы> values (<значение1>, <значение2>, ...)`
- `select from <имя_таблицы>`
- `select from <имя_таблицы> where <столбец> = <значение>`
- `update <имя_таблицы> set <столбец> = <новое_значение> where <столбец> = <значение>`
- `delete from <имя_таблицы> where <столбец> = <значение>`
- `info <имя_таблицы>`
- `help`
- `exit`

## Декораторы и замыкания

- `handle_db_errors`: централизованная обработка `FileNotFoundError`, `KeyError`, `ValueError`.
- `confirm_action(action_name)`: подтверждение опасных операций (`drop_table`, `delete`).
- `log_time`: вывод времени выполнения (`insert`, `select`).
- `create_cacher()`: замыкание для кэширования повторных `select`-запросов.

Пример подтверждения:

```text
Введите команду: drop_table users
Вы уверены, что хотите выполнить "удаление таблицы"? [y/n]: n
Операция отменена.
```

## Структура проекта

- `src/primitive_db/main.py` — точка входа.
- `src/primitive_db/engine.py` — цикл CLI и диспетчеризация команд.
- `src/primitive_db/core.py` — бизнес-логика таблиц и CRUD.
- `src/primitive_db/utils.py` — загрузка/сохранение JSON.
- `src/primitive_db/parser.py` — парсинг `values`, `where`, `set`.
- `src/primitive_db/decorators.py` — декораторы и кэш-замыкание.
- `src/primitive_db/constants.py` — константы проекта.

## Демонстрация (asciinema)

После записи вставьте embed-ссылку:

```markdown
[![asciicast](https://asciinema.org/a/<CAST_ID>.svg)](https://asciinema.org/a/<CAST_ID>)
```

Рекомендуемый сценарий:

1. Запуск `database`.
2. `create_table users name:str age:int is_active:bool`.
3. `insert into users values ("Sergei", 28, true)`.
4. `select from users where age = 28`.
5. `update users set age = 29 where name = "Sergei"`.
6. `delete from users where ID = 1` (с подтверждением).
7. `drop_table users` (с подтверждением).
8. `exit`.
