import shlex
from json import dumps

import prompt
from prettytable import PrettyTable

from src.primitive_db.constants import META_FILE
from src.primitive_db.core import (
    create_table,
    delete,
    drop_table,
    get_table_info,
    insert,
    list_tables,
    normalize_where_clause,
    select,
    update,
)
from src.primitive_db.decorators import create_cacher
from src.primitive_db.parser import parse_condition, parse_scalar, split_csv_values
from src.primitive_db.utils import (
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)

select_cache = create_cacher()


def print_help() -> None:
    """Print available commands for database mode."""
    print("***Операции с данными***")
    print("Функции:")
    print(
        "<command> insert into <имя_таблицы> values "
        "(<значение1>, <значение2>, ...) - создать запись."
    )
    print(
        "<command> select from <имя_таблицы> where "
        "<столбец> = <значение> - прочитать записи по условию."
    )
    print("<command> select from <имя_таблицы> - прочитать все записи.")
    print(
        "<command> update <имя_таблицы> set <столбец1> = "
        "<новое_значение1> where <столбец_условия> = "
        "<значение_условия> - обновить запись."
    )
    print(
        "<command> delete from <имя_таблицы> where <столбец> = "
        "<значение> - удалить запись."
    )
    print("<command> info <имя_таблицы> - вывести информацию о таблице.")
    print(
        "<command> create_table <имя_таблицы> <столбец1:тип> "
        "<столбец2:тип> .. - создать таблицу."
    )
    print("<command> list_tables - показать список всех таблиц.")
    print("<command> drop_table <имя_таблицы> - удалить таблицу.")
    print("<command> exit - выход из программы.")
    print("<command> help - справочная информация.")


def _format_columns(metadata: dict, table_name: str) -> str:
    """Format table columns for user output."""
    columns = metadata[table_name]["columns"]
    return ", ".join(f'{column["name"]}:{column["type"]}' for column in columns)


def _render_select_table(columns: list[dict], rows: list[dict]) -> None:
    """Render rows with PrettyTable."""
    table = PrettyTable()
    table.field_names = [column["name"] for column in columns]
    for row in rows:
        table.add_row([row.get(column["name"]) for column in columns])
    print(table)


def run() -> None:
    """Run interactive database REPL."""
    print("***База данных***")
    print_help()

    while True:
        metadata = load_metadata(META_FILE)
        user_input = prompt.string("Введите команду: ").strip()

        if not user_input:
            continue

        try:
            parts = shlex.split(user_input)
        except ValueError:
            print("Некорректное значение: ошибка разбора команды. Попробуйте снова.")
            continue

        command = parts[0]

        if user_input == "exit":
            break
        if user_input == "help":
            print_help()
            continue

        if command == "list_tables":
            if user_input != "list_tables":
                print("Некорректное значение: list_tables. Попробуйте снова.")
                continue
            tables = list_tables(metadata)
            if not tables:
                print("Список таблиц пуст.")
                continue
            for table_name in tables:
                print(f"- {table_name}")
            continue

        if command == "create_table":
            if len(parts) < 3:
                print("Некорректное значение: create_table. Попробуйте снова.")
                continue

            table_name = parts[1]
            columns = parts[2:]
            updated_metadata = create_table(metadata, table_name, columns)
            if updated_metadata is None:
                continue

            save_metadata(META_FILE, updated_metadata)
            print(
                f'Таблица "{table_name}" успешно создана со столбцами: '
                f"{_format_columns(updated_metadata, table_name)}"
            )
            continue

        if command == "drop_table":
            if len(parts) != 2:
                print("Некорректное значение: drop_table. Попробуйте снова.")
                continue

            table_name = parts[1]
            updated_metadata = drop_table(metadata, table_name)
            if updated_metadata is None:
                continue

            save_metadata(META_FILE, updated_metadata)
            save_table_data(table_name, [])
            print(f'Таблица "{table_name}" успешно удалена.')
            continue

        if user_input.startswith("insert into "):
            if " values " not in user_input:
                print("Некорректное значение: insert. Попробуйте снова.")
                continue

            head, values_part = user_input.split(" values ", 1)
            head_parts = head.split()
            if len(head_parts) != 3:
                print("Некорректное значение: insert. Попробуйте снова.")
                continue
            table_name = head_parts[2]

            values_part = values_part.strip()
            if not (values_part.startswith("(") and values_part.endswith(")")):
                print("Некорректное значение: insert. Попробуйте снова.")
                continue

            try:
                parsed_values = [
                    parse_scalar(value)
                    for value in split_csv_values(values_part[1:-1].strip())
                ]
            except ValueError as error:
                print(error)
                continue

            table_data = load_table_data(table_name)
            insert_result = insert(metadata, table_name, parsed_values, table_data)
            if insert_result is None:
                continue
            updated_data, new_id = insert_result

            save_table_data(table_name, updated_data)
            print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')
            continue

        if user_input.startswith("select from "):
            select_parts = user_input.split()
            if len(select_parts) < 3:
                print("Некорректное значение: select. Попробуйте снова.")
                continue

            table_name = select_parts[2]
            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            where_clause = None
            if " where " in user_input:
                _, where_part = user_input.split(" where ", 1)
                try:
                    where_clause = parse_condition(where_part.split())
                except ValueError as error:
                    print(error)
                    continue

                where_clause = normalize_where_clause(
                    metadata, table_name, where_clause
                )
                if where_clause is None:
                    continue

            table_data = load_table_data(table_name)
            cache_key = (
                table_name,
                dumps(where_clause, sort_keys=True, ensure_ascii=False),
                dumps(table_data, sort_keys=True, ensure_ascii=False),
            )
            rows = select_cache(cache_key, lambda: select(table_data, where_clause))
            if rows is None:
                continue
            if not rows:
                print("Записей не найдено.")
                continue

            _render_select_table(metadata[table_name]["columns"], rows)
            continue

        if user_input.startswith("update "):
            if " set " not in user_input or " where " not in user_input:
                print("Некорректное значение: update. Попробуйте снова.")
                continue

            update_head, set_tail = user_input.split(" set ", 1)
            table_parts = update_head.split()
            if len(table_parts) != 2:
                print("Некорректное значение: update. Попробуйте снова.")
                continue
            table_name = table_parts[1]

            set_part, where_part = set_tail.split(" where ", 1)
            try:
                set_clause = parse_condition(set_part.split())
                where_clause = parse_condition(where_part.split())
            except ValueError as error:
                print(error)
                continue

            table_data = load_table_data(table_name)
            update_result = update(
                metadata, table_name, table_data, set_clause, where_clause
            )
            if update_result is None:
                continue
            updated_data, updated_ids = update_result

            if not updated_ids:
                print("Записи не найдены.")
                continue

            save_table_data(table_name, updated_data)
            for updated_id in updated_ids:
                print(
                    f'Запись с ID={updated_id} в таблице "{table_name}" '
                    "успешно обновлена."
                )
            continue

        if user_input.startswith("delete from "):
            if " where " not in user_input:
                print("Некорректное значение: delete. Попробуйте снова.")
                continue

            delete_head, where_part = user_input.split(" where ", 1)
            delete_parts = delete_head.split()
            if len(delete_parts) != 3:
                print("Некорректное значение: delete. Попробуйте снова.")
                continue
            table_name = delete_parts[2]

            try:
                where_clause = parse_condition(where_part.split())
            except ValueError as error:
                print(error)
                continue

            table_data = load_table_data(table_name)
            delete_result = delete(metadata, table_name, table_data, where_clause)
            if delete_result is None:
                continue
            updated_data, deleted_ids = delete_result

            if not deleted_ids:
                print("Записи не найдены.")
                continue

            save_table_data(table_name, updated_data)
            for deleted_id in deleted_ids:
                print(
                    f'Запись с ID={deleted_id} успешно удалена из '
                    f'таблицы "{table_name}".'
                )
            continue

        if command == "info":
            if len(parts) != 2:
                print("Некорректное значение: info. Попробуйте снова.")
                continue

            table_name = parts[1]
            table_data = load_table_data(table_name)
            info = get_table_info(metadata, table_name, table_data)
            if info is None:
                continue

            print(f'Таблица: {info["table"]}')
            print(f'Столбцы: {info["columns"]}')
            print(f'Количество записей: {info["rows_count"]}')
            continue

        print(f"Функции {command} нет. Попробуйте снова.")
