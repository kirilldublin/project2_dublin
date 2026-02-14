"""Business logic for schema and CRUD operations."""

from src.primitive_db.constants import ALLOWED_TYPES
from src.primitive_db.decorators import confirm_action, handle_db_errors, log_time


def _parse_columns(raw_columns: list[str]) -> list[dict[str, str]]:
    """Validate and parse table columns from command tokens."""
    parsed_columns: list[dict[str, str]] = []
    used_names: set[str] = {"ID"}

    for raw_column in raw_columns:
        if ":" not in raw_column:
            raise ValueError(f"Некорректное значение: {raw_column}. Попробуйте снова.")

        column_name, column_type = raw_column.split(":", 1)
        column_name = column_name.strip()
        column_type = column_type.strip()

        if not column_name or column_name.upper() == "ID":
            raise ValueError(f"Некорректное значение: {raw_column}. Попробуйте снова.")

        if column_name in used_names or column_type not in ALLOWED_TYPES:
            raise ValueError(f"Некорректное значение: {raw_column}. Попробуйте снова.")

        used_names.add(column_name)
        parsed_columns.append({"name": column_name, "type": column_type})

    return parsed_columns


@handle_db_errors
def create_table(metadata: dict, table_name: str, columns: list[str]) -> dict:
    """Create table schema with auto-added ID:int column."""
    if table_name in metadata:
        raise ValueError(f'Таблица "{table_name}" уже существует.')
    if not columns:
        raise ValueError("Некорректное значение: нет столбцов. Попробуйте снова.")

    parsed_columns = _parse_columns(columns)
    metadata[table_name] = {
        "columns": [{"name": "ID", "type": "int"}, *parsed_columns],
    }
    return metadata


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata: dict, table_name: str) -> dict:
    """Drop existing table schema."""
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')

    del metadata[table_name]
    return metadata


def list_tables(metadata: dict) -> list[str]:
    """Return sorted table names."""
    return sorted(metadata.keys())


def _ensure_table_exists(metadata: dict, table_name: str) -> dict:
    """Get table metadata or raise ValueError."""
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    return metadata[table_name]


def _column_types(table_meta: dict) -> dict[str, str]:
    """Build map column->type."""
    return {column["name"]: column["type"] for column in table_meta["columns"]}


def _coerce_value(value: object, expected_type: str) -> object:
    """Convert value to expected column type."""
    if expected_type == "int":
        if isinstance(value, bool):
            raise ValueError
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            return int(value)
        raise ValueError

    if expected_type == "str":
        if isinstance(value, str):
            return value
        raise ValueError

    if expected_type == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.lower()
            if lowered == "true":
                return True
            if lowered == "false":
                return False
        raise ValueError

    raise ValueError


@handle_db_errors
def normalize_where_clause(metadata: dict, table_name: str, where_clause: dict) -> dict:
    """Validate and normalize where clause value types."""
    table_meta = _ensure_table_exists(metadata, table_name)
    types = _column_types(table_meta)
    where_column, where_value = next(iter(where_clause.items()))

    if where_column not in types:
        raise ValueError(
            "Некорректное значение: неизвестный столбец. Попробуйте снова."
        )

    try:
        normalized_where_value = _coerce_value(where_value, types[where_column])
    except ValueError as error:
        raise ValueError(
            "Некорректное значение: ошибка типов. Попробуйте снова."
        ) from error

    return {where_column: normalized_where_value}


@handle_db_errors
@log_time
def insert(
    metadata: dict,
    table_name: str,
    values: list[object],
    table_data: list[dict],
) -> tuple[list[dict], int]:
    """Insert new row and return updated rows with generated ID."""
    table_meta = _ensure_table_exists(metadata, table_name)
    columns = table_meta["columns"]
    user_columns = columns[1:]

    if len(values) != len(user_columns):
        raise ValueError("Некорректное значение: неверное количество значений.")

    record: dict[str, object] = {}
    for column, value in zip(user_columns, values):
        try:
            record[column["name"]] = _coerce_value(value, column["type"])
        except ValueError as error:
            raise ValueError(
                f"Некорректное значение: {value}. Попробуйте снова."
            ) from error

    next_id = max((row["ID"] for row in table_data), default=0) + 1
    row = {"ID": next_id, **record}
    table_data.append(row)
    return table_data, next_id


@handle_db_errors
@log_time
def select(table_data: list[dict], where_clause: dict | None = None) -> list[dict]:
    """Select all rows or rows matching where clause."""
    if where_clause is None:
        return table_data

    where_column, where_value = next(iter(where_clause.items()))
    return [row for row in table_data if row.get(where_column) == where_value]


@handle_db_errors
def update(
    metadata: dict,
    table_name: str,
    table_data: list[dict],
    set_clause: dict,
    where_clause: dict,
) -> tuple[list[dict], list[int]]:
    """Update rows by where clause and return updated IDs."""
    table_meta = _ensure_table_exists(metadata, table_name)
    types = _column_types(table_meta)

    set_column, set_value = next(iter(set_clause.items()))
    where_column, where_value = next(iter(where_clause.items()))

    if set_column not in types or where_column not in types:
        raise ValueError(
            "Некорректное значение: неизвестный столбец. Попробуйте снова."
        )
    if set_column == "ID":
        raise ValueError("Некорректное значение: ID нельзя изменять. Попробуйте снова.")

    try:
        normalized_set_value = _coerce_value(set_value, types[set_column])
        normalized_where_value = _coerce_value(where_value, types[where_column])
    except ValueError as error:
        raise ValueError(
            "Некорректное значение: ошибка типов. Попробуйте снова."
        ) from error

    updated_ids: list[int] = []
    for row in table_data:
        if row.get(where_column) == normalized_where_value:
            row[set_column] = normalized_set_value
            updated_ids.append(row["ID"])

    return table_data, updated_ids


@handle_db_errors
@confirm_action("удаление записи")
def delete(
    metadata: dict,
    table_name: str,
    table_data: list[dict],
    where_clause: dict,
) -> tuple[list[dict], list[int]]:
    """Delete rows by where clause and return deleted IDs."""
    table_meta = _ensure_table_exists(metadata, table_name)
    types = _column_types(table_meta)
    where_column, where_value = next(iter(where_clause.items()))

    if where_column not in types:
        raise ValueError(
            "Некорректное значение: неизвестный столбец. Попробуйте снова."
        )

    try:
        normalized_where_value = _coerce_value(where_value, types[where_column])
    except ValueError as error:
        raise ValueError(
            "Некорректное значение: ошибка типов. Попробуйте снова."
        ) from error

    kept_rows: list[dict] = []
    deleted_ids: list[int] = []
    for row in table_data:
        if row.get(where_column) == normalized_where_value:
            deleted_ids.append(row["ID"])
            continue
        kept_rows.append(row)

    return kept_rows, deleted_ids


@handle_db_errors
def get_table_info(
    metadata: dict, table_name: str, table_data: list[dict]
) -> dict[str, object]:
    """Return human-readable table info."""
    table_meta = _ensure_table_exists(metadata, table_name)
    columns = ", ".join(
        f'{column["name"]}:{column["type"]}' for column in table_meta["columns"]
    )
    return {
        "table": table_name,
        "columns": columns,
        "rows_count": len(table_data),
    }
