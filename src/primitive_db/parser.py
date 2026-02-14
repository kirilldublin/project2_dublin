"""Utilities for parsing CLI command fragments."""


def parse_scalar(value_text: str) -> object:
    """Parse scalar value into str/int/bool."""
    value_text = value_text.strip()
    if value_text.startswith('"') and value_text.endswith('"') and len(value_text) >= 2:
        return value_text[1:-1]

    lowered = value_text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    try:
        return int(value_text)
    except ValueError as error:
        raise ValueError(
            f"Некорректное значение: {value_text}. Попробуйте снова."
        ) from error


def split_csv_values(values_text: str) -> list[str]:
    """Split CSV-like values preserving commas inside quotes."""
    values: list[str] = []
    current: list[str] = []
    in_quotes = False

    for char in values_text:
        if char == '"':
            in_quotes = not in_quotes
            current.append(char)
            continue
        if char == "," and not in_quotes:
            values.append("".join(current).strip())
            current = []
            continue
        current.append(char)

    if in_quotes:
        raise ValueError("Некорректное значение: незакрытая кавычка. Попробуйте снова.")

    if current:
        values.append("".join(current).strip())

    if not values or any(not value for value in values):
        raise ValueError("Некорректное значение: пустое значение. Попробуйте снова.")
    return values


def parse_condition(tokens: list[str]) -> dict:
    """Parse `<column> = <value>` tokens to dictionary."""
    if len(tokens) < 3 or "=" not in tokens:
        raise ValueError("Некорректное значение: условие where/set. Попробуйте снова.")

    eq_index = tokens.index("=")
    column = "".join(tokens[:eq_index]).strip()
    value_text = " ".join(tokens[eq_index + 1 :]).strip()

    if not column or not value_text:
        raise ValueError("Некорректное значение: условие where/set. Попробуйте снова.")

    return {column: parse_scalar(value_text)}
