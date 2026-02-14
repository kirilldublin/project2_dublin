"""File helpers for metadata and table data."""

import json
from pathlib import Path

from src.primitive_db.constants import DATA_DIR


def load_metadata(filepath: str) -> dict:
    """Load metadata JSON, return empty dict if file not found."""
    try:
        with Path(filepath).open("r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}


def save_metadata(filepath: str, data: dict) -> None:
    """Save metadata JSON to file."""
    with Path(filepath).open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def load_table_data(table_name: str) -> list[dict]:
    """Load table rows from data/<table>.json."""
    filepath = DATA_DIR / f"{table_name}.json"
    try:
        with filepath.open("r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        return []


def save_table_data(table_name: str, data: list[dict]) -> None:
    """Save table rows to data/<table>.json."""
    DATA_DIR.mkdir(exist_ok=True)
    filepath = DATA_DIR / f"{table_name}.json"
    with filepath.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
