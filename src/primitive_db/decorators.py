"""Decorators and closure helpers for DB operations."""

import time
from functools import wraps


def handle_db_errors(func):
    """Catch and print expected DB errors in one place."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print(
                "Ошибка: Файл данных не найден. Возможно, база данных не "
                "инициализирована."
            )
            return None
        except KeyError as error:
            print(f"Ошибка: Таблица или столбец {error} не найден.")
            return None
        except ValueError as error:
            print(f"Ошибка валидации: {error}")
            return None
        except Exception as error:
            print(f"Произошла непредвиденная ошибка: {error}")
            return None

    return wrapper


def confirm_action(action_name):
    """Ask user confirmation before dangerous operation."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            answer = input(
                f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            ).strip().lower()
            if answer != "y":
                print("Операция отменена.")
                return None
            return func(*args, **kwargs)

        return wrapper

    return decorator


def log_time(func):
    """Log execution time in seconds."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = time.monotonic() - start
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.")
        return result

    return wrapper


def create_cacher():
    """Create closure-based key/value cache."""
    cache: dict = {}

    def cache_result(key, value_func):
        if key in cache:
            return cache[key]

        value = value_func()
        cache[key] = value
        return value

    return cache_result
