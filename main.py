from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Union

import bson


def aggregate_data(
    dt_from: str,
    dt_upto: str,
    group_type: str,
    file_path: str = "db/sample_collection.bson",
) -> Dict[str, List[Union[int, str]]]:
    """
    Агрегирует данные о платежах из BSON файла в заданном диапазоне дат
    и группирует их по указанному типу.

    Args:
        dt_from (str): Начальная дата в формате ISO (YYYY-MM-DDTHH:MM:SS).
        dt_upto (str): Конечная дата в формате ISO (YYYY-MM-DDTHH:MM:SS).
        group_type (str): Тип группировки ('hour', 'day' или 'month').
        file_path (str): Путь к BSON файлу, содержащему данные о платежах.

    Returns:
        Dict[str, List]: Словарь, содержащий агрегированные данные с метками
        и наборами данных.
    """
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)

    with open(file_path, "rb") as file:
        data = bson.decode_all(file.read())

    def get_group_key_and_next(
        date: datetime, group_type: str
    ) -> Tuple[str, datetime]:
        """
        Генерирует ключ группы на основе даты и типа группировки
        и вычисляет следующую дату для группы.

        Args:
            date (datetime): Текущая дата.
            group_type (str): Тип группировки ('hour', 'day' или 'month').

        Returns:
            Tuple[str, datetime]: Кортеж, содержащий ключ группы
            и следующую дату.
        """
        if group_type == "hour":
            return date.strftime("%Y-%m-%dT%H:00:00"), date + timedelta(
                hours=1
            )
        elif group_type == "day":
            return date.strftime("%Y-%m-%dT00:00:00"), date + timedelta(days=1)
        elif group_type == "month":
            next_month = (date.month % 12) + 1
            next_year = date.year + (date.month // 12)
            return date.strftime("%Y-%m-01T00:00:00"), datetime(
                next_year, next_month, 1
            )
        else:
            raise ValueError(f"Unsupported group_type: {group_type}")

    grouped_data = defaultdict(int)

    # Инициализация групп нулевыми значениями
    current_date = dt_from
    while current_date <= dt_upto:
        group_key, current_date = get_group_key_and_next(
            current_date, group_type
        )
        grouped_data[group_key] = 0

    # Агрегация значений платежей в группы
    for payment in data:
        payment_date = payment["dt"]
        if dt_from <= payment_date <= dt_upto:
            group_key, _ = get_group_key_and_next(payment_date, group_type)
            grouped_data[group_key] += payment["value"]

    sorted_keys = sorted(grouped_data.keys())

    result = {
        "dataset": [grouped_data[key] for key in sorted_keys],
        "labels": sorted_keys,
    }

    return result
