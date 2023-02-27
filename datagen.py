import csv
import os
import random
import uuid
from typing import Union, List, Iterable


def random_str() -> str:
    return str(uuid.uuid4())


def random_float() -> float:
    return random.uniform(-50_000.0, 50_000.0)


def fields() -> List[str]:
    return [
        "extraction_time_utc",
        "extraction_time_utc_ymd",
        "sold_time_utc",
        "sold_time_utc_ymd",
        "region_id",
        "red_version",
        "red_url",
        "property_id",
        "listing_id",
        "mls_id",
        "sq_footage",
        "lot_size",
        "bedrooms",
        "bathrooms",
        "lat",
        "lng",
        "sale_price",
    ]


def generate_item() -> dict[str, Union[str, float]]:
    return {
        "extraction_time_utc": random_str(),
        "extraction_time_utc_ymd": random_str(),
        "sold_time_utc": random_str(),
        "sold_time_utc_ymd": random_str(),
        "region_id": random_str(),
        "red_version": random_str(),
        "red_url": random_str(),
        "property_id": random_str(),
        "listing_id": random_str(),
        "mls_id": random_str(),
        "sq_footage": random_float(),
        "lot_size": random_float(),
        "bedrooms": random_float(),
        "bathrooms": random_float(),
        "lat": random_float(),
        "lng": random_float(),
        "sale_price": random_float(),
    }


def generate_item_csv_data() -> Iterable[Union[str, float]]:
    item = generate_item()
    return map(lambda k: item[k], fields())


def generate_items_to_csv(csv_path: str, num_items: int) -> None:
    with open(csv_path, 'w') as csvfile:
        w = csv.writer(csvfile)
        for i in range(num_items):
            w.writerow(generate_item_csv_data())


def main():
    os.mkdir('data')
    generate_items_to_csv("./data/data_100k.csv", 100_000)


if __name__ == "__main__":
    main()
