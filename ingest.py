import asyncio
import csv
import functools
import logging
import os
import sys
from asyncio import Queue
from dataclasses import dataclass
from datetime import timedelta
from time import perf_counter_ns
from typing import Callable, Union, Awaitable, Optional, List, Tuple

import momento
from hdrh.histogram import HdrHistogram
from momento import config as momento_config
from momento.auth import CredentialProvider
from momento.responses import CacheDictionarySetFields

import datagen

logger = logging.getLogger("momento-csv-sandbox")


def histogram_summary(histogram: HdrHistogram) -> str:
    return f"""
    count: {histogram.total_count}
      min: {histogram.min_value}
      p50: {histogram.get_value_at_percentile(50)}
      p90: {histogram.get_value_at_percentile(90)}
      p99: {histogram.get_value_at_percentile(99)}
    p99.9: {histogram.get_value_at_percentile(99.9)}
      max: {histogram.max_value}
"""


def get_elapsed_millis(start_time: float) -> int:
    end_time = perf_counter_ns()
    result = round((end_time - start_time) / 1e6)
    return result


def verify_env_var(env_var_name):
    result = os.getenv(env_var_name)
    if result is None:
        raise Exception(f"Missing required env var {env_var_name}")
    return result


def _convert_reduce_fn(acc: dict[str, str], item: Tuple[str, Union[str, float]]) -> dict[str, str]:
    k, v = item
    if isinstance(v, str):
        val = v
    elif isinstance(v, float):
        val = str(v)
    else:
        raise Exception(f"Unsupported value type for dict conversion: {v}")
    acc[k] = val
    return acc


def convert_dict(data: dict[str, Union[str, float]]) -> dict[str, str]:
    return functools.reduce(_convert_reduce_fn, data.items(), dict())


@dataclass
class WorkItem:
    key: str
    item: dict[str, Union[str, float]]


def launch_workers(
        num_workers: int,
        hist: HdrHistogram,
        total_processed_item_count: int,
        work_queue: Queue[Optional[WorkItem]],
        ingest_fn: Callable[[str, dict[str, Union[str, float]]], Awaitable[None]],
) -> List[Awaitable[int]]:
    logger.info("Creating workers")

    async def do_work(worker_num: int) -> int:
        nonlocal total_processed_item_count
        this_worker_processed_items_count = 0
        while True:
            item = await work_queue.get()
            # logger.info(f"Worker {worker_num} got an item from the queue")
            work_queue.task_done()
            if item is not None:
                write_start_time = perf_counter_ns()
                await ingest_fn(item.key, item.item)
                this_worker_processed_items_count += 1
                total_processed_item_count += 1
                hist.record_value(get_elapsed_millis(write_start_time))
                if total_processed_item_count % 10_000 == 0:
                    logger.info(f"Ingested {total_processed_item_count} items")
                    logger.info(f"\n{histogram_summary(hist)}\n\n")
            else:
                return this_worker_processed_items_count

    return list(map(lambda i: asyncio.create_task(do_work(i)), range(num_workers)))


async def momento_ingest(momento_auth_token, run_id: str, csv_path: str, num_workers: int) -> None:
    logger.info("momento ingest")
    m = momento.SimpleCacheClientAsync(
        momento_config.Laptop.latest(),
        CredentialProvider.from_string(momento_auth_token),
        default_ttl=timedelta(seconds=60 * 30),
    )

    async def ingest_fn(key: str, item: dict[str, Union[str, float]]) -> None:
        set_response = await m.dictionary_set_fields(
            "default-cache", key, convert_dict(item)
        )
        if not isinstance(set_response, CacheDictionarySetFields.Success):
            raise Exception(f"Unable to store dictionary: {set_response}")

    hist = HdrHistogram(
        lowest_trackable_value=1,
        highest_trackable_value=10000 * 60,
        significant_figures=1,
    )
    ingest_start_time = perf_counter_ns()
    with open(csv_path) as csvfile:
        reader = csv.reader(csvfile)
        fields = datagen.fields()

        queue_size = num_workers * 2
        work_queue: Queue[Optional[WorkItem]] = Queue(maxsize=queue_size)

        logger.info(f"Launching {num_workers} workers")
        processed_item_count = 0
        workers = launch_workers(
            num_workers, hist, processed_item_count, work_queue, ingest_fn
        )

        logger.info(f"Enqueuing work items from CSV")
        row_count = 0
        for row in reader:
            row_count += 1
            key = f"run_{run_id}_row{row_count}"
            item = dict(zip(fields, row))
            await work_queue.put(WorkItem(key, item))

        logger.info(f"Work items enqueued")

        # Add a None for each worker to indicate there is no more work to do
        for worker_num in range(num_workers):
            await work_queue.put(None)

        logger.info("Waiting for workers to complete")
        results = await asyncio.gather(*workers)
        logger.info(f"Worker results: {results}")

    logger.info(f"Ingest completed in {get_elapsed_millis(ingest_start_time)} ms")


async def main() -> None:
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.DEBUG,
    )

    if len(sys.argv) < 3:
        logger.error(f"""
Missing required CLI argument(s)

Usage:

{sys.argv[0]} <run_id> <csv_path>

Example:

{sys.argv[0]} 1 ./data/data_100k.csv
        """)
        sys.exit(1)
    run_id = sys.argv[1]
    csv_path = sys.argv[2]

    if not os.path.exists(csv_path):
        logger.error(f"Specified csv path does not exist: {csv_path}")
        sys.exit(1)
    momento_auth_token = verify_env_var("MOMENTO_AUTH_TOKEN")

    logger.info("Launching momento ingest")
    mi = momento_ingest(momento_auth_token, run_id, csv_path, 100)
    logger.info("Awaiting completion of momento ingest")
    await mi


if __name__ == "__main__":
    asyncio.run(main())
