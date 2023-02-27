import asyncio
import logging
import os
import random
import sys
from datetime import timedelta
from math import ceil

import momento
from momento import config as momento_config
from momento.auth import CredentialProvider
from momento.responses import CacheDictionaryFetch

logger = logging.getLogger("momento-csv-sandbox")


def verify_env_var(env_var_name):
    result = os.getenv(env_var_name)
    if result is None:
        raise Exception(f"Missing required env var {env_var_name}")
    return result


async def momento_verify(momento_auth_token: str, num_runs: int, num_rows: int):
    percent_to_verify = 0.5
    num_verifications: int = ceil((percent_to_verify / 100.0) * (num_runs * num_rows))

    m = momento.SimpleCacheClientAsync(
        momento_config.Laptop.latest(),
        CredentialProvider.from_string(momento_auth_token),
        default_ttl=timedelta(seconds=60 * 30),
    )

    logger.info(f"Verifying {num_verifications} of {num_runs * num_rows} values")
    for i in range(num_verifications):
        run_id = random.randrange(num_runs) + 1
        row_id = random.randrange(num_rows) + 1
        key = f"run_{run_id}_row{row_id}"
        fetch_result = await m.dictionary_fetch('default-cache', key)
        if isinstance(fetch_result, CacheDictionaryFetch.Hit):
            logger.info(f"Success!  fetched {key}: {fetch_result.value_dictionary_string_string}")
        else:
            raise Exception(f"Something went wrong when trying to fetch {key}: {fetch_result}")
    logger.info(f"Verified {num_verifications} values")


async def main() -> None:
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.DEBUG,
    )

    if len(sys.argv) < 3:
        logger.error(f"""
Missing required CLI argument(s)

Usage:

{sys.argv[0]} <num runs> <num rows> 

Example:

{sys.argv[0]} 8 100000
        """)
        sys.exit(1)

    num_runs = int(sys.argv[1])
    num_rows = int(sys.argv[2])

    momento_auth_token = verify_env_var("MOMENTO_AUTH_TOKEN")

    logger.info("Launching momento verify")
    mv = momento_verify(momento_auth_token, num_runs, num_rows)
    logger.info("Awaiting completion of momento verify")
    await mv


if __name__ == "__main__":
    asyncio.run(main())
