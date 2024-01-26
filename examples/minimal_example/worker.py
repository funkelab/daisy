import daisy
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_worker():
    client = daisy.Client()

    while True:
        logger.info("getting block")
        with client.acquire_block() as block:
            if block is None:
                break

            logger.info(f"got block {block}")

            # pretend to do some work
            time.sleep(0.5)

            logger.info(f"releasing block: {block}")


if __name__ == "__main__":
    test_worker()
