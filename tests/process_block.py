import daisy

import time
import random


client = daisy.Client()

while True:
    with client.acquire_block() as block:
        if block is None:
            break
        else:
            time.sleep(random.random())
