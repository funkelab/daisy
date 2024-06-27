import daisy
import logging
import time
from funlib.persistence.arrays import open_ds, Array
from skimage import filters
import sys
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def smooth_in_block(block: daisy.Block, config: dict):
    sigma = config["sigma"]
    
    raw_ds = open_ds(config["input_zarr"], config["input_group"], "r",)
    data = raw_ds.to_ndarray(block.read_roi)
    smoothed = filters.gaussian(data, sigma=sigma, channel_axis=0)
    
    output_ds = open_ds(config["output_zarr"], config["output_group"], 'a')
    
    smoothed = Array(smoothed, roi=block.read_roi, voxel_size=(1, 1))
    output_ds[block.write_roi] = smoothed.to_ndarray(block.write_roi)


if __name__ == "__main__":
    config_path = sys.argv[1]
    with open(config_path) as f:
        config = json.load(f)

    client = daisy.Client()
    print("Client:", client)

    while True:
        logger.info("getting block")
        with client.acquire_block() as block:

            if block is None:
                break

            logger.info(f"got block {block}")
            smooth_in_block(block, config)

            logger.info(f"releasing block: {block}")
