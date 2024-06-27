import daisy
import logging
import time
from funlib.persistence.arrays import open_ds, Array
from skimage import filters
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def smooth_in_block(block: daisy.Block):
    sigma = 5.0
    
    raw_ds = open_ds('sample_data.zarr', 'raw', "r",)
    data = raw_ds.to_ndarray(block.read_roi, fill_value=0)
    smoothed = filters.gaussian(data, sigma=sigma, channel_axis=0)
    
    output_ds = open_ds('sample_data.zarr', 'smoothed_subprocess', 'a')
    
    smoothed = Array(smoothed, roi=block.read_roi, voxel_size=(1, 1))
    output_ds[block.write_roi] = smoothed.to_ndarray(block.write_roi)


if __name__ == "__main__":
    client = daisy.Client()
    print("Client:", client)

    # simlate long setup time (e.g. loading a model)
    time.sleep(50)

    while True:
        logger.info("getting block")
        with client.acquire_block() as block:

            if block is None:
                break

            logger.info(f"got block {block}")
            smooth_in_block(block)

            logger.info(f"releasing block: {block}")
