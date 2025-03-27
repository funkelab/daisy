import daisy
import time
from funlib.persistence.arrays import open_ds, Array
from skimage import filters
import sys
import json


# This function is the same as the local function, but we can pass as many different arguments as we want, and we don't need to import inside it
def smooth_in_block(block: daisy.Block, config: dict):
    sigma = config["sigma"]
    raw_ds = open_ds(
        f"{config['input_zarr']}/{config['input_group']}",
        "r",
    )
    data = raw_ds.to_ndarray(block.read_roi, fill_value=0)
    smoothed = filters.gaussian(data, sigma=sigma, channel_axis=0)
    output_ds = open_ds(f"{config['output_zarr']}/{config['output_group']}", "a")
    smoothed = Array(smoothed, offset=block.read_roi.offset, voxel_size=(1, 1))
    output_ds[block.write_roi] = smoothed.to_ndarray(block.write_roi)


if __name__ == "__main__":
    # load a config path or other parameters from the sysargs (recommended to use argparse argument parser for anything more complex)
    config_path = sys.argv[1]

    # load the config
    with open(config_path) as f:
        config = json.load(f)

    # simulate long setup time (e.g. loading a model)
    time.sleep(20)

    # set up the daisy client (this is done by daisy automatically in the local example)
    # it depends on environment variables to determine configuration
    client = daisy.Client()

    while True:
        # ask for a block from the scheduler
        with client.acquire_block() as block:
            # The scheduler will return None when there are no more blocks left
            if block is None:
                break

            # process your block!
            # Note: you can now define whatever function signature you want, rather than being limited to one block argument
            smooth_in_block(block, config)
