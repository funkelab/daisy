import argparse
import sys
import copy

import daisy
from gaussian_smoothing2 import GaussianSmoothingTask

if __name__ == "__main__":

    ap = argparse.ArgumentParser()
    ap.add_argument("in_file", type=str, help="The input container")
    ap.add_argument("in_ds_name", type=str, help="The name of the dataset")
    ap.add_argument(
        "--out_file",
        type=str,
        default=None,
        help="The output container, defaults to be the same as in_file",
    )
    ap.add_argument(
        "--sigma", "-s", type=float, help="Sigma to use for gaussian filter", default=2
    )
    ap.add_argument(
        "--block_read_size",
        "-r",
        nargs="+",
        help="Size of block read region",
        default=[20, 200, 200],
    )
    ap.add_argument(
        "--block_write_size",
        "-w",
        nargs="+",
        help="Size of block write region",
        default=[18, 180, 180],
    )

    config = GaussianSmoothingTask.parse_args(ap)

    config1 = copy.deepcopy(config)
    config1["out_ds_name"] = "volumes/raw_smoothed"
    daisy_task1 = GaussianSmoothingTask(config1, task_id="Gaussian1").prepare_task()

    # here we reuse parameters but set the output dataset of the previous
    # task as input
    config2 = copy.deepcopy(config)
    config2["in_ds_name"] = "volumes/raw_smoothed"
    config2["out_ds_name"] = "volumes/raw_smoothed_smoothed"
    daisy_task2 = GaussianSmoothingTask(config2, task_id="Gaussian2").prepare_task(
        upstream_tasks=[daisy_task1]
    )

    done = daisy.run_blockwise([daisy_task1, daisy_task2])

    if done:
        print("Ran all blocks successfully!")
    else:
        print("Did not run all blocks successfully...")
