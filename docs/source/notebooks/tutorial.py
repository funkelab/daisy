# %% [markdown]
# # Daisy Tutorial

#  %% [markdown]
# ## Needed Libraries for this Tutorial
# Some recurring libraries that we will use for all of our examples are:
#
# `funlib.geometry`: This is a helper library that defines two classes `Coordinate` and `Roi`.
# - `Coordinate` is basically just an tuple of `n` integers with element wise operations defined on it, defining a point in n-D space.
# - `Roi` defines a bounding box in n-D space, parameterized by two `Coordinates`: `offset` and `shape`
# 
# `funlib.persistence`: A helper library that provides a convenient storage interface to `zarr` and `sql` dbs for array and graph data respectively.
# - Provides convenience functions such as `prepare_ds` and `open_ds` to create and open arrays
# - Allows automatic parsing of metadata including voxel size and offset for various arrays, allowing us to index using `Roi`and `Coordinate` objects in
#   world units instead of voxel units.
#
# ```bash
# pip install funlib.geometry
# pip install funlib.persistence
# ```

# %% [markdown]
# ## Introduction and overview
#
# In this tutorial we will cover a basic Daisy `Task` and build up to array processing
#
# `daisy` has a two classes that you must be familiar with to get the most out of this library:
#
# 1. **daisy.Task**: All code you would like to distribute into blocks must be wrapped into a `Task` to be executed by daisy.
#
# 2. **daisy.Client**: If you would like to start workers on a separate computer, you can do that using the `Client` as long
#    you can support a tcp connection between the computer running the worker and the process executing the `Task`. This is
#    usually supported on most slurm/lsf clusters.
#

# %% [markdown]
# ## Environment setup
# If you have not already done so, I highly recommend you create an environment first.
# You can do this with `uv` via:
#
# ```bash
# uv init --python 3.12
# ```
# Activating the environment is as simple as `source .venv/bin/activate`
#
# Then, you can:
# 1. add daisy directly to your dependencies with:
#     ```bash
#     uv add daisy
#     ```
# 2. install daisy using pip by name or via GitHub:
#     ```bash
#     pip install daisy
#     ```
#     ```bash
#     pip install git+https://github.com/funkelab/daisy.git
#     ```
#

# %%
import multiprocessing

multiprocessing.set_start_method("fork", force=True)

import daisy
from funlib.geometry import Coordinate, Roi

# %% [markdown]
# ### The simplest possible task

# %%
import time

# Create a super simple task
dummy_task = daisy.Task(
    "Dummy",  # We give the task a name
    total_roi=Roi((0,), (100,)),  # a 1-D bounding box [0,100)
    read_roi=Roi((0,), (1,)),  # We read in blocks of size [0,1)
    write_roi=Roi((0,), (1,)),  # We write in blocks of size [0, 1)
    process_function=lambda block: time.sleep(
        0.05
    ),  # Our process function takes the block and simply waits
    num_workers=5,
)

# execute the task without any multiprocessing
daisy.run_blockwise([dummy_task], multiprocessing=False)
daisy.run_blockwise([dummy_task], multiprocessing=True)
# %% [markdown]
# Since there are 100 blocks to process, and we always sleep for 0.05 seconds, it takes 5 seconds
# to run all the blocks without multiprocessing, but only 1 second with `multiprocessing=True`.
# When you run a task with `daisy`, you get a progress bar along with a summary
# of the execution after you have finished processing. Here no errors were thrown so we completed all 100 blocks.
#
# `daisy` also programatically returns a dictionary containing a summary of the execution for all given
# tasks so that you can decide how to procede based on whether any blocks failed.
#
# Next lets look see what we can do to modify this task

# %% [markdown]
# ### Failing blocks
# %%

import random


def random_fail(block: daisy.Block):
    if random.random() < 0.2:
        block.status = daisy.BlockStatus.FAILED
    return block


# Create a super simple task
failing_task = daisy.Task(
    "Failures",  # We give the task a name
    total_roi=Roi((0,), (100,)),  # a 1-D bounding box [0,100)
    read_roi=Roi((0,), (1,)),  # We read in blocks of size [0,1)
    write_roi=Roi((0,), (1,)),  # We write in blocks of size [0, 1)
    process_function=random_fail,  # Our process function takes the block and simply waits
    max_retries=0,  # We do not retry failed blocks
)

# execute the task without any multiprocessing
daisy.run_blockwise([failing_task], multiprocessing=False)

# %% [markdown]

# In this case, we have a 20% chance of failing each block. As you see from the execution summary,
# about 20% of the blocks failed.
# If your blocks are failing, you may want to rerun the task and only process failed blocks. This
# is where we use the `check_function` argument in the `daisy.Task`.

# %%
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory() as tmpdir:

    def random_fail(block: daisy.Block):
        if random.random() < 0.2:
            block.status = daisy.BlockStatus.FAILED
        else:
            Path(tmpdir, f"{block.block_id[1]}").touch()
        return block

    def check_block(block: daisy.Block) -> bool:
        return Path(tmpdir, f"{block.block_id[1]}").exists()

    while True:
        # Create a super simple task
        check_block_task = daisy.Task(
            "Checking-Blocks",  # We give the task a name
            total_roi=Roi((0,), (100,)),  # a 1-D bounding box [0,100)
            read_roi=Roi((0,), (1,)),  # We read in blocks of size [0,1)
            write_roi=Roi((0,), (1,)),  # We write in blocks of size [0, 1)
            process_function=random_fail,  # Our process function takes the block and simply waits
            check_function=check_block,  # Check if a block has been completed or not
            max_retries=0,
        )

        # execute the task without any multiprocessing
        task_state = daisy.run_blockwise([check_block_task], multiprocessing=False)
        if task_state["Checking-Blocks"].failed_count == 0:
            break

# %% [markdown]

# It took a few tries to complete the task since approximately 20% of the remaining blocks fail
# on each attempt. We could also have set `max_retries` to a higher number to allow for retrying failed
# blocks during the same task execution. This is to account for random failures due to network issues or
# other random uncontrollable factors, but will not help if there is a logical bug in your code. In this
# case since the failure is random, retrying does help.

# %% [markdown]
# ### Different read/write sizes and boundary handling


# %%

# fit: "valid", "overhang", "shrink"

# Create a super simple task
overhang_task = daisy.Task(
    "overhang",  # We give the task a name
    total_roi=Roi((0,), (100,)),  # a 1-D bounding box [0,100)
    read_roi=Roi((0,), (9,)),  # We read in blocks of size [0,1)
    write_roi=Roi((2,), (5,)),  # We write in blocks of size [0, 1)
    process_function=lambda b: ...,  # Our process function takes the block and simply waits
    fit="overhang",
)
valid_task = daisy.Task(
    "valid",  # We give the task a name
    total_roi=Roi((0,), (100,)),  # a 1-D bounding box [0,100)
    read_roi=Roi((0,), (9,)),  # We read in blocks of size [0,1)
    write_roi=Roi((2,), (5,)),  # We write in blocks of size [0, 1)
    process_function=lambda b: ...,  # Our process function takes the block and simply waits
    fit="valid",
)

# %% [markdown]
# because we now have:
#
# total roi: [0, 100)
#
# read roi: [0, 9)
#
# write roi: [2, 7)
#
# There is a "context" of 2 pixels between the read_roi and write roi.
# Removing this context from the total roi gives us a "total write roi" of [2, 98).
# Note that the total number of pixels to tile is 96, which is not evenly divisible
# by the chunk size of 5. So our final block will have write roi of [97, 102) and a
# read roi of [95, 104). Depending on your task, reading, and especially writing outside
# of the given total roi bounds can cause problems.
# in these cases we can handle the final block in a few ways. "valid": ignore the
# final block. "overhang": process the final block. "shrink": shrink the read/write
# rois until the read roi is fully contained in the total roi. In this case the "shrink"
# strategy would shrink the read roi to [95, 100) and the write roi to [97, 98).
#
# Now when we executing the overhang task it will have one more block processed than the
# valid task

# %%
daisy.run_blockwise([overhang_task], multiprocessing=False)
daisy.run_blockwise([valid_task], multiprocessing=False)
# %% [markdown]
# ### Data Processing
#
# Now lets prepare some `zarr` arrays. `zarr` is particularly convenient
# for blockwise processing since it saves array data to disk in chunks. If
# you setup your zarr correctly, you can have your write rois always overlap
# with exactly one chunk allowing for perfect parallelization of blocks without
# the need of any file locks and guaranteeing no side effects from race conditions.

# Here we use `funlib.persistence` to generate compatible `zarr` arrays

# %%

from funlib.persistence import prepare_ds, Array
import numpy as np
from scipy.ndimage import gaussian_filter

# lets define our total roi. Normally you have a dataset which dictates
# some of your shapes, but for now lets assume we have an image of intensities
# of shape (200,200). And assuming a voxel size of (4,4) nanometers

# we will process blocks of size (10, 10)
write_size = Coordinate(10, 10)

in_array = prepare_ds(
    "test_data.zarr/in",
    shape=(200, 200),
    offset=(0, 0),
    voxel_size=(4, 4),
    axis_names=["y", "x"],
    units=["nm", "nm"],
    dtype="uint8",
    chunk_shape=write_size,
)

out_array = prepare_ds(
    "test_data.zarr/out",
    shape=(200, 200),
    offset=(0, 0),
    voxel_size=(4, 4),
    axis_names=["y", "x"],
    units=["nm", "nm"],
    dtype="uint8",
    chunk_shape=write_size,
)

write_roi = Roi((0, 0), write_size) * in_array.voxel_size
context = Coordinate(5, 5) * in_array.voxel_size
read_roi = write_roi.grow(context, context)

in_array[:] = np.random.randint(0, 255, size=in_array.shape, dtype=np.uint8)


def blur_block(b: daisy.Block):
    # instead of directly indexing, we use `to_ndarray` to handle out of
    # bounds reads. This fetches any contained data, and pads with zeros
    # for any out of bounds reads
    in_data = in_array.to_ndarray(b.read_roi, fill_value=0)
    blurred_data = Array(
        gaussian_filter(in_data, sigma=3),
        offset=b.read_roi.offset,
        voxel_size=in_array.voxel_size,
        axis_names=in_array.axis_names,
        units=in_array.units,
    )
    out_array[b.write_roi] = blurred_data[b.write_roi]


blur_task = daisy.Task(
    "blur",
    total_roi=in_array.roi.grow(context, context),
    read_roi=read_roi,
    write_roi=write_roi,
    process_function=blur_block,
)

daisy.run_blockwise([blur_task], multiprocessing=False)

# %%
import matplotlib.pyplot as plt

fix, ax = plt.subplots(1, 2)
ax[0].imshow(in_array[:])
ax[1].imshow(out_array[:])
plt.show()


# %% [markdown]
# ### Task Chaining
#
# It is also possible to chain multiple tasks together in such a way that
# they either run in parallel or in sequence. If running in parallel, we
# simply start the workers for both tasks at once. If running in sequence,
# blocks will only be realeased once their dependencies in previous tasks
# have been completed.

# %%
total_roi = Roi((0, 0), (1250, 1250))
read_roi_shape = (400, 400)
write_roi_shape = (250, 250)
context = Coordinate(75, 75)

task_a_to_b = daisy.Task(
    "A_to_B",
    total_roi=total_roi.grow(
        context,
        context,
    ),
    read_roi=Roi((0, 0), read_roi_shape),
    write_roi=Roi(context, write_roi_shape),
    process_function=lambda b: ...,
    read_write_conflict=False,
)
task_b_to_c = daisy.Task(
    "B_to_C",
    total_roi=total_roi.grow(
        context,
        context,
    ),
    read_roi=Roi((0, 0), read_roi_shape),
    write_roi=Roi(context, write_roi_shape),
    process_function=lambda b: ...,
    read_write_conflict=False,
    upstream_tasks=[task_a_to_b],
)

daisy.run_blockwise([task_a_to_b, task_b_to_c], multiprocessing=False)

# %% [markdown]
#
# Here is an mp4 visualization of the above task
#
# ![task chaining](../_static/task_chaining.gif)
