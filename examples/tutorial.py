# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Daisy Tutorial
# Daisy is a library for processing large volumes in parallel. While other libraries (e.g. dask) can perform similar tasks, daisy is optimized for extremely large volumes, out-of-memory operations, and operations where neighboring blocks should not be run at the same time.
#
# In this tutorial, we will cover:
# - daisy terminology and concepts
# - running daisy locally with multiprocessing
# - running daisy with independent worker processes (e.g., on a compute cluster)

# %%
# %pip install scikit-image
# %pip install zarr
# %pip install matplotlib
# %pip install funlib.persistence

# %% [markdown]
# # Building Blocks

# %% [markdown]
# Daisy is designed for processing volumetric data. Therefore, it has specific ways to describe locations in a volume. We will demonstrate the common terms and utilities using this image of astronaut Eileen Collins.

# %%
from skimage import data
import numpy as np
import matplotlib.pyplot as plt

raw_data = np.flip(data.astronaut(), 0)
axes_image = plt.imshow(raw_data, zorder=1, origin="lower")

# %% [markdown]
# ## Coordinate
# - A daisy Coordinate is essentially a tuple with one value per spatial dimension. In our case, the Coordinates are two-dimensional.
# - Daisy coordinates can represent points in the volume, or distances in the volume.
# - Daisy mostly passes around abstract placeholders of the data. Therefore, a Coordinate does not contain data, it is simply a pointer to a location in a volume.
# - The main difference between a Coordinate and a tuple is that operations (e.g. addition) between Coordinates are performed pointwise, to match the spatial definition.
#
# `daisy.Coordinate` is an alias of [`funlib.geometry.Coordinate`](https://github.com/funkelab/funlib.geometry/blob/main/funlib/geometry/coordinate.py)
#
# Here are some example Coordinates, and a visualization of their location on the Eileen Collins volume.

# %%
import daisy

p1 = daisy.Coordinate(10,10)  # white
p2 = daisy.Coordinate(452, 250)  # yellow
p3 = p2 - p1  # orange


# %%
def display_coord(axes, coord, color):
    x = coord[1]
    y = coord[0]
    axes.scatter(x, y, color=color, edgecolors="black", zorder=3)
figure = axes_image.figure
axes = figure.axes[0]
for point, color in zip([p1, p2, p3], [ "white", "yellow", "orange"]):
    display_coord(axes, point, color=color)
figure

# %% [markdown]
# ## Roi
# A Roi (Region of interest) is a bounding box in a volume. It is defined by two Coordinates:
# - offset: the starting corner of the bounding box relative to the origin
# - shape: The extent of the bounding box in each dimension
#
# `daisy.Roi` is an alias of [`funlib.geometry.Roi`](https://github.com/funkelab/funlib.geometry/blob/main/funlib/geometry/roi.py). Rois have operations like grow, shift, and intersect, that represent spatial manipulations
#
# Here are some example Rois and their visualization in our Eileen Collins volume. Remember, the Roi does not contain the data! It is simply a bounding box.

# %%
head = daisy.Roi(offset=(320, 150), shape=(180, 150)) # purple
# to get the Roi of the nose, we will shrink the head Roi by a certain amount in x and y on each side of the Roi
# the first argument will move the corner closest to the origin, and the second argument will move the corner furthest from the origin
nose = head.grow(daisy.Coordinate(-60, -55), daisy.Coordinate(-90, -55))  # orange 

body = daisy.Roi(offset=p1, shape=(330, 350)) # grey
# to get the neck, we will intersect the body and the head rois
neck = head.intersect(body)  # blue

# %%
from matplotlib.patches import Rectangle

def display_roi(axes, roi, color):
    xy = (roi.offset[1], roi.offset[0])
    width = roi.shape[1]
    height = roi.shape[0]
    rect = Rectangle(xy, width, height, alpha=0.4, color=color, zorder=2)
    axes.add_patch(rect)

def fresh_image():
    plt.close()
    axes_image = plt.imshow(raw_data, zorder=1, origin="lower")
    figure = axes_image.figure
    axes = figure.axes[0]
    return figure

figure = fresh_image()
for roi, color in zip([head, nose, body, neck], ["purple", "orange", "grey", "blue"]):
    display_roi(figure.axes[0], roi, color=color)

# %% [markdown]
# ## Array
# So far we have seen how to specify regions of the data with Rois and Coordinates, which do not contain any data. However, eventually you will need to access the actual data using your Rois! For this, we use [`funlib.persistence.arrays.Array`](https://github.com/funkelab/funlib.persistence/blob/f5310dddb346585a28f3cb44f577f77d4f5da07c/funlib/persistence/arrays/array.py). If you are familiar with dask, this is the daisy equivalent of dask arrays.
#
# The core information about the `funlib.persistence.arrays.Array` class is that you can slice them with Rois, along with normal numpy-like slicing. However, in order to support this type of slicing, we need to also know the Roi of the whole Array. Here we show you how to create an array from our raw data that is held in memory as a numpy array. However, we highly recommend using a zarr backend, and will show this in our simple example next!

# %%
from funlib.persistence.arrays import Array

# daisy Arrays expect the channel dimension first, but our sklearn loaded image has channels last - let's fix that
raw_data_reshaped = raw_data.transpose(2, 0, 1)
print("New data shape:", raw_data_reshaped.shape)

# we need the spatial extend of the data
data_spatial_shape = raw_data_reshaped.shape[1:]
print("Spatial shape:", data_spatial_shape)

# Roi of the whole volume
total_roi = daisy.Roi(offset=(0,0), shape=data_spatial_shape)
print("Total dataset roi:", total_roi)

raw_array = Array(
    data=raw_data_reshaped,
    roi=total_roi,
    voxel_size=daisy.Coordinate(1,1),
)

# %% [markdown]
# Now we can demonstrate how to access data from an Array using a Roi

# %%
# slicing an Array with a Roi gives you another Array
head_array = raw_array[head]
print("Head array type:", type(head_array))

# the to_ndarray() function gives you the actual numpy array with the data
head_data = head_array.to_ndarray()
plt.close()
plt.imshow(head_data.transpose(1, 2, 0), origin="lower")  # need to transpose channels back to the end for matplotlib to work

# %%
# you can also combine the two steps
body_data = raw_array.to_ndarray(body)
plt.close()
plt.imshow(body_data.transpose(1, 2, 0), origin="lower")

# %% [markdown]
# ## Block
#
# Daisy is a blockwise task scheduler. Therefore, the concept of a block is central to Daisy. To efficiently process large volumes, Daisy splits the whole volume into a set of adjacent blocks that cover the whole image. These blocks are what is passed between the scheduler and the workers.
#
# A Block is simply a (set of) Roi(s), and does not contain data. In practice, it has additional information that is useful to the daisy server and workers to help them perform their task, which we will decribe below:

# %%
# let's define a Block of our Eileen Collins volume
# In practice, you will never need to make a block - the daisy scheduler will do this for you

block_size = daisy.Coordinate(64, 64)
block_origin = daisy.Coordinate(128, 128)
block_roi = daisy.Roi(block_origin, block_size)

block = daisy.Block(
    total_roi = total_roi,
    read_roi = block_roi,
    write_roi = block_roi,
)

# Here are all the attributes of the block
print("Block id:", block.block_id)  # a unique ID for each block given the total roi of the volume
print("Block read roi:", block.read_roi)  # the Roi which represents the location in the volume of the input data to the process
print("Block write roi:", block.write_roi)  # the Roi which represents the location in the volume where the process should write the output data
print("Block status:", block.status)  # The status of the block (e.g. created, in progress, succeeded, failed)

# let's look at the write roi of our block on top of the original figure
figure = fresh_image()
display_roi(figure.axes[0], block.write_roi, color="white")

# %% [markdown]
# You may be wondering why the block has a read roi and a write roi - this will be illustrated next in our simple daisy example!

# %% [markdown]
# # A Simple Example: Local Smoothing
# In this next example, we will use gaussian smoothing to illustrate how to parallize a task on your local machine using daisy.

# %% [markdown]
# ## Dataset Preparation
# As mentioned earlier, we highly recommend using a zarr/n5 backend for your volume. Daisy is designed such that no data is transmitted between the worker and the scheduler, including the output of the processing. That means that each worker is responsible for saving the results in the given block write_roi. With a zarr backend, each worker can write to a specific region of the zarr in parallel, assuming that the chunk size is a multiple of and aligned with the write_roi. The zarr dataset must exist before you start scheudling though - we recommend using [`funlib.persistence.prepare_ds`](https://github.com/funkelab/funlib.persistence/blob/f5310dddb346585a28f3cb44f577f77d4f5da07c/funlib/persistence/arrays/datasets.py#L423) function to prepare the dataset. Then later, you can use [`funlib.persistence.open_ds`](https://github.com/funkelab/funlib.persistence/blob/f5310dddb346585a28f3cb44f577f77d4f5da07c/funlib/persistence/arrays/datasets.py#L328) to open the dataset and it will automatically read the metadata and wrap it into a `funlib.persistence.Array`.

# %%
import zarr
# convert our data to float, because gaussian smoothing in scikit expects float input and output
# recall that we already reshaped it to put channel dimension first, as the funlib.persistence Array expects
raw_data_float = raw_data_reshaped.astype(np.float32)/255.0

# store image in zarr container
f = zarr.open('sample_data.zarr', 'w')
f['raw'] = raw_data_float
f['raw'].attrs['offset'] = daisy.Coordinate((0,0))
f['raw'].attrs['resolution'] = daisy.Coordinate((1,1))  # this attribute holds the voxel size

# %%
from funlib.persistence import prepare_ds
# prepare an output dataset with a chunk size that is a divisor of the block roi

n_channels = 3 # our output will be an RGB image as well
prepare_ds(
    "sample_data.zarr",
    "smoothed",
    total_roi=total_roi,  # if your output has a different total_roi than your input, you would need to change this
    voxel_size=daisy.Coordinate((1,1)),
    dtype=raw_data_float.dtype,
    write_size=block_size,
    num_channels=n_channels,
)
print("Shape of output dataset:", f["smoothed"].shape)
print("Chunk size in output dataset:",f['smoothed'].chunks)


# %% [markdown]
# ## Define our Process Function
# When run locally, daisy process functions must take a block as the only argument. Depending on the multiprocessing spawn function settings on your computer, the function might not inherit the imports and variables of the scope where the scheudler is run, so it is always safer to import and define everything inside the function.
#
# Here is an example for smoothing. Generally, daisy process functions have the following three steps:
# 1. Load the data from disk
# 2. Process the data
# 3. Save the result to disk
#
# Note that for now, the worker has to know where to load the data from and save the result to. Later we will show you ways around the rule that the process function must only take the block as input, to allow you to pass that information in when you start the scheduler.

# %%
def smooth(block: daisy.Block):
    # imports and hyperaparmeters inside scope, to be safe
    from funlib.persistence.arrays import open_ds
    from skimage import filters
    sigma = 5.0

    # open the raw dataset as an Array
    raw_ds = open_ds('sample_data.zarr', 'raw', "r",)
    # Read the data in the block read roi and turn it into a numpy array
    data = raw_ds.to_ndarray(block.read_roi)
    # smooth the data using the gaussian filter from skimage
    smoothed = filters.gaussian(data, sigma=sigma, channel_axis=0)
    # open the output smoothed dataset as an Array
    output_ds = open_ds('sample_data.zarr', 'smoothed', 'a')
    # save the result in the output dataset using the block write roi
    output_ds[block.write_roi] = smoothed


# %%
# Let's test the data on our block that we defined earlier and visualize the result
smooth(block)
plt.imshow(zarr.open('sample_data.zarr', 'r')['smoothed'][:].transpose(1, 2, 0), origin="lower")

# %% [markdown]
# ## Run daisy with local multiprocessing
# We are about ready to run daisy! We need to tell the scheduler the following pieces of information:
# - The process function, which takes a block as an argument
# - The total roi to process (in our case, the whole image)
# - The read roi and write roi of each block (the shape and relative offset are what is important, since they will be shifted as a pair to tile the total_roi)
# - How many workers to spawn
#
# These pieces of information get wrapped into a [`daisy.Task`](https://github.com/funkelab/daisy/blob/master/daisy/task.py), along with a name for the task. Then the `daisy.run_blockwise` function starts the scheduler, which creates all the blocks that tile the total roi, spawns the workers, distributes the blocks to the workers, and reports if the blocks were successfully processed.

# %%
daisy.run_blockwise([
        daisy.Task(
            "Smoothing",  # task name
            process_function=smooth,  # a function that takes a block as argument
            total_roi=total_roi,  # The whole roi of the image
            read_roi=block_roi,  # The roi that the worker should read from
            write_roi=block_roi,  # the roi that the worker should write to
            num_workers=5, 
        )
    ]
)

# %%
plt.imshow(zarr.open('sample_data.zarr', 'r')['smoothed'][:].transpose(1, 2, 0), origin="lower")

# %% [markdown]
# ### Take 2: Add context!
# The task ran successfully, but you'll notice that there are edge artefacts where the blocks border each other. This is because each worker only sees the inside of the block, and it needs more context to smooth seamlessly between blocks. If we increase the size of the read_roi so that each block sees all pixels that contribute meaningfully to the smoothed values in the interior (write_roi) of the block, the edge artefacts should disappear.

# %%
context = 2*sigma  # pixels beyond 2*sigma contribute almost nothing to the output
block_read_roi = block_roi.grow(context, context)
block_write_roi = block_roi
# we also grow the total roi by the context, so that the write_rois are still in the same place when we tile
total_read_roi = total_roi.grow(context, context) 

block = daisy.Block(
    total_roi = total_roi,
    read_roi = block_read_roi,
    write_roi = block_write_roi,
)

# let's look at the new block rois
figure = fresh_image()
display_roi(figure.axes[0], block.read_roi, color="purple")
display_roi(figure.axes[0], block.write_roi, color="white")


# %% [markdown]
# Let's prepare another dataset to store our new and improved smoothing result in. We will be doing this repeatedly through the rest of the tutorial, so we define a helper function to prepare a smoothing result in a given group in the sample_data.zarr.

# %%
def prepare_smoothing_ds(group):
    prepare_ds(
        "sample_data.zarr",
        group,
        total_roi=total_roi,
        voxel_size=daisy.Coordinate((1,1)),
        dtype=raw_data_float.dtype,
        write_size=block_size,
        num_channels=3,
    )
output_group = "smoothed_with_context"
prepare_smoothing_ds(output_group)


# %% [markdown]
# Now we have to adapt our process function to crop the output before saving. It would be nice to be able to pass the output group in as an argument, so we will show you a workaround using `functools.partial` to partially evaluate the function. To use this workaround, your process function must have the block as the last argument.

# %%
def smooth_in_block(output_group: str, block: daisy.Block):
    # imports and hyperaparmeters inside scope, to be safe
    from funlib.persistence.arrays import open_ds, Array
    from skimage import filters
    import time
    sigma = 5.0
    # open the raw dataset as an Array
    raw_ds = open_ds('sample_data.zarr', 'raw', "r",)
    # Read the data in the block read roi and turn it into a numpy array
    data = raw_ds.to_ndarray(block.read_roi, fill_value=0)  # NOTE: this fill value allows you to read outside the total_roi without erroring
    # smooth the data using the gaussian filter from skimage
    smoothed = filters.gaussian(data, sigma=sigma, channel_axis=0)
    # open the output smoothed dataset as an Array
    output_ds = open_ds('sample_data.zarr', output_group, 'a')
    # turn the smoothed result into an Array so we can crop it with a Roi (you can also center crop it by the context manually, but this is easier!)
    smoothed = Array(smoothed, roi=block.read_roi, voxel_size=(1, 1))
    # save the result in the output dataset using the block write roi
    output_ds[block.write_roi] = smoothed.to_ndarray(block.write_roi)


# %% [markdown]
# Now we can re-run daisy. Note these changes from the previous example:
# - using `functools.partial` to partially evaluate our `smooth_in_block` function , turning it into a function that only takes the block as an argument
# - the total_roi is now exapnded to include the context, as is the read_roi

# %%
from functools import partial
daisy.run_blockwise([
        daisy.Task(
            "Smoothing with context",
            process_function=partial(smooth_in_block, output_group),
            total_roi=total_read_roi,
            read_roi=block_read_roi,
            write_roi=block_write_roi,
            num_workers=5,
        )
    ]
)

# %%
plt.imshow(zarr.open('sample_data.zarr', 'r')['smoothed_with_context'][:].transpose(1, 2, 0), origin="lower")

# %% [markdown]
# Success! Notice that there is a fade to black at the border, due to the `fill_value=0` argument used when reading the data from the input Array.
# Smoothing is poorly defined at the border of the volume - if you want different behavior, you can expand the input array to include extended data of your choice at the border, or shrink the total output roi by the context to only include the section of the output that depends on existing data.

# %% [markdown]
# ## Conclusion: Dask and Daisy
#
# Congrats! You have learned the basics of Daisy. In this example, we only parallelized the processing using our local computer's resources, and our "volume" was very small.
#
# If your task is similar to this example, you can use dask to do the same task with many fewer lines of code:

# %%
# %pip install dask

# %%
import dask
import dask.array as da
from skimage import filters


f = zarr.open('sample_data.zarr', 'r')
raw = da.from_array(f['raw'], chunks=(3, 64, 64))
print("Raw dask array:", raw)
sigma = 5.0
context = int(sigma) * 2

def smooth_in_block_dask(x):
    return filters.gaussian(x, sigma=sigma, channel_axis=0)

smoothed = raw.map_overlap(smooth_in_block_dask, depth=(0, context, context))
plt.imshow(smoothed.transpose((1, 2, 0)), origin="lower")

# %% [markdown]
# For some tasks, dask is much simpler and better suited. One key difference between dask and daisy is that in dask, functions are not supposed to have side effects. In daisy, functions are expected to have "side effects" - saving the output, rather than returning it. In general, daisy is designed for...
# - Cases where the output is too big to be kept in memory
# - Cases where you want to be able to pick up where you left off after an error, rather than starting the whole task over (because blocks that finished saved their results to disk)
# - Cases where blocks should be executed in a particular order, so that certain blocks see other blocks outputs (without passing the output through the scheduler)
# - Cases where the worker function needs setup and teardown that takes longer than processing a block (see our next example!)

# %% [markdown]
# # Distributing on the Cluster
# While daisy can run locally, it is designed to shine in a cluster computing environment. The only information passed between scheduler and workers are Blocks, which are extremely lightweight and are communicated through TCP. Therefore, workers can be distributed on the cluster with minimal communication overhead.
#
# Let's re-do our smoothing, but this time run each worker as a completely separate subprocess, as would be needed on a cluster. First, we prepare the output dataset.

# %%
# first, prepare the dataset
prepare_smoothing_ds("smoothed_subprocess")


# %% [markdown]
# Then, we prepare our process function. This time, it has two parts. The first part is the function defined in the cell below, and essentially just calls `subprocess.run` locally or with bsub, as an example compute environment. The second part is the external python script that is actually executed in the `subprocess.run` call.

# %%
# new process function to start the worker subprocess
def start_subprocess_worker(cluster="local"):
    import subprocess
    if cluster == "bsub":
        # this is where you define your cluster arguments specific to your task (gpus, cpus, etc)
        num_cpus_per_worker = 1
        subprocess.run(["bsub", "-I", f"-n {num_cpus_per_worker}", "python", "./tutorial_worker.py", "tutorial_config.json"])
    elif cluster== "local":
        subprocess.run(["python", "./tutorial_worker.py", "tutorial_config.json"])
    else:
        raise ValueError("Only bsub and local currently supported for this tutorial")

# %% [markdown]
# Code from tutorial_worker.py, copied here for convenience (Note: running this cell won't run the code, because it is a markdown cell)
# ``` python
# import daisy
# import logging
# import time
# from funlib.persistence.arrays import open_ds, Array
# from skimage import filters
# import sys
# import json
#
#
# # This function is the same as the local function, but we can pass as many different arguments as we want, and we don't need to import inside it
# def smooth_in_block(block: daisy.Block, config: dict):
#     sigma = config["sigma"]
#     raw_ds = open_ds(config["input_zarr"], config["input_group"], "r",)
#     data = raw_ds.to_ndarray(block.read_roi, fill_value=0)
#     smoothed = filters.gaussian(data, sigma=sigma, channel_axis=0)
#     output_ds = open_ds(config["output_zarr"], config["output_group"], 'a')
#     smoothed = Array(smoothed, roi=block.read_roi, voxel_size=(1, 1))
#     output_ds[block.write_roi] = smoothed.to_ndarray(block.write_roi)
#
#
# if __name__ == "__main__":
#     # load a config path or other parameters from the sysargs (recommended to use argparse argument parser for anything more complex)
#     config_path = sys.argv[1]
#     
#     # load the config
#     with open(config_path) as f:
#         config = json.load(f)
#     
#     # simulate long setup time (e.g. loading a model)
#     time.sleep(20)
#     
#     # set up the daisy client (this is done by daisy automatically in the local example)
#     # it depends on environment variables to determine configuration
#     client = daisy.Client()
#
#     while True:
#         # ask for a block from the scheduler
#         with client.acquire_block() as block:
#
#             # The scheduler will return None when there are no more blocks left
#             if block is None:
#                 break
#
#             # process your block! 
#             # Note: you can now define whatever function signature you want, rather than being limited to one block argument
#             smooth_in_block(block, config)
#
# ```


# %% [markdown]
# The most important thing to notice about the new worker script is the use of the `client.acquire_block()` function. No longer does our process function accept a block as input - instead, it has no arguments, and is expected to specifically request a block. This means that rather than spawning one worker per block, the workers are persistent for the full time the task is running, and can request process and return many blocks.
#
# This is particularly helpful when worker startup is expensive - loading saved network weights can be more expensive than actually predicting for one block, so you definitely would not want to load the model separately for each block. We have simulated this by using time.sleep() in the setup of the worker, so when you run the next cell, it should take 20 seconds to start up and then the blocks should process quickly after that.

# %%
# note: Must be on submit node to run this with bsub argument
tutorial_task = daisy.Task(
    "smoothing_subprocess",
    total_roi=total_read_roi,
    read_roi=read_roi,
    write_roi=block_roi,
    process_function=partial(start_subprocess_worker, "local"),
    num_workers=2,
)

daisy.run_blockwise([tutorial_task])

# %%
plt.imshow(zarr.open('sample_data.zarr', 'r')['smoothed_subprocess'][:].transpose(1, 2, 0), origin="lower")

# %% [markdown]
# # Important Features

# %% [markdown]
# ## Fault tolerance and the pre-check function

# %%
prepare_ds(
    "sample_data.zarr",
    "fault_tolerance",
    total_roi=total_write_roi,
    voxel_size=daisy.Coordinate((1,1)),
    dtype=raw_data_float.dtype,
    write_size=block_size,
    num_channels=n_channels,
)


# %%
# simulate failing 50% of the time
def smooth_in_block_with_failure(block: daisy.Block):
    import random

    if random.random() < 0.5:
        raise ValueError("Simulating random failure")

    from funlib.persistence.arrays import open_ds, Array
    from skimage import filters
    
    sigma = 5.0
    
    raw_ds = open_ds('sample_data.zarr', 'raw', "r",)
    data = raw_ds.to_ndarray(block.read_roi, fill_value=0)
    smoothed = filters.gaussian(data, sigma=sigma, channel_axis=0)
    
    output_ds = open_ds('sample_data.zarr', 'fault_tolerance', 'a')
    
    smoothed = Array(smoothed, roi=block.read_roi, voxel_size=(1, 1))
    output_ds[block.write_roi] = smoothed.to_ndarray(block.write_roi)


# %%
sigma = 5
context = int(sigma) * 2
read_roi = block_roi.grow(context, context)

daisy.run_blockwise([
        daisy.Task(
            "fault tolerance test",
            process_function=smooth_in_block_with_failure,
            total_roi=total_read_roi,
            read_roi=read_roi,
            write_roi=block_roi,
            fit="shrink",
            read_write_conflict=False,
            num_workers=5,
        )
    ]
)

# %%
plt.imshow(zarr.open('sample_data.zarr', 'r')['fault_tolerance'][:].transpose(1, 2, 0), origin="lower")

# %% [markdown]
# Why is so much more than 60% done? Answer: "max retries"

# %%
root = zarr.open("sample_data.zarr", 'a')
del root['fault_tolerance']

# %%
prepare_ds(
    "sample_data.zarr",
    "fault_tolerance",
    total_roi=total_write_roi,
    voxel_size=daisy.Coordinate((1,1)),
    dtype=raw_data_float.dtype,
    write_size=block_size,
    num_channels=n_channels,
)

daisy.run_blockwise([
        daisy.Task(
            "fault tolerance test",
            process_function=smooth_in_block_with_failure,
            total_roi=total_read_roi,
            read_roi=read_roi,
            write_roi=block_roi,
            fit="shrink",
            read_write_conflict=False,
            max_retries=1,
            num_workers=1,
        )
    ]
)

# %%
plt.imshow(zarr.open('sample_data.zarr', 'r')['fault_tolerance'][:].transpose(1, 2, 0), origin="lower")

# %% [markdown]
# If we re-run enough times, eventually all the holes will fill. But, we can do something smarter!

# %%
from functools import partial
def check_complete(output_group, block):
    from funlib.persistence.arrays import open_ds
    import numpy as np
    output_ds = open_ds('sample_data.zarr', output_group, 'r')
    if np.max(output_ds.to_ndarray(block.write_roi)) > 0:
        return True
    else:
        return False
    

daisy.run_blockwise([
        daisy.Task(
            "fault tolerance test",
            process_function=smooth_in_block_with_failure,
            total_roi=total_read_roi,
            read_roi=read_roi,
            write_roi=block_roi,
            fit="shrink",
            read_write_conflict=False,
            max_retries=1,
            check_function=partial(check_complete, "fault_tolerance")
        )
    ]
)

# %%
plt.imshow(zarr.open('sample_data.zarr', 'r')['fault_tolerance'][:].transpose(1, 2, 0), origin="lower")


# %% [markdown]
# Note: your pre-check function has to be faster than your actual function for this to be worth it. We recommend saving the block id as a file in a shared file system or database at the end of the worker function.

# %% [markdown]
# ## Task chaining

# %% [markdown]
# Say we have a function to segment out instances of blue objects in an image, and we want to apply it after smoothing. We can define two tasks and run them sequentially in the scheduler.

# %%
def smooth_in_block(output_group, block: daisy.Block):
    from funlib.persistence.arrays import open_ds, Array
    from skimage import filters
    
    sigma = 5.0
    
    raw_ds = open_ds('sample_data.zarr', 'raw', "r",)
    data = raw_ds.to_ndarray(block.read_roi, fill_value=0)
    smoothed = filters.gaussian(data, sigma=sigma, channel_axis=0)
    
    output_ds = open_ds('sample_data.zarr', output_group, 'a')
    
    smoothed = Array(smoothed, roi=block.read_roi, voxel_size=(1, 1))
    output_ds[block.write_roi] = smoothed.to_ndarray(block.write_roi)


# %%
# %pip install opencv-python

# %%
def segment_blue_objects(input_group, output_group, block):
    import cv2
    from funlib.persistence.arrays import open_ds, Array
    import numpy as np
    import skimage
    
    input_ds = open_ds('sample_data.zarr', input_group, "r",)
    data = input_ds.to_ndarray(block.read_roi)
    
    back_to_skimage = (data.transpose(1,2,0) * 255).astype(np.uint8)
    cv2_image = cv2.cvtColor(skimage.util.img_as_ubyte(back_to_skimage), cv2.COLOR_RGB2BGR)
    hsv_image = cv2.cvtColor(cv2_image, cv2.COLOR_BGR2HSV)
    # Define the color range for detection
    lower_blue = np.array([100,30,0])
    upper_blue = np.array([150,255,255])
    # Threshold the image to get only blue colors
    mask = cv2.inRange(hsv_image, lower_blue, upper_blue)
    mask = mask.astype(np.uint16)
    
    mask = mask // 255
    labels = skimage.measure.label(mask)
    # get a unique ID for each element in the whole volume (avoid repeats between blocks)
    block_id_mask = mask * (block.block_id[1])
    labels = labels + block_id_mask

    output_ds = open_ds('sample_data.zarr', output_group, 'a')
    output_ds[block.write_roi] = labels


# %%

prepare_ds(
    "sample_data.zarr",
    "smoothed_for_seg",
    total_roi=total_write_roi,
    voxel_size=daisy.Coordinate((1,1)),
    dtype=raw_data_float.dtype,
    write_size=block_size,
    num_channels=n_channels,
)

sigma = 5
context = int(sigma) * 2
read_roi = block_roi.grow(context, context)

smoothing_task = daisy.Task(
    "smooth_for_seg",
    process_function=partial(smooth_in_block, "smoothed_for_seg"),
    total_roi=total_read_roi,
    read_roi=read_roi,
    write_roi=block_roi,
    fit="shrink",
    num_workers=5,
    read_write_conflict=False,
    check_function=partial(check_complete, "smoothed_for_seg")
)

# %%
# you can have different block sizes in different tasks
seg_block_roi = daisy.Roi((0,0), (128, 128))

prepare_ds(
    "sample_data.zarr",
    "blue_objects",
    total_roi=total_write_roi,
    voxel_size=daisy.Coordinate((1,1)),
    dtype=np.uint8,
    write_size=seg_block_roi.shape,
)

seg_task = daisy.Task(
    "segmentation",
    process_function=partial(segment_blue_objects, "smoothed_for_seg", "blue_objects"),
    total_roi=total_write_roi, # Note: This task does not have context (yet...?)
    read_roi=seg_block_roi,
    write_roi=seg_block_roi,
    fit="shrink",
    read_write_conflict=False,
    num_workers=5,
    upstream_tasks=[smoothing_task],
)

# %%
daisy.run_blockwise([smoothing_task, seg_task])

# %%
from skimage.color import label2rgb
figure, axes = plt.subplots(1, 2)
axes[0].imshow(zarr.open('sample_data.zarr', 'r')['smoothed_for_seg'][:].transpose(1,2,0), origin="lower")
axes[1].imshow(label2rgb(zarr.open('sample_data.zarr', 'r')['blue_objects'][:]), origin="lower")


# %% [markdown]
# ## Process functions that need to read their neighbor's output (and the "read_write_conflict" flag)

# %% [markdown]
# How can we resolve the labels of adjacent blocks?

# %%
def segment_blue_objects_with_context(input_group, output_group, block):
    import cv2
    from funlib.persistence.arrays import open_ds, Array
    import numpy as np
    import skimage
    import logging
    import time
    
    def get_overlapping_labels(array1, array2):
        array1 = array1.flatten()
        array2 = array2.flatten()
        # get indices where both are not zero (ignore background)
        # this speeds up computation significantly
        non_zero_indices = np.logical_and(array1, array2)
        flattened_stacked = np.array([array1[non_zero_indices], array2[non_zero_indices]])
        intersections = np.unique(flattened_stacked, axis=1)
        return intersections  # a <number of pairs> x 2 nparray
    
    input_ds = open_ds('sample_data.zarr', input_group, "r",)
    output_ds = open_ds('sample_data.zarr', output_group, 'a')
    data = input_ds.to_ndarray(block.read_roi, fill_value=0)
    existing_labels = output_ds.to_ndarray(block.read_roi, fill_value=0)
    
    back_to_skimage = (data.transpose(1,2,0) * 255).astype(np.uint8)
    cv2_image = cv2.cvtColor(skimage.util.img_as_ubyte(back_to_skimage), cv2.COLOR_RGB2BGR)
    hsv_image = cv2.cvtColor(cv2_image, cv2.COLOR_BGR2HSV)
    # Define the color range for detection
    lower_blue = np.array([100,30,0])
    upper_blue = np.array([150,255,255])
    # Threshold the image to get only blue colors
    mask = cv2.inRange(hsv_image, lower_blue, upper_blue)
    mask = mask.astype(np.uint16)
    
    mask = mask // 255
    labels = skimage.measure.label(mask)
    # get a unique ID for each element in the whole volume (avoid repeats between blocks)
    block_id_mask = mask * (block.block_id[1])
    labels = labels + block_id_mask

    # if there are existing labels, change the label to match
    # note: This only works if objects never span multiple rows/columns. If you have long objects like neurons, you need to do true agglomeration
    intersections = get_overlapping_labels(labels, existing_labels)
    for index in range(intersections.shape[1]):
        new_label, existing_label = intersections[:, index]
        labels[labels == new_label] = existing_label

    time.sleep(0.5)
    output_ds = open_ds('sample_data.zarr', output_group, 'a')
    output_array = Array(labels, roi=block.read_roi, voxel_size=(1, 1))
    output_ds[block.write_roi] = output_array.to_ndarray(block.write_roi)


# %%
seg_block_roi = daisy.Roi((0,0), (128, 128))
seg_block_read_roi = seg_block_roi.grow(context, context)

prepare_ds(
    "sample_data.zarr",
    "blue_objects_with_context",
    total_roi=total_write_roi,
    voxel_size=daisy.Coordinate((1,1)),
    dtype=np.uint8,
    write_size=seg_block_roi.shape,
)

seg_task = daisy.Task(
    "segmentation_with_context",
    process_function=partial(segment_blue_objects_with_context, "smoothed_for_seg", "blue_objects_with_context"),
    total_roi=total_read_roi,
    read_roi=seg_block_read_roi,
    write_roi=seg_block_roi,
    fit="shrink",
    read_write_conflict=False,
    num_workers=10,
    upstream_tasks=[smoothing_task],
)
daisy.run_blockwise([seg_task])

# %%
from skimage.color import label2rgb
figure, axes = plt.subplots(1, 2)
axes[0].imshow(zarr.open('sample_data.zarr', 'r')['smoothed_for_seg'][:].transpose(1,2,0), origin="lower")
axes[1].imshow(label2rgb(zarr.open('sample_data.zarr', 'r')['blue_objects_with_context'][:]), origin="lower")

# %%
