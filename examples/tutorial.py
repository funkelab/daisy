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
import multiprocessing
# multiprocessing.set_start_method("fork")

# %% [markdown]
# ## Building Blocks

# %% [markdown]
# Daisy is designed for processing volumetric data. Therefore, it has specific ways to describe locations in a volume. We will demonstrate the common terms and utilities using this image of astronaut Eileen Collins.

# %%
from skimage import data
import numpy as np
import matplotlib.pyplot as plt

raw_data = np.flip(data.astronaut(), 0)[0:512, 0:400]
axes_image = plt.imshow(raw_data, zorder=1, origin="lower")

# %% [markdown]
# ### Coordinate
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
# ### Roi
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
# ### Array
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
# ### Block
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

# let's look at the read roi of our block on top of the original figure
figure = fresh_image()
display_roi(figure.axes[0], block.read_roi, color="white")

# %% [markdown]
# You may be wondering why the block has a read roi and a write roi - this will be illustrated next in our simple daisy example!

# %% [markdown]
# ## A Simple Example: Local Smoothing
# TODO: Intro

# %% [markdown]
# ### Dataset Preparation
# As mentioned earlier, we highly recommend using a zarr backend for your volume. Daisy is designed such that no data is transmitted between the worker and the scheduler, including the output of the processing. That means that each worker is responsible for saving the results in the given block write_roi. With a zarr backend, each worker can write to a specific region of the zarr in parallel, assuming that the chunk size is a multiple of the write_roi.
#
# TODO: ....
#

# %%
import zarr
# convert our data to float, because gaussian smoothing in scikit expects float input and output
# recall that we already reshaped it to put channel dimension first, as the funlib.persistence Array expects
raw_data_float = raw_data_reshaped.astype(np.float32)/255.0

# store image in zarr container
f = zarr.open('sample_data.zarr', 'w')
f['raw'] = raw_data_float
f['raw'].attrs['offset'] = daisy.Coordinate((0,0))
f['raw'].attrs['resolution'] = daisy.Coordinate((1,1))

# %%
from funlib.persistence import prepare_ds
# prepare an output dataset with a chunk size that is a divisor of the block roi
n_channels = 3
prepare_ds(
    "sample_data.zarr",
    "smoothed",
    total_roi=total_roi,
    voxel_size=daisy.Coordinate((1,1)),
    dtype=raw_data_float.dtype,
    write_size=block_size,
    num_channels=n_channels,
)
print("Shape of output dataset:", f["smoothed"].shape)
print("Chunk size in output dataset:",f['smoothed'].chunks)


# %% [markdown]
# ### Define our Process Function
# TODO: Describe

# %%
def smooth_in_roi(block: daisy.Block):
    from funlib.persistence.arrays import open_ds
    from skimage import filters
    sigma = 5.0
    raw_ds = open_ds('sample_data.zarr', 'raw', "r",)
    data = raw_ds.to_ndarray(block.read_roi)
    smoothed = filters.gaussian(data, sigma=sigma, channel_axis=0)
    output_ds = open_ds('sample_data.zarr', 'smoothed', 'a')
    output_ds[block.write_roi] = smoothed


# %%
smooth_in_roi(block)

# %%
plt.imshow(zarr.open('sample_data.zarr', 'r')['smoothed'][:].transpose(1, 2, 0), origin="lower")

# %% [markdown]
# ### Run daisy with local multiprocessing
# TODO: Describe

# %%
daisy.run_blockwise([
        daisy.Task(
            "Smoothing",
            process_function=smooth_in_roi,
            total_roi=total_roi,
            read_roi=block_roi,
            write_roi=block_roi,
            fit="shrink",
        )
    ]
)

# %%
plt.imshow(zarr.open('sample_data.zarr', 'r')['smoothed'][:].transpose(1, 2, 0), origin="lower")

# %% [markdown]
# #### Take 2: Add context!

# %%
prepare_ds(
    "sample_data.zarr",
    "smoothed_with_context",
    total_roi=total_roi,
    voxel_size=daisy.Coordinate((1,1)),
    dtype=raw_data_float.dtype,
    write_size=block_size,
    num_channels=n_channels,
)


# %%
def smooth_in_roi_with_context(block: daisy.Block):
    from funlib.persistence.arrays import open_ds, Array
    from skimage import filters
    sigma = 5.0
    context = int(sigma) * 2
    grown_roi = block.read_roi.grow(context, context)
    
    raw_ds = open_ds('sample_data.zarr', 'raw', "r",)
    data = raw_ds.to_ndarray(grown_roi)
    smoothed = filters.gaussian(data, sigma=sigma, channel_axis=0)
    
    output_ds = open_ds('sample_data.zarr', 'smoothed_with_context', 'a')
    
    smoothed = Array(smoothed, roi=grown_roi, voxel_size=(1, 1))
    output_ds[block.write_roi] = smoothed.to_ndarray(block.write_roi)


# %%
daisy.run_blockwise([
        daisy.Task(
            "Smoothing with context",
            process_function=smooth_in_roi_with_context,
            total_roi=total_roi,
            read_roi=block_roi,
            write_roi=block_roi,
            fit="shrink",
        )
    ]
)

# %%
plt.imshow(zarr.open('sample_data.zarr', 'r')['smoothed_with_context'][:].transpose(1, 2, 0), origin="lower")

# %% [markdown]
# #### Take 3: The Daisy Way

# %%
prepare_ds(
    "sample_data.zarr",
    "smoothed_blockwise",
    total_roi=total_roi,
    voxel_size=daisy.Coordinate((1,1)),
    dtype=raw_data_float.dtype,
    write_size=block_size,
    num_channels=n_channels,
)


# %%
def smooth_in_block(block: daisy.Block):
    from funlib.persistence.arrays import open_ds, Array
    from skimage import filters
    
    sigma = 5.0
    
    raw_ds = open_ds('sample_data.zarr', 'raw', "r",)
    data = raw_ds.to_ndarray(block.read_roi)
    smoothed = filters.gaussian(data, sigma=sigma, channel_axis=0)
    
    output_ds = open_ds('sample_data.zarr', 'smoothed_blockwise', 'a')
    
    smoothed = Array(smoothed, roi=block.read_roi, voxel_size=(1, 1))
    output_ds[block.write_roi] = smoothed.to_ndarray(block.write_roi)


# %%
sigma = 5
context = int(sigma) * 2
read_roi = block_roi.grow(context, context)

daisy.run_blockwise([
        daisy.Task(
            "Smoothing with context",
            process_function=smooth_in_block,
            total_roi=total_roi,
            read_roi=read_roi,
            write_roi=block_roi,
            fit="shrink",
        )
    ]
)

# %%
plt.imshow(zarr.open('sample_data.zarr', 'r')['smoothed_blockwise'][:].transpose(1, 2, 0), origin="lower")

# %% [markdown]
# ## Distributing on the Cluster

# %%
