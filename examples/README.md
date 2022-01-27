# Examples

## Dataset

To demonstrate the usages of Daisy, we have a few examples here. But first, we need a dataset - a good one is the CREMI segmentation challenge[^1].

```sh
wget https://cremi.org/static/data/sample_A_20160501.hdf --no-check-certificate
```

To make our life easier, we'll transcode the `.hdf` file to `.zarr` format (using Daisy!).
The following command generates `sample_A_20160501.zarr` based on the downloaded file.

```sh
python hdf_to_zarr.py sample_A_20160501.hdf volumes/raw
```

## Simple example: Gaussian smoothing

Run the following command to smooth out our dataset in 3D using eight cores in parallel:
```sh
python gaussian_smoothing1.py sample_A_20160501.hdf volumes/raw --out_file sample_A_20160501.zarr --out_ds_name volumes/raw_smoothed --num_workers 8
```

In this example, one thing to note is the call to make `daisy.Task`:
```python
    task = daisy.Task(
            'GaussianSmoothingTask',
            total_roi=total_roi,
            read_roi=block_read_roi,
            write_roi=block_write_roi,
            process_function=lambda b: smooth(
                b, dataset, output_dataset, sigma=config.sigma),
            read_write_conflict=False,
            num_workers=config.num_workers,
            fit='shrink'
            )
```
Here, `total_roi` is the total region-of-interest which corresponds to the input array shape. `read_roi`/`write_roi`
refers to the read/write size of each block (adjustable with `--block_read_size` and `--block_write_size`).
Note that `read_roi` is always larger than `write_roi` in order to provide enough surrounding context for the
Gaussian smoothing function to work properly.

`process_function` is the function that Daisy will call for each block `b`. Each independent `b` defines its own
`read_roi` and `write_roi` that the function can use to read the correct portion of the dataset, as shown in the
`smooth()` function:
```python
def smooth(block, dataset, output, sigma=5):
    logger.debug("Block: %s" % block)

    # read data in block.read_roi
    daisy_array = dataset[block.read_roi]
    data = daisy_array.to_ndarray()
    logger.debug("Got data of shape %s" % str(data.shape))

    # apply gaussian filter
    r = scipy.ndimage.gaussian_filter(
            data, sigma=sigma, mode='constant')

    # write result to output dataset in block.write_roi
    to_write = daisy.Array(
            data=r,
            roi=block.read_roi,
            voxel_size=dataset.voxel_size)
    output[block.write_roi] = to_write[block.write_roi]
    logger.debug("Done")
    return 0
```

`num_workers` tells Daisy how many workers to automatically launch. You can read more about the other arguments in
[daisy.Task](https://github.com/funkelab/daisy/blob/master/daisy/task.py).

## Visualizing

To visualize the result, run the following command:
```sh
python visualize.py --file sample_A_20160501.zarr --datasets volumes/raw volumes/raw_smoothed
```

You might need to install some dependencies first:
```sh
pip install git+https://github.com/funkelab/funlib.show.neuroglancer
```
Notice that--especially in the XZ view--there seems to be black line artifacts in the smoothed output.
This is due to insufficient context in the Z direction. How would you fix this? (Hint: take a look at the defaults for `--block_read_size`)


## Track progress with persistent database

Note that rerunning `gaussian_smoothing1.py` will restart the task from the beginning. This is fine for small tasks, but
unacceptable for tasks that can take weeks to run--a scheduler crash can disastrously wipe out a lot of work.
In `gaussian_smoothing2.py`, we solved this problem by factoring our code and make an object that inherits from
`BatchTask`, a wrapper class that automatically setups a persistent database (either SQLite3 or MongoDB) and tracks
which blocks are finished. Try running the following command multiple times and/or `Ctrl+C` in the middle of execution
to see how Daisy can be augmented to recover from not only worker crashes but also scheduler crashes.

```sh
python gaussian_smoothing2.py sample_A_20160501.hdf volumes/raw --out_file sample_A_20160501.zarr --out_ds_name volumes/raw_smoothed --num_workers 1
```

## Spawn workers on demand over network

As an added bonus of refactoring and implementing `BatchTask`, you can now also run workers from the command line, potentially
on a different computer and/or submit this command as a batch file with SLURM as long as they can reach the scheduler through TCP/IP!
This command varies from run to run, but looks something like this, and is printed on the terminal when the task is started.
Run this command as many time as needed to add workers.

```sh
Terminal command: DAISY_CONTEXT=hostname=10.11.144.145:port=45851:task_id=GaussianSmoothingTask_7791fd57:worker_id=0 python /n/groups/htem/Segmentation/tmn7/daisy-refactor-220124/examples/gaussian_smoothing2.py run_worker .run_configs/GaussianSmoothingTask_7791fd57d5da43e1f6603a0cc2b036d4.config
```

## Task chaining

Your workflow might require multiple steps, with the following steps requiring the results of the previous steps. With many steps,
the dependency graph between steps can get pretty hairy quickly, and it is also harder to track which one is finished and which
to run next. Thankfully, Daisy makes this process easier AND faster through a mechanism called "daisy chaining" (pun intended).
In this example, we chain together two Gaussian smoothing step and get them to run concurrently:

```sh
python chaining_example.py sample_A_20160501.zarr volumes/raw --num_workers 2 --overwrite 2
```

Check that the `Gaussian2` task started as soon as enough `Gaussian1` blocks are finished! This is only possible because Daisy
calculates and updates the block-wise dependency graph dynamically and as soon as individual blocks are returned by the workers.

Programmatically, the task dependency chain is specified through `upstream_tasks` that can be generalized to any
directed graph. `task_id` will need to be unique for each task, though `BatchTask` will take care of it if
the user does not manually specify task names:
```python
    daisy_task1 = GaussianSmoothingTask(config1, task_id='Gaussian1').prepare_task()
    daisy_task2 = GaussianSmoothingTask(config2, task_id='Gaussian2').prepare_task(upstream_tasks=[daisy_task1])
    done = daisy.run_blockwise([daisy_task1, daisy_task2])
```

To visualize that Gaussian smoothing is chained correctly, run the following command:
```sh
python visualize.py --file sample_A_20160501.zarr --datasets volumes/raw volumes/raw_smoothed volumes/raw_smoothed_smoothed
```

Have fun! And please reach out through `Issues` if there's anything that we can help!


[^1]: https://cremi.org/

