import multiprocessing

# workaround for MacOS:
# this needs to be set before importing any library that uses multiprocessing
multiprocessing.set_start_method("fork")
import logging
import daisy
import subprocess


logging.basicConfig(level=logging.INFO)


def run_scheduler(total_roi, read_roi, write_roi, num_workers):

    dummy_task = daisy.Task(
        "dummy_task",
        total_roi=total_roi,
        read_roi=read_roi,
        write_roi=write_roi,
        process_function=start_worker,
        num_workers=num_workers,
        read_write_conflict=False,
        fit="shrink",
    )

    daisy.run_blockwise([dummy_task])


def start_worker():

    worker_id = daisy.Context.from_env()["worker_id"]
    task_id = daisy.Context.from_env()["task_id"]

    print(f"worker {worker_id} started for task {task_id}...")

    subprocess.run(["python", "./worker.py"])

    # do the same on a cluster node:
    # num_cpus_per_worker = 4
    # subprocess.run(["bsub", "-I", f"-n {num_cpus_per_worker}", "python", "./worker.py"])


if __name__ == "__main__":

    total_roi = daisy.Roi((0, 0, 0), (1024, 1024, 1024))
    write_roi = daisy.Roi((0, 0, 0), (256, 256, 256))
    read_roi = write_roi.grow((16, 16, 16), (16, 16, 16))

    run_scheduler(total_roi, read_roi, write_roi, num_workers=10)
