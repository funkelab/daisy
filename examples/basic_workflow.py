import daisy
import argparse


def process_function(b):
    # Normal workflow:
    #   read data in b.read_roi
    #   process
    #   write result to b.write_roi
    print(b)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--total_roi_size",
        "-t",
        nargs="+",
        help="Size of total region to process",
        default=[100, 100],
    )
    parser.add_argument(
        "--block_read_size",
        "-r",
        nargs="+",
        help="Size of block read region",
        default=[20, 20],
    )
    parser.add_argument(
        "--block_write_size",
        "-w",
        nargs="+",
        help="Size of block write region",
        default=[16, 16],
    )
    parser.add_argument(
        "--num_workers", "-nw", type=int, help="Number of processes to spawn", default=1
    )
    parser.add_argument(
        "--read_write_conflict",
        "-rwc",
        action="store_true",
        help="Flag to not schedule overlapping blocks"
        " at the same time. Default is false",
    )
    args = parser.parse_args()

    ndims = len(args.total_roi_size)

    # define total region of interest (roi)
    total_roi_start = daisy.Coordinate((0,) * ndims)
    total_roi_size = daisy.Coordinate(args.total_roi_size)
    total_roi = daisy.Roi(total_roi_start, total_roi_size)

    # define block read and write rois
    block_read_size = daisy.Coordinate(args.block_read_size)
    block_write_size = daisy.Coordinate(args.block_write_size)
    context = (block_read_size - block_write_size) / 2
    block_read_roi = daisy.Roi(total_roi_start, block_read_size)
    block_write_roi = daisy.Roi(context, block_write_size)

    # call run_blockwise
    daisy.run_blockwise(
        total_roi,
        block_read_roi,
        block_write_roi,
        process_function=process_function,
        read_write_conflict=args.read_write_conflict,
        num_workers=args.num_workers,
    )
