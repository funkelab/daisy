import daisy


def test_block_function_setting_success_status_completes_the_run():
    """A Server + Client round trip through the public `run_blockwise` entry
    point; the block function sets the status itself, as daisy 1.x code did."""

    def process(block):
        block.status = daisy.BlockStatus.SUCCESS

    task = daisy.Task(
        task_id="compat_client",
        total_roi=daisy.Roi((0, 0, 0), (10, 10, 10)),
        read_roi=daisy.Roi((0, 0, 0), (10, 10, 10)),
        write_roi=daisy.Roi((0, 0, 0), (10, 10, 10)),
        process_function=process,
        read_write_conflict=False,
        max_workers=1,
    )
    assert daisy.run_blockwise([task], multiprocessing=True, progress=False)
