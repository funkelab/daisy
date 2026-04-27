import multiprocessing as mp

import pytest

import daisy

# `daisy.messages` and `daisy.tcp` were daisy 1.x's internal protocol modules
# (pickle-over-tornado). daisy v2's wire protocol is bincode over tokio TCP,
# implemented in Rust; the Python layer does not expose the protocol message
# types or a reusable TCPServer. The original test below is preserved for
# documentation and is xfailed; a migrated version that exercises the public
# Client+Server API follows.
#
# from daisy.messages import AcquireBlock, ReleaseBlock, SendBlock, ExceptionMessage
# from daisy.tcp import TCPServer


def run_test_server(block, conn):
    # Original implementation depended on the removed `daisy.messages` and
    # `daisy.tcp` symbols. Body is preserved here only as a reference.
    raise NotImplementedError(
        "daisy v2 does not expose protocol messages or TCPServer; "
        "see test_basic_migrated below"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "daisy v2 does not expose `daisy.messages` or `daisy.tcp`. "
        "The wire protocol is bincode over tokio TCP and lives in the Rust "
        "core. See test_basic_migrated for a v2-style equivalent."
    ),
)
def test_basic():
    roi = daisy.Roi((0, 0, 0), (10, 10, 10))
    task_id = 1
    block = daisy.Block(roi, roi, roi, block_id=1, task_id=task_id)
    parent_conn, child_conn = mp.Pipe()
    server_process = mp.Process(target=run_test_server, args=(block, child_conn))
    server_process.start()
    host, port = parent_conn.recv()
    context = daisy.Context(hostname=host, port=port, task_id=task_id, worker_id=1)
    client = daisy.Client(context=context)
    with client.acquire_block() as block:
        block.status = daisy.BlockStatus.SUCCESS

    success = parent_conn.recv()
    server_process.join()
    assert success


def test_basic_migrated():
    """v2 equivalent: drive a real Server + Client roundtrip via the public
    `run_blockwise` entry point. The Rust core handles the TCP/bincode
    plumbing the original test was asserting on."""

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
