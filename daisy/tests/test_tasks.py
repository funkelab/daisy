import daisy
from daisy import Task, Roi, Parameter

import pytest


class TestTask(Task):
    a = Parameter(default=True)
    b = Parameter(default=False)
    c = Parameter()

    def __init__(self, **kwargs):
        super().__init__(
            "TestTask",
            total_roi=Roi((0,), (4,)),
            read_roi=Roi((0,), (3,)),
            write_roi=Roi((0,), (1,)),
            process_function=None,
            check_function=None,
            **kwargs
        )

    def requires(self):
        return []


def test_task_creation():

    with pytest.raises(RuntimeError):
        t = TestTask()

    with pytest.raises(RuntimeError):
        t = TestTask(d=42)

    t = TestTask(c=42)

    assert t.a is True
    assert t.b is False
    assert t.c == 42

    t = TestTask(global_config={"TestTask": {"a": 23, "b": 42, "c": 3.14}})

    assert t.a == 23
    assert t.b == 42
    assert t.c == 3.14
