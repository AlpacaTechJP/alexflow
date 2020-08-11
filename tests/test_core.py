from alexflow import Task


def test_task():
    task = Task()
    assert task.resource_spec is None