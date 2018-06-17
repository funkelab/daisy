from distributed import Client
import time
import random

def task(n, *args):

    print("Starting task %d"%n)
    time.sleep(random.random()*5)
    print("Task %d done"%n)

    if random.random() < 0.1:
        raise RuntimeError("Oops...")

    return n*n

if __name__ == "__main__":

    client = Client()

    # create a dependency chain
    tasks = {
        'task-0': (task, 1)
    }
    tasks.update({
        'task-%d'%i: (task, i, 'task-%d'%(i-1))
        for i in range(1, 100)
    })

    result = client.get(tasks, 'task-99', retries=10)

    print("Tasks done")
    print(result)

    print("Hit enter to quit")
    raw_input()
