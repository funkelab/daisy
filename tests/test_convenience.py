import daisy

done = []

def process_function(read_roi, write_roi):
    print("Processing in %s"%write_roi)
    done.append(write_roi)

def check_function(write_roi):
    print("Checking if %s is done"%write_roi)
    for d in done:
        if d == write_roi:
            return True
    return False

if __name__ == "__main__":

    daisy.process_blockwise(
        daisy.Roi((0,), (100,)),
        daisy.Roi((0,), (20,)),
        daisy.Roi((5,), (15,)),
        process_function,
        check_function,
        1) # this test only works with one worker, since we have global state
