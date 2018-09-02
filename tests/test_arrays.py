import daisy
import zarr
import numpy as np

if __name__ == "__main__":

    data = np.arange(3*10*10).reshape((3, 10, 10))

    # create a test dataset
    root = zarr.open('test_arrays.zarr', 'w')
    ds = root.create_dataset(
        'test',
        data=data)
    ds.attrs['offset'] = (20, 20)
    ds.attrs['resolution'] = (1, 2)

    array = daisy.open_ds('test_arrays.zarr', 'test', mode='a')

    print(array.roi)
    print(array.voxel_size)
    print(array.n_channel_dims)
    print(array.data.shape)
    print(array.to_ndarray())

    b = array[daisy.Roi((20, 20), (10, 10))]

    print(b.roi)
    print(b.voxel_size)
    print(b.n_channel_dims)
    print(b.data.shape)
    print(b.to_ndarray())

    c = array.intersect(daisy.Roi((20, 20), (100, 2)))

    print(c.roi)
    print(c.voxel_size)
    print(c.n_channel_dims)
    print(c.data.shape)
    print(c.to_ndarray())

    d = array.to_ndarray(daisy.Roi((20, 20), (100, 2)), fill_value=-1)
    print(d)

    array[daisy.Roi((20, 20), (10, 10))] = array[daisy.Roi((20, 30), (10, 10))]
    print(array.to_ndarray())

    array[daisy.Roi((20, 20), (10, 10))] = 0
    print(array.to_ndarray())

    try:
        fail = array.to_ndarray(daisy.Roi((0, 0), (1, 1)))
    except:
        print("Out-of-bounds access successfully detected")
    else:
        raise RuntimeError("Out-of-bounds access not detected")

    print(array.roi)
    print(array[daisy.Coordinate((20, 20))])
    print(array[daisy.Coordinate((29, 39))])
    try:
        print(array[daisy.Coordinate((29, 40))])
    except:
        print("Out-of-bounds access successfully detected")
    else:
        raise RuntimeError("Out-of-bounds access not detected")

    array[array.roi] = 0
