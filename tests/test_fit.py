from __future__ import print_function
import daisy

def print_block(r, w):

    p = ' '*r.get_begin()[0]
    p += 'r'*(w.get_begin()[0] - r.get_begin()[0])
    p += 'w'*(w.size())
    p += 'r'*(r.get_end()[0] - w.get_end()[0])

    print(p)

if __name__ == "__main__":

    total_roi = daisy.Roi((0,), (10,))
    read_roi = daisy.Roi((0,), (5,))
    write_roi = daisy.Roi((1,), (3,))

    for fit in ['valid', 'overhang', 'shrink']:

        print()
        print("%s fitting:"%fit)

        deb_graph = daisy.create_dependency_graph(
            total_roi,
            read_roi,
            write_roi,
            fit=fit)

        print('-'*total_roi.size())
        for r, w, c in deb_graph:
            print_block(r, w)
