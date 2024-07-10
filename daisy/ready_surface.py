class ReadySurface:
    """
    The ready surface is a datastructure to keep track of the nodes in a
    dependency graph that are ready to be scheduled. Nodes can be marked as
    success or failure. All nodes downstream of a failure are considered a
    failure.
    The purpose is to minimize the in-memory storage of blocks.

    nodes can have 3 states:
    1: SURFACE
    2: BOUNDARY
    3: OTHER

    A node is a SURFACE node iff
        1) it has been marked as successfully completed
        2) it has downstream dependencies marked as OTHER
    A node is a BOUNDARY node iff
        1) it has been marked as failed
        2) it has upstream dependencies marked as SURFACE
    """

    def __init__(self, get_downstream_nodes, get_upstream_nodes):
        self.downstream = lambda x: list(get_downstream_nodes(x))
        self.upstream = lambda x: list(get_upstream_nodes(x))

        self.surface = set()
        self.boundary = set()

    def mark_success(self, node):
        """
        Update surface and boundary to account for a `node` marked as a
        success.

        args:
            node: `hashable`:
                The node that has been marked as successful. It is assumed that
                all of the upstream dependencies of node are in the surface.
                If this is not true, behavior is not defined.

        returns:
            list of nodes that are now free to be scheduled.
        """

        up_nodes = self.upstream(node)

        # check if node is a valid input
        assert all(
            up_node in self.surface for up_node in up_nodes
        ), f"Not all upstream dependencies of {node} are in the surface"

        self.surface.add(node)
        new_ready_nodes = []
        # check if any downstream nodes need to be added to the boundary
        for down_node in self.downstream(node):
            if not self.__add_to_boundary(down_node):
                if all(up_node in self.surface for up_node in self.upstream(down_node)):
                    new_ready_nodes.append(down_node)

        # check if any of the upstream nodes can be removed from surface
        for up_node in up_nodes:
            down_nodes = self.downstream(up_node)
            # check down node state: 1 = SURFACE, -1 = BOUNDARY, 0 = OTHER
            contained = [
                (down_node in self.surface) - (down_node in self.boundary)
                for down_node in down_nodes
            ]
            if all(abs(x) > 0 for x in contained):
                self.surface.remove(up_node)
                for down_node, c in zip(down_nodes, contained):
                    if c < 0:
                        # node was in boundary. maybe we can remove it
                        self.__remove_from_boundary(down_node)

        if len(list(self.downstream(node))) == 0:
            self.surface.remove(node)

        return new_ready_nodes

    def mark_failure(self, node, count_all_orphans=False):
        """
        Update surface and boundary to account for a `node` marked as a
        failure.

        args:
            node: `hashable`:
                The node that has been marked as a failure. It is assumed that
                all of the upstream dependencies of node are in the surface.
                If this is not true, behavior is not defined.
            count_all_orphans: bool:
                Whether or not to count every orphan, or stop recursing though
                the dependency tree as early as possible. if False will still
                return a lower bound on the number of orphans.
        """
        up_nodes = self.upstream(node)

        # check if node is a valid input
        assert all(
            up_node in self.surface for up_node in up_nodes
        ), f"Not all upstream dependencies of {node} are in the surface"

        self.boundary.add(node)

        # recurse through downstream nodes, adding them to boundary if
        # necessary
        down_nodes = set(self.downstream(node))
        orphans = set()
        while len(down_nodes) > 0:
            down_node = down_nodes.pop()
            if self.__add_to_boundary(down_node):
                # check if any nodes downstream of this node are also boundary
                # nodes.
                new_nodes = set(self.downstream(down_node)) - orphans
                down_nodes = down_nodes.union(new_nodes)
                orphans.add(down_node)

        # check if any of the upstream nodes can be removed from surface
        for up_node in up_nodes:
            down_nodes = self.downstream(up_node)
            # check down node state: 1 = SURFACE, -1 = BOUNDARY, 0 = OTHER
            contained = [
                (down_node in self.surface) - (down_node in self.boundary)
                for down_node in down_nodes
            ]
            if all(abs(x) > 0 for x in contained):
                self.surface.remove(up_node)
                for down_node, c in zip(down_nodes, contained):
                    if c < 0:
                        # node was in boundary. maybe we can remove it
                        self.__remove_from_boundary(down_node)

        if len(list(self.upstream(node))) == 0:
            self.boundary.remove(node)

        return orphans

    def __add_to_boundary(self, node):
        up_nodes = self.upstream(node)
        if node not in self.boundary and any(
            up_node in self.boundary for up_node in up_nodes
        ):
            self.boundary.add(node)
            return True
        else:
            return False

    def __remove_from_boundary(self, node):
        up_nodes = self.upstream(node)
        if node in self.boundary and all(
            up_node not in self.surface for up_node in up_nodes
        ):
            self.boundary.remove(node)
            return True
        else:
            return False
