package com.tkb.delab.alg;

import com.tkb.delab.model.Edge;
import com.tkb.delab.model.Triangle;
import gnu.trove.set.hash.THashSet;

/**
 * An abstract triangulation interface.
 *
 * @author Akis Papadopoulos
 */
public interface Triangulator {

    /**
     * An abstract method listing all triangles within a graph represented by a
     * given edge set.
     *
     * @param edges edge set graph induced by.
     * @return a set of all triangles within the graph.
     */
    public THashSet<Triangle> list(THashSet<Edge> edges);
}
