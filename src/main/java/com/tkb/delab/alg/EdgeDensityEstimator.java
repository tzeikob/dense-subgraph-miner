package com.tkb.delab.alg;

import com.tkb.delab.model.AugmentedRange;
import com.tkb.delab.model.Edge;
import com.tkb.delab.model.Triangle;
import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.THashSet;

/**
 * An edge lambda density estimator scoring each edge within a given graph
 * regarding the number of triangles the edge belongs to, as an indicator of the
 * neighborhood density.
 *
 * @author Akis Papadopoulos
 */
public interface EdgeDensityEstimator {

    /**
     * A method scoring each edge with a neighborhood density value given the
     * set of triangles of the graph.
     *
     * @param triangles the set of the triangles within the graph.
     * @return a map of lambda density values for each edge.
     */
    public THashMap<Edge, AugmentedRange> estimate(THashSet<Triangle> triangles);
}
