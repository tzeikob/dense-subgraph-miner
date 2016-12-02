package com.tkb.delab.alg;

import com.tkb.delab.model.AugmentedRange;
import com.tkb.delab.model.Edge;
import com.tkb.delab.model.Triangle;
import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.THashSet;

/**
 * An interface for algorithms scoring each edge within the given graph
 * participates at least in one triangle, as an indicator of the neighborhood
 * density.
 *
 * @author Akis Papadopoulos
 */
public interface EdgeDensityEstimator {

    /**
     * A method scoring for each edge the neighborhood density, given the
     * triangles within.
     *
     * @param triangles a set of the triangles within the graph.
     * @return a hash map between edge and its lambda density score.
     */
    public THashMap<Edge, AugmentedRange> estimate(THashSet<Triangle> triangles);
}
