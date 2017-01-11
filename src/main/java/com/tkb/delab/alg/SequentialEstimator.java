package com.tkb.delab.alg;

import com.tkb.delab.model.AugmentedRange;
import com.tkb.delab.model.Edge;
import com.tkb.delab.model.Triangle;
import gnu.trove.iterator.hash.TObjectHashIterator;
import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.THashSet;
import java.util.Map.Entry;

/**
 * An edge neighborhood density estimator based on triangulation using a
 * repetitive sequential search mode.
 *
 * @author Akis Papadopoulos
 */
public class SequentialEstimator implements EdgeDensityEstimator {

    // Maximum number of iterations
    private int iterations;

    /**
     * A default constructor creating a sequential edge density estimator
     * setting the default maximum number of iterations.
     */
    public SequentialEstimator() {
        this.iterations = 10;
    }

    /**
     * A constructor creating a sequential edge density estimator given the
     * number of maximum iterations.
     *
     * @param iterations the maximum numbers of iterations.
     */
    public SequentialEstimator(int iterations) {
        this.iterations = iterations;
    }

    /**
     * A method scoring each edge with a neighborhood density value given the
     * set of triangles of the graph.
     *
     * @param triangles the set of the triangles within the graph.
     * @return a map of lambda density values for each edge.
     */
    @Override
    public THashMap<Edge, AugmentedRange> estimate(THashSet<Triangle> triangles) {
        THashMap<Edge, AugmentedRange> edges = new THashMap<Edge, AugmentedRange>();

        // Scoring each edge with the number of triangles belongs to
        for (Triangle t : triangles) {
            Edge e1 = new Edge(t.v, t.u);

            if (edges.contains(e1)) {
                edges.get(e1).upper += 1;
            } else {
                edges.put(e1, new AugmentedRange(1, 1, 0));
            }

            Edge e2 = new Edge(t.u, t.w);

            if (edges.contains(e2)) {
                edges.get(e2).upper += 1;
            } else {
                edges.put(e2, new AugmentedRange(1, 1, 0));
            }

            Edge e3 = new Edge(t.v, t.w);

            if (edges.contains(e3)) {
                edges.get(e3).upper += 1;
            } else {
                edges.put(e3, new AugmentedRange(1, 1, 0));
            }
        }

        // Estimating density values in a 2-phase repetitive process
        boolean converged;

        int it = 0;

        do {
            // Resetting the converge indicator
            converged = true;

            // Iterating triangles re-calculating each edge support value
            TObjectHashIterator<Triangle> tit = triangles.iterator();

            while (tit.hasNext()) {
                Triangle triangle = tit.next();

                // Extracting the triangle's edges
                Edge[] e = new Edge[3];
                e[0] = new Edge(triangle.v, triangle.u);
                e[1] = new Edge(triangle.u, triangle.w);
                e[2] = new Edge(triangle.v, triangle.w);

                for (int i = 0; i < e.length; i++) {
                    // Getting next edge lambda bound range
                    AugmentedRange range = edges.get(e[i]);

                    // Getting the lambda medium of each incident edge
                    int li = range.upper;
                    int l1 = edges.get(e[(i + 1) % 3]).upper;
                    int l2 = edges.get(e[(i + 2) % 3]).upper;

                    // Increasing the support value of the pivot edge
                    if (li <= Math.min(l1, l2)) {
                        range.support += 1;
                    }
                }
            }

            // Iterating edges re-calculating the lambda bounds
            for (Entry<Edge, AugmentedRange> entry : edges.entrySet()) {
                // Getting the lambda bound range of the next edge
                AugmentedRange range = entry.getValue();

                // Checking if the edge converged to a valid lambda bound
                if (range.support < range.upper) {
                    // Updating the bounds of the lambda range
                    range.upper -= 1;

                    // Marking the process as not converged
                    converged = false;
                }

                // Resetting the support of the edge
                range.support = 0;
            }

            it++;
        } while (!converged && it <= iterations - 1);

        return edges;
    }
}
