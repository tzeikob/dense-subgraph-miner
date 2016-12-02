package com.tkb.delab.reduce;

import com.tkb.delab.alg.BinaryEstimator;
import com.tkb.delab.alg.EdgeDensityEstimator;
import com.tkb.delab.alg.Forward;
import com.tkb.delab.alg.Triangulator;
import com.tkb.delab.model.AugmentedRange;
import com.tkb.delab.model.Edge;
import com.tkb.delab.model.Triangle;
import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Quad;
import com.tkb.delab.io.Triple;
import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.THashSet;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A local lambda estimation reducer.
 *
 * @author Akis Papadopoulos
 */
public class LocalLambdaEstimationReducer extends Reducer<Triple, Pair, Triple, Quad> {

    /**
     * A reduce method collecting for an indexed partition a subset of sorted
     * edges, listing all the triangles within estimating the lambda bounds,
     * emitting each triangle found three times one for each of its incident
     * edges augmented by its lambda bounds found respectively.
     *
     * @param key indexes of the edge partition.
     * @param values the subset of unique sorted edges.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(Triple key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
        // Getting the list iterator
        Iterator<Pair> it = values.iterator();

        // Creating an empty edge set
        THashSet<Edge> edges = new THashSet<Edge>();

        // Iterating through the pairs list
        while (it.hasNext()) {
            // Getting the next pair
            Pair pair = it.next();

            // Adding the edge into the edge set
            edges.add(new Edge(pair.v, pair.u));
        }

        // Creating a forward tringulator
        Triangulator forward = new Forward();

        // Listing all sorted triangles within
        THashSet<Triangle> triangles = forward.list(edges);

        // Creating an edge density estimator
        EdgeDensityEstimator estimator = new BinaryEstimator(50);

        // Estimating density of each edge
        THashMap<Edge, AugmentedRange> emap = estimator.estimate(triangles);

        // Iterating through each triangle
        for (Triangle t : triangles) {
            // Getting the first edge
            Edge e1 = new Edge(t.v, t.u);

            // Getting the lambda estimation of the edge
            int lambda1 = emap.get(e1).upper;

            // Emitting the triangle followed by the first edge augmented by its lambda bounds
            context.write(new Triple(t.v, t.u, t.w), new Quad(t.v, t.u, lambda1, lambda1));

            // Getting the second edge
            Edge e2 = new Edge(t.u, t.w);

            // Getting the lambda estimation of the edge
            int lambda2 = emap.get(e2).upper;

            // Emitting the triangle followed by the second edge augmented by its lambda bounds
            context.write(new Triple(t.v, t.u, t.w), new Quad(t.u, t.w, lambda2, lambda2));

            // Getting the third edge
            Edge e3 = new Edge(t.v, t.w);

            // Getting the lambda estimation of the edge
            int lambda3 = emap.get(e3).upper;

            // Emitting the triangle followed by the third edge augmented by its lambda bounds
            context.write(new Triple(t.v, t.u, t.w), new Quad(t.v, t.w, lambda3, lambda3));
        }
    }
}
