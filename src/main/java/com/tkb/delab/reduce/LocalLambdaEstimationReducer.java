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
 * A reducer collecting a set of edges hashed into the same partition,
 * calculating the optimal local lambda density value regarding the triangles
 * each edge participating to. Be aware the edges must be in the form in which
 * the integer vertices should be order in ascending order.
 *
 * Input: <code><i,j,k>, list of <v,u></code>
 *
 * Output:
 * <code>
 * <v,u,w>, <v,u,kappa,lambda>
 * <v,u,w>, <v,u,kappa,lambda>
 * ...
 * <v,u,w>, <v,u,kappa,lambda>
 * </code>
 *
 * @author Akis Papadopoulos
 */
public class LocalLambdaEstimationReducer extends Reducer<Triple, Pair, Triple, Quad> {

    /**
     * A reduce method collecting for an indexed partition a subset of edges,
     * listing all the triangles within estimating the lambda lower and upper
     * bounds for each edge, emitting each triangle found three times one for
     * each of its incident edges attached with its lambda bounds.
     *
     * @param key indexes of the edge partition.
     * @param values the list of edges.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(Triple key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
        Iterator<Pair> it = values.iterator();

        // Collecting the edge set discarding duplicates
        THashSet<Edge> edges = new THashSet<Edge>();

        while (it.hasNext()) {
            Pair pair = it.next();

            edges.add(new Edge(pair.v, pair.u));
        }

        // Calculating the triangles
        Triangulator forward = new Forward();

        THashSet<Triangle> triangles = forward.list(edges);

        // Estimating the lambda density value of each edge
        EdgeDensityEstimator estimator = new BinaryEstimator(50);

        THashMap<Edge, AugmentedRange> lambdas = estimator.estimate(triangles);

        // For each triangle emit the incident edges by the lambda bounds
        for (Triangle t : triangles) {
            // Getting the lambda values fot the first edge
            Edge e1 = new Edge(t.v, t.u);

            AugmentedRange lambda1 = lambdas.get(e1);

            // Emitting the triangle followed by the edge attached with the lambda bounds
            context.write(new Triple(t.v, t.u, t.w), new Quad(t.v, t.u, lambda1.lower, lambda1.upper));

            // Getting the lambda values fot the second edge
            Edge e2 = new Edge(t.u, t.w);

            AugmentedRange lambda2 = lambdas.get(e2);

            // Emitting the triangle followed by the edge attached with the lambda bounds
            context.write(new Triple(t.v, t.u, t.w), new Quad(t.u, t.w, lambda2.lower, lambda2.upper));

            // Getting the lambda values fot the third edge
            Edge e3 = new Edge(t.v, t.w);

            AugmentedRange lambda3 = lambdas.get(e3);

            // Emitting the triangle followed by the edge attached with the lambda bounds
            context.write(new Triple(t.v, t.u, t.w), new Quad(t.v, t.w, lambda3.lower, lambda3.upper));
        }
    }
}
