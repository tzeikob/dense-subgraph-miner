package com.tkb.delab.reduce;

import com.tkb.delab.model.Edge;
import com.tkb.delab.model.Range;
import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Quad;
import com.tkb.delab.io.Sequence;
import com.tkb.delab.io.Triple;
import gnu.trove.map.hash.THashMap;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A reducer collecting for a triangle all the edges participating in that
 * triangle along with the corresponding kappa and lambda values, checking for
 * each incident edge if its lambda value supported by the lambda values of the
 * other edges. This function is emitting each one of the edges followed by the
 * lambda values the triangle and the support value indicating that the edge is
 * supported (1) or not (0). This function takes care in case of duplicate edges
 * updating the lambda values accordingly.
 *
 * Input: <code><v,u,w> list of <v,u,kappa,lambda></code>
 *
 * Output:
 * <code>
 * <v,u>, <kappa,lambda,v,u,w,support>
 * <u,w>, <kappa,lambda,v,u,w,support>
 * <v,w>, <kappa,lambda,v,u,w,support>
 * </code>
 *
 * @author Akis Papadopoulos
 */
public class SupportComputationReducer extends Reducer<Triple, Quad, Pair, Sequence> {

    // Lambda search mode
    private int mode;

    /**
     * A reduce method collecting for a triangle the incident edges attached
     * with the corresponding lambda bounds checking if the lambda bound is
     * supported regarding the other edges.
     *
     * @param key a triangle.
     * @param values the incident edges attached with its lambda bounds.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(Triple key, Iterable<Quad> values, Context context) throws IOException, InterruptedException {
        Iterator<Quad> it = values.iterator();

        // Hashing for each edge the lambda bounds
        THashMap<Edge, Range> edges = new THashMap<Edge, Range>();

        while (it.hasNext()) {
            Quad quad = it.next();

            Edge edge = new Edge(quad.v, quad.u);

            if (edges.containsKey(edge)) {
                // Updating lambda values for duplicate edges
                int lambda = quad.z;

                // Checking to the already hashed lambda values
                Range range = edges.get(edge);

                if (lambda > range.upper) {
                    range.upper = lambda;
                } else if (lambda < range.lower) {
                    range.lower = lambda;
                }
            } else {
                edges.put(edge, new Range(quad.w, quad.z));
            }
        }

        // Iterating through each edge
        Object[] array = edges.keySet().toArray();

        for (int i = 0; i < array.length; i++) {
            // Getting the i-th edge lambda values
            Edge ei = (Edge) array[i];

            int kappa = edges.get(ei).lower;
            int medium = edges.get(ei).medium();
            int lambda = edges.get(ei).upper;

            // Getting the other edges participating in the triangle
            Edge e1 = (Edge) array[(i + 1) % 3];
            Edge e2 = (Edge) array[(i + 2) % 3];

            // Choosing sequential (0) or binary (1) search mode
            if (mode == 0) {
                // Saving the minimum lambda of the other edges
                int min = Math.min(edges.get(e1).upper, edges.get(e2).upper);

                // Checking if the edge is supported by the others edges
                if (lambda <= min) {
                    context.write(new Pair(ei.v, ei.u),
                            new Sequence(new IntWritable(kappa),
                                    new IntWritable(lambda),
                                    new IntWritable(key.v),
                                    new IntWritable(key.u),
                                    new IntWritable(key.w),
                                    new IntWritable(1)));
                } else {
                    context.write(new Pair(ei.v, ei.u),
                            new Sequence(new IntWritable(kappa),
                                    new IntWritable(lambda),
                                    new IntWritable(key.v),
                                    new IntWritable(key.u),
                                    new IntWritable(key.w),
                                    new IntWritable(0)));
                }
            } else if (mode == 1) {
                // Saving the minimum medium lambda of the other edges
                int min = Math.min(edges.get(e1).medium(), edges.get(e2).medium());

                // Checking if the edge is supported by the other edges
                if (medium <= min) {
                    context.write(new Pair(ei.v, ei.u),
                            new Sequence(new IntWritable(kappa),
                                    new IntWritable(lambda),
                                    new IntWritable(key.v),
                                    new IntWritable(key.u),
                                    new IntWritable(key.w),
                                    new IntWritable(1)));
                } else {
                    context.write(new Pair(ei.v, ei.u),
                            new Sequence(new IntWritable(kappa),
                                    new IntWritable(lambda),
                                    new IntWritable(key.v),
                                    new IntWritable(key.u),
                                    new IntWritable(key.w),
                                    new IntWritable(0)));
                }
            }
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        mode = conf.getInt("lambda.search.mode", 0);

        // Fallback to sequential mode
        if (mode < 0 || mode > 1) {
            mode = 0;
        }
    }
}
