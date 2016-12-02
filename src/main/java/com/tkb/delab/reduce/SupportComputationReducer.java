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
 * A support computation reducer.
 *
 * @author Akis Papadopoulos
 */
public class SupportComputationReducer extends Reducer<Triple, Quad, Pair, Sequence> {

    // Lambda search mode
    private int mode;

    /**
     * A reduce method collecting for a sorted triangle the incident augmented
     * sorted edges, setting their lambda bounds and checking if the medium
     * lambda bound is valid, emitting each one edge followed by the triangle
     * and the support value.
     *
     * @param key a sorted triangle.
     * @param values the triangle incident edges augmented by its lambda bounds.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(Triple key, Iterable<Quad> values, Context context) throws IOException, InterruptedException {
        // Getting the list iterator
        Iterator<Quad> it = values.iterator();

        // Creating an empty map of edges
        THashMap<Edge, Range> edges = new THashMap<Edge, Range>();

        // Iterating through the list of quads
        while (it.hasNext()) {
            // Getting the next quad
            Quad quad = it.next();

            // Creating the next edge
            Edge edge = new Edge(quad.v, quad.u);

            // Checking if the edge is already hashed in case of duplicated edge
            if (edges.containsKey(edge)) {
                // Getting the local lambda in case of local density estimation
                int lambda = quad.z;

                // Getting the already lambda bound range
                Range range = edges.get(edge);

                // Updating the lower and upper lambda bounds
                if (lambda > range.upper) {
                    range.upper = lambda;
                } else if (lambda < range.lower) {
                    range.lower = lambda;
                }
            } else {
                // Hashing the edge into the map at first time
                edges.put(edge, new Range(quad.w, quad.z));
            }
        }

        // Passing the edge set into an array of objects
        Object[] array = edges.keySet().toArray();

        // Iterating through the edge list
        for (int i = 0; i < array.length; i++) {
            // Getting the i-th edge
            Edge ei = (Edge) array[i];

            // Getting the lower lambda bound
            int kappa = edges.get(ei).lower;

            // Getting the upper lambda bound
            int lambda = edges.get(ei).upper;

            // Getting the other i-th edge
            Edge e1 = (Edge) array[(i + 1) % 3];

            // Getting the other i-th edge
            Edge e2 = (Edge) array[(i + 2) % 3];

            // Checking the lambda search mode
            if (mode == 0) {
                // Checking if the edge supported by the others checking the lambda bounds
                if (edges.get(ei).upper <= Math.min(edges.get(e1).upper, edges.get(e2).upper)) {
                    // Emitting the augmented edge followed by the lambda bounds, the triangle and the support value
                    context.write(new Pair(ei.v, ei.u), new Sequence(new IntWritable(kappa), new IntWritable(lambda),
                            new IntWritable(key.v), new IntWritable(key.u), new IntWritable(key.w), new IntWritable(1)));
                } else {
                    // Emitting the augmented edge followed by the lambda bounds, the triangle and the support value
                    context.write(new Pair(ei.v, ei.u), new Sequence(new IntWritable(kappa), new IntWritable(lambda),
                            new IntWritable(key.v), new IntWritable(key.u), new IntWritable(key.w), new IntWritable(0)));
                }
            } else if (mode == 1) {
                // Checking if the edge supported by the others checking the lambda medium bounds
                if (edges.get(ei).medium() <= Math.min(edges.get(e1).medium(), edges.get(e2).medium())) {
                    // Emitting the augmented edge followed by the lambda bounds, the triangle and the support value
                    context.write(new Pair(ei.v, ei.u), new Sequence(new IntWritable(kappa), new IntWritable(lambda),
                            new IntWritable(key.v), new IntWritable(key.u), new IntWritable(key.w), new IntWritable(1)));
                } else {
                    // Emitting the augmented edge followed by the lambda bounds, the triangle and the support value
                    context.write(new Pair(ei.v, ei.u), new Sequence(new IntWritable(kappa), new IntWritable(lambda),
                            new IntWritable(key.v), new IntWritable(key.u), new IntWritable(key.w), new IntWritable(0)));
                }
            }
        }
    }

    /**
     * A method to setting up the environment for the reducer.
     *
     * @param context a helpful object reading job information from.
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        mode = conf.getInt("lambda.search.mode", 0);

        //Checking for an invalid search mode
        if (mode != 0 && mode != 1) {
            mode = 0;
        }
    }
}
