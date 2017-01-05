package com.tkb.delab.reduce;

import com.tkb.delab.model.Triangle;
import com.tkb.delab.model.Counter;
import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Quad;
import com.tkb.delab.io.Sequence;
import com.tkb.delab.io.Triple;
import gnu.trove.set.hash.THashSet;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A reducer collecting for an edge all the triangles it participates along with
 * the corresponding lower and upper lambda values and the support value,
 * calculating the new lambda lower and upper bound values regarding the total
 * support value using a sequential or a binary search mode. This function emits
 * each triangle followed by the edge and its new lambda values. In addition it
 * counts each unconverged edge.
 *
 * Input: <code><v,u>, list of <kappa,lambda,v,u,w,support></code>
 *
 * Output:
 * <code>
 * <v,u,w>, <v,u,kappa,lambda>
 * <v,u,w>, <v,u,kappa,lambda>
 * ...
 * <v,u,w>, <v,u,kappa,lambda>
 * </code>
 */
public class SearchReducer extends Reducer<Pair, Sequence, Triple, Quad> {

    // Lambda search mode
    private int mode;

    /**
     * A reduce method collecting for an edge, its lambda bounds and its
     * triangles followed by the support value, calculating the total support
     * value and searching for the new optimal valid lambda upper bound,
     * emitting each triangle followed by the edge and the new lambda bounds.
     *
     * @param key an edge attached with its lambda bounds.
     * @param values the list of triangles along with the support value and the
     * lambda bounds.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(Pair key, Iterable<Sequence> values, Context context) throws IOException, InterruptedException {
        Iterator<Sequence> it = values.iterator();

        // Calculating the total support value along with other data
        int kappa = 0;
        int lambda = 0;
        int total = 0;

        THashSet<Triangle> triangles = new THashSet<Triangle>();

        while (it.hasNext()) {
            Sequence seq = it.next();

            // Saving the maximum lower and upper lambda bound
            int newKappa = ((IntWritable) seq.get(0)).get();

            if (kappa < newKappa) {
                kappa = newKappa;
            }

            int newLambda = ((IntWritable) seq.get(1)).get();

            if (lambda < newLambda) {
                lambda = newLambda;
            }

            // Saving the triangle
            int v = ((IntWritable) seq.get(2)).get();
            int u = ((IntWritable) seq.get(3)).get();
            int w = ((IntWritable) seq.get(4)).get();

            triangles.add(new Triangle(v, u, w));

            // Updating the total support value
            total += ((IntWritable) seq.get(5)).get();
        }

        // Choosing sequential (0) or binary (1) search mode
        if (mode == 0) {
            // Decrease upper lambda if not an optimal value
            if (total < lambda) {
                lambda -= 1;

                // Counting another unconverged edge
                context.getCounter(Counter.UNCONVERGED_EDGES).increment(1L);
            }
        } else if (mode == 1) {
            // Update lower and upper lambda if upper lambda not optimal
            if (kappa < lambda) {
                int mu = (lambda + kappa + 1) / 2;

                // Update lower or upper bounds regarding medium and total value
                if (total < mu) {
                    lambda = mu - 1;
                } else {
                    kappa = mu;
                }

                // Counting another unconverged edge
                context.getCounter(Counter.UNCONVERGED_EDGES).increment(1L);
            }
        }

        // Emitting each triangle followed by the edge and the new lambda bounds
        for (Triangle t : triangles) {
            context.write(new Triple(t.v, t.u, t.w), new Quad(key.v, key.u, kappa, lambda));
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
