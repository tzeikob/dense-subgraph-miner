package com.tkb.delab.reduce;

import com.tkb.delab.model.Edge;
import com.tkb.delab.io.Pair;
import gnu.trove.set.hash.THashSet;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A reducer collecting edges hashed into the same disjoint partition discarding
 * duplicates. Be aware we are assuming the edges coming with their vertices
 * sorted in ascending order.
 *
 * Input: <code><hash(v), list of <v,u></code>
 *
 * Output:
 * <code>
 * <v,u>
 * <v,u>
 * ...
 * <v,u>
 * </code>
 *
 * @author Akis Papadopoulos
 */
public class EdgeUndirectionReducer extends Reducer<IntWritable, Pair, Pair, Pair> {

    /**
     * A reduce method collecting a set of edges hashed into the same partition
     * discarding duplicates.
     *
     * @param key the index of the edge partition.
     * @param values the list of sorted edges hashed into partition.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(IntWritable key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
        Iterator<Pair> it = values.iterator();

        // Collecting unique edges
        THashSet<Edge> edges = new THashSet<Edge>();

        while (it.hasNext()) {
            Pair pair = it.next();

            // Discarding duplicates
            boolean added = edges.add(new Edge(pair.v, pair.u));

            if (added) {
                context.write(pair, null);
            }
        }
    }
}
