package com.tkb.delab.reduce;

import com.tkb.delab.alg.Forward;
import com.tkb.delab.alg.Triangulator;
import com.tkb.delab.model.Edge;
import com.tkb.delab.model.Triangle;
import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Triple;
import gnu.trove.set.hash.THashSet;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A reducer collecting a set of edges hashed into the same partition, listing
 * all the triangles applying a local triangulation algorithm. Be aware the
 * edges must be in the form in which the integer vertices should be order in
 * ascending order.
 *
 * Input: <code><i,j,k>, list of <v,u></code>
 *
 * Output:
 * <code>
 * <v,u,w>
 * <v,u,w>
 * ...
 * <v,u,w>
 * </code>
 *
 * @author Akis Papadopoulos
 */
public class LocalTriangulationReducer extends Reducer<Triple, Pair, Triple, Pair> {

    /**
     * A reduce method collecting for an indexed partition a subset of sorted
     * edges, listing all the triangles within and emitting each triangle found.
     *
     * @param key indexes of the edge partition.
     * @param values the subset of unique sorted edges.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(Triple key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
        Iterator<Pair> it = values.iterator();

        // Discarding duplicate edges
        THashSet<Edge> edges = new THashSet<Edge>();

        while (it.hasNext()) {
            Pair pair = it.next();

            edges.add(new Edge(pair.v, pair.u));
        }

        // Applying local tringulation
        Triangulator forward = new Forward();

        THashSet<Triangle> triangles = forward.list(edges);

        // Emitting all the triangles found
        for (Triangle t : triangles) {
            context.write(new Triple(t.v, t.u, t.w), null);
        }
    }
}
