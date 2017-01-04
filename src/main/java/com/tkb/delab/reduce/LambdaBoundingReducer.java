package com.tkb.delab.reduce;

import com.tkb.delab.model.Triangle;
import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Quad;
import com.tkb.delab.io.Triple;
import gnu.trove.set.hash.THashSet;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A reducer collecting triangles an edge belongs to marking it with an upper
 * (lambda) and lower (kappa) bound of lambda values indicating the total number
 * of triangles that edge participates. Be aware that this function assumes the
 * vertices are ordered in ascending order both for edges and triangles.
 *
 * Input: <code><v,u>, list of <v,u,w></code>
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
public class LambdaBoundingReducer extends Reducer<Pair, Triple, Triple, Quad> {

    /**
     * A reduce method collecting for a sorted edge all the sorted triangles it
     * participates, initializing its lambda bounds and emitting each sorted
     * triangle followed by that edge and the lambda values.
     *
     * @param key an edge with vertices sorted in ascending order.
     * @param values list of triangles with sorted vertices the edge
     * participates.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(Pair key, Iterable<Triple> values, Context context) throws IOException, InterruptedException {
        // Enumerating the triangles the edge belongs to
        Iterator<Triple> it = values.iterator();

        THashSet<Triangle> triangles = new THashSet<Triangle>();

        while (it.hasNext()) {
            Triple triple = it.next();

            // Discarding possible duplicates
            triangles.add(new Triangle(triple.v, triple.u, triple.w));
        }

        // Setting as lambda upper bound the number of triangles
        int lambda = triangles.size();

        // Emitting each triangle followed by the edge plus its lambda bounds
        for (Triangle t : triangles) {
            context.write(new Triple(t.v, t.u, t.w), new Quad(key.v, key.u, 1, lambda));
        }
    }
}
