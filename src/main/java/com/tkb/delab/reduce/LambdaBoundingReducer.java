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
 * A lambda bounding reducer.
 *
 * @author Akis Papadopoulos
 */
public class LambdaBoundingReducer extends Reducer<Pair, Triple, Triple, Quad> {

    /**
     * A reduce method collecting for a sorted edge all the sorted triangles
     * participates, initializing its lambda bounds emitting each sorted
     * triangle followed by that edge.
     *
     * @param key a sorted edge.
     * @param values the list of sorted triangles edge participates.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(Pair key, Iterable<Triple> values, Context context) throws IOException, InterruptedException {
        // Getting the list iterator
        Iterator<Triple> it = values.iterator();

        // Creating an empty set of triangles
        THashSet<Triangle> triangles = new THashSet<Triangle>();

        // Iterating through the list of triples
        while (it.hasNext()) {
            // Getting the next triple
            Triple triple = it.next();

            // Adding next triangle into the set
            triangles.add(new Triangle(triple.v, triple.u, triple.w));
        }

        // Setting the edge lambda upper bound to the number of triangles
        int lambda = triangles.size();

        // Iterating through the set of triangles
        for (Triangle t : triangles) {
            // Emitting the next triangle followed by the edge augmented by its lambda bounds
            context.write(new Triple(t.v, t.u, t.w), new Quad(key.v, key.u, 1, lambda));
        }
    }
}
