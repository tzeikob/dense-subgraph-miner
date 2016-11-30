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
 * A local triangulation reducer.
 *
 * @author Akis Papadopoulos
 */
public class LocalTriangulationReducer extends Reducer<Triple, Pair, Triple, Pair> {

    /**
     * A reduce method collecting for an indexed partition a subset of sorted
     * edges, listing all the triangles within, emitting each triangle found.
     *
     * @param key indexes of the edge partition.
     * @param values the subset of unique sorted edges.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(Triple key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
        //Getting the list iterator
        Iterator<Pair> it = values.iterator();

        //Creating an empty edge set
        THashSet<Edge> edges = new THashSet<Edge>();

        //Iterating through the pairs list
        while (it.hasNext()) {
            //Getting the next pair
            Pair pair = it.next();

            //Adding the edge into the edge set
            edges.add(new Edge(pair.v, pair.u));
        }

        //Creating a forward tringulator
        Triangulator forward = new Forward();

        //Listing all sorted triangles within
        THashSet<Triangle> triangles = forward.list(edges);

        //Iterating through each triangle
        for (Triangle t : triangles) {
            //Emitting next triangle
            context.write(new Triple(t.v, t.u, t.w), null);
        }
    }
}
