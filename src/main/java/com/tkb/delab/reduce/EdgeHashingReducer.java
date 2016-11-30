package com.tkb.delab.reduce;

import com.tkb.delab.model.LambdaEdge;
import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Quad;
import com.tkb.delab.io.Triple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * An edge hashing reducer.
 *
 * @author Akis Papadopoulos
 */
public class EdgeHashingReducer extends Reducer<Triple, Quad, IntWritable, Pair> {

    /**
     * A reduce method collecting for a sorted triangle all the incident sorted
     * edges, emitting each one firstly by its optimal lambda upper bound, then
     * by the other edges lambda which are less than it.
     *
     * @param key a sorted triangle.
     * @param values the triangle augmented edges augmented by its lambda bound.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(Triple key, Iterable<Quad> values, Context context) throws IOException, InterruptedException {
        //Getting the list iterator
        Iterator<Quad> it = values.iterator();

        //Creating an empty list of augmented edges
        List<LambdaEdge> edges = new ArrayList<LambdaEdge>(3);

        //Iterating through the list of augmented edges
        while (it.hasNext()) {
            //Getting the next augmented edge
            Quad quad = it.next();

            //Adding next edge into the list
            edges.add(new LambdaEdge(quad.v, quad.u, quad.z, quad.z));
        }

        //Iterating through the incident edges of the triangle
        for (int i = 0; i < edges.size(); i++) {
            //Extracting the edge
            LambdaEdge edge = edges.get(i);

            //Extracting the optimal lambda upper bound of the i-th edge
            int lambda = edge.lambda;

            //Emitting the edge lambda followed by the edge
            context.write(new IntWritable(lambda), new Pair(edge.v, edge.u));

            //Checking the size of the edge list
            if (edges.size() == 3) {
                //Extracting the upper lambda bound of the other edge
                int lambda1 = edges.get((i + 1) % 3).lambda;

                if (lambda > lambda1) {
                    //Emitting the other edges lambda followed by the edge
                    context.write(new IntWritable(lambda1), new Pair(edge.v, edge.u));
                }

                //Extracting the upper lambda bound of the other edge
                int lambda2 = edges.get((i + 2) % 3).lambda;

                if (lambda > lambda2) {
                    //Emitting the other edges lambda followed by the edge
                    context.write(new IntWritable(lambda2), new Pair(edge.v, edge.u));
                }
            } else if (edges.size() == 2) {
                //Extracting the upper lambda bound of the other edge
                int lambda1 = edges.get((i + 1) % 2).lambda;

                if (lambda > lambda1) {
                    //Emitting the other edges lambda followed by the edge
                    context.write(new IntWritable(lambda1), new Pair(edge.v, edge.u));
                }
            }
        }
    }
}
