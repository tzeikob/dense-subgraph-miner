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
 * A lambda estimation binary search reducer.
 *
 * @author Akis Papadopoulos
 */
public class SearchReducer extends Reducer<Pair, Sequence, Triple, Quad> {

    //Lambda search mode
    private int mode;

    /**
     * A reduce method collecting for a sorted edge, its lambda bounds, all its
     * sorted triangles followed by the support value summing to the total
     * support and searching for an new optimal valid lambda upper bound,
     * emitting each of the triangles participates followed by the new lambda
     * bounds found
     *
     * @param key a sorted edge augmented by its lambda bounds.
     * @param values the list of edge triangles augmented by the support value
     * and the lambda bounds.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(Pair key, Iterable<Sequence> values, Context context) throws IOException, InterruptedException {
        //Getting the sequences iterator
        Iterator<Sequence> it = values.iterator();

        //Creating a lambda lower bound reference
        int kappa = 0;

        //Creating a lambda upper bound reference
        int lambda = 0;

        //Creating an empty list of triangles
        THashSet<Triangle> triangles = new THashSet<Triangle>();

        //Storing the total support of the edge
        int total = 0;

        //Checking if any sequence is provided
        if (it.hasNext()) {
            //Getting the first sequence
            Sequence seq = it.next();

            //Getting the lower lambda bound
            kappa = ((IntWritable) seq.get(0)).get();

            //Getting the upper lambda bound
            lambda = ((IntWritable) seq.get(1)).get();

            //Getting the vertices of the next tringle
            int v = ((IntWritable) seq.get(2)).get();
            int u = ((IntWritable) seq.get(3)).get();
            int w = ((IntWritable) seq.get(4)).get();

            //Adding next triangle into the set
            triangles.add(new Triangle(v, u, w));

            //Updating the support value
            total += ((IntWritable) seq.get(5)).get();
        }

        //Iterating through the rest of sequences
        while (it.hasNext()) {
            //Getting the next sequence
            Sequence seq = it.next();

            //Getting the new lower lambda bound
            int newKappa = ((IntWritable) seq.get(0)).get();

            //Updating the old lower lambda bound
            if (kappa < newKappa) {
                kappa = newKappa;
            }

            //Getting the new upper lambda bound
            int newLambda = ((IntWritable) seq.get(1)).get();

            //Updating the old upper lambda bound
            if (lambda < newLambda) {
                lambda = newLambda;
            }

            //Getting the vertices of the next tringle
            int v = ((IntWritable) seq.get(2)).get();
            int u = ((IntWritable) seq.get(3)).get();
            int w = ((IntWritable) seq.get(4)).get();

            //Adding next triangle into the set
            triangles.add(new Triangle(v, u, w));

            //Updating the total support value
            total += ((IntWritable) seq.get(5)).get();
        }

        //Counting another lambda bound
        //context.getCounter(Counter.SUM_OF_LAMBDA).increment(lambda);

        //Checking the lambda search mode
        if (mode == 0) {
            //Checking if the lambda upper bound is not valid and optimal
            if (total < lambda) {
                //Updating lambda upper bound
                lambda -= 1;

                //Counting another edge not converge
                context.getCounter(Counter.UNCONVERGED_EDGES).increment(1L);
            }
        } else if (mode == 1) {
            //Checking if the optimal lambda upper bound is not found
            if (kappa < lambda) {
                //Calculating the median lambda bound
                int mu = (lambda + kappa + 1) / 2;

                //Checking if the median bound is not valid
                if (total < mu) {
                    //Updating the lambda upper bound
                    lambda = mu - 1;
                } else {
                    //Updating the lambda lower bound
                    kappa = mu;
                }

                //Counting another edge not converge
                context.getCounter(Counter.UNCONVERGED_EDGES).increment(1L);
            }
        }

        //Iterating through the set of triangles
        for (Triangle t : triangles) {
            //Emitting next triangle followed by the augmented edge with the new lambda bounds
            context.write(new Triple(t.v, t.u, t.w), new Quad(key.v, key.u, kappa, lambda));
        }
    }

    /**
     * A method to setting up the environment for the reducer.
     *
     * @param context a helpful object reading job information from.
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        //Getting the configuration of the job
        Configuration conf = context.getConfiguration();

        //Reading the lambda search mode
        mode = conf.getInt("lambda.search.mode", 0);

        //Checking for an invalid search mode
        if (mode != 0 && mode != 1) {
            //Setting the default
            mode = 0;
        }
    }
}
