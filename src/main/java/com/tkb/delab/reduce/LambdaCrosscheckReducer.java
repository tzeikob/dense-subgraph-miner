package com.tkb.delab.reduce;

import com.tkb.delab.io.Pair;
import gnu.trove.map.hash.TIntIntHashMap;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A lambda crosscheck estimation reducer.
 *
 * @author Akis Papadopoulos
 */
public class LambdaCrosscheckReducer extends Reducer<Pair, Pair, Pair, Text> {

    /**
     * A reduce method collecting for a sorted edge all its lambda estimations
     * emitting it followed the lambda estimation in the method based order.
     *
     * @param key a sorted edge.
     * @param values the list of lambda estimations of the edge.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(Pair key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
        //Getting the list iterator
        Iterator<Pair> it = values.iterator();

        //Creating an empty hash map of lambda estimation values per method
        TIntIntHashMap lambdas = new TIntIntHashMap();

        //Iterating through the list of pairs
        while (it.hasNext()) {
            //Getting the next pair
            Pair pair = it.next();

            //Getting the lambda value
            int lambda = pair.v;

            //Getting the lambda estimation method markup
            int method = pair.u;

            //Hashing the lambda into the map
            lambdas.put(method, lambda);
        }

        //Emitting the edge followed by the lambda metadata values for each method
        context.write(new Pair(key.v, key.u), new Text(lambdas.get(0) + ","
                + lambdas.get(1) + ","
                + lambdas.get(2) + ","
                + lambdas.get(3)));
    }
}
