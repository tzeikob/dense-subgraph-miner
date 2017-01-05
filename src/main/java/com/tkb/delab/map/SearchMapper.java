package com.tkb.delab.map;

import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Sequence;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper getting as input a line consisting of an edge followed by the kappa
 * and lambda values followed by the triangles it participates along with the
 * support value, emitting the lambda values, the triangle and the support value
 * keyed by that edge. This function is discarding invalid or malformed input.
 * Be aware the vertices must be integer values only.
 *
 * Input: <code><v,u,kappa,lambda,v,u,w,support></code>
 *
 * Output: <code><v,u>, <kappa,lambda,v,u,w,support></code>
 *
 * @author Akis Papadopoulos
 */
public class SearchMapper extends Mapper<LongWritable, Text, Pair, Sequence> {

    /**
     * A map method getting as input an edge augmented by its lambda lower and
     * upper bounds followed by a triangle it participates and the support
     * value, emitting the lambda values, the triangle and the support value
     * keyed by that edge.
     *
     * @param key the offset of the line within the input file.
     * @param value a line in <code><v,u,kappa,lambda,v,u,w,support></code>
     * form.
     * @param context object to collect the output.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");

        if (tokens.length == 8) {
            try {
                // Extracting the edge
                int v = Integer.parseInt(tokens[0]);
                int u = Integer.parseInt(tokens[1]);

                // Extracting the lambda bounds
                int kappa = Integer.parseInt(tokens[2]);
                int lambda = Integer.parseInt(tokens[3]);

                // Extracting the triangle
                int tv = Integer.parseInt(tokens[4]);
                int tu = Integer.parseInt(tokens[5]);
                int tw = Integer.parseInt(tokens[6]);

                // Extracting the support value
                int support = Integer.parseInt(tokens[7]);

                // Emitting the lambdas, the triangle and the support value keyed by the edge
                context.write(new Pair(v, u), new Sequence(new IntWritable(kappa),
                        new IntWritable(lambda),
                        new IntWritable(tv),
                        new IntWritable(tu),
                        new IntWritable(tw),
                        new IntWritable(support)));
            } catch (NumberFormatException exc) {
            }
        }
    }
}
