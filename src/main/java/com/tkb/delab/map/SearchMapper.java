package com.tkb.delab.map;

import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Sequence;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A lambda estimation binary search mapper.
 *
 * @author Akis Papadopoulos
 */
public class SearchMapper extends Mapper<LongWritable, Text, Pair, Sequence> {

    /**
     * A map method getting as input a sorted edge augmented by its lambda lower
     * and upper bounds followed one by its triangles augmented by the support
     * value, emitting as it is.
     *
     * @param key the offset of the line within the input file.
     * @param value a line in <code><v,u,kappa,lambda,v,u,w,support></code>
     * form.
     * @param context object to collect the output.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Getting the value as a raw line
        String line = value.toString();

        // Extracting all the tokens within this line
        String[] tokens = line.split(",");

        // Setting the edge first vertex
        int v = Integer.parseInt(tokens[0]);

        // Setting the edge second vertex
        int u = Integer.parseInt(tokens[1]);

        // Setting the edge lower lambda bound
        int kappa = Integer.parseInt(tokens[2]);

        // Setting the edge upper lambda bound
        int lambda = Integer.parseInt(tokens[3]);

        // Setting the triangle first vertex
        int tv = Integer.parseInt(tokens[4]);

        // Setting the triangle second vertex
        int tu = Integer.parseInt(tokens[5]);

        // Setting the triangle first vertex
        int tw = Integer.parseInt(tokens[6]);

        // Setting the triangle support value
        int support = Integer.parseInt(tokens[7]);

        // Emitting the egde augmented by the lambda bounds followed by the supported triangle
        context.write(new Pair(v, u), new Sequence(new IntWritable(kappa), new IntWritable(lambda),
                new IntWritable(tv), new IntWritable(tu), new IntWritable(tw), new IntWritable(support)));
    }
}
