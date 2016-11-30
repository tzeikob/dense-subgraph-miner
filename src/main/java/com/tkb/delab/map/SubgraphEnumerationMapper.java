package com.tkb.delab.map;

import com.tkb.delab.io.Pair;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A subgraph enumeration mapper.
 *
 * @author Akis Papadopoulos
 */
public class SubgraphEnumerationMapper extends Mapper<LongWritable, Text, IntWritable, Pair> {

    /**
     * A map method getting as input a lambda value followed by its edge,
     * emitting as it is.
     *
     * @param key the offset of the line within the input file.
     * @param value a line in <code><lambda,v,u></code> form.
     * @param context object to collect the output.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Getting the value as a raw line
        String line = value.toString();

        //Extracting all the tokens within this line
        String[] tokens = line.split(",");

        //Extracting the optimal lambda upper bound
        int lambda = Integer.parseInt(tokens[0]);

        //Setting the edge first vertex
        int v = Integer.parseInt(tokens[1]);

        //Setting the edge second vertex
        int u = Integer.parseInt(tokens[2]);

        //Emitting the lambda value followed by its edge
        context.write(new IntWritable(lambda), new Pair(v, u));
    }
}
