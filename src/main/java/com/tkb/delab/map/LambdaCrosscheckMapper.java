package com.tkb.delab.map;

import com.tkb.delab.io.Pair;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A lambda estimation crosscheck mapper.
 *
 * @author Akis Papadopoulos
 */
public class LambdaCrosscheckMapper extends Mapper<LongWritable, Text, Pair, Pair> {

    /**
     * A map method getting as input a triangle followed by one of its edges
     * augmented by the lambda estimation value, binning the edge followed by
     * its lambda bound estimation and a method markup number.
     *
     * @param key the offset of the line within the input file.
     * @param value a comma separated line
     *              in <code><v,u,w,v,u,kappa,lambda,method></code> form.
     * @param context object to collect the output.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Getting the value as a raw line
        String line = value.toString();

        //Extracting all the tokens within this line
        String[] tokens = line.split(",");

        //Setting the edge first vertex
        int v = Integer.parseInt(tokens[3]);

        //Setting the edge second vertex
        int u = Integer.parseInt(tokens[4]);

        //Setting the edge lambda bound
        int lambda = Integer.parseInt(tokens[6]);
        
        //Setting the lambda estimation method markup number
        int method = Integer.parseInt(tokens[7]);

        //Passing the edge followed by the lambda upper bound estimation
        context.write(new Pair(v, u), new Pair(lambda, method));
    }
}
