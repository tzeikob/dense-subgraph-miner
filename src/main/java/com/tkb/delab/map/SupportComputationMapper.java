package com.tkb.delab.map;

import com.tkb.delab.io.Quad;
import com.tkb.delab.io.Triple;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper getting as input a line consisting of a triangle followed by one of
 * its edges attached with the kappa and lambda values emitting the edge keyed
 * by the triangle, discarding invalid or malformed input. Be aware the vertices
 * must be integer values only.
 *
 * Input: <code><v,u,w,v,u,kappa,lambda></code>
 *
 * Output: <code><v,u,w>, <v,u,kappa,lambda></code>
 *
 * @author Akis Papadopoulos
 */
public class SupportComputationMapper extends Mapper<LongWritable, Text, Triple, Quad> {

    /**
     * A map method getting as input a triangle followed by one of its edges
     * attached with its kappa and lambda bounds, emitting as it is.
     *
     * @param key the offset of the line within the input file.
     * @param value a line in <code><v,u,w,v,u,kappa,lambda></code> form.
     * @param context object to collect the output.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");

        if (tokens.length == 7) {
            try {
                // Extracting the triangle
                int v = Integer.parseInt(tokens[0]);
                int u = Integer.parseInt(tokens[1]);
                int w = Integer.parseInt(tokens[2]);

                // Extracting the edge
                int ev = Integer.parseInt(tokens[3]);
                int eu = Integer.parseInt(tokens[4]);

                // Extracting the kappa and lambda bounds
                int kappa = Integer.parseInt(tokens[5]);
                int lambda = Integer.parseInt(tokens[6]);

                // Emitting the edge attached with its lambda bounds with key the triangle
                context.write(new Triple(v, u, w), new Quad(ev, eu, kappa, lambda));
            } catch (NumberFormatException exc) {
            }
        }
    }
}
