package com.tkb.delab.map;

import com.tkb.delab.io.Quad;
import com.tkb.delab.io.Triple;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * An edge hashing mapper.
 *
 * @author Akis Papadopoulos
 */
public class EdgeHashingMapper extends Mapper<LongWritable, Text, Triple, Quad> {

    /**
     * A map method getting as input a sorted triangle followed by on of the
     * edges augmented by its lambda bounds, emitting as it is.
     *
     * @param key the offset of the line within the input file.
     * @param value a line in <code><v, u, w, v, u, kappa, lambda></code> form.
     * @param context object to collect the output.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Getting the value as a raw line
        String line = value.toString();

        // Extracting all the tokens within this line
        String[] tokens = line.split(",");

        // Setting the triangle first vertex
        int v = Integer.parseInt(tokens[0]);

        // Setting the triangle second vertex
        int u = Integer.parseInt(tokens[1]);

        // Setting the triangle third vertex
        int w = Integer.parseInt(tokens[2]);

        // Setting the edge first vertex
        int ev = Integer.parseInt(tokens[3]);

        // Setting the edge second vertex
        int eu = Integer.parseInt(tokens[4]);

        // Setting the edge lambda bound
        int lambda = Integer.parseInt(tokens[6]);

        // Emitting the triangle follwoed by the edge augmented by its lambda bounds
        context.write(new Triple(v, u, w), new Quad(ev, eu, lambda, lambda));
    }
}
