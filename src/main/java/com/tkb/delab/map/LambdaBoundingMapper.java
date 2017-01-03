package com.tkb.delab.map;

import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Triple;
import com.tkb.delab.model.Triangle;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper emitting all possible edges followed by the input triangle they
 * belong to, discarding invalid or malformed triples. Be aware the vertices
 * must be integer values only.
 *
 * Input: <code><v,u,w></code>
 *
 * Output:
 * <code>
 * <v,u>, <v,u,w>
 * <u,w>, <v,u,w>
 * <v,w>, <v,u,w>
 * </code>
 *
 * @author Akis Papadopoulos
 */
public class LambdaBoundingMapper extends Mapper<LongWritable, Text, Pair, Triple> {

    // Input delimiter character
    private String delimiter;

    // Sorting vertices mode
    private boolean sort;

    /**
     * A map method getting as input a triangle of integer vertices, emitting
     * all the possible edges followed by that triangle.
     *
     * @param key the offset of the line within the input file.
     * @param value a line in <code><v,u,w></code> form.
     * @param context object to collect the output.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Extracting the triangle's vertices
        String line = value.toString();

        String[] tokens = line.split(delimiter);

        // Discarding invalid and malformed triples
        if (tokens.length == 3) {
            try {
                int v = Integer.parseInt(tokens[0]);
                int u = Integer.parseInt(tokens[1]);
                int w = Integer.parseInt(tokens[2]);

                Triangle t = new Triangle(v, u, w);

                // Sorting the vertices in ascending order
                if (sort) {
                    t.sort();
                }

                // Emitting the triangle's edges folowed by the triangle
                context.write(new Pair(t.v, t.u), new Triple(t.v, t.u, t.w));

                context.write(new Pair(t.u, t.w), new Triple(t.v, t.u, t.w));

                context.write(new Pair(t.v, t.w), new Triple(t.v, t.u, t.w));
            } catch (NumberFormatException exc) {
            }
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        delimiter = conf.get("input.text.delimiter", "\t");
        sort = conf.getBoolean("vertices.sorting.mode", true);
    }
}
