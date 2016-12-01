package com.tkb.delab.map;

import com.tkb.delab.io.Pair;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * A graph direction transformer mapper.
 *
 * @author Akis Papadopoulos
 */
public class EdgeUndirectionMapper extends Mapper<LongWritable, Text, IntWritable, Pair> {

    private static final Logger logger = Logger.getLogger(EdgeUndirectionMapper.class);

    // Input delimiter character
    private String delimiter;

    // Number of disjoint edge partitions
    private int rho;

    /**
     * A map method getting an edge as a pair of vertices, sorting and hashing
     * it by the lowest vertex into a disjoint edge partition, discarding loops
     * and any invalid formated edge.
     *
     * @param key the offset of the line within the input file.
     * @param value a line in <code><v, u></code> form.
     * @param context object to collect the output.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Extracting the input line into tokens
        String[] tokens = value.toString().split(delimiter);

        if (tokens.length == 2) {
            try {
                // Getting the source and target vertices
                int v = Integer.parseInt(tokens[0]);
                int u = Integer.parseInt(tokens[1]);

                // Hashing the edge by the lowest vertex
                if (v < u) {
                    int hv = v % rho;

                    context.write(new IntWritable(hv), new Pair(v, u));
                } else if (v > u) {
                    int hu = u % rho;

                    context.write(new IntWritable(hu), new Pair(u, v));
                } else {
                    logger.warn("Ignoring loop edge for input '" + value.toString() + "'");
                }

            } catch (NumberFormatException exc) {
                logger.warn("Invalid number format exception for input '" + value.toString() + "'");
            }
        } else {
            logger.warn("Invalid format exception for input '" + value.toString() + "'");
        }
    }

    /**
     * A method to setting up the environment for the mapper.
     *
     * @param context a helpful object reading job information from.
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        delimiter = conf.get("dataset.text.delimiter", ",");
        rho = conf.getInt("dataset.edgeset.rho", 1);
    }
}
