package com.tkb.delab.map;

import com.tkb.delab.io.Pair;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A graph direction transformer mapper.
 *
 * @author Akis Papadopoulos
 */
public class EdgeUndirectionMapper extends Mapper<LongWritable, Text, IntWritable, Pair> {

    //Delimiter separator
    private String delimiter;
    //Number of disjoint edge partitions
    private int rho;

    /**
     * A map method getting an unsorted edge sorting it and hashing it by its
     * first ordered vertex into a disjoint edge partition, discarding also any
     * loop edge.
     *
     * @param key the offset of the line within the input file.
     * @param value a line in <code><v, u></code> form.
     * @param context object to collect the output.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Getting the value as a raw line
        String line = value.toString();

        //Extracting all the tokens within this line
        String[] tokens = line.split(delimiter);

        //Checking if the raw line represents an edge otherwise ignore
        if (tokens.length == 2) {
            //Setting the source vertex
            int v = Integer.parseInt(tokens[0]);

            //Setting the target vertex
            int u = Integer.parseInt(tokens[1]);

            //Ordering vertices within edge
            if (v < u) {
                //Hashing the first ordered vertex
                int hv = v % rho;

                //Emitting the sorted edge into the bin keyed by the hash value
                context.write(new IntWritable(hv), new Pair(v, u));
            } else if (v > u) {
                //Hashing the first ordered vertex
                int hu = u % rho;

                //Emitting the sorted edge into the bin keyed by the hash value
                context.write(new IntWritable(hu), new Pair(u, v));
            }
        }
    }

    /**
     * A method to setting up the environment for the mapper.
     *
     * @param context a helpful object reading job information from.
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        //Getting the configuration of the job
        Configuration conf = context.getConfiguration();

        //Reading the delimiter vertex separator
        delimiter = conf.get("dataset.text.delimiter", "\t");

        //Reading the number of edge partitions
        rho = conf.getInt("dataset.edgeset.rho", 1);

        //Checking for the lower acceptable bound
        if (rho < 1) {
            //Setting the default value
            rho = 1;
        }
    }
}
