package com.tkb.delab.map;

import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Triple;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A multi covered edge partition mapper.
 *
 * @author Akis Papadopoulos
 */
public class EdgePartitioningMapper extends Mapper<LongWritable, Text, Triple, Pair> {

    // Number of disjoint vertex partitions
    private int rho;
    
    // OPT
    // Prime number of the 2-universal hash function
    //private int prime;
    // Alpha constant of the 2-universal hash function
    //private int alpha;
    // Beta constant of the 2-universal hash function
    //private int beta;
    // OPT

    /**
     * A map method getting as input a sorted edge, binning it into multiple
     * partitions, where its indexes is a superset of the set induced by the
     * hash values on the end vertices of that edge.
     *
     * @param key the offset of the line within the input file.
     * @param value a line in <code><v, u></code> form.
     * @param context object to collect the output.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Getting the value as a raw line
        String line = value.toString();

        // Extracting all the tokens within this line
        String[] tokens = line.split(",");

        // Setting the first vertex
        int v = Integer.parseInt(tokens[0]);

        // Setting the second vertex
        int u = Integer.parseInt(tokens[1]);

        // Checking for loops
        if (v != u) {
            // OPT
            // Hashing the first vertex
            int hv = v % rho;
            //int hv = ((alpha * v + beta) % prime) % rho;
            // OPT

            // OPT
            // Hashing the second vertex
            int hu = u % rho;
            //int hu = ((alpha * u + beta) % prime) % rho;
            // OPT

            // Iterating through each edge partition
            for (int i = 0; i < rho; i++) {
                for (int j = i + 1; j < rho; j++) {
                    for (int k = j + 1; k < rho; k++) {
                        // Checking if end vertex hash values is a subset of partition indexes
                        if (((hv == i) || (hv == j) || (hv == k)) && ((hu == i) || (hu == j) || (hu == k))) {
                            // Emitting the edge into the partition keyed by given indexes
                            context.write(new Triple(i, j, k), new Pair(v, u));
                        }
                    }
                }
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
        // Getting the configuration of the job
        Configuration conf = context.getConfiguration();

        // Reading the number of disjoint vertex partitions
        rho = conf.getInt("dataset.vertexset.rho", 3);

        // Checking for the lower acceptable bound
        if (rho < 3) {
            // Setting the default value
            rho = 3;
        }

        // OPT
        // Reading the prime number of the 2-universal hash function
        //prime = conf.getInt("hashing.range.prime", 3);

        // Reading the alpha constant of the 2-universal hash function
        //alpha = conf.getInt("hashing.constants.alpha", 3);

        // Reading the beta constant of the 2-universal hash function
        //beta = conf.getInt("hashing.constants.beta", 3);
        // OPT
    }
}
