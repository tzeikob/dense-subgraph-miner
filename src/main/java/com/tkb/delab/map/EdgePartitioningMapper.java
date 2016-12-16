package com.tkb.delab.map;

import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Triple;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper hashing sorted edges into multiple vertex partitions, discarding
 * loops and invalid malformed input. Be aware that the hashing is not a
 * 2-universal hashing ((alpha * v + beta) % prime) % rho).
 *
 * @author Akis Papadopoulos
 */
public class EdgePartitioningMapper extends Mapper<LongWritable, Text, Triple, Pair> {

    // Input delimiter character
    private String delimiter;

    // Number of vertex partitions
    private int rho;

    /**
     * A map method getting as input a sorted edge, hashing it into multiple
     * vertex partitions. The indexes of each partition is a superset of the set
     * induced by the hash values on the vertices of that edge.
     *
     * @param key the offset of the line within the input file.
     * @param value a line in <code><v, u></code> form.
     * @param context object to collect the output.
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Extracting the vertices of the edge
        String[] tokens = line.split(delimiter);

        try {
            int v = Integer.parseInt(tokens[0]);
            int u = Integer.parseInt(tokens[1]);

            if (v != u) {
                // Hashing the vertices
                int hv = v % rho;
                int hu = u % rho;

                // Hashing the edge into multiple vertex partitions
                for (int i = 0; i < rho; i++) {
                    for (int j = i + 1; j < rho; j++) {
                        for (int k = j + 1; k < rho; k++) {
                            if (((hv == i) || (hv == j) || (hv == k))
                                    && ((hu == i) || (hu == j) || (hu == k))) {
                                context.write(new Triple(i, j, k), new Pair(v, u));
                            }
                        }
                    }
                }
            }
        } catch (NumberFormatException exc) {
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        delimiter = conf.get("input.text.delimiter", "\t");
        rho = conf.getInt("vertex.partitions.number", 3);
    }
}
