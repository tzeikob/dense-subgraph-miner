package com.tkb.delab.map;

import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Triple;
import com.tkb.delab.model.Edge;
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
 * Input: <code><v,u></code>
 *
 * Output:
 * <code>
 * <i,j,k>, <v,u>
 * <i,j,k>, <v,u>
 * ...
 * <i,j,k>, <v,u>
 * </code>
 *
 * @author Akis Papadopoulos
 */
public class EdgePartitioningMapper extends Mapper<LongWritable, Text, Triple, Pair> {

    // Input delimiter character
    private String delimiter;

    // Number of vertex partitions
    private int rho;

    // Sorting vetices mode
    private boolean sort;

    /**
     * A map method getting as input a sorted edge, hashing it into multiple
     * vertex partitions. The indexes of each partition is a superset of the set
     * induced by the hash values on the vertices of that edge.
     *
     * @param key the offset of the line within the input file.
     * @param value a line in <code><v, u></code> form.
     * @param context object to collect the output.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Extracting the vertices of the edge
        String[] tokens = value.toString().split(delimiter);

        if (tokens.length == 2) {
            try {
                int v = Integer.parseInt(tokens[0]);
                int u = Integer.parseInt(tokens[1]);

                if (v != u) {
                    Edge e = new Edge(v, u);

                    // Sorting vertices in ascending order
                    if (sort) {
                        e.sort();
                    }

                    // Hashing the vertices
                    int hv = e.v % rho;
                    int hu = e.u % rho;

                    // Hashing the edge into multiple vertex partitions
                    for (int i = 0; i < rho; i++) {
                        for (int j = i + 1; j < rho; j++) {
                            for (int k = j + 1; k < rho; k++) {
                                if (((hv == i) || (hv == j) || (hv == k))
                                        && ((hu == i) || (hu == j) || (hu == k))) {
                                    context.write(new Triple(i, j, k), new Pair(e.v, e.u));
                                }
                            }
                        }
                    }
                }
            } catch (NumberFormatException exc) {
            }
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        delimiter = conf.get("input.text.delimiter", "\t");
        rho = conf.getInt("vertex.partitions.number", 3);
        sort = conf.getBoolean("vertices.sorting.mode", true);

        // Fallback to default rho value
        if (rho < 3) {
            rho = 3;
        }
    }
}
