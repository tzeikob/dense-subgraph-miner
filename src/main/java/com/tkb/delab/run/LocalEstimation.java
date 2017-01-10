package com.tkb.delab.run;

import com.tkb.delab.util.AbnormalExitException;
import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Quad;
import com.tkb.delab.io.Triple;
import com.tkb.delab.map.EdgePartitioningMapper;
import com.tkb.delab.reduce.LocalLambdaEstimationReducer;
import java.text.DecimalFormat;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * A map reduce sprint job entry, calculating an optimal local lambda value for
 * each edge, the number of triangles the edge belongs to in a disjoint
 * partition of edges, as a density factor of the subgraphs that edge belongs
 * to.
 *
 * @author Akis Papadopoulos
 */
public class LocalEstimation extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(LocalEstimation.class);

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new LocalEstimation(), args);

        System.exit(code);
    }

    @Override
    public int run(String[] args) throws Exception {
        String name = this.getClass().getSimpleName();

        if (args.length != 6) {
            logger.error("Unable to run sprint job entry " + name + " with args " + Arrays.asList(args));
            logger.error("Please check the documentation, https://github.com/tzeikob/dense-subgraph-miner");
            logger.error("Usage: hadoop jar <jar-file> " + name + " [genericOptions] <input> <delimiter> <rho> <sort> <tasks> <output>\n");

            System.out.println("Arguments required are");
            System.out.println(" <input> \tpath in DFS to data given as a list of edges per line");
            System.out.println(" <delimiter> \tcharacter used in order to separate the integer vertices of each edge");
            System.out.println(" <rho> \tnumber of disjoint vertex partitions");
            System.out.println(" <sort> \ttrue to sort vertices in ascending order before processing otherwise false");
            System.out.println(" <tasks> \tnumber of the reducer tasks used");
            System.out.println(" <output> \tpath in DFS to save the list of triangles along with the edges attached with the optimal lambda values\n");
            ToolRunner.printGenericCommandUsage(System.err);

            return -1;
        }

        // Setting configuration parameters
        Configuration conf = this.getConf();

        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("input.text.delimiter", args[1]);
        conf.set("vertex.partitions.number", args[2]);
        conf.set("vertices.sorting.mode", args[3]);

        int exitCode = 0;

        try {
            // Setting up the local lambda estimation job
            Job lambda = new Job(conf, name);
            lambda.setJarByClass(LocalEstimation.class);

            // Setting the mapper and the reducer
            lambda.setMapperClass(EdgePartitioningMapper.class);
            lambda.setMapOutputKeyClass(Triple.class);
            lambda.setMapOutputValueClass(Pair.class);

            lambda.setReducerClass(LocalLambdaEstimationReducer.class);
            lambda.setOutputKeyClass(Triple.class);
            lambda.setOutputValueClass(Quad.class);
            lambda.setNumReduceTasks(Integer.parseInt(args[4]));

            // Setting the input and output
            lambda.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(lambda, new Path(args[0]));

            lambda.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(lambda, new Path(args[5] + "/lambda"));

            // Running the sprint job
            logger.info("Sprint job with entry name '" + lambda.getJobName() + "' started");

            long start = System.currentTimeMillis();

            exitCode = lambda.waitForCompletion(true) ? 0 : 1;

            long end = System.currentTimeMillis();

            if (exitCode != 0) {
                throw new AbnormalExitException("Abnormal exit occurred running "
                        + " sprint job with entry name '" + lambda.getJobName() + "'");
            }

            logger.info("Sprint job with entry name '" + name + "' completed in "
                    + new DecimalFormat(".###").format(((double) (end - start) / 1000 / 60))
                    + " min (" + (end - start) + " ms)");
        } catch (AbnormalExitException exc) {
            logger.error(exc.getMessage(), exc);

            // Cleaning up the hdfs
            FileSystem.get(conf).delete(new Path(args[5]), true);

            return exitCode;
        }

        return exitCode;
    }
}
