package com.tkb.delab.run;

import com.tkb.delab.reduce.LocalTriangulationReducer;
import com.tkb.delab.map.EdgePartitioningMapper;
import com.tkb.delab.util.AbnormalExitException;
import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Triple;
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
 * A map reduce sprint job entry, hashing each edge into multiple vertex
 * partitions and enumerating all the triangles within each partition using
 * local triangulation. Be aware the list of triangles may includes duplicates.
 *
 * @author Akis Papadopoulos
 */
public class Triangulation extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(Triangulation.class);

    public static final String name = "triangulation";

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Triangulation(), args);

        System.exit(code);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            logger.error("Unable to run sprint job entry " + name + " with args " + Arrays.asList(args));
            logger.error("Please check the documentation, https://github.com/tzeikob/dense-subgraph-miner");
            logger.error("Usage: hadoop jar <jar-file> " + name + " [genericOptions] <input> <delimiter> <rho> <tasks> <output>\n");

            ToolRunner.printGenericCommandUsage(System.err);

            return -1;
        }

        // Setting configuration parameters
        Configuration conf = this.getConf();

        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("input.text.delimiter", args[1]);
        conf.set("vertex.partitions.number", args[2]);

        int exitCode = 0;

        try {
            // Setting up the triangulation job
            Job triangulation = new Job(conf, name);
            triangulation.setJarByClass(Triangulation.class);

            // Setting the mapper and the reducer
            triangulation.setMapperClass(EdgePartitioningMapper.class);
            triangulation.setMapOutputKeyClass(Triple.class);
            triangulation.setMapOutputValueClass(Pair.class);

            triangulation.setReducerClass(LocalTriangulationReducer.class);
            triangulation.setOutputKeyClass(Triple.class);
            triangulation.setOutputValueClass(Pair.class);
            triangulation.setNumReduceTasks(Integer.parseInt(args[3]));

            // Setting the input and output
            triangulation.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(triangulation, new Path(args[0]));

            triangulation.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(triangulation, new Path(args[4]));

            // Running the sprint job
            String jobName = this.getClass().getSimpleName();

            logger.info("Sprint job " + jobName + " with entry name '" + name + "' started");

            long start = System.currentTimeMillis();
            exitCode = triangulation.waitForCompletion(true) ? 0 : 1;
            long end = System.currentTimeMillis();

            if (exitCode != 0) {
                throw new AbnormalExitException("Abnormal exit occurred running "
                        + jobName + " sprint job with entry name '" + name + "'");
            }

            // Getting the number of triangles enumerated
            long counter = triangulation.getCounters()
                    .findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS")
                    .getValue();

            logger.info("Sprint job " + jobName + " with entry name '" + name + "' completed in "
                    + new DecimalFormat(".###").format(((double) (end - start) / 1000 / 60)) + " min (" + (end - start) + " ms)"
                    + " found " + counter + " total triangles.");
        } catch (AbnormalExitException exc) {
            logger.error(exc.getMessage(), exc);

            // Cleaning up the hdfs
            FileSystem.get(conf).delete(new Path(args[4]), true);

            return exitCode;
        }

        return exitCode;
    }
}
