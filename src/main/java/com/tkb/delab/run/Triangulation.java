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

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Triangulation(), args);

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
            System.out.println(" <input> \tpath in DFS to data of an undirected graph given as a list of edges per line");
            System.out.println(" <delimiter> \tcharacter used in order to separate the integer vertices of each edge");
            System.out.println(" <rho> \t\tnumber of disjoint vertex partitions, equal or greater than 3");
            System.out.println(" <sort> \ttrue to sort vertices in ascending order before processing otherwise false");
            System.out.println(" <tasks> \tnumber of the reducer tasks used");
            System.out.println(" <output> \tpath in DFS to save the triangle list including possible duplicates\n");
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
            triangulation.setNumReduceTasks(Integer.parseInt(args[4]));

            // Setting the input and output
            triangulation.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(triangulation, new Path(args[0]));

            triangulation.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(triangulation, new Path(args[5]));

            // Running the sprint job
            logger.info("Sprint job with entry name '" + triangulation.getJobName() + "' started");

            long start = System.currentTimeMillis();
            exitCode = triangulation.waitForCompletion(true) ? 0 : 1;
            long end = System.currentTimeMillis();

            if (exitCode != 0) {
                throw new AbnormalExitException("Abnormal exit occurred running "
                        + " sprint job with entry name '" + triangulation.getJobName() + "'");
            }

            // Getting the number of triangles enumerated
            long counter = triangulation.getCounters()
                    .findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS")
                    .getValue();

            logger.info("Sprint job with entry name '" + triangulation.getJobName() + "' completed in "
                    + new DecimalFormat(".###").format(((double) (end - start) / 1000 / 60))
                    + " min (" + (end - start) + " ms)"
                    + " found " + counter + " total triangles");
        } catch (AbnormalExitException exc) {
            logger.error(exc.getMessage(), exc);

            // Cleaning up the hdfs
            FileSystem.get(conf).delete(new Path(args[5]), true);

            return exitCode;
        }

        return exitCode;
    }
}
