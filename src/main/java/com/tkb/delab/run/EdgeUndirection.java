package com.tkb.delab.run;

import com.tkb.delab.util.AbnormalExitException;
import com.tkb.delab.io.Pair;
import com.tkb.delab.map.EdgeUndirectionMapper;
import com.tkb.delab.reduce.EdgeUndirectionReducer;
import java.text.DecimalFormat;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * A map reduce sprint job entry, converting an directed to an undirected graph
 * removing duplicate edges, sorting them by the lowest vertex and hashing them
 * into disjoint edge partitions.
 *
 * @author Akis Papadopoulos
 */
public class EdgeUndirection extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(EdgeUndirection.class);

    public static final String name = "undirect";

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new EdgeUndirection(), args);

        System.exit(code);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            logger.error("Unable to run sprint job entry " + name + " with args " + Arrays.asList(args));
            logger.error("Please check the documentation, https://github.com/tzeikob/dense-subgraph-miner");
            logger.error("Usage: hadoop jar [genericOptions] <jar-file> " + name + " <input> <delimiter> <rho> <tasks> <output>\n");

            ToolRunner.printGenericCommandUsage(System.err);

            return -1;
        }

        // Setting configuration parameters
        Configuration conf = this.getConf();

        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("dataset.text.delimiter", args[1]);
        conf.set("dataset.edgeset.rho", args[2]);

        int exitCode = 0;

        try {
            // Creating the underectioning job
            Job undirect = new Job(conf, name);
            undirect.setJarByClass(EdgeUndirection.class);

            // Setting the mapper and the reducer
            undirect.setMapperClass(EdgeUndirectionMapper.class);
            undirect.setMapOutputKeyClass(IntWritable.class);
            undirect.setMapOutputValueClass(Pair.class);

            undirect.setReducerClass(EdgeUndirectionReducer.class);
            undirect.setOutputKeyClass(Pair.class);
            undirect.setOutputValueClass(Pair.class);
            undirect.setNumReduceTasks(Integer.parseInt(args[3]));

            // Setting the input and output
            undirect.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(undirect, new Path(args[0]));

            undirect.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(undirect, new Path(args[4]));

            String jobName = this.getClass().getSimpleName();

            logger.info("Sprint job " + jobName + " with entry name '" + name + "' started");

            long start = System.currentTimeMillis();
            exitCode = undirect.waitForCompletion(true) ? 0 : 1;
            long end = System.currentTimeMillis();

            if (exitCode != 0) {
                throw new AbnormalExitException("Abnormal exit occurred running "
                        + jobName + " sprint job with entry name '" + name + "'");
            }

            logger.info("Sprint job " + jobName + " with entry name '" + name + "' completed in "
                    + new DecimalFormat(".###").format(((double) (end - start) / 1000 / 60)) + " min (" + (end - start) + " ms)");
        } catch (AbnormalExitException exc) {
            logger.error(exc.getMessage(), exc);

            // Cleaning up the hdfs
            FileSystem.get(conf).delete(new Path(args[4]), true);

            return exitCode;
        }

        return exitCode;
    }
}
