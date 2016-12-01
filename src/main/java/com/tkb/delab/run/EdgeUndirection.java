package com.tkb.delab.run;

import com.tkb.delab.util.AbnormalExitException;
import com.tkb.delab.io.Pair;
import com.tkb.delab.map.EdgeUndirectionMapper;
import com.tkb.delab.reduce.EdgeUndirectionReducer;
import java.text.DecimalFormat;
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
 * A transformer converting an directed graph to an undirected one map/reduce
 * sprint.
 *
 * @author Akis Papadopoulos
 */
public class EdgeUndirection extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(EdgeUndirection.class);

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new EdgeUndirection(), args);
        System.exit(code);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            logger.error("Usage: " + this.getClass().getName()
                    + " [generic options] <input> <delimiter> <rho> <tasks> <output>");

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
            Job d2u = new Job(conf, "d2u");
            d2u.setJarByClass(EdgeUndirection.class);

            // Setting the mapper
            d2u.setMapperClass(EdgeUndirectionMapper.class);
            d2u.setMapOutputKeyClass(IntWritable.class);
            d2u.setMapOutputValueClass(Pair.class);

            // Setting the reducer
            d2u.setReducerClass(EdgeUndirectionReducer.class);
            d2u.setOutputKeyClass(Pair.class);
            d2u.setOutputValueClass(Pair.class);

            // Setting the number of reducer tasks
            d2u.setNumReduceTasks(Integer.parseInt(args[3]));

            // Setting the input
            d2u.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(d2u, new Path(args[0]));

            // Setting the output
            d2u.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(d2u, new Path(args[4]));

            logger.info("Edge undirection sprint started");

            // Starting the job and wait for completion
            long start = System.currentTimeMillis();
            exitCode = d2u.waitForCompletion(true) ? 0 : 1;
            long end = System.currentTimeMillis();

            // Throwing abnormal exit
            if (exitCode != 0) {
                throw new AbnormalExitException("An abnormal exit exception occurred at edge undirection sprint");
            }

            logger.info("Edge undirection sprint completed successfuly");
            logger.info("Input: " + args[0]);
            logger.info("Rho: " + args[2]);
            logger.info("Tasks: " + args[3]);
            logger.info("Ellapsed: " + new DecimalFormat(".###").format(((double) (end - start) / 1000 / 60)) + " Min (" + (end - start) + " ms)");
            logger.info("Output: " + args[4]);
        } catch (AbnormalExitException exc) {
            logger.error(exc.getMessage(), exc);

            // Cleaning up the hdfs
            FileSystem.get(conf).delete(new Path(args[4]), true);

            return exitCode;
        }

        return exitCode;
    }
}
