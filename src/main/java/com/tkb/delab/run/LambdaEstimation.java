package com.tkb.delab.run;

import com.tkb.delab.reduce.SupportComputationReducer;
import com.tkb.delab.reduce.SearchReducer;
import com.tkb.delab.reduce.LambdaBoundingReducer;
import com.tkb.delab.map.SupportComputationMapper;
import com.tkb.delab.map.SearchMapper;
import com.tkb.delab.map.LambdaBoundingMapper;
import com.tkb.delab.model.Counter;
import com.tkb.delab.util.AbnormalExitException;
import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Quad;
import com.tkb.delab.io.Sequence;
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
 * A map reduce sprint job entry, calculating an optimal lambda value for each
 * edge, the number of triangles the edge belongs to, a density factor of the
 * subgraphs that edge belongs to. The job is a repetitive process re-estimating
 * lambda values until we reach a maximum bound of iterations or there is no
 * edge marked as unconverged.
 *
 * @author Akis Papadopoulos
 */
public class LambdaEstimation extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(LambdaEstimation.class);

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new LambdaEstimation(), args);

        System.exit(code);
    }

    @Override
    public int run(String[] args) throws Exception {
        String name = this.getClass().getSimpleName();

        if (args.length != 6) {
            logger.error("Unable to run sprint job entry " + name + " with args " + Arrays.asList(args));
            logger.error("Please check the documentation, https://github.com/tzeikob/dense-subgraph-miner");
            logger.error("Usage: hadoop jar <jar-file> " + name + " [genericOptions] <input> <delimiter> <iter> <mode> <tasks> <output>\n");

            System.out.println("Arguments required are");
            System.out.println(" <input> \tpath in DFS to data given as a list of triangles per line");
            System.out.println(" <delimiter> \tcharacter used in order to separate the integer vertices of each triangle");
            System.out.println(" <iter> \tnumber of maximum iterations");
            System.out.println(" <mode> \tlambda search mode");
            System.out.println(" <tasks> \tnumber of the reducer tasks used");
            System.out.println(" <output> \tpath in DFS to save the list of edges along with the lambda values\n");
            ToolRunner.printGenericCommandUsage(System.err);

            return -1;
        }

        // Setting configuration parameters
        Configuration conf = this.getConf();

        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("input.text.delimiter", args[1]);
        conf.set("lambda.search.mode", args[3]);

        int exitCode = 0;

        try {
            // Setting up the lambda initialization job
            Job init = new Job(conf, name + ".initialization");
            init.setJarByClass(LambdaEstimation.class);

            // Setting the mapper and the reducer
            init.setMapperClass(LambdaBoundingMapper.class);
            init.setMapOutputKeyClass(Pair.class);
            init.setMapOutputValueClass(Triple.class);

            init.setReducerClass(LambdaBoundingReducer.class);
            init.setOutputKeyClass(Triple.class);
            init.setOutputValueClass(Quad.class);
            init.setNumReduceTasks(Integer.parseInt(args[4]));

            // Setting the input and output
            init.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(init, new Path(args[0]));

            init.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(init, new Path(args[5] + "/lambda"));

            // Running the sprint job
            logger.info("Sprint job with entry name '" + init.getJobName() + "' started");

            long start = System.currentTimeMillis();

            exitCode = init.waitForCompletion(true) ? 0 : 1;

            if (exitCode != 0) {
                throw new AbnormalExitException("Abnormal exit occurred running "
                        + " sprint job with entry name '" + init.getJobName() + "'");
            }

            logger.info("Sprint job with entry name '" + init.getJobName() + "' finished");

            // Iterating to find the optimal valid lambda upper bounds
            int max = Integer.parseInt(args[2]);
            int iterations = 0;
            long unconverged = Integer.MAX_VALUE;

            while (unconverged > 0 && iterations < max) {
                // Setting up the lambda support job
                Job support = new Job(conf, name + "[" + (iterations + 1) + "].support");
                support.setJarByClass(LambdaEstimation.class);

                // Setting the mapper and the reducer
                support.setMapperClass(SupportComputationMapper.class);
                support.setMapOutputKeyClass(Triple.class);
                support.setMapOutputValueClass(Quad.class);

                support.setReducerClass(SupportComputationReducer.class);
                support.setOutputKeyClass(Pair.class);
                support.setOutputValueClass(Sequence.class);
                support.setNumReduceTasks(Integer.parseInt(args[4]));

                // Setting the input
                support.setInputFormatClass(TextInputFormat.class);
                FileInputFormat.addInputPath(support, new Path(args[5] + "/lambda"));

                // Setting the output, deleting the previous iteration output
                FileSystem.get(conf).delete(new Path(args[5] + "/tmp/"), true);
                support.setOutputFormatClass(TextOutputFormat.class);
                FileOutputFormat.setOutputPath(support, new Path(args[5] + "/tmp/"));

                // Running the sprint job
                logger.info("Sprint job with entry name '" + support.getJobName() + "' started");

                exitCode = support.waitForCompletion(true) ? 0 : 1;

                if (exitCode != 0) {
                    throw new AbnormalExitException("Abnormal exit occurred running "
                            + " sprint job with entry name '" + support.getJobName() + "'");
                }

                logger.info("Sprint job with entry name '" + support.getJobName() + "' finished");

                // Setting up the binray lambda search job
                Job search = new Job(conf, name + "[" + (iterations + 1) + "].search");
                search.setJarByClass(LambdaEstimation.class);

                // Setting the mapper and the reducer
                search.setMapperClass(SearchMapper.class);
                search.setMapOutputKeyClass(Pair.class);
                search.setMapOutputValueClass(Sequence.class);

                search.setReducerClass(SearchReducer.class);
                search.setOutputKeyClass(Triple.class);
                search.setOutputValueClass(Quad.class);
                search.setNumReduceTasks(Integer.parseInt(args[4]));

                // Setting the input
                search.setInputFormatClass(TextInputFormat.class);
                FileInputFormat.addInputPath(search, new Path(args[5] + "/tmp/"));

                // Setting the output, deleting previous iteration output
                FileSystem.get(conf).delete(new Path(args[5] + "/lambda/"), true);
                search.setOutputFormatClass(TextOutputFormat.class);
                FileOutputFormat.setOutputPath(search, new Path(args[5] + "/lambda/"));

                // Running the sprint job
                logger.info("Sprint job with entry name '" + search.getJobName() + "' started");

                exitCode = search.waitForCompletion(true) ? 0 : 1;

                if (exitCode != 0) {
                    throw new AbnormalExitException("Abnormal exit occurred running "
                            + " sprint job with entry name '" + search.getJobName() + "'");
                }

                // Getting the number of unconverged edges
                unconverged = search.getCounters().findCounter(Counter.UNCONVERGED_EDGES).getValue();

                logger.info("Sprint job with entry name '" + search.getJobName()
                        + "' finished with " + unconverged + " unconverged edges");

                iterations++;
            }

            long end = System.currentTimeMillis();

            logger.info("Sprint job with entry name '" + name + "' completed in "
                    + new DecimalFormat(".###").format(((double) (end - start) / 1000 / 60))
                    + " min (" + (end - start) + " ms)");
        } catch (AbnormalExitException exc) {
            logger.error(exc.getMessage(), exc);

            // Cleaning up the hdfs
            FileSystem.get(conf).delete(new Path(args[5]), true);

            return exitCode;
        } finally {
            // Deleting the final not needed support output from the hdfs
            FileSystem.get(conf).delete(new Path(args[5] + "/tmp/"), true);
        }

        return exitCode;
    }
}
