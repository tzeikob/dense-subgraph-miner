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
 * removing duplicate edges, sorting them by the lowest vertex, discarding
 * possible loops as well as invalid malformed input.
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
        String name = this.getClass().getSimpleName();
        
        if (args.length != 5) {
            logger.error("Unable to run sprint job entry " + name + " with args " + Arrays.asList(args));
            logger.error("Please check the documentation, https://github.com/tzeikob/dense-subgraph-miner");
            logger.error("Usage: hadoop jar <jar-file> " + name + " [genericOptions] <input> <delimiter> <rho> <tasks> <output>\n");
            
            System.out.println("Arguments required are");
            System.out.println(" <input> \tpath in DFS to data of an undirected graph given as a list of edges per line");
            System.out.println(" <delimiter> \tcharacter used in order to seperate the integer vertices of each edge");
            System.out.println(" <rho> \t\tnumber of disjoint edge partitions");
            System.out.println(" <tasks> \tnumber of the reducer tasks used");
            System.out.println(" <output> \tpath in DFS to save the edge list of the new undirected graph\n");
            
            ToolRunner.printGenericCommandUsage(System.err);

            return -1;
        }

        // Setting configuration parameters
        Configuration conf = this.getConf();

        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("input.text.delimiter", args[1]);
        conf.set("disjoint.partitions.number", args[2]);

        int exitCode = 0;

        try {
            // Setting up the undirection sprint job
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

            // Running the sprint job
            logger.info("Sprint job with entry name '" + name + "' started");

            long start = System.currentTimeMillis();
            exitCode = undirect.waitForCompletion(true) ? 0 : 1;
            long end = System.currentTimeMillis();

            if (exitCode != 0) {
                throw new AbnormalExitException("Abnormal exit occurred running "
                        + "sprint job with entry name '" + name + "'");
            }

            logger.info("Sprint job with entry name '" + name + "' completed in "
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
