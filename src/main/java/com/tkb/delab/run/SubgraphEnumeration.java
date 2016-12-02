package com.tkb.delab.run;

import com.tkb.delab.reduce.EdgeHashingReducer;
import com.tkb.delab.map.EdgeHashingMapper;
import com.tkb.delab.util.AbnormalExitException;
import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Quad;
import com.tkb.delab.io.Triple;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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

/**
 * A subgraph enumeration map/reduce sprint.
 *
 * @author Akis Papadopoulos
 */
public class SubgraphEnumeration extends Configured implements Tool {

    /**
     * A main method to run the tool runner.
     *
     * @param args the command line arguments.
     */
    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new SubgraphEnumeration(), args);
        System.exit(code);
    }

    /**
     * A method to run the subgraph enumeration sprint map/reduce jobs.
     *
     * @param args the command line arguments.
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <tasks>\n", this.getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // Creating a simple date formatter
        SimpleDateFormat dater = new SimpleDateFormat("yy/MM/dd HH:mm:ss");

        // Creating a simple float number formatter
        DecimalFormat decimaler = new DecimalFormat(".###");

        // Getting the configuration
        Configuration conf = this.getConf();

        // Setting the key-value separator character, default is tab
        conf.set("mapred.textoutputformat.separator", ",");

        // Storing the exit code returned by each job
        int exitCode = 0;

        try {
            // Creating the edge hashing job
            Job has = new Job(conf, "has");
            has.setJarByClass(SubgraphEnumeration.class);

            // Setting the mapper class and the output key-value pair
            has.setMapperClass(EdgeHashingMapper.class);
            has.setMapOutputKeyClass(Triple.class);
            has.setMapOutputValueClass(Quad.class);

            // Setting the reducer class and output key-value pair
            has.setReducerClass(EdgeHashingReducer.class);
            has.setOutputKeyClass(IntWritable.class);
            has.setOutputValueClass(Pair.class);

            // Setting the number of reducer tasks
            has.setNumReduceTasks(Integer.parseInt(args[1]));

            // Setting the input and output file format
            has.setInputFormatClass(TextInputFormat.class);
            has.setOutputFormatClass(TextOutputFormat.class);

            // Setting the input path of the files, in hdfs
            FileInputFormat.addInputPath(has, new Path(args[0]));

            // Setting the output path, in hdfs as input to the next phase
            FileOutputFormat.setOutputPath(has, new Path("out/miner/enu/has/"));

            // OUT
            for (int i = 0; i < 60; i++) {
                System.out.print((char) 8226 + " ");
            }
            System.out.println();
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.SubgraphEnumerationSprint: edge hashing sprint started");
            // OUT

            // Storing the starting time of the process
            long start = System.currentTimeMillis();

            // Run the job and wait for completion
            exitCode = has.waitForCompletion(true) ? 0 : 1;

            // Checking for an abnormal exit code
            if (exitCode != 0) {
                //Throwing an exception
                throw new AbnormalExitException("An abnormal exit exception occurred at edge hashing sprint");
            }

            // Storing the finish time of the process
            long end = System.currentTimeMillis();

            // OUT
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.SubgraphEnumerationSprint: edge hashing sprint finished");
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.SubgraphEnumerationSprint: process finished successfuly");
            System.out.printf("%-10s %s\n", "Input", args[0]);
            System.out.printf("%-10s %s\n", "Tasks", args[1]);
            System.out.printf("%-10s %s\n", "Ellapsed", decimaler.format(((double) (end - start) / 1000 / 60)) + " Min (" + (end - start) + " msec)");
            System.out.printf("%-10s %s\n", "Output", "out/miner/enu/has/");
            // OUT
        } catch (AbnormalExitException ex) {
            // Cleaning the hdfs
            FileSystem.get(conf).delete(new Path("out/miner/enu/"), true);

            // OUT
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.SubgraphEnumerationSprint: " + ex.getMessage());
            // OUT

            // Returning from abnormal exit
            return exitCode;
        }

        return exitCode;
    }
}
