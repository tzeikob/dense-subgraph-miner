package com.tkb.delab.run;

import com.tkb.delab.util.AbnormalExitException;
import com.tkb.delab.io.Pair;
import com.tkb.delab.map.EdgeUndirectionMapper;
import com.tkb.delab.reduce.EdgeUndirectionReducer;
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
 * A transformer converting an directed graph to an undirected one map/reduce
 * sprint.
 *
 * @author Akis Papadopoulos
 */
public class EdgeUndirection extends Configured implements Tool {

    /**
     * A main method to run the tool runner.
     *
     * @param args the command line arguments.
     */
    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new EdgeUndirection(), args);
        System.exit(code);
    }

    /**
     * A method to run the graph transformer sprint map/reduce jobs.
     *
     * @param args the command line arguments.
     */
    @Override
    public int run(String[] args) throws Exception {
        //Checking for the completion of the arguments
        if (args.length != 4) {
            System.err.printf("Usage: %s [generic options] <input> <delimiter> <rho> <tasks>\n", this.getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        //Creating a simple date formatter
        SimpleDateFormat dater = new SimpleDateFormat("yy/MM/dd HH:mm:ss");

        //Creating a simple float number formatter
        DecimalFormat decimaler = new DecimalFormat(".###");

        //Getting the configuration
        Configuration conf = this.getConf();

        //Setting the key-value separator character, default is tab
        conf.set("mapred.textoutputformat.separator", ",");

        //Setting the delimiter separator
        conf.set("dataset.text.delimiter", args[1]);

        //Setting the number of edge partitions
        conf.set("dataset.edgeset.rho", args[2]);

        //Storing the exit code returned by each job
        int exitCode = 0;

        try {
            //Creating the underectioning job
            Job d2u = new Job(conf, "d2u");
            d2u.setJarByClass(EdgeUndirection.class);

            //Setting the mapper class and the output key-value pair
            d2u.setMapperClass(EdgeUndirectionMapper.class);
            d2u.setMapOutputKeyClass(IntWritable.class);
            d2u.setMapOutputValueClass(Pair.class);

            //Setting the reducer class and output key-value pair
            d2u.setReducerClass(EdgeUndirectionReducer.class);
            d2u.setOutputKeyClass(Pair.class);
            d2u.setOutputValueClass(Pair.class);

            //Setting the number of reducer tasks
            d2u.setNumReduceTasks(Integer.parseInt(args[3]));

            //Setting the input and output file format
            d2u.setInputFormatClass(TextInputFormat.class);
            d2u.setOutputFormatClass(TextOutputFormat.class);

            //Setting the input path of the files, in hdfs
            FileInputFormat.addInputPath(d2u, new Path(args[0]));

            //Setting the output path, in hdfs
            FileOutputFormat.setOutputPath(d2u, new Path("out/miner/d2u/"));

            //OUT
            for (int i = 0; i < 60; i++) {
                System.out.print((char) 8226 + " ");
            }
            System.out.println();
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.EdgeUndirectionSprint: edge undirection sprint started");
            //OUT

            //Storing the starting time of the process
            long start = System.currentTimeMillis();

            //Run the job and wait for completion
            exitCode = d2u.waitForCompletion(true) ? 0 : 1;

            //Checking if the job completed successfuly
            if (exitCode != 0) {
                //Throwing an exception
                throw new AbnormalExitException("An abnormal exit exception occurred at edge undirection sprint");
            }

            //Storing the finish time of the process
            long end = System.currentTimeMillis();

            //OUT
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.EdgeUndirectionSprint: edge undirection sprint finished successfuly");
            System.out.printf("%-10s %s\n", "Input", args[0]);
            System.out.printf("%-10s %s\n", "Rho", args[2]);
            System.out.printf("%-10s %s\n", "Tasks", args[3]);
            System.out.printf("%-10s %s\n", "Ellapsed", decimaler.format(((double) (end - start) / 1000 / 60)) + " Min (" + (end - start) + " ms)");
            System.out.printf("%-10s %s\n", "Output", "out/miner/d2u/");
            //OUT
        } catch (AbnormalExitException ex) {
            //Deleting the previous hashing output from the hdfs
            FileSystem.get(conf).delete(new Path("out/miner/d2u"), true);

            //OUT
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.EdgeUndirectionSprint: " + ex.getMessage());
            //OUT

            //Returning from abnormal exit
            return exitCode;
        }

        return exitCode;
    }
}
