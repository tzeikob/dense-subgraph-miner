/*
 * Miner: Dense Subgraph Enumeration MapReduce Tool
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.tkb.delab.run;

import com.tkb.delab.reduce.SupportComputationReducer;
import com.tkb.delab.reduce.SearchReducer;
import com.tkb.delab.reduce.LocalLambdaEstimationReducer;
import com.tkb.delab.map.SupportComputationMapper;
import com.tkb.delab.map.EdgePartitioningMapper;
import com.tkb.delab.map.SearchMapper;
import com.tkb.delab.model.Counter;
import com.tkb.delab.util.AbnormalExitException;
import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Quad;
import com.tkb.delab.io.Sequence;
import com.tkb.delab.io.Triple;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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

/**
 * A divide and conquer based edge lambda estimation map/reduce sprint.
 *
 * @author Akis Papadopoulos
 */
public class DivideAndConquer extends Configured implements Tool {

    /**
     * A main method to run the tool runner.
     *
     * @param args the command line arguments.
     */
    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new DivideAndConquer(), args);
        System.exit(code);
    }

    /**
     * A method to run the lambda estimation sprint map/reduce jobs.
     *
     * @param args the command line arguments.
     */
    @Override
    public int run(String[] args) throws Exception {
        //Checking for the completion of the arguments
        if (args.length != 5) {
            System.err.printf("Usage: %s [generic options] <input> <rho> <iterations> <tasks> <mode>\n", this.getClass().getSimpleName());
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

        //Setting the number of disjoint vertex partitions
        conf.set("dataset.vertexset.rho", args[1]);

        //Setting the lambda search mode
        conf.set("lambda.search.mode", args[4]);

        //Storing the exit code returned by each job
        int exitCode = 0;

        try {
            //Creating the local density estimation job
            Job loc = new Job(conf, "loc");
            loc.setJarByClass(DivideAndConquer.class);

            //Setting the mapper class and the output key-value pair
            loc.setMapperClass(EdgePartitioningMapper.class);
            loc.setMapOutputKeyClass(Triple.class);
            loc.setMapOutputValueClass(Pair.class);

            //Setting the reducer class and output key-value pair
            loc.setReducerClass(LocalLambdaEstimationReducer.class);
            loc.setOutputKeyClass(Triple.class);
            loc.setOutputValueClass(Quad.class);

            //Setting the number of reducer tasks
            loc.setNumReduceTasks(Integer.parseInt(args[3]));

            //Setting the input and output file format
            loc.setInputFormatClass(TextInputFormat.class);
            loc.setOutputFormatClass(TextOutputFormat.class);

            //Setting the input path of the files, in hdfs
            FileInputFormat.addInputPath(loc, new Path(args[0]));

            //Setting the output path, in hdfs as input to the next phase
            FileOutputFormat.setOutputPath(loc, new Path("out/miner/dnc/lambda/"));

            //OUT
            for (int i = 0; i < 60; i++) {
                System.out.print((char) 8226 + " ");
            }
            System.out.println();
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.DivideAndConquerSprint: local lambda estimation sprint started");
            //OUT

            //Storing the starting time of the process
            long start = System.currentTimeMillis();

            //Run the job and wait for completion
            exitCode = loc.waitForCompletion(true) ? 0 : 1;

            //Checking for an abnormal exit code
            if (exitCode != 0) {
                //Throwing an exception
                throw new AbnormalExitException("An abnormal exit exception occurred at local lambda estimation sprint");
            }

            //OUT
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.DivideAndConquerSprint: local lambda estimation sprint finished");
            //OUT

            //Setting the max iterations requested
            int max = Integer.parseInt(args[2]);

            //Ititializaing the iterations counter
            int iterations = 0;

            //Initializing the number of unconverged edges
            long unconverged = Integer.MAX_VALUE;

            //Iterating to find the optimal valid lambda upper bounds
            while (unconverged > 0 && iterations < max) {
                //OUT
                for (int i = 0; i < 60; i++) {
                    System.out.print((char) 8226 + " ");
                }
                System.out.println();
                System.out.println(dater.format(new Date()) + " INFO miner.sprints.DivideAndConquerSprint: loop " + (iterations + 1) + " started");
                //OUT

                //Creating the support job
                Job sup = new Job(conf, "sup-" + (iterations + 1));
                sup.setJarByClass(DivideAndConquer.class);

                //Setting the mapper class and the output key-value pair
                sup.setMapperClass(SupportComputationMapper.class);
                sup.setMapOutputKeyClass(Triple.class);
                sup.setMapOutputValueClass(Quad.class);

                //Setting the reducer class and output key-value pair
                sup.setReducerClass(SupportComputationReducer.class);
                sup.setOutputKeyClass(Pair.class);
                sup.setOutputValueClass(Sequence.class);

                //Setting the number of reducer tasks
                sup.setNumReduceTasks(Integer.parseInt(args[3]));

                //Setting the input and output file format
                sup.setInputFormatClass(TextInputFormat.class);
                sup.setOutputFormatClass(TextOutputFormat.class);

                //Setting the input path of the files, in hdfs
                FileInputFormat.addInputPath(sup, new Path("out/miner/dnc/lambda/"));

                //Deleting the previous stored support output from hdfs
                FileSystem.get(conf).delete(new Path("out/miner/dnc/sup/"), true);

                //Setting the output path, in hdfs as input to the next phase
                FileOutputFormat.setOutputPath(sup, new Path("out/miner/dnc/sup/"));

                //OUT
                System.out.println(dater.format(new Date()) + " INFO miner.sprints.DivideAndConquerSprint: support computation sprint started");
                //OUT

                //Run the job and wait for completion
                exitCode = sup.waitForCompletion(true) ? 0 : 1;

                //Checking for an abnormal exit code
                if (exitCode != 0) {
                    //Throwing an exception
                    throw new AbnormalExitException("An abnormal exit exception occurred in loop " + (iterations + 1) + " at support computation sprint");
                }

                //OUT
                System.out.println(dater.format(new Date()) + " INFO miner.sprints.DivideAndConquerSprint: support computation sprint finished");
                //OUT

                //Creating the binray search job
                Job sea = new Job(conf, "sea-" + (iterations + 1));
                sea.setJarByClass(DivideAndConquer.class);

                //Setting the mapper class and the output key-value pair
                sea.setMapperClass(SearchMapper.class);
                sea.setMapOutputKeyClass(Pair.class);
                sea.setMapOutputValueClass(Sequence.class);

                //Setting the reducer class and output key-value pair
                sea.setReducerClass(SearchReducer.class);
                sea.setOutputKeyClass(Triple.class);
                sea.setOutputValueClass(Quad.class);

                //Setting the number of reducer tasks
                sea.setNumReduceTasks(Integer.parseInt(args[3]));

                //Setting the input and output file format
                sea.setInputFormatClass(TextInputFormat.class);
                sea.setOutputFormatClass(TextOutputFormat.class);

                //Setting the input path of the files, in hdfs
                FileInputFormat.addInputPath(sea, new Path("out/miner/dnc/sup/"));

                //Deleting the previous search output from the hdfs
                FileSystem.get(conf).delete(new Path("out/miner/dnc/lambda/"), true);

                //Setting the output path, in hdfs as input to the next phase
                FileOutputFormat.setOutputPath(sea, new Path("out/miner/dnc/lambda/"));

                //OUT
                System.out.println(dater.format(new Date()) + " INFO miner.sprints.DivideAndConquerSprint: binary search sprint started");
                //OUT

                //Run the job and wait for completion
                exitCode = sea.waitForCompletion(true) ? 0 : 1;

                //Checking for an abnormal exit code
                if (exitCode != 0) {
                    //Throwing an exception
                    throw new AbnormalExitException("An abnormal exit exception occurred in loop " + (iterations + 1) + " at binary search sprint");
                }

                //Getting the number of unconverged edges
                unconverged = sea.getCounters().findCounter(Counter.UNCONVERGED_EDGES).getValue();

                //OUT
                System.out.println(dater.format(new Date()) + " INFO miner.sprints.DivideAndConquerSprint: binary search sprint finished");
                if (unconverged > 0) {
                    System.out.println(dater.format(new Date()) + " INFO miner.sprints.DivideAndConquerSprint: loop " + (iterations + 1) + " finished with " + unconverged + " unconverged edges");
                } else {
                    System.out.println(dater.format(new Date()) + " INFO miner.sprints.DivideAndConquerSprint: process converged after " + (iterations + 1) + " loops");
                }
                //OUT

                //Updating the iterations counter
                iterations++;
            }

            //Storing the finish time of the process
            long end = System.currentTimeMillis();

            //Deleting the final not needed support output from the hdfs
            FileSystem.get(conf).delete(new Path("out/miner/dnc/sup/"), true);

            //OUT
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.DivideAndConquerSprint: process finished successfuly");
            System.out.printf("%-10s %s\n", "Input", args[0]);
            System.out.printf("%-10s %s\n", "Rho", args[1]);
            System.out.printf("%-10s %s\n", "Iterations", args[2]);
            System.out.printf("%-10s %s\n", "Tasks", args[3]);
            System.out.printf("%-10s %s\n", "Mode", args[4]);
            System.out.printf("%-10s %s\n", "Ellapsed", decimaler.format(((double) (end - start) / 1000 / 60)) + " Min (" + (end - start) + " msec)");
            System.out.printf("%-10s %s\n", "Output", "out/miner/dnc/lambda/");
            //OUT
        } catch (AbnormalExitException ex) {
            //Cleaning the hdfs
            FileSystem.get(conf).delete(new Path("out/miner/dnc/"), true);

            //OUT
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.DivideAndConquerSprint: " + ex.getMessage());
            //OUT

            //Returning from abnormal exit
            return exitCode;
        }

        return exitCode;
    }
}
