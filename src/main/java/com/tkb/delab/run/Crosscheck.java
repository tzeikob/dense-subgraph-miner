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

import com.tkb.delab.reduce.LambdaCrosscheckReducer;
import com.tkb.delab.map.LambdaCrosscheckMapper;
import com.tkb.delab.util.AbnormalExitException;
import com.tkb.delab.io.Pair;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A lambda estimation crosscheck map/reduce sprint.
 *
 * @author Akis Papadopoulos
 */
public class Crosscheck extends Configured implements Tool {

    /**
     * A main method to run the tool runner.
     *
     * @param args the command line arguments.
     */
    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Crosscheck(), args);
        System.exit(code);
    }

    /**
     * A method to run the lambda estimation sprint crosscheck map/reduce jobs.
     *
     * @param args the command line arguments.
     */
    @Override
    public int run(String[] args) throws Exception {
        //Checking for the completion of the arguments
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <tasks>\n", this.getClass().getSimpleName());
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

        //Storing the exit code returned by each job
        int exitCode = 0;

        try {
            //Creating the crosscheck job
            Job cch = new Job(conf, "cch");
            cch.setJarByClass(Crosscheck.class);

            //Setting the mapper class and the output key-value pair
            cch.setMapperClass(LambdaCrosscheckMapper.class);
            cch.setMapOutputKeyClass(Pair.class);
            cch.setMapOutputValueClass(Pair.class);

            //Setting the reducer class and output key-value pair
            cch.setReducerClass(LambdaCrosscheckReducer.class);
            cch.setOutputKeyClass(Pair.class);
            cch.setOutputValueClass(Text.class);

            //Setting the number of reduce tasks
            cch.setNumReduceTasks(Integer.parseInt(args[1]));

            //Setting the input and output file format
            cch.setInputFormatClass(TextInputFormat.class);
            cch.setOutputFormatClass(TextOutputFormat.class);

            //Setting the input path of the files, in hdfs
            FileInputFormat.addInputPath(cch, new Path(args[0]));

            //Setting the output path, in hdfs as input to the next phase
            FileOutputFormat.setOutputPath(cch, new Path("out/miner/cch/"));

            //OUT
            for (int i = 0; i < 60; i++) {
                System.out.print((char) 8226 + " ");
            }
            System.out.println();
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.CrosscheckSprint: lambda crosscheck sprint started");
            //OUT

            //Storing the starting time of the process
            long start = System.currentTimeMillis();

            //Run the job and wait for completion
            exitCode = cch.waitForCompletion(true) ? 0 : 1;

            //Storing the finish time of the process
            long end = System.currentTimeMillis();

            //Checking for an abnormal exit code
            if (exitCode != 0) {
                //Throwing an exception
                throw new AbnormalExitException("An abnormal exit exception occurred at crooscheck sprint");
            }

            //OUT
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.CrosscheckSprint: lambda crosscheck sprint finished");
            System.out.printf("%-10s %s\n", "Input", args[0]);
            System.out.printf("%-10s %s\n", "Tasks", args[1]);
            System.out.printf("%-10s %s\n", "Ellapsed", decimaler.format(((double) (end - start) / 1000 / 60)) + " Min (" + (end - start) + " msec)");
            System.out.printf("%-10s %s\n", "Output", "out/miner/cch/");
            //OUT
        } catch (AbnormalExitException ex) {
            //Cleaning the hdfs
            FileSystem.get(conf).delete(new Path("out/miner/cch/"), true);

            //OUT
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.CrosscheckSprint: " + ex.getMessage());
            //OUT

            //Returning from abnormal exit
            return exitCode;
        }

        return exitCode;
    }
}
