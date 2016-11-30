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

import com.tkb.delab.reduce.LocalTriangulationReducer;
import com.tkb.delab.map.EdgePartitioningMapper;
import com.tkb.delab.util.AbnormalExitException;
import com.tkb.delab.io.Pair;
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
 * A triangulation map/reduce sprint.
 *
 * @author Akis Papadopoulos
 */
public class Triangulation extends Configured implements Tool {

    /**
     * A main method to run the tool runner.
     *
     * @param args the command line arguments.
     */
    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Triangulation(), args);
        System.exit(code);
    }

    /**
     * A method to run the triangulation sprint map/reduce jobs.
     *
     * @param args the command line arguments.
     */
    @Override
    public int run(String[] args) throws Exception {
        //Checking for the completion of the arguments
        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input> <rho> <tasks>\n", this.getClass().getSimpleName());
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

        //OPT
        //Getting the size of vertex set graph induced by
        //int size = Integer.parseInt(args[1]);

        //Creating a generator of prime numbers
        //Generator primes = new PrimeGenerator();

        //Picking a large prime number greater or equal than the vertex set size
        //int prime = primes.generate(size);

        //Setting the prime number of the 2-universal hash function
        //conf.setInt("hashing.range.prime", prime);

        //Setting the random generator seed value
        //long seed = Long.parseLong(args[3]);

        //Defining a reference of a random generator
        //Random generator;

        //Checking for a given or a random seed value
        //if (seed < 0L) {
        //Creating a generator with a random seed
        //generator = new Random();
        //} else {
        //Creating a generator with a given seed
        //generator = new Random(seed);
        //}

        //Picking the alpha constant at random avoiding zero values
        //int alpha;
        //do {
        //alpha = generator.nextInt(prime);
        //} while (alpha == 0);

        //Setting the alpha constant of the 2-universal hash function
        //conf.setInt("hashing.constants.alpha", alpha);

        //Picking the beta constant at random
        //int beta = generator.nextInt(prime);

        //Setting the beta constant of the 2-universal hash function
        //conf.setInt("hash.constants.beta", beta);
        //OPT

        //Storing the exit code returned by each job
        int exitCode = 0;

        try {
            //Creating the triangulation job
            Job tri = new Job(conf, "tri");
            tri.setJarByClass(Triangulation.class);

            //Setting the mapper class and the output key-value pair
            tri.setMapperClass(EdgePartitioningMapper.class);
            tri.setMapOutputKeyClass(Triple.class);
            tri.setMapOutputValueClass(Pair.class);

            //Setting the reducer class and output key-value pair
            tri.setReducerClass(LocalTriangulationReducer.class);
            tri.setOutputKeyClass(Triple.class);
            tri.setOutputValueClass(Pair.class);

            //Setting the number of reducer tasks
            tri.setNumReduceTasks(Integer.parseInt(args[2]));

            //Setting the input and output file format
            tri.setInputFormatClass(TextInputFormat.class);
            tri.setOutputFormatClass(TextOutputFormat.class);

            //Setting the input path of the files, in hdfs
            FileInputFormat.addInputPath(tri, new Path(args[0]));

            //Setting the output path, in hdfs as input to the next phase
            FileOutputFormat.setOutputPath(tri, new Path("out/miner/tri/"));

            //OUT
            for (int i = 0; i < 60; i++) {
                System.out.print((char) 8226 + " ");
            }
            System.out.println();
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.TriangulationSprint: triangulation sprint started");
            //OUT

            //Storing the starting time of the process
            long start = System.currentTimeMillis();

            //Run the job and wait for completion
            exitCode = tri.waitForCompletion(true) ? 0 : 1;

            //Storing the finish time of the process
            long end = System.currentTimeMillis();

            //Checking for an abnormal exit code
            if (exitCode != 0) {
                //Throwing an exception
                throw new AbnormalExitException("An abnormal exit exception occurred at triangulation sprint");
            }

            //Getting the number of triangles emitted by the distinction process
            long triangles = tri.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();

            //OUT
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.TriangulationSprint: triangulation sprint finished found " + triangles + " triangles");
            System.out.printf("%-10s %s\n", "Input", args[0]);
            System.out.printf("%-10s %s\n", "Rho", args[1]);
            System.out.printf("%-10s %s\n", "Tasks", args[2]);
            System.out.printf("%-10s %s\n", "Ellapsed", decimaler.format(((double) (end - start) / 1000 / 60)) + " Min (" + (end - start) + " msec)");
            System.out.printf("%-10s %s\n", "Output", "out/miner/tri/");
            //OUT
        } catch (AbnormalExitException ex) {
            //Cleaning the hdfs
            FileSystem.get(conf).delete(new Path("out/miner/tri/"), true);

            //OUT
            System.out.println(dater.format(new Date()) + " INFO miner.sprints.TriangulationSprint: " + ex.getMessage());
            //OUT

            //Returning from abnormal exit
            return exitCode;
        }

        return exitCode;
    }
}
