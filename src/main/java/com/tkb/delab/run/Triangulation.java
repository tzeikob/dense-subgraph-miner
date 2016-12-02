package com.tkb.delab.run;

import com.tkb.delab.reduce.LocalTriangulationReducer;
import com.tkb.delab.map.EdgePartitioningMapper;
import com.tkb.delab.util.AbnormalExitException;
import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Triple;
import java.text.DecimalFormat;
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
 * A triangulation map/reduce sprint.
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
        if (args.length != 4) {
            logger.error("Usage: " + this.getClass() + " [generic options] <input> <delimiter> <rho> <tasks> <output>\n");
            
            ToolRunner.printGenericCommandUsage(System.err);
            
            return -1;
        }

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
                logger.info((char) 8226 + " ");
            }
            logger.info("triangulation sprint started");
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
            logger.info("triangulation sprint finished found " + triangles + " triangles");
            logger.info("Input: " + args[0]);
            logger.info("Rho: " + args[1]);
            logger.info("Tasks: " + args[2]);
            logger.info("Ellapsed: " + new DecimalFormat(".###").format(((double) (end - start) / 1000 / 60)) + " Min (" + (end - start) + " msec)");
            logger.info("Output: " + "out/miner/tri/");
            //OUT
        } catch (AbnormalExitException ex) {
            //Cleaning the hdfs
            FileSystem.get(conf).delete(new Path("out/miner/tri/"), true);

            //OUT
            logger.error(ex.getMessage(), ex);
            //OUT

            //Returning from abnormal exit
            return exitCode;
        }

        return exitCode;
    }
}
