package com.tkb.delab.run;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * A hadoop dispatcher of map reduce executable sprint jobs.
 *
 * Run as: hadoop jar <jar-file> <job-entry> [genericOptions] <args>
 *
 * @author Akis Papadopoulos
 */
public class Dispatcher {

    private static final Logger logger = Logger.getLogger(Dispatcher.class);

    // Sprint job entries map
    private static final Map<String, Class<?>> entries;

    static {
        // Setting up sprint job entries map
        entries = new HashMap<String, Class<?>>();

        entries.put(EdgeUndirection.class.getSimpleName(), EdgeUndirection.class);
        entries.put(Triangulation.class.getSimpleName(), Triangulation.class);
        entries.put(LambdaEstimation.class.getSimpleName(), LambdaEstimation.class);
        entries.put(LocalEstimation.class.getSimpleName(), LocalEstimation.class);
    }

    public static void main(String[] args) throws Exception {
        // Printing the license notice
        try (BufferedReader br = new BufferedReader(new FileReader("LICENSE"))) {
            String line = null;

            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException exc) {
            System.err.println("Error: Application aborted, missing LICENSE file.");
            System.exit(1);
        }

        try {
            // Invoking the chosen entry job
            final Class<?> entry = entries.get(args[0]);

            if (entry != null) {
                // Passing the rest of the arguments
                final Object arguments = Arrays.copyOfRange(args, 1, args.length);

                entry.getMethod("main", String[].class).invoke(null, arguments);
            } else {
                throw new IllegalArgumentException("No executable sprint job entry found");
            }
        } catch (Exception exc) {
            logger.error(exc.getMessage());
            logger.error("Unable to run sprint job entry with args " + Arrays.asList(args));
            logger.error("Please check the documentation, https://github.com/tzeikob/dense-subgraph-miner");
            logger.error("Usage: hadoop jar <jar-file> <job-entry> [genericOptions] <args>\n");

            System.out.println("Sprint job entries supported are");
            for (String key : entries.keySet()) {
                System.out.println(" " + key);
            }
            System.out.println();

            ToolRunner.printGenericCommandUsage(System.err);

            System.exit(1);
        }
    }
}
