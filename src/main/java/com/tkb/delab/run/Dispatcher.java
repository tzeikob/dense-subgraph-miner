package com.tkb.delab.run;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * A hadoop dispatcher of map reduce executable sprint jobs.
 *
 * Run as: hadoop jar [genericOptions] <jar-file> <job-entry> <args>
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

        entries.put(EdgeUndirection.name, EdgeUndirection.class);
    }

    public static void main(String[] args) throws Exception {
        // Printing the license notice
        System.out.println("\nCopyright 2016 Akis Papadopoulos, github.com/tzeikob\n");
        System.out.println("Licensed under the Apache License, Version 2.0 (the \"License\"); you may not use this");
        System.out.println("file except in compliance with the License. You may obtain a copy of the License at \n");
        System.out.println("http://www.apache.org/licenses/LICENSE-2.0 \n");
        System.out.println("Unless required by applicable law or agreed to in writing, software distributed under the");
        System.out.println("License is distributed on an \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,");
        System.out.println("either express or implied. See the License for the specific language governing permissions");
        System.out.println("and limitations under the License.\n");

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
            logger.error("Usage: hadoop jar [genericOptions] <jar-file> <job-entry> <args>\n");

            ToolRunner.printGenericCommandUsage(System.err);

            System.exit(1);
        }
    }
}