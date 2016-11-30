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
package com.tkb.delab.reduce;

import com.tkb.delab.model.Edge;
import com.tkb.delab.io.Pair;
import gnu.trove.set.hash.THashSet;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A graph direction transformer reducer.
 *
 * @author Akis Papadopoulos
 */
public class EdgeUndirectionReducer extends Reducer<IntWritable, Pair, Pair, Pair> {

    /**
     * A reduce method collecting for a indexed partition a subset of sorted
     * edges, emitting them discarding any duplicate.
     *
     * @param key the index of the edge partition.
     * @param values the list of sorted edges hashed into partition.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(IntWritable key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
        //Getting the list iterator
        Iterator<Pair> it = values.iterator();

        //Creating an empty hash set of edges
        THashSet<Edge> edges = new THashSet<Edge>();

        //Iterating through the edge list
        while (it.hasNext()) {
            //Getting the next value as an edge
            Pair pair = it.next();

            //Creating the edge
            Edge edge = new Edge(pair.v, pair.u);

            //Adding next edge into the hash set
            boolean added = edges.add(edge);

            //Checking if edge is added
            if (added) {
                //Emitting another unique edge
                context.write(new Pair(edge.v, edge.u), null);
            }
        }
    }
}
