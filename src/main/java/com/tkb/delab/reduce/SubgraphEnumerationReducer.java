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

import com.tkb.delab.alg.Enumerator;
import com.tkb.delab.alg.NaiveEnumerator;
import com.tkb.delab.model.Edge;
import com.tkb.delab.io.Pair;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.THashSet;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A subgraph enumeration reducer.
 *
 * @author Akis Papadopoulos
 */
public class SubgraphEnumerationReducer extends Reducer<IntWritable, Pair, Text, Pair> {

    /**
     * A reduce method collecting for a lambda value all the sorted edges
     * estimated with that lambda, discarding duplicates if any, enumerating all
     * the connected subgraphs, emitting each edge keyed by the lambda value and
     * the subgraph id number.
     *
     * @param key a lambda value.
     * @param values the list of edges scored with the lambda value.
     * @param context object to collect the output.
     */
    @Override
    public void reduce(IntWritable key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
        //Getting the list iterator
        Iterator<Pair> it = values.iterator();

        //Creating an empty hashset of unique edges
        THashSet<Edge> edges = new THashSet<Edge>();

        //Iterating through the list of edges
        while (it.hasNext()) {
            //Getting the next edge
            Pair pair = it.next();

            //Adding next edge into hashset
            edges.add(new Edge(pair.v, pair.u));
        }

        //Creating a naive subgraph enumerator
        Enumerator naive = new NaiveEnumerator();

        //Enumerating each connected subgraph within edge set
        TIntObjectHashMap<THashSet<Edge>> subgraphs = naive.enumerate(edges);

        //Getting the iterator
        TIntObjectIterator<THashSet<Edge>> sit = subgraphs.iterator();

        //Iterating through subgraph list
        while (sit.hasNext()) {
            //Getting the next subgraph's id
            int id = sit.key();

            //Getting the edge set subgraph induced by
            THashSet<Edge> edgeset = sit.value();

            //Iterating through the edge set
            for (Edge edge : edgeset) {
                //Emitting the next edge keyed by the lambda value followed by the subgraph id
                context.write(new Text(key + "," + id), new Pair(edge.v, edge.u));
            }
        }
    }
}
