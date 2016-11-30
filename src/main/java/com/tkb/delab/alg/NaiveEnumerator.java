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
package com.tkb.delab.alg;

import com.tkb.delab.model.Edge;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.hash.TObjectHashIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.THashSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * A class implementing an naive algorithm enumerating all the connected
 * subgraphs induced by an edge set, assuming unique edges in which vertices
 * sorted lexicographically.
 *
 * @author Akis Papadopoulos
 */
public class NaiveEnumerator implements Enumerator {

    /**
     * A method enumerating all the connected subgraphs induced by an edge set,
     * mapping each subgraph with an identifier number.
     *
     * @param edges a set of edges.
     * @return the list of connected subgraphs induced by edges within edge set.
     */
    @Override
    public TIntObjectHashMap<THashSet<Edge>> enumerate(THashSet<Edge> edges) {
        //Creating an empty hashmap of subgraphs induced by an edge set
        TIntObjectHashMap<THashSet<Edge>> subgraphs = new TIntObjectHashMap<THashSet<Edge>>();

        //Initializing the first candidate subgraph id
        int next = 0;

        //Getting the iterator of the edge set
        TObjectHashIterator<Edge> it = edges.iterator();

        //Creating an empty hashset
        THashSet<Edge> set = new THashSet<Edge>();

        //Adding the first edge
        set.add(it.next());

        //Hashing the first edge in one-edge subgraph
        subgraphs.put(next++, set);

        //Iterating through the rest of the edge set
        while (it.hasNext()) {
            //Getting the next pivot edge
            Edge edge = it.next();

            //Marking pivot edge as not added yet
            boolean added = false;

            //Creating an empty list of subgraph ids pivot edge anchored with 
            TIntHashSet ids = new TIntHashSet();

            //Getting the subgrpahs iterator
            TIntObjectIterator<THashSet<Edge>> sit = subgraphs.iterator();

            //Iterating through the subgraphs
            while (sit.hasNext()) {
                //Getting the id of the next subgraph
                int id = sit.key();

                //Getting the edge set of the subgraph
                THashSet<Edge> edgeset = sit.value();

                //Checking if pivot edge not contained within subgraph
                if (!edgeset.contains(edge)) {
                    //Iterating through the edges within the subgraph
                    for (Edge e : edgeset) {
                        //Checking if the pivot edge anchored with the next edge
                        if (e.v == edge.v || e.v == edge.u || e.u == edge.v || e.u == edge.u) {
                            //Adding the subgraph id pivot edge anchored with
                            ids.add(id);

                            //Breaking the not needed iterations
                            break;
                        }
                    }
                } else {
                    //Marking the pivot edge as added
                    added = true;

                    //Breaking the not needed iterations
                    break;
                }
            }

            //Checking if the pivot edge anchored with two subgraphs
            if (ids.size() > 1) {
                //OPT
                //Getting the id of the first subgraph
                int id1 = ids.toArray()[0];

                //Getting the id of the second subgraph
                int id2 = ids.toArray()[1];

                //Getting the edge set of the first subgraph
                THashSet<Edge> set1 = subgraphs.get(id1);

                //Getting the edge set of the second subgraph
                THashSet<Edge> set2 = subgraphs.get(id2);

                //Merging the two subgraphs into one
                set1.addAll(set2);

                //Adding the pivot edge too
                set1.add(edge);

                //Removing the first subgraph from the hashmap
                subgraphs.remove(id1);

                //Removing the second subgraph from the hashmap
                subgraphs.remove(id2);

                //Hashing the new merged subgraph into the hashmap
                subgraphs.put(next++, set1);

                //Marking the pivot edge as added
                added = true;
                //OPT
            } else if (ids.size() == 1) {
                //Getting the id of the subgraph pivot edge anchored with
                int id = ids.toArray()[0];

                //Adding the edge into the edge set of the subgraph
                subgraphs.get(id).add(edge);

                //Marking the pivot edge as added
                added = true;
            }

            //Checking if the pivot edge isn't added already
            if (!added) {
                //Creating an empty set
                THashSet<Edge> hashset = new THashSet<Edge>();

                //Adding the pivot edge within
                hashset.add(edge);

                //Hashing the new one-edge subgraph within the hashmap
                subgraphs.put(next++, hashset);
            }
        }

        return subgraphs;
    }
}
