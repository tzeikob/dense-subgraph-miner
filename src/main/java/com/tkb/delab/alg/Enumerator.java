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
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.THashSet;

/**
 * An interface for algorithms enumerating subgraphs induced by an edge set.
 *
 * @author Akis Papadopoulos
 */
public interface Enumerator {

    /**
     * An abstract method enumerating subgraphs induced by an edge set,
     * subgraphs mapped with an identifier number.
     *
     * @param edges a set of edges.
     * @return the list of subgraphs induced by edges within edge set.
     */
    public TIntObjectHashMap<THashSet<Edge>> enumerate(THashSet<Edge> edges);
}
