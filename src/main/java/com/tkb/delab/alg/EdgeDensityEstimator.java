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

import com.tkb.delab.model.AugmentedRange;
import com.tkb.delab.model.Edge;
import com.tkb.delab.model.Triangle;
import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.THashSet;

/**
 * An interface for algorithms scoring each edge within the given graph
 * participates at least in one triangle, as an indicator of the neighborhood
 * density.
 *
 * @author Akis Papadopoulos
 */
public interface EdgeDensityEstimator {

    /**
     * An abstract method scoring for each edge the neighborhood density, given
     * the triangles within.
     *
     * @param triangles a set of the triangles within the graph.
     * @return a hash map between edge and its lambda density score.
     */
    public THashMap<Edge, AugmentedRange> estimate(THashSet<Triangle> triangles);
}
