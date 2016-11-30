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
package com.tkb.delab.model;

/**
 * A set of counters.
 *
 * @author Akis Papadopoulos
 */
public enum Counter {

    //Number of edges not converged to an optimal valid lambda bound
    UNCONVERGED_EDGES,
    //Number of edges converged to an optimal valid lambda bound
    CONVERGED_EDGES,
    //Total sum of the lambda each edge has
    SUM_OF_LAMBDA
}
