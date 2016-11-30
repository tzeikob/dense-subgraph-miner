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
 * A pair of lower and upper values representing an interval range of bounds
 * augmented by the abstract support value.
 *
 * @author Akis Papadopoulos
 */
public class AugmentedRange extends Range {

    //Support value
    public int support;

    /**
     * A constructor setting up an interval.
     *
     * @param lower the lower value.
     * @param upper the upper value.
     * @param support the support value.
     */
    public AugmentedRange(int lower, int upper, int support) {
        super(lower, upper);
        this.support = support;
    }

    /**
     * A method returning an alphanumeric representation of the interval.
     *
     * @return the alphanumeric representation of the interval.
     */
    @Override
    public String toString() {
        return lower + "," + upper + "," + support;
    }
}
