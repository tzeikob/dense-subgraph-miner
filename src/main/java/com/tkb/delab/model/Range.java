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
 * A pair of lower and upper values representing an interval range of bounds.
 * 
 * @author Akis Papadopoulos
 */
public class Range {
    
    //Lower bound value
    public int lower;
    //Upper bound value
    public int upper;

    /**
     * A constructor setting up the interval.
     * 
     * @param lower the lower bound.
     * @param upper the upper bound.
     */
    public Range(int lower, int upper) {
        this.lower = lower;
        this.upper = upper;
    }

    /**
     * A method returning the medium of the interval.
     *
     * @return the medium of the lower and upper bounds.
     */
    public int medium() {
        return (lower + upper + 1) / 2;
    }

    /**
     * A method returning an alphanumeric representation of the interval.
     *
     * @return the alphanumeric representation of the interval.
     */
    @Override
    public String toString() {
        return lower + "," + upper;
    }
}
