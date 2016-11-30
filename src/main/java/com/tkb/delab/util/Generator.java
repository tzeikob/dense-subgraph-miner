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
package com.tkb.delab.util;

/**
 * An interface defines a method to produce random numbers, random primes, prime
 * greater or equal than a limit etc.
 *
 * @author Akis Papadopoulos
 */
public interface Generator {

    /**
     * An abstract method generates numbers taking into account the limit.
     *
     * @param limit a limit bounding a range of possible numbers.
     * @return an integer number.
     */
    public int generate(int limit);
}
