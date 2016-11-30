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
 * A math library of general use.
 *
 * @author Akis Papadopoulos
 */
public final class BasicMath {

    /**
     * A basic method to calculate the factorial of a number.
     *
     * @param x an integer number.
     * @return the factorial of the number.
     */
    public static long factorial(int x) {
        //Initializing the factorial
        long fact = 1L;

        //Iterating through all the multiples
        for (int i = 2; i <= x; i++) {
            //Updating the factorial value
            fact *= i;
        }

        return fact;
    }
}
