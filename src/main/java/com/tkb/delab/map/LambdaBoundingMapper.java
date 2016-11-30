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
package com.tkb.delab.map;

import com.tkb.delab.io.Pair;
import com.tkb.delab.io.Triple;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A lambda bounding mapper.
 *
 * @author Akis Papadopoulos
 */
public class LambdaBoundingMapper extends Mapper<LongWritable, Text, Pair, Triple> {

    /**
     * A map method getting as input a sorted triangle, emitting its one of its
     * edges followed by the triangle.
     *
     * @param key the offset of the line within the input file.
     * @param value a line in <code><v,u,w></code> form.
     * @param context object to collect the output.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Getting the value as a raw line
        String line = value.toString();

        //Extracting all the tokens within this line
        String[] tokens = line.split(",");

        //Setting the first vertex
        int v = Integer.parseInt(tokens[0]);

        //Setting the second vertex
        int u = Integer.parseInt(tokens[1]);

        //Setting the third vertex
        int w = Integer.parseInt(tokens[2]);

        //Emitting the first edge folowed by the triangle
        context.write(new Pair(v, u), new Triple(v, u, w));

        //Emitting the second edge folowed by the triangle
        context.write(new Pair(u, w), new Triple(v, u, w));

        //Emitting the third edge folowed by the triangle
        context.write(new Pair(v, w), new Triple(v, u, w));
    }
}
