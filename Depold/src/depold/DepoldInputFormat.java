/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package depold;

import depold.THALS;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

public class DepoldInputFormat extends
        TextVertexInputFormat<LongWritable, THALS, FloatWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split,
                                               TaskAttemptContext context) {
        return new DepoldVertexReader();
    }

    class DepoldVertexReader extends
            TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
                    JSONException> {

        @Override
        protected JSONArray preprocessLine(Text line) throws JSONException {
            return new JSONArray(line.toString());
        }

        @Override
        protected LongWritable getId(JSONArray jsonVertex) throws JSONException,
                IOException {
            return new LongWritable(jsonVertex.getLong(0));
        }

        @Override
        protected THALS getValue(JSONArray jsonVertex) throws
                JSONException, IOException {

            return new THALS();
        }

        @Override
        protected Iterable<Edge<LongWritable, FloatWritable>> getEdges(
                JSONArray jsonVertex) throws JSONException, IOException {
            JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
            List<Edge<LongWritable, FloatWritable>> edges =
                    Lists.newArrayListWithCapacity(jsonEdgeArray.length());
            for (int i = 0; i < jsonEdgeArray.length(); ++i) {
                JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
                edges.add(EdgeFactory.create(new LongWritable(jsonEdge.getLong(0)),
                        new FloatWritable((float) jsonEdge.getDouble(1))));
            }
            return edges;
        }

        @Override
        protected Vertex<LongWritable, THALS, FloatWritable>
        handleException(Text line, JSONArray jsonVertex, JSONException e) {
            throw new IllegalArgumentException(
                    "Couldn't get vertex from line " + line, e);
        }

    }
}

