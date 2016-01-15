/*
 * Copyright 2016 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.semantics.jena.client;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.jena.riot.WriterGraphRIOT;

import com.hp.hpl.jena.graph.Graph;
import com.marklogic.client.io.OutputStreamSender;

/**
 * Encapsulates a writer that can send an output stream from RIOT to a MarkLogic
 * Java CLient API OutputStreamHandle.
 */
public class OutputStreamRIOTSender implements OutputStreamSender {

    private WriterGraphRIOT writer;
    private Graph graph;

    public OutputStreamRIOTSender(WriterGraphRIOT writer) {
        this.writer = writer;
    }

    @Override
    public void write(OutputStream out) throws IOException {
        this.writer.write(out, graph, null, null, null);
    }

    public void setGraph(Graph graph) {
        this.graph = graph;
    }

}
