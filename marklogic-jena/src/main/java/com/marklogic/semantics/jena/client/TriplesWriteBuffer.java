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

import java.util.Date;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.graph.GraphFactory;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;

/**
 * a timer task that flushes a cache of pending triple add statements
 * periodically.
 */
public class TriplesWriteBuffer extends TripleBuffer {

    private static Logger log = LoggerFactory.getLogger(TriplesWriteBuffer.class);

    public TriplesWriteBuffer(JenaDatabaseClient client) {
        super(client);
    }

    protected synchronized void flush() {
        for (Node graphNode : cache.keySet()) {
            log.debug("Persisting " + graphNode);
            client.mergeGraph(graphNode.getURI(), cache.get(graphNode));
        }
        lastCacheAccess = new Date();
        cache.clear();
    }
}