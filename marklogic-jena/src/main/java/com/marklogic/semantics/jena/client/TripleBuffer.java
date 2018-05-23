/*
 * Copyright 2016-2018 MarkLogic Corporation
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

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.graph.GraphFactory;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class for buffer than handles deletes
 * and adds for graphs backed by MarkLogic
 */
public abstract class TripleBuffer extends TimerTask {

    class TriplesHashMap extends ConcurrentHashMap<Node, Graph> {

        public int triplesCount() {
            return values().stream().mapToInt( v -> v.size()).sum();
        }
    }

    protected TriplesHashMap cache;
    protected JenaDatabaseClient client;

    protected final static long DEFAULT_CACHE_SIZE = 199;
    protected long cacheSize = DEFAULT_CACHE_SIZE;
    protected final static long DEFAULT_CACHE_MILLIS = 750;
    protected final static long DEFAULT_INITIAL_DELAY = 750;
    protected long cacheMillis = DEFAULT_CACHE_MILLIS;
    protected Date lastCacheAccess = new Date();
    protected static Node DEFAULT_GRAPH_NODE = NodeFactory
            .createURI(MarkLogicDatasetGraph.DEFAULT_GRAPH_URI);

    private static Logger log = LoggerFactory
            .getLogger(TripleBuffer.class);

    public TripleBuffer(JenaDatabaseClient client) {
        super();
        this.cache = new TriplesHashMap();
        this.client = client;
    }

    public void setCacheInterval(long millis) {
        this.cacheMillis = millis;
    }

    @Override
    public void run() {
        Date now = new Date();
        if (cache.triplesCount() > cacheSize || cache.size() > 0
                && now.getTime() - lastCacheAccess.getTime() > cacheMillis) {
            log.debug("Flushing triples buffer.");
            flush();
        } else {
            return;
        }
    }

    protected abstract void flush();

    public void forceRun() {
        flush();
    }

    public synchronized void add(Node g, Node s, Node p, Node o) {
        Triple newTiple = new Triple(s, p, o);
        if (g == null) {
            g = DEFAULT_GRAPH_NODE;
        }
        if (cache.containsKey(g)) {
            cache.get(g).add(newTiple);
        } else {
            Graph graph = GraphFactory.createGraphMem();
            graph.add(newTiple);
            cache.put(g, graph);
        }
        if (cache.triplesCount() > cacheSize) {
            log.debug("Size of cache big enough to flush.");
            flush();
        }
    }
}
