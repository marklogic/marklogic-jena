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

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;

/**
 * A timer task that flushes a cache of pending triple add statements
 * periodically.
 */
public class WriteCacheTimerTask extends TimerTask {

    private ConcurrentHashMap<Node, Graph> cache;
    private JenaDatabaseClient client;

    private static long DEFAULT_CACHE_SIZE = 500;
    private long cacheSize = DEFAULT_CACHE_SIZE;
    private static long DEFAULT_CACHE_MILLIS = 1000;
    private long cacheMillis = DEFAULT_CACHE_MILLIS;
    private Date lastCacheAccess = new Date();
    private static Node DEFAULT_GRAPH_NODE = NodeFactory
            .createURI(MarkLogicDatasetGraph.DEFAULT_GRAPH_URI);

    private static Logger log = LoggerFactory
            .getLogger(WriteCacheTimerTask.class);

    public WriteCacheTimerTask(JenaDatabaseClient client) {
        super();
        this.cache = new ConcurrentHashMap<Node, Graph>();
        this.client = client;
    }

    @Override
    public void run() {
        Date now = new Date();
        if (cache.size() > cacheSize || cache.size() > 0
                && now.getTime() - lastCacheAccess.getTime() > cacheMillis) {
            log.debug("Cache stale, flushing");
            flush();
        } else {
            return;
        }
    }

    private synchronized void flush() {
        for (Node graphNode : cache.keySet()) {
            log.debug("Persisting " + graphNode);
            client.mergeGraph(graphNode.getURI(), cache.get(graphNode));
        }
        lastCacheAccess = new Date();
        cache.clear();
    }

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
    }
}