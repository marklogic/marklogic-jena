package com.marklogic.semantics.jena.client;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
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
            int tripleCount = 0;
            for (Graph g : values() ) {
                tripleCount += g.size();
            }
            return tripleCount;
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
