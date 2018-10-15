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
import org.apache.jena.graph.Triple;
import com.marklogic.client.semantics.SPARQLBindings;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Timer task that accumulates triples to remove from
 * the DatasetGraph and periodically executes SPARQL UPDATE
 * to delete them.
 */
public class TriplesDeleteBuffer extends TripleBuffer {

    private static Logger log = LoggerFactory.getLogger(TriplesDeleteBuffer.class);


    public TriplesDeleteBuffer(JenaDatabaseClient client) {
        super(client);
    }

    protected synchronized void flush() {
        if (cache.isEmpty()) { return; }
        int bindNumber = 1;
        SPARQLQueryDefinition qdef = client.newQueryDefinition("TMP");
        SPARQLBindings bindings = qdef.getBindings();
        StringBuffer entireQuery = new StringBuffer();
        entireQuery.append("DELETE DATA { ");
        for (Node graphNode : cache.keySet()) {
            Graph g = cache.get(graphNode);
            bindings.bind("g" + bindNumber, graphNode.getURI().toString());
            String graphWrapper = "GRAPH ?g" + bindNumber + " { ";

            List<String> graphPatterns = new ArrayList<>();
            Iterator<Triple> triples = g.find(Node.ANY, Node.ANY, Node.ANY);
            while (triples.hasNext()) {
                Triple t = triples.next();
                graphPatterns.add("?s" + bindNumber + " ?p" + bindNumber + " ?o" + bindNumber);
                bindings.bind("s" + bindNumber, t.getSubject().getURI().toString() );
                bindings.bind("p" + bindNumber, t.getPredicate().getURI().toString() );
                MarkLogicDatasetGraph.bindObject(qdef, "o" + bindNumber, t.getObject());
                bindNumber++;
            }
            graphWrapper += StringUtils.join(graphPatterns, " . ") + " } ";
            entireQuery.append(graphWrapper);
        }
        entireQuery.append("} ");
        // log.debug(entireQuery.toString());
        qdef.setSparql(entireQuery.toString());

        client.executeUpdate(qdef);
        lastCacheAccess = new Date();
        cache.clear();
    }

    public void forceRun() {
        flush();
    }

}
