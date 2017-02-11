/*
 * Copyright 2016-2017 MarkLogic Corporation
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

import java.io.InputStream;
import java.util.Iterator;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.resultset.JSONInput;

/**
 * Returns quads as elements in an iterator, by processing the special purpose
 * SELECT ?g ?s ?p ?o pattern
 */
public class QuadsIterator implements Iterator<Quad> {

    private ResultSet results;
    private String graphName = null;

    public QuadsIterator(InputStream inputStream) {
        results = JSONInput.fromJSON(inputStream);
    }

    public QuadsIterator(String graphName, InputStream inputStream) {
        this.graphName = graphName;
        results = JSONInput.fromJSON(inputStream);
    }

    @Override
    public boolean hasNext() {
        return results.hasNext();
    }

    @Override
    public Quad next() {
        QuerySolution solution = results.next();
        Node s = solution.get("s").asNode();
        Node p = solution.get("p").asNode();
        Node o = solution.get("o").asNode();
        Node g = null;
        if (solution.get("g") != null) {
            g = solution.get("g").asNode();
        } else {
            if (graphName != null) {
                g = NodeFactory.createURI(graphName);
            }
        }
        Quad quad = new Quad(g, s, p, o);
        return quad;
    }

    @Override
    public void remove() {
        results.remove();
    }
}