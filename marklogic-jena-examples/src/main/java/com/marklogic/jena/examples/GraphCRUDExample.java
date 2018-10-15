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
package com.marklogic.jena.examples;

import java.io.StringReader;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.graph.GraphFactory;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;

public class GraphCRUDExample {
    
    private MarkLogicDatasetGraph dsg;

    public GraphCRUDExample() {
        dsg = ExampleUtils.loadPropsAndInit();
    }

    private void run() {
        // Make some triples
        dsg.clear();
        Node graphNode = NodeFactory.createURI("http://example.org/graphs/charles");
        
        String turtle = "@prefix foaf: <http://xmlns.com/foaf/0.1/> ."
                + "@prefix : <http://example.org/> ."
                +":charles a foaf:Person ; "
                + "        foaf:name \"Charles\" ;"
                + "        foaf:knows :jim ."
                + ":jim    a foaf:Person ;"
                + "        foaf:name \"Jim\" ;"
                + "        foaf:knows :charles .";
        
        System.out.println("Make a graph and load the turtle into it (client-side)");
        Graph graph = GraphFactory.createDefaultGraph();
        RDFDataMgr.read(graph,  new StringReader(turtle), "", Lang.TURTLE);

        System.out.println("Store the graph in MarkLogic.");
        dsg.addGraph(graphNode,  graph);
        
        System.out.println("Make a triple by hand.");
        Graph moreTriples = GraphFactory.createDefaultGraph();
        moreTriples.add(new Triple(
                NodeFactory.createURI("http://example.org/charles"),
                NodeFactory.createURI("http://example.org/hasDog"),
                NodeFactory.createURI("http://example.org/vashko")));
        
        System.out.println("Merge this graph with the original");
        dsg.mergeGraph(graphNode, moreTriples);
        
        System.out.println("Get it back into a newGraph (union of two original ones)");
        Graph retrievedGraph = dsg.getGraph(graphNode);
        
        System.out.println("Remove graph from MarkLogic");
        dsg.removeGraph(graphNode);
    }
    
    public static void main(String... args) {
        GraphCRUDExample example = new GraphCRUDExample();
        example.run();
     }

}
