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
