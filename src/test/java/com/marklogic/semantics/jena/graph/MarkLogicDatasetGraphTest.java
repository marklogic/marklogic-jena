/*
 * Copyright 2015 MarkLogic Corporation
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
package com.marklogic.semantics.jena.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.apache.jena.riot.RDFDataMgr;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateRequest;
import com.marklogic.semantics.jena.JenaTestBase;

public class MarkLogicDatasetGraphTest extends JenaTestBase {

	@Test
	public void testFirstRead() {
		DatasetGraph datasetGraph = getJenaDatasetGraph("testData.trig");
		DatasetGraph markLogicDatasetGraph = getMarkLogicDatasetGraph("testData.trig");

		Iterator<Node> jenaGraphs = datasetGraph.listGraphNodes();
		Iterator<Node> markLogicGraphs = markLogicDatasetGraph.listGraphNodes();

		while (jenaGraphs.hasNext()) {
			Node jenaGraphNode = jenaGraphs.next();
			assertTrue(markLogicGraphs.hasNext());

			// list must be at least as long as jena's
			Node markLogicNode = markLogicGraphs.next();

			Graph jenaGraph = datasetGraph.getGraph(jenaGraphNode);
			Graph markLogicGraph = markLogicDatasetGraph.getGraph(jenaGraphNode);

			//RDFDataMgr.write(System.out, jenaGraph, Lang.TURTLE);
			//RDFDataMgr.write(System.out, markLogicGraph, Lang.TURTLE);

			assertTrue("Graphs from jena and MarkLogic are not isomorphic.  Graph name: " + jenaGraphNode.getURI(), jenaGraph.isIsomorphicWith(markLogicGraph));
		}
	}
	
	@Test
	public void testGraphCRUD() {
	    // initialize MarkLogic
	    DatasetGraph markLogicDatasetGraph = getMarkLogicDatasetGraph("testData.trig");

	    Graph g1 = markLogicDatasetGraph.getGraph(NodeFactory.createURI("http://example.org/g1"));

	    Triple triple = new Triple(NodeFactory.createURI("s10"), 
	            NodeFactory.createURI("p10"), 
	            NodeFactory.createURI("o10"));
	    g1.add(triple);

	    Node g10Node = NodeFactory.createURI("http://example.org/g10");

	    // add modified graph to new name
	    markLogicDatasetGraph.addGraph(g10Node, g1);
	    assertTrue("MarkLogic contains the graph", markLogicDatasetGraph.containsGraph(g10Node));
	    
	    Graph g10 =  markLogicDatasetGraph.getGraph(g10Node);

	    assertTrue(g1.isIsomorphicWith(g10));
	    //verify two tripes
	    assertEquals("G10 has two triples", 2, g10.size());
	    
	    markLogicDatasetGraph.delete(new Quad(g10Node, triple));
	    g10 =  markLogicDatasetGraph.getGraph(g10Node);
	    g1 = markLogicDatasetGraph.getGraph(NodeFactory.createURI("http://example.org/g1"));

	    assertTrue(g1.isIsomorphicWith(g10));

	    markLogicDatasetGraph.removeGraph(g10Node);
	    assertFalse("MarkLogic no longer contains the graph", markLogicDatasetGraph.containsGraph(g10Node));

	    Graph defaultGraph = markLogicDatasetGraph.getDefaultGraph();

	    int graphSize = defaultGraph.size();
	    defaultGraph.add(triple);
	    assertEquals(graphSize + 1, defaultGraph.size());

	    markLogicDatasetGraph.setDefaultGraph(defaultGraph);
	    defaultGraph = markLogicDatasetGraph.getDefaultGraph();
        assertEquals(graphSize + 1, defaultGraph.size());
	}
	
	@Test
	public void testQuadsView() {
	
		Node newSubject = NodeFactory.createURI("newSubject");
		Node newProperty = NodeFactory.createURI("newProperty");
		Node newValue = NodeFactory.createLiteral("All New Value!");
		Node newGraph = NodeFactory.createURI("newGraph");
		Quad newQuad = new Quad(newGraph, 
				new Triple(newSubject,
						newProperty,
						newValue));
		
	    DatasetGraph markLogicDatasetGraph = getMarkLogicDatasetGraph();
	    Dataset ds = DatasetFactory.create(markLogicDatasetGraph);
	    String askQuery = "ASK WHERE { GRAPH <newGraph> { <newSubject> ?p ?o } }";
	    markLogicDatasetGraph.add(newQuad);
	    
	    QueryExecution queryExec = QueryExecutionFactory.create(askQuery, ds);
	    assertTrue("add quad inserted a graph", queryExec.execAsk());
	    
	    markLogicDatasetGraph.delete(newQuad);
	    
	    queryExec = QueryExecutionFactory.create(askQuery, ds);
	    assertFalse("delete quad deleted the quad", queryExec.execAsk());
	    
	    markLogicDatasetGraph.add(newGraph, newSubject, newProperty, newValue);
	    
	    queryExec = QueryExecutionFactory.create(askQuery, ds);
	    assertTrue("add node inserted", queryExec.execAsk());
	    
	    markLogicDatasetGraph.delete(newGraph, NodeFactory.createURI("blah"), newProperty, newValue);
	    
	    queryExec = QueryExecutionFactory.create(askQuery, ds);
	    assertTrue("add node still there inserted", queryExec.execAsk());
	    
	    markLogicDatasetGraph.delete(newGraph, newSubject, newProperty, newValue);
	    
	    queryExec = QueryExecutionFactory.create(askQuery, ds);
		assertFalse("delete nodes deleted the quad", queryExec.execAsk());

		// insert so I can delete
	    markLogicDatasetGraph.add(newGraph, newSubject, newProperty, newValue);
	    markLogicDatasetGraph.deleteAny(Node.ANY, Node.ANY, NodeFactory.createURI("blah"), Node.ANY);
	    // no delete
	    
	    queryExec = QueryExecutionFactory.create(askQuery, ds);
		assertTrue("no delete occurs", queryExec.execAsk());
		markLogicDatasetGraph.deleteAny(Node.ANY, Node.ANY, newProperty, Node.ANY);
		
		queryExec = QueryExecutionFactory.create(askQuery, ds);
		assertFalse("delete nodes deleted the quad", queryExec.execAsk());

	    RDFDataMgr.read(markLogicDatasetGraph, "testData.trig");
	    
	    Iterator<Quad> quads = markLogicDatasetGraph.find();
	    
	    // run through iterator
	    int i=0;
	    while (quads.hasNext()) {
	    	Quad q = quads.next();
	    	i++;
	    	assertNotNull(q.getSubject());
	    	assertNotNull(q.getPredicate());
	    	assertNotNull(q.getObject());
	    }
	    assertEquals("Got back all the quads",19, i);
//	find()
//	find(Quad)
//	find(Node, Node, Node, Node)
//	findNG(Node, Node, Node, Node)
//	contains(Node, Node, Node, Node)
//	contains(Quad)
	}

	@Test
	@Ignore
	public void testGraphPermissions() {
		
	}
	
	@Test
	public void testTransactions() {
		MarkLogicDatasetGraph markLogicDatasetGraph = getMarkLogicDatasetGraph();
		
		Node g1 = NodeFactory.createURI("transact1");
		Triple triple = new Triple(NodeFactory.createURI("s10"),
				NodeFactory.createURI("p10"), NodeFactory.createURI("o10"));
		Graph transGraph = GraphFactory.createGraphMem();
		transGraph.add(triple);
		
		// insert a graph within a transaction, rollback
		try {
			markLogicDatasetGraph.begin(ReadWrite.READ);
			fail("MarkLogic only supports write transactions");
		} catch (MarkLogicTransactionException e) {
			// pass
		}
		assertFalse(markLogicDatasetGraph.isInTransaction());
		markLogicDatasetGraph.begin(ReadWrite.WRITE);
		assertTrue(markLogicDatasetGraph.isInTransaction());
		markLogicDatasetGraph.addGraph(g1, transGraph);
		markLogicDatasetGraph.abort();
		assertFalse(markLogicDatasetGraph.isInTransaction());

    	QueryExecution queryExec = QueryExecutionFactory.create("ASK WHERE { ?s ?p ?o }",
    			markLogicDatasetGraph.toDataset());
		assertFalse("transact1 graph must not exist after rollback", queryExec.execAsk());
		
		markLogicDatasetGraph.begin(ReadWrite.WRITE);
		assertTrue(markLogicDatasetGraph.isInTransaction());
		markLogicDatasetGraph.addGraph(g1, transGraph);
		markLogicDatasetGraph.commit();
		assertFalse(markLogicDatasetGraph.isInTransaction());

    	queryExec = QueryExecutionFactory.create("ASK WHERE { ?s ?p ?o }",
    			markLogicDatasetGraph.toDataset());
		assertTrue("transact1 graph exists after commit", queryExec.execAsk());
		
	}
	
	@After
	public void cleanTrans() {
		MarkLogicDatasetGraph markLogicDatasetGraph = getMarkLogicDatasetGraph();
		
		UpdateAction.execute(new UpdateRequest().add("DROP GRAPH <transact1>"), markLogicDatasetGraph);
	}
	
}
