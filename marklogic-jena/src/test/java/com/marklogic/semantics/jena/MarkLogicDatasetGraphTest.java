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
package com.marklogic.semantics.jena;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateRequest;
import com.marklogic.client.semantics.Capability;
import com.marklogic.client.semantics.GraphPermissions;

public class MarkLogicDatasetGraphTest extends JenaTestBase {

	private static Logger log = LoggerFactory.getLogger(MarkLogicDatasetGraphTest.class);

	@Test
	public void testFirstRead() {
		DatasetGraph datasetGraph = getJenaDatasetGraph("testdata/testData.trig");
		DatasetGraph markLogicDatasetGraph = getMarkLogicDatasetGraph("testdata/testData.trig");

		Iterator<Node> jenaGraphs = datasetGraph.listGraphNodes();
		Iterator<Node> markLogicGraphs = markLogicDatasetGraph.listGraphNodes();

		while (jenaGraphs.hasNext()) {
			Node jenaGraphNode = jenaGraphs.next();
			assertTrue(markLogicGraphs.hasNext());

			// list must be at least as long as jena's
			@SuppressWarnings("unused")
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
	    DatasetGraph markLogicDatasetGraph = getMarkLogicDatasetGraph("testdata/testData.trig");

	    Graph g1 = markLogicDatasetGraph.getGraph(NodeFactory.createURI("http://example.org/g1"));

	    Triple triple = new Triple(NodeFactory.createURI("s10"), 
	            NodeFactory.createURI("p10"), 
	            NodeFactory.createURI("o10"));
	    g1.add(triple);

	    Node n10Node = NodeFactory.createURI("http://example.org/n10");

	    // add modified graph to new name
	    markLogicDatasetGraph.addGraph(n10Node, g1);
	    assertTrue("MarkLogic contains the graph", markLogicDatasetGraph.containsGraph(n10Node));
	    
	    Graph n10 =  markLogicDatasetGraph.getGraph(n10Node);

	    assertTrue(g1.isIsomorphicWith(n10));
	    //verify two tripes
	    assertEquals("G10 has two triples", 2, n10.size());
	    
	    markLogicDatasetGraph.delete(new Quad(n10Node, triple));
	    n10 =  markLogicDatasetGraph.getGraph(n10Node);
	    g1 = markLogicDatasetGraph.getGraph(NodeFactory.createURI("http://example.org/g1"));

	    assertTrue(g1.isIsomorphicWith(n10));

	    markLogicDatasetGraph.removeGraph(n10Node);
	    assertFalse("MarkLogic no longer contains the graph", markLogicDatasetGraph.containsGraph(n10Node));

	    Graph defaultGraph = markLogicDatasetGraph.getDefaultGraph();

	    int graphSize = defaultGraph.size();
	    defaultGraph.add(triple);
	    assertEquals(graphSize + 1, defaultGraph.size());

	    markLogicDatasetGraph.setDefaultGraph(defaultGraph);
	    defaultGraph = markLogicDatasetGraph.getDefaultGraph();
        assertEquals(graphSize + 1, defaultGraph.size());
        
		markLogicDatasetGraph.deleteAny(Node.ANY, triple.getSubject(), triple.getPredicate(), triple.getObject());

	}
	
	@Test
	public void testQuadsView() {
	
		Node newSubject = NodeFactory.createURI("newSubject");
		Node newProperty = NodeFactory.createURI("newProperty");
		// note, untyped literals are rdf 1.0 and do not round-trip
		Node newValue = NodeFactory.createLiteral("All New Value!", XSDDatatype.XSDstring);
		Node newGraph = NodeFactory.createURI("newGraph");
		Quad newQuad = new Quad(newGraph, 
				new Triple(newSubject,
						newProperty,
						newValue));
		
	    DatasetGraph markLogicDatasetGraph = getMarkLogicDatasetGraph();
	    Dataset ds = DatasetFactory.create(markLogicDatasetGraph);
	    String askQuery = "ASK WHERE { GRAPH <newGraph> { <newSubject> ?p ?o } }";
	    markLogicDatasetGraph.add(newQuad);
	    ((MarkLogicDatasetGraph) markLogicDatasetGraph).sync();
	    
	    QueryExecution queryExec = QueryExecutionFactory.create(askQuery, ds);
	    assertTrue("add quad inserted a graph", queryExec.execAsk());
	    assertTrue(markLogicDatasetGraph.contains(newQuad));
	
	    markLogicDatasetGraph.delete(newQuad);
	    
	    queryExec = QueryExecutionFactory.create(askQuery, ds);
	    assertFalse("delete quad must delete quad", queryExec.execAsk());
	    assertFalse(markLogicDatasetGraph.contains(newQuad));

	    markLogicDatasetGraph.add(newGraph, newSubject, newProperty, newValue);
	    
	    queryExec = QueryExecutionFactory.create(askQuery, ds);
	    assertTrue("add node inserted", queryExec.execAsk());
	    
	    markLogicDatasetGraph.delete(newGraph, NodeFactory.createURI("blah"), newProperty, newValue);
	    
	    queryExec = QueryExecutionFactory.create(askQuery, ds);
	    assertTrue("add node still there inserted", queryExec.execAsk());
	    assertTrue(markLogicDatasetGraph.contains(newGraph, newSubject, newProperty, newValue));

	    markLogicDatasetGraph.delete(newGraph, newSubject, newProperty, newValue);
	    assertFalse(markLogicDatasetGraph.contains(newGraph, newSubject, newProperty, newValue));

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

	    RDFDataMgr.read(markLogicDatasetGraph, "testdata/testData.trig");
	    
	    Iterator<Quad> quads = markLogicDatasetGraph.find();

	    // run through iterator
	    int i=0;
	    while (quads.hasNext()) {
	    	// filter for just the test corpus.
	    	Quad q = quads.next();
	    	if (q.getGraph() != null) {
	    		String gName = q.getGraph().getURI();
		    	if (gName != null) {
		    		if (gName.equals("http://marklogic.com/semantics#default-graph") ||
		    			gName.matches("^http:\\/\\/example.org\\/[go].*")) {
		    			i++;
		    			assertNotNull(q.getSubject());
		    			assertNotNull(q.getPredicate());
		    			assertNotNull(q.getObject());
		    		}

	                log.debug("" + q);
		    	}
		    	else {
		    		log.debug("Some test didn't clean up " + gName);
	    		}
	    	}
	    }
	    
	    assertEquals("Got back all the quads",14, i);
	    
	    // find(*)
	    markLogicDatasetGraph.add(newGraph, newSubject, newProperty, newValue);
	    quads = markLogicDatasetGraph.find(newQuad);
	    assertTrue(quads.hasNext());
	    Quad q = quads.next();
	    assertEquals("find() returns proper quad with identity match", newQuad, q);
	    assertFalse(quads.hasNext());
	
	    quads = markLogicDatasetGraph.find(newGraph, newSubject, newProperty, newValue);
	    assertTrue(quads.hasNext());
	    q = quads.next();
	    assertEquals("Found the right quad.", q, newQuad);
	    assertFalse(quads.hasNext());
	    
		
	    quads = markLogicDatasetGraph.findNG(null, null, null, null);
	    i=0;
	    while (quads.hasNext()) {
	    	// filter for just the test corpus.
	    	q = quads.next();
	    	if (q.getGraph() != null) {
	    		String gName = q.getGraph().getURI();
		    	if (gName != null) {
		    		if (gName.matches("^http:\\/\\/example.org\\/[go].*")) {
		    			i++;
		    			assertNotNull(q.getSubject());
		    			assertNotNull(q.getPredicate());
		    			assertNotNull(q.getObject());
		    		}
		    	}
		    	else {
		    		log.debug("Some test didn't clean up " + gName);
	    		}
	    	}
	    }
	    
	    // issue #3 gave NPE here:
	    quads = markLogicDatasetGraph.find(null, null, null, null);
        
	    
	    assertEquals("Got back all the quads except default", 9, i);
	}

	@Test
	public void testGraphPermissions() {
		MarkLogicDatasetGraph markLogicDatasetGraph = getMarkLogicDatasetGraph();

		Node g1 = NodeFactory.createURI("perms1");
		Triple triple = new Triple(NodeFactory.createURI("s232"),
				NodeFactory.createURI("p232"), NodeFactory.createURI("o232"));
		Graph transGraph = GraphFactory.createGraphMem();
		transGraph.add(triple);
		markLogicDatasetGraph.addGraph(g1, transGraph);
		
		GraphPermissions graphPermissions = markLogicDatasetGraph.getPermissions(g1);
		assertTrue(graphPermissions.get("rest-reader").contains(Capability.READ));
		assertTrue(graphPermissions.get("rest-writer").contains(Capability.UPDATE));
		
		markLogicDatasetGraph.addPermissions(g1, graphPermissions.permission("semantics-peon-role", Capability.READ));
		
		graphPermissions = markLogicDatasetGraph.getPermissions(g1);
		assertTrue(graphPermissions.get("rest-reader").contains(Capability.READ));
		assertTrue(graphPermissions.get("rest-writer").contains(Capability.UPDATE));
		assertTrue(graphPermissions.get("semantics-peon-role").contains(Capability.READ));

		markLogicDatasetGraph.clearPermissions(g1);
		graphPermissions = markLogicDatasetGraph.getPermissions(g1);
		assertNull(graphPermissions.get("semantics-peon-role"));

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

    	QueryExecution queryExec = QueryExecutionFactory.create("ASK WHERE { graph <transact1> { ?s ?p ?o }}",
    			markLogicDatasetGraph.toDataset());
		assertFalse("transact1 graph must not exist after rollback", queryExec.execAsk());
		
		markLogicDatasetGraph.begin(ReadWrite.WRITE);
		assertTrue(markLogicDatasetGraph.isInTransaction());
		markLogicDatasetGraph.addGraph(g1, transGraph);
		markLogicDatasetGraph.commit();
		assertFalse(markLogicDatasetGraph.isInTransaction());

    	queryExec = QueryExecutionFactory.create("ASK WHERE {  graph <transact1> { ?s ?p ?o }}",
    			markLogicDatasetGraph.toDataset());
		assertTrue("transact1 graph exists after commit", queryExec.execAsk());
		
		markLogicDatasetGraph.deleteAny(Node.ANY, triple.getSubject(), triple.getPredicate(), triple.getObject());
		
	}
	
	@After
	public void clearGraphs() {
		MarkLogicDatasetGraph markLogicDatasetGraph = getMarkLogicDatasetGraph();
		
		UpdateAction.execute(new UpdateRequest().add("DROP SILENT ALL"), markLogicDatasetGraph);
	}
	
	@Test
	public void testLargerDataset() {
		MarkLogicDatasetGraph dsg = getMarkLogicDatasetGraph("testdata/test.owl");
		
		Iterator<Quad> quads = dsg.find();
		while (quads.hasNext()) {
			Quad q = quads.next();
			System.out.println(q);
		}
	}
	
	@Test(expected = MarkLogicJenaException.class)
	public void testLifeCycle() {
	    MarkLogicDatasetGraph dsg = getMarkLogicDatasetGraph("testdata/testData.trig");
	    Triple triple = new Triple(NodeFactory.createURI("s5"), NodeFactory.createURI("p5"), NodeFactory.createURI("o5"));
	    Graph g1 = GraphFactory.createDefaultGraph();
	    g1.add(triple);
	    Node n1 = NodeFactory.createURI("http://example.org/jenaAdd");
	    dsg.addGraph(n1, g1);
	    dsg.close();
	    @SuppressWarnings("unused")
        Graph graphClosed = dsg.getGraph(n1);
	    fail("Closed connection allowed operation");
	}
	
	@Test
    public void testSmallFileInsert() {
        Dataset dataSet = getMarkLogicDatasetGraph("testdata/smallfile.nt").toDataset();
        Query query = QueryFactory.create("select (count(?s) as ?ct) where { ?s ?p ?o }");
        QueryExecution execution = QueryExecutionFactory.create(query, dataSet);
        ResultSet results = execution.execSelect();
        int i;
        for (i=0; results.hasNext(); i++) {
            results.next();
        }
        assertEquals("One triple inserted", 1, i);
    }
	
	@Test
	public void testRIOTWrite() {
        Dataset dataSet = getMarkLogicDatasetGraph("testdata/smallfile.nt").toDataset();
	    RDFDataMgr.write(System.out, dataSet, RDFFormat.TRIG_PRETTY);
	}
	
}
