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
package com.marklogic.jena.functionaltests;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.util.Iterator;

import org.apache.jena.riot.RDFDataMgr;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.Quad;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.semantics.Capability;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JenaGraphTests extends ConnectedRESTQA {
	private static String dbName = "Jena-JavaAPI-Functional";
	private static String[] fNames = { "Jena-JavaAPI-Functional-1" };
	private static String restServerName = "REST-Java-Client-JenaAPI-Server";
	private static int restPort = 8014;
	private static int uberPort = 8000;
	private DatabaseClient adminClient = null;
	private DatabaseClient writerClient = null;
	private DatabaseClient readerClient = null;
	private DatabaseClient evalClient = null;
	private static String datasource = "src/test/java/com/marklogic/jena/functionaltest/data/";
	private static MarkLogicDatasetGraph markLogicDatasetGraphWriter;
	private static MarkLogicDatasetGraph markLogicDatasetGraphReader;
	private static MarkLogicDatasetGraph markLogicDatasetGraphAdmin;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.out.println("In setup");
		setupJavaRESTServer(dbName, fNames[0], restServerName, restPort);
		setupAppServicesConstraint(dbName);
		enableCollectionLexicon(dbName);
		enableTripleIndex(dbName);

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		System.out.println("In tear down");
		// Delete database first. Otherwise axis and collection cannot be
		// deleted
		tearDownJavaRESTServer(dbName, fNames, restServerName);
		deleteRESTUser("eval-user");
		deleteUserRole("test-eval");
	}

	@After
	public void testCleanUp() throws Exception {
		clearDB(restPort);
		adminClient.release();
		writerClient.release();
		readerClient.release();
		System.out.println("Running clear script");
	}

	@Before
	public void setUp() throws Exception {
		createUserRolesWithPrevilages("test-eval", "xdbc:eval", "xdbc:eval-in", "xdmp:eval-in", "any-uri", "xdbc:invoke");
		createRESTUser("eval-user", "x", "test-eval", "rest-admin", "rest-writer", "rest-reader");
		adminClient = DatabaseClientFactory.newClient("localhost", restPort, dbName, "rest-admin", "x", Authentication.DIGEST);
		writerClient = DatabaseClientFactory.newClient("localhost", restPort, dbName, "rest-writer", "x", Authentication.DIGEST);
		readerClient = DatabaseClientFactory.newClient("localhost", restPort, dbName, "rest-reader", "x", Authentication.DIGEST);
		evalClient = DatabaseClientFactory.newClient("localhost", uberPort, dbName, "eval-user", "x", Authentication.DIGEST);
		markLogicDatasetGraphWriter = MarkLogicDatasetGraphFactory.createDatasetGraph(writerClient);
		markLogicDatasetGraphReader = MarkLogicDatasetGraphFactory.createDatasetGraph(readerClient);
		markLogicDatasetGraphAdmin = MarkLogicDatasetGraphFactory.createDatasetGraph(adminClient);
	}

	/*
	 * With AdminUser Get Default Graph , Add Triples into Graph and validate
	 * the Triples Add Named Graph and validate Clear Data and Validate Close
	 * Dataset
	 */

	@Test
	public void testCrud_admin() {
		// Insert Triples into Graph
		Graph g1 = markLogicDatasetGraphAdmin.getDefaultGraph();
		Triple triple = new Triple(NodeFactory.createURI("s5"), NodeFactory.createURI("p5"), NodeFactory.createURI("o5"));
		g1.add(triple);
		Node n1 = NodeFactory.createURI("http://example.org/jenaAdd");
		// Add Named Graph and validate triples
		markLogicDatasetGraphAdmin.addGraph(n1, g1);
		Graph g2 = markLogicDatasetGraphAdmin.getGraph(n1);
		assertTrue("did not match Triples", g2.contains(triple));
		// Get Permissions of the named Graph and Validate
		GraphPermissions permissions = markLogicDatasetGraphAdmin.getPermissions(n1);
		assertTrue("Didnot have expected permissions, returned " + permissions, permissions.get("rest-writer").contains(Capability.UPDATE)
				&& permissions.get("rest-reader").contains(Capability.READ));
		// Remove all data and validate
		markLogicDatasetGraphAdmin.clear();
		Graph g3 = markLogicDatasetGraphAdmin.getGraph(n1);
		assertTrue("Expecting empty graph, received " + g3.toString(), g3.toString().contains("{}"));

		// Close DataSet
		markLogicDatasetGraphAdmin.close();

	}

	/*
	 * 
	 */

	@Test
	public void testListGraphs_WriteUser() throws FileNotFoundException {
		// Write New graph with ntriples mimetype & Validate
		String file = datasource + "relative1.nt";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraphWriter, file);
		markLogicDatasetGraphWriter.sync();

		// Add Triples into Named Graph
		Graph g = markLogicDatasetGraphWriter.getDefaultGraph();
		Node newgraph = NodeFactory.createURI("http://jena.example.org/fileWrite");
		markLogicDatasetGraphWriter.addGraph(newgraph, g);

		// Get the list of graphs and validate
		Iterator<Node> markLogicGraphs = markLogicDatasetGraphWriter.listGraphNodes();
		while (markLogicGraphs.hasNext()) {
			Node graphs = markLogicGraphs.next();
			assertTrue(
					"did not find Node in :: " + graphs.toString(),
					graphs.toString().contains("http://jena.example.org/fileWrite")
							|| graphs.toString().contains(MarkLogicDatasetGraph.DEFAULT_GRAPH_URI)
							|| graphs.toString().contains("http://marklogic.com/semantics#graphs"));
		}

	}

	@Test
	public void testAdd_Quads() throws Exception {
		// Add and validate Quad
		markLogicDatasetGraphWriter.add(NodeFactory.createURI("testing/quad_add"), NodeFactory.createURI("testing/subject_1"),
				NodeFactory.createURI("testing/predicate_1"), NodeFactory.createLiteral("testing/Object_1"));
		Node graphNode = NodeFactory.createURI("testing/quad_add");
		Boolean found = markLogicDatasetGraphWriter.containsGraph(graphNode);
		assertTrue("Did not find the Graph Node ::" + graphNode + "Returned" + found, found);
		
		
		// Add and Validate Quad
		Quad quad = new Quad(NodeFactory.createURI("http://originalGraph"), NodeFactory.createURI("#electricVehicle2"),
				NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				NodeFactory.createURI("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle"));
		markLogicDatasetGraphWriter.add(quad);
		found = markLogicDatasetGraphWriter.contains(quad);
		assertTrue("Did not find the Quad Node ::" + quad + "Returned" + found, found);
		Iterator<Quad> quads = markLogicDatasetGraphWriter.find(quad);
			while (quads.hasNext()) {
				Quad quad1 = quads.next();
				assertTrue(quad1.equals(quad));
			}

			markLogicDatasetGraphWriter.delete(quad);
		
		try{
			Iterator<Quad> quads1 = markLogicDatasetGraphWriter.find(quad);
			while (quads1.hasNext()) {
				Quad quad1 = quads1.next();
				assertFalse(quad1.equals(quad));
			}
			
		}catch(Exception e){
			System.out.println(e);
		}
		
		try{
		Iterator<Quad> quads1 = markLogicDatasetGraphWriter.find();
		while(quads.hasNext()){
			Quad quad2 = quads.next();
			assertFalse(quad2.equals(quad));
				}
			}catch(Exception e){
				System.out.println(e);
		}
			 
		// Merge Graphs and Validate triples
		String file = datasource + "semantics.nq";
		RDFDataMgr.read(markLogicDatasetGraphWriter, file);
		markLogicDatasetGraphWriter.sync();
		Graph g = markLogicDatasetGraphWriter.getGraph(NodeFactory.createURI("http://en.wikipedia.org/wiki/Apollo_13?oldid=495374925#absolute-line=6"));
		markLogicDatasetGraphWriter.mergeGraph(graphNode, g);
		Graph mergedgraph = markLogicDatasetGraphWriter.getGraph(graphNode);
		 assertTrue("Merged graph dpes not have expected number of triples", mergedgraph.size() == 5);
		 
				// Remove Graph and validate
		markLogicDatasetGraphWriter.removeGraph(graphNode);
		assertTrue("The graph Shold not Exist after delete",
				!(markLogicDatasetGraphWriter.containsGraph(graphNode)));

		markLogicDatasetGraphWriter.close();
	}

	/*
	 * Add Quad with Read User and Catch ForbiddenUser Exception
	 */

	@Test
	public void testAdd_ReadUser() throws FileNotFoundException {
		try {
			Quad quad = new Quad(NodeFactory.createURI("http://originalGraph1"), new Triple(NodeFactory.createURI("#electricVehicle3"),
					NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"),
					NodeFactory.createLiteral("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle1")));
			markLogicDatasetGraphReader.add(quad);
			markLogicDatasetGraphReader.sync();
			assertFalse(markLogicDatasetGraphReader.contains(quad));
		} catch (Exception e) {
			assertTrue("Should catch ForbiddenUserException ", e.toString().contains("ForbiddenUserException"));

		}
	}

	/*
	 * Add Quad with Admin User and Read with Read user
	 */
	@Test
	public void testAddRead_AdminUser() throws Exception {
		try {
			Quad quad = new Quad(NodeFactory.createURI("http://originalGraph1"), new Triple(NodeFactory.createURI("#electricVehicle3"),
					NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"),
					NodeFactory.createLiteral("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle1")));
			markLogicDatasetGraphAdmin.add(quad);
			markLogicDatasetGraphAdmin.sync();
			assertTrue("Did not find  Quad in Dataset, Received " + markLogicDatasetGraphReader.contains(quad),
					markLogicDatasetGraphReader.contains(quad));
			markLogicDatasetGraphReader.close();

			Node n1 = NodeFactory.createURI("http://originalGraph1");

			markLogicDatasetGraphAdmin.clearPermissions(n1);
			GraphPermissions permissions = markLogicDatasetGraphAdmin.getPermissions(n1);
			assertTrue("Didnot have expected permissions, returned " + permissions,
					permissions.get("rest-writer").contains(Capability.UPDATE) && permissions.get("rest-reader").contains(Capability.READ));
		} catch (Exception e) {

		}
	}

	@Test
	public void testSetDefaultGraph_admin() {

		String file = datasource + "relative1.nt";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraphWriter, file);
		markLogicDatasetGraphWriter.sync();
		Graph g1 = markLogicDatasetGraphWriter.getDefaultGraph();

		assertTrue("did not match Triples", g1.toString().contains("#electricVehicle2"));

		// Create New graph and add triples from defaultgraph to new graph
		Triple triple = new Triple(NodeFactory.createURI("s5"), NodeFactory.createURI("p5"), NodeFactory.createURI("o5"));
		Quad quad = new Quad(NodeFactory.createURI("new-graph-fordefault"), triple);
		Node n1 = NodeFactory.createURI("new-graph-fordefault");
		markLogicDatasetGraphWriter.add(quad);
		markLogicDatasetGraphWriter.sync();
		Graph g2 = markLogicDatasetGraphWriter.getGraph(n1);
		assertTrue("did not match Triples", g2.contains(triple));
		// Set DefaultGraph to be NamedGraph
		markLogicDatasetGraphWriter.setDefaultGraph(g2);
		Graph defaultG = markLogicDatasetGraphWriter.getDefaultGraph();
		assertTrue("did not match Triples", defaultG.contains(triple));
	}

	/*
	 * Delete triple and quad and set empty graph as default graph
	 */
	@Test
	// isEmpty
	public void testDelete_admin() {
		Triple triple = new Triple(NodeFactory.createURI("s5"), NodeFactory.createURI("p5"), NodeFactory.createURI("o5"));
		Quad quad = new Quad(NodeFactory.createURI("new-graph-fordefault2"), triple);
		Node n2 = NodeFactory.createURI("new-graph-fordefault2");
		markLogicDatasetGraphWriter.add(quad);
		markLogicDatasetGraphWriter.sync();
		Graph g3 = markLogicDatasetGraphWriter.getGraph(n2);
		g3.delete(triple);
		markLogicDatasetGraphWriter.sync();
		assertTrue("did not match Triples", g3.size() == 0);

		markLogicDatasetGraphWriter.delete(quad);
		assertTrue("Quad Should be deleted , but looks like its not", !markLogicDatasetGraphWriter.contains(quad));

		markLogicDatasetGraphWriter.setDefaultGraph(g3);
		Graph defaultG = markLogicDatasetGraphWriter.getDefaultGraph();
		assertTrue("did not match Triples", defaultG.size() == 0);
	}

	@Test
	public void testAddDelete_permissions() {
		String file = datasource + "rdfxml1.rdf";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraphWriter, file);
		markLogicDatasetGraphWriter.sync();

		// Add Triples into Named Graph
		Graph g = markLogicDatasetGraphWriter.getDefaultGraph();
		Node newgraph = NodeFactory.createURI("http://jena.example.org/perm");
		markLogicDatasetGraphWriter.addGraph(newgraph, g);
		GraphPermissions permissions = markLogicDatasetGraphAdmin.getPermissions(newgraph);
		markLogicDatasetGraphWriter.addPermissions(newgraph, permissions.permission("test-eval", Capability.EXECUTE));
		permissions = markLogicDatasetGraphWriter.getPermissions(newgraph);
		System.out.println(markLogicDatasetGraphWriter.getPermissions(newgraph));
		assertTrue("Did not have permission looking for", permissions.get("test-eval").contains(Capability.EXECUTE));
		markLogicDatasetGraphWriter.clearPermissions(newgraph);
		markLogicDatasetGraphWriter.sync();
		permissions = markLogicDatasetGraphWriter.getPermissions(newgraph);
		System.out.println(permissions);
		assertTrue("Should not have Execute for test-eval",!( permissions.containsValue("test-eval")));

	}

	@Test
	public void testAddDelete_permissions_inTrx() {
		String file = datasource + "rdfxml1.rdf";
		// Read triples into dataset
		markLogicDatasetGraphWriter.begin(ReadWrite.WRITE);
		RDFDataMgr.read(markLogicDatasetGraphWriter, file);
		markLogicDatasetGraphWriter.sync();

		// Add Triples into Named Graph
		Graph g = markLogicDatasetGraphWriter.getDefaultGraph();
		Node newgraph = NodeFactory.createURI("http://jena.example.org/perm");
		markLogicDatasetGraphWriter.addGraph(newgraph, g);
		markLogicDatasetGraphWriter.commit();

		assertFalse(markLogicDatasetGraphWriter.isInTransaction());

		GraphPermissions permissions = markLogicDatasetGraphAdmin.getPermissions(newgraph);

		// Add Permission and validate
		markLogicDatasetGraphWriter.begin(ReadWrite.WRITE);
		markLogicDatasetGraphWriter.addPermissions(newgraph, permissions.permission("test-eval", Capability.EXECUTE));
		markLogicDatasetGraphWriter.commit();
		permissions = markLogicDatasetGraphWriter.getPermissions(newgraph);
		System.out.println(markLogicDatasetGraphWriter.getPermissions(newgraph));
		assertTrue("Did not have permission looking for", permissions.get("test-eval").contains(Capability.EXECUTE));

		// Clear Permission and Abort and validate permission exist
		markLogicDatasetGraphWriter.begin(ReadWrite.WRITE);
		markLogicDatasetGraphWriter.clearPermissions(newgraph);
		markLogicDatasetGraphWriter.end();
		assertFalse(markLogicDatasetGraphWriter.isInTransaction());
		permissions = markLogicDatasetGraphWriter.getPermissions(newgraph);
		assertTrue("Did not have permission looking for", permissions.get("test-eval").contains(Capability.EXECUTE));

		// Clear Permission And validate
		markLogicDatasetGraphWriter.begin(ReadWrite.WRITE);
		markLogicDatasetGraphWriter.clearPermissions(newgraph);
		markLogicDatasetGraphWriter.commit();
		assertFalse(markLogicDatasetGraphWriter.isInTransaction());
		System.out.println(markLogicDatasetGraphWriter.getPermissions(newgraph));
		permissions = markLogicDatasetGraphWriter.getPermissions(newgraph);
		assertTrue("Should not contain test-eval=[EXECUTE]", !(permissions.toString().contains("test-eval=[EXECUTE]")));
	}

	/*
	 * Delete Quad and graph within permission
	 */
	@Test
	public void testDelete_InTrx() {

		String file = datasource + "rdfxml1.rdf";
		// Read triples into dataset
		markLogicDatasetGraphWriter.begin(ReadWrite.WRITE);
		RDFDataMgr.read(markLogicDatasetGraphWriter, file);
		markLogicDatasetGraphWriter.sync();
		Graph g = markLogicDatasetGraphWriter.getDefaultGraph();
		Node newgraph = NodeFactory.createURI("http://jena.example.org/perm");
		markLogicDatasetGraphWriter.addGraph(newgraph, g);
		markLogicDatasetGraphWriter.commit();

		// Delete one Triple and Validate
		markLogicDatasetGraphWriter.begin(ReadWrite.WRITE);
		markLogicDatasetGraphWriter.deleteAny(newgraph, Node.ANY, NodeFactory.createURI("http://example.org/kennedy/sameAs"), Node.ANY);
		markLogicDatasetGraphWriter.commit();
		Quad quad = new Quad(newgraph, Node.ANY, NodeFactory.createURI("http://example.org/kennedy/sameAs"), Node.ANY);
		assertFalse(markLogicDatasetGraphWriter.contains(quad));

		// Merge Graphs and validate
		markLogicDatasetGraphWriter.begin(ReadWrite.WRITE);
		markLogicDatasetGraphWriter.mergeGraph(newgraph, g);
		assertTrue(markLogicDatasetGraphWriter.contains(quad));
		markLogicDatasetGraphWriter.commit();

		// Delete Graph , Abort trx and Validate graph exists
		markLogicDatasetGraphWriter.begin(ReadWrite.WRITE);
		markLogicDatasetGraphWriter.removeGraph(newgraph);
		markLogicDatasetGraphWriter.abort();
		assertFalse(markLogicDatasetGraphWriter.isInTransaction());
		assertTrue(markLogicDatasetGraphWriter.contains(newgraph, Node.ANY, Node.ANY, Node.ANY));

		// Delete Graph and Validate 
		markLogicDatasetGraphWriter.begin(ReadWrite.WRITE);
		markLogicDatasetGraphWriter.removeGraph(newgraph);
		markLogicDatasetGraphWriter.commit();
		assertFalse(markLogicDatasetGraphWriter.containsGraph(newgraph));

	}
	//TODO find  ..quads
	
	
	//TODO ? Support for triplexml miemtype ..?
	// TODO? removeGraph(Remove all data associated with the named graph), deletes graph as well..? along with data within graph
	//TODO ? any special exception handling required for Jena.?
	
	public void testStubs_datasetGraph() {
		markLogicDatasetGraphWriter.abort();// D
		Quad quad = null;
		markLogicDatasetGraphWriter.add(quad); // D
		Node g = null, s = null, p = null, o = null;
		markLogicDatasetGraphWriter.add(g, s, p, o); // D
		Node graphName = null;
		Graph graph = null;
		markLogicDatasetGraphWriter.addGraph(graphName, graph);// D
		GraphPermissions permissions = null;
		markLogicDatasetGraphWriter.addPermissions(graphName, permissions);// D
		ReadWrite readWrite = null;
		markLogicDatasetGraphWriter.begin(readWrite);// D
		markLogicDatasetGraphWriter.clear();// D
		markLogicDatasetGraphWriter.clearPermissions(graphName);// D
		markLogicDatasetGraphWriter.commit();// D
		markLogicDatasetGraphWriter.close();// D
		markLogicDatasetGraphWriter.contains(quad); // D
		Node graphNode = null;
		markLogicDatasetGraphWriter.containsGraph(graphNode);// D
		markLogicDatasetGraphWriter.getDefaultGraph(); // D
		markLogicDatasetGraphWriter.getGraph(graphNode); // D
		markLogicDatasetGraphWriter.getPermissions(graphName);// D
		markLogicDatasetGraphWriter.isInTransaction();// D
		markLogicDatasetGraphWriter.listGraphNodes(); // D
		markLogicDatasetGraphWriter.mergeGraph(graphName, graph); // D
		markLogicDatasetGraphWriter.removeGraph(graphName); // D
		markLogicDatasetGraphWriter.setDefaultGraph(graph);// D
		markLogicDatasetGraphWriter.size(); // D
		markLogicDatasetGraphWriter.sync(); // D
		
		QueryDefinition constrainingQueryDefinition = null;
		markLogicDatasetGraphWriter.setConstrainingQueryDefinition(constrainingQueryDefinition);
		SPARQLRuleset rulesets = null;
		markLogicDatasetGraphWriter.setRulesets(rulesets);
		markLogicDatasetGraphWriter.getRulesets();
		markLogicDatasetGraphWriter.setSPARQLUpdatePermissions(permissions);
		markLogicDatasetGraphWriter.getSPARQLUpdatePermissions();
		markLogicDatasetGraphWriter.startRequest();
		markLogicDatasetGraphWriter.end();
		markLogicDatasetGraphWriter.finishRequest();
		markLogicDatasetGraphWriter.getConstrainingQueryDefinition();
		markLogicDatasetGraphWriter.getDatabaseClient();
		markLogicDatasetGraphWriter.toDataset();
		markLogicDatasetGraphWriter.withRulesets(rulesets);
		SPARQLQueryDefinition qdef = null;
		String variableName = null;
		Node objectNode = null;
		markLogicDatasetGraphWriter.bindObject(qdef, variableName, objectNode);

	}

}
