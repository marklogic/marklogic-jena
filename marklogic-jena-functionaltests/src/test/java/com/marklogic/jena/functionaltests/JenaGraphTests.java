/*
 * Copyright 2015-2017 MarkLogic Corporation
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.util.Iterator;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shared.Lock;
import org.apache.jena.sparql.core.Quad;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.semantics.Capability;
import com.marklogic.client.semantics.GraphPermissions;
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
		if(markLogicDatasetGraphAdmin.getDatabaseClient() != null){
			markLogicDatasetGraphAdmin.close();
		}
		if(markLogicDatasetGraphWriter.getDatabaseClient() != null){
			markLogicDatasetGraphWriter.close();
		}
		if(markLogicDatasetGraphReader.getDatabaseClient() != null){
			markLogicDatasetGraphReader.close();
		}
	
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
		adminClient = DatabaseClientFactory.newClient(TEST_HOST, restPort, dbName, "rest-admin", "x", Authentication.DIGEST);
		writerClient = DatabaseClientFactory.newClient(TEST_HOST, restPort, dbName, "rest-writer", "x", Authentication.DIGEST);
		readerClient = DatabaseClientFactory.newClient(TEST_HOST, restPort, dbName, "rest-reader", "x", Authentication.DIGEST);
		evalClient = DatabaseClientFactory.newClient(TEST_HOST, uberPort, dbName, "eval-user", "x", Authentication.DIGEST);
		markLogicDatasetGraphWriter = MarkLogicDatasetGraphFactory.createDatasetGraph(writerClient);
		markLogicDatasetGraphReader = MarkLogicDatasetGraphFactory.createDatasetGraph(readerClient);
		markLogicDatasetGraphAdmin = MarkLogicDatasetGraphFactory.createDatasetGraph(TEST_HOST, 8014, "rest-admin", "x",
				Authentication.DIGEST);
	}

	/*
	 * With AdminUser Get Default Graph , Add Triples into Graph and validate
	 * the Triples Add Named Graph and validate Clear Data and Validate Close
	 * Dataset
	 */

	@Test
	public void testCrud_admin() {
		// Insert Triples into Graph
		markLogicDatasetGraphAdmin.clear();
		Graph g1 = markLogicDatasetGraphAdmin.getDefaultGraph();
		assertTrue(g1.isEmpty());
		assertNotNull(g1);
		Triple triple = new Triple(NodeFactory.createURI("s5"), NodeFactory.createURI("p5"), NodeFactory.createURI("o5"));
		g1.add(triple);
		Node n1 = NodeFactory.createURI("http://example.org/jenaAdd");
		Quad quad = new Quad(n1, triple);
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

		// AFTER CLEAR add new graph and Quad with same triple and Graph node
		markLogicDatasetGraphAdmin.addGraph(n1, g1);
		markLogicDatasetGraphAdmin.add(quad);


		Iterator<Quad> quads = markLogicDatasetGraphAdmin.find(null, null, null, null);
		while (quads.hasNext()) {
			Quad quad1 = quads.next();
			assertTrue(quad1.equals(quad));
		}

		Iterator<Quad> quads1 = markLogicDatasetGraphAdmin.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
		while (quads1.hasNext()) {
			Quad quad1 = quads1.next();
			assertTrue(quad1.equals(quad));
		}

		// Delete All triples in Named Graph and verify
		markLogicDatasetGraphAdmin.deleteAny(n1, null, null, null);
		assertFalse(markLogicDatasetGraphAdmin.getGraph(n1).contains(triple));

		// Get Size on DataSet and Catch UnSupported Exception
		Exception exp = null;
		try{
			markLogicDatasetGraphAdmin.size();
		}catch(Exception e){
			exp =e;
		}
		assertTrue("Size not supported",exp.toString().contains("UnsupportedOperationException") && exp != null);

		// Get Lock on DataSet and and verify LockNone
		Lock lck =	markLogicDatasetGraphAdmin.getLock();
		System.out.println(lck.toString());
		assertTrue("getLock not supported",lck != null);

		markLogicDatasetGraphAdmin.close();
		exp = null;
		try {
			markLogicDatasetGraphAdmin.addGraph(n1, g1);
		} catch (Exception e) {
			System.out.println("EXCEPTION AFTER CLOSE" + e);
			exp = e;
		}
		assertTrue("Should Throw DatabaseGraph is closed Exception", exp.toString().contains("DatabaseGraph is closed"));
	}

	/*
	 * List All the Graph using rest write user
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
		markLogicDatasetGraphWriter.deleteAny(NodeFactory.createURI("testing/quad_add"), Node.ANY, Node.ANY, Node.ANY);

		// Add and Validate Quad
		Quad quad = new Quad(NodeFactory.createURI("http://originalGraph"), NodeFactory.createURI("#electricVehicle2"),
				NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				NodeFactory.createURI("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle"));
		Quad quad2 = new Quad(NodeFactory.createURI("http://originalGraph"), NodeFactory.createURI("#electricVehicle21"),
				NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"),
				NodeFactory.createURI("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle1"));
		markLogicDatasetGraphWriter.add(quad);

		// Validate quad using contains node's
		found = markLogicDatasetGraphWriter.contains(NodeFactory.createURI("http://originalGraph"),
				NodeFactory.createURI("#electricVehicle2"), NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				NodeFactory.createURI("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle"));

		assertTrue("Did not find the Quad Node ::" + quad + "Returned" + found, found);
		Iterator<Quad> quads = markLogicDatasetGraphWriter.find(quad);
		while (quads.hasNext()) {
			Quad quad1 = quads.next();
			assertTrue(quad1.equals(quad));
		}

		// Delete Non existing quad and validate Inserted quad exists
		markLogicDatasetGraphWriter.delete(quad2);
		quads = markLogicDatasetGraphWriter.find();
		while (quads.hasNext()) {
			Quad quad1 = quads.next();
			assertTrue(quad1.equals(quad));
		}

		// Delete existing Quad and validate
		markLogicDatasetGraphWriter.delete(NodeFactory.createURI("http://originalGraph"), NodeFactory.createURI("#electricVehicle2"),
				NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				NodeFactory.createURI("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle"));

		assertFalse(markLogicDatasetGraphWriter.contains(NodeFactory.createURI("http://originalGraph"),
				NodeFactory.createURI("#electricVehicle2"), NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				NodeFactory.createURI("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle")));

		// Find and validate Quad doesnot exist

		Iterator<Quad> quads1 = markLogicDatasetGraphWriter.find(quad);
		Boolean quads1has = quads1.hasNext();
		assertFalse(quads1.hasNext());

		quads1 = markLogicDatasetGraphWriter.find();
		assertFalse(quads1.hasNext());

		// Merge Graphs and Validate triples
		String file = datasource + "semantics.nq";
		RDFDataMgr.read(markLogicDatasetGraphWriter, file);
		markLogicDatasetGraphWriter.sync();
		Graph g = markLogicDatasetGraphWriter.getGraph(NodeFactory
				.createURI("http://en.wikipedia.org/wiki/Apollo_13?oldid=495374925#absolute-line=6"));
		markLogicDatasetGraphWriter.mergeGraph(graphNode, g);
		Graph mergedgraph = markLogicDatasetGraphWriter.getGraph(graphNode);
		int size = mergedgraph.size();
		assertTrue("Merged graph dpes not have expected number of triples", mergedgraph.size() == 4);

		// Remove Graph and validate
		markLogicDatasetGraphWriter.removeGraph(graphNode);
		assertTrue("The graph Shold not Exist after delete", !(markLogicDatasetGraphWriter.containsGraph(graphNode)));

		// Delete non existing graph, expected to throw ResourceNotfound
		// Exception
		Exception exp = null;
		try {
			markLogicDatasetGraphWriter.removeGraph(graphNode);
		} catch (Exception e) {
			exp = e;

		}
		assertTrue("Deleting non Existing Grpah should throw ResourceNot found exception, but it did not",
				exp.toString().contains("ResourceNotFoundException"));
		markLogicDatasetGraphWriter.close();
	}

	/*
	 * Add Quad with Read User and Catch ForbiddenUser Exception
	 */

	@Test
	public void testAdd_ReadUser() throws FileNotFoundException {
		Exception exp = null;
		try {
			Quad quad = new Quad(NodeFactory.createURI("http://originalGraph1"), new Triple(NodeFactory.createURI("#electricVehicle3"),
					NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"),
					NodeFactory.createLiteral("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle1")));
			markLogicDatasetGraphReader.add(quad);
			markLogicDatasetGraphReader.sync();
			assertFalse(markLogicDatasetGraphReader.contains(quad));
		} catch (Exception e) {
			exp = e;

		}
		assertTrue("Should catch ForbiddenUserException ", exp.toString().contains("ForbiddenUserException") && exp != null);

	}

	/*
	 * Add Quad with Admin User and Read with Read user and validate using find,
	 * findNG with null and ANY, contains
	 */

	@Test
	public void testAddRead_AdminUser() throws Exception {

		Quad quad = new Quad(NodeFactory.createURI("http://originalGraph1"), new Triple(NodeFactory.createURI("#electricVehicle3"),
				NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"),
				NodeFactory.createLiteral("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle1")));
		markLogicDatasetGraphAdmin.add(quad);
		markLogicDatasetGraphAdmin.sync();
		// Contains Node of type quad
		assertTrue(
				"Did not find  Quad in Dataset, Received " + markLogicDatasetGraphReader.contains(quad),
				markLogicDatasetGraphReader.contains(NodeFactory.createURI("http://originalGraph1"),
						NodeFactory.createURI("#electricVehicle3"),
						NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"),
						NodeFactory.createLiteral("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle1")));
		// find node
		Iterator<Quad> result = markLogicDatasetGraphReader.find(NodeFactory.createURI("http://originalGraph1"),
				NodeFactory.createURI("#electricVehicle3"), NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"),
				NodeFactory.createLiteral("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle1"));
		while (result.hasNext()) {
			Quad quad1 = result.next();
			assertTrue(
					"returned" + quad1,
					quad1.matches(NodeFactory.createURI("http://originalGraph1"), NodeFactory.createURI("#electricVehicle3"),
							NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"),
							NodeFactory.createLiteral("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle1")));
		}

		// find node with pattern null and any
		Iterator<Quad> result1 = markLogicDatasetGraphReader.find(null, NodeFactory.createURI("#electricVehicle3"), Node.ANY,
				NodeFactory.createLiteral("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle1"));
		while (result1.hasNext()) {
			Quad quad1 = result1.next();
			assertTrue(
					"returned" + quad1,
					quad1.matches(NodeFactory.createURI("http://originalGraph1"), NodeFactory.createURI("#electricVehicle3"),
							NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"),
							NodeFactory.createLiteral("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle1")));
		}

		// findNG with any and null
		Iterator<Quad> result2 = markLogicDatasetGraphReader.findNG(NodeFactory.createURI("http://originalGraph1"), Node.ANY,
				NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"), null);
		while (result2.hasNext()) {
			Quad quad1 = result2.next();
			assertTrue(
					"returned" + quad1,
					quad1.matches(NodeFactory.createURI("http://originalGraph1"), NodeFactory.createURI("#electricVehicle3"),
							NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type1"),
							NodeFactory.createLiteral("http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle1")));
		}

		Node n1 = NodeFactory.createURI("http://originalGraph1");

		markLogicDatasetGraphAdmin.clearPermissions(n1);
		GraphPermissions permissions = markLogicDatasetGraphAdmin.getPermissions(n1);
		assertTrue("Didnot have expected permissions, returned " + permissions, permissions.get("rest-writer").contains(Capability.UPDATE)
				&& permissions.get("rest-reader").contains(Capability.READ));

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
		// Add Graph and Validate
		markLogicDatasetGraphWriter.addGraph(newgraph, g);
		markLogicDatasetGraphWriter.sync();
		markLogicDatasetGraphWriter.clearPermissions(newgraph);//
		assertTrue(markLogicDatasetGraphWriter.containsGraph(newgraph));
		GraphPermissions permissions = markLogicDatasetGraphAdmin.getPermissions(newgraph);
		markLogicDatasetGraphWriter.addPermissions(newgraph, permissions.permission("test-eval", Capability.EXECUTE));
		permissions = markLogicDatasetGraphWriter.getPermissions(newgraph);
		System.out.println(markLogicDatasetGraphWriter.getPermissions(newgraph));
		assertTrue("Did not have permission looking for", permissions.get("test-eval").contains(Capability.EXECUTE));
		//
		markLogicDatasetGraphWriter.addPermissions(newgraph, permissions.permission("test-eval", Capability.UPDATE));
		System.out.println(" added one more capability ===="+markLogicDatasetGraphWriter.getPermissions(newgraph));
		assertTrue(permissions.get("test-eval").size() == 2);
		//
		
		markLogicDatasetGraphWriter.clearPermissions(newgraph);
		markLogicDatasetGraphWriter.sync();
		permissions = markLogicDatasetGraphWriter.getPermissions(newgraph);
		System.out.println(permissions);
		assertTrue("Should not have Execute for test-eval", !(permissions.containsValue("test-eval")));

		// Set Execute permissions and validate
		permissions = permissions.permission("test-eval", Capability.EXECUTE);
		markLogicDatasetGraphWriter.writePermissions(newgraph, permissions);
		assertTrue("Did not have permission looking for", permissions.get("test-eval").contains(Capability.EXECUTE));

		// Set UPDATE permissions and validate
		permissions.clear();
		 permissions = permissions.permission("test-eval", Capability.UPDATE);
		markLogicDatasetGraphWriter.writePermissions(newgraph, permissions);
		System.out.println(" added one more capability ===="+markLogicDatasetGraphWriter.getPermissions(newgraph));
		permissions = markLogicDatasetGraphWriter.getPermissions(newgraph);
		assertTrue(permissions.get("test-eval").size() == 1);
		assertTrue("Did not have permission looking for", permissions.get("test-eval").contains(Capability.UPDATE));

		// Set the same permission for the same graph
		markLogicDatasetGraphWriter.writePermissions(newgraph, permissions);
		assertTrue(permissions.get("test-eval").size() == 1);

	}

	@Test
	public void testAddDelete_permissions_inTrx() throws Exception {
		String file = datasource + "rdfxml1.rdf";
		try {
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
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			if (markLogicDatasetGraphWriter.isInTransaction())
				markLogicDatasetGraphWriter.end();
		}
	}

	/*
	 * Add/Delete Quad and graph within Transaction
	 */
	@Test
	public void testCRUD_InTrx() throws Exception {

		String file = datasource + "rdfxml1.rdf";
		try {
			Node newgraph = NodeFactory.createURI("http://jena.example.org/perm");
			// Read triples into dataset & Add to named graph
			markLogicDatasetGraphWriter.begin(ReadWrite.WRITE);
			RDFDataMgr.read(markLogicDatasetGraphWriter, file);
			markLogicDatasetGraphWriter.sync();
			Graph g = markLogicDatasetGraphWriter.getDefaultGraph();

			markLogicDatasetGraphWriter.addGraph(newgraph, g);
			markLogicDatasetGraphWriter.commit();

			// Delete one Triple and Validate
			markLogicDatasetGraphWriter.begin(ReadWrite.WRITE);
			markLogicDatasetGraphWriter.deleteAny(newgraph, Node.ANY, NodeFactory.createURI("http://example.org/kennedy/sameAs"), Node.ANY);
			markLogicDatasetGraphWriter.commit();
			markLogicDatasetGraphWriter.sync();
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

			// Add Graph and Find Quads
			markLogicDatasetGraphWriter.begin(ReadWrite.WRITE);
			Node newgraph1 = NodeFactory.createURI("http://jena.example.org/perm1");
			markLogicDatasetGraphWriter.addGraph(newgraph1, g);
			Iterator<Quad> quads = markLogicDatasetGraphWriter.find();
			assertTrue(quads.hasNext());
			while (quads.hasNext()) {
				Quad quad1 = quads.next();
				System.out.println(quad1.getSubject());
				assertTrue(quad1.getSubject().matches(NodeFactory.createURI("http://example.org/kennedy/person1")));
			}
			// Commit and validate outside trx
			markLogicDatasetGraphWriter.commit();

			quads = markLogicDatasetGraphWriter.find();
			assertTrue(quads.hasNext());
			while (quads.hasNext()) {
				Quad quad1 = quads.next();
				System.out.println(quad1.getSubject());
				assertTrue(quad1.getSubject().matches(NodeFactory.createURI("http://example.org/kennedy/person1")));
			}
			// READ trx
			Exception exp = null;
			try {
				markLogicDatasetGraphWriter.begin(ReadWrite.READ);
				Iterator<Quad> quadsRead = markLogicDatasetGraphWriter.find();
				assertTrue(quads.hasNext());
				while (quadsRead.hasNext()) {
					Quad quad1 = quadsRead.next();
					System.out.println(quad1.getSubject());
					assertTrue(quad1.getSubject().matches(NodeFactory.createURI("http://example.org/kennedy/person1")));
				}
			} catch (Exception e) {
				exp = e;
				System.out.println(e);
			}
			assertTrue("should throw:: MarkLogic only supports write transactions",
					exp.toString().contains(" MarkLogic only supports write transactions"));
			markLogicDatasetGraphWriter.end();
		} catch (Exception e) {

		} finally {
			if (markLogicDatasetGraphWriter.isInTransaction())
				markLogicDatasetGraphWriter.end();
		}
	}

	/*
	 * -ve parsing with wrong format
	 */

	@Test
	public void testCRUD_triplexml() {

		String file = datasource + "triplexml1.xml";
		Exception exp = null;
		// Read triples into dataset
		try {
			RDFDataMgr.read(markLogicDatasetGraphWriter, file);
			markLogicDatasetGraphWriter.sync();
			Graph g1 = markLogicDatasetGraphWriter.getDefaultGraph();

			assertTrue("did not match Triples", g1.toString().contains("Anna's Homepage"));

		} catch (Exception e) {
			exp = e;

		}
		assertTrue(exp.toString().contains("RiotException") && exp != null);
	}
	
	
	/*
	 * Add Triples to dataset using multiple threads wihtout a tracsaction, 
	 * Adds 400 triples to datastore 
	 */
	@Test
	public void testMultiThreadAdd1() throws InterruptedException{
		final Node uri = NodeFactory.createURI("http://multithreadgraph");  

		class MyRunnable implements Runnable {
			@Override
			public void run(){
				try {
					markLogicDatasetGraphAdmin.getGraph(uri);
					for (int j =0 ;j < 100; j++){

						Node subject = NodeFactory.createURI("testNamewithID/"+Thread.currentThread().getId()+"/"+"#lastName");
						Node predicate = NodeFactory.createURI("testNamewithID/"+Thread.currentThread().getId()+"/"+"#firstName");
						Node object = NodeFactory.createLiteral(Thread.currentThread().getId()+ "-" + j +"-" +"Object");
						markLogicDatasetGraphAdmin.add(uri, subject, predicate, object);
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}  

		Thread t1,t2,t3,t4;
		t1 = new Thread(new MyRunnable());
		t1.setName("T1");

		t2 = new Thread(new MyRunnable());
		t2.setName("T2");

		t3 = new Thread(new MyRunnable());
		t3.setName("T3");

		t4 = new Thread(new MyRunnable());
		t4.setName("T4");

		t1.start();
		t2.start();
		t3.start();
		t4.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();

		Graph mergedgraph = markLogicDatasetGraphAdmin.getGraph(uri);
		System.out.println("SIZE++++++====="+mergedgraph.size());
		assertTrue("Graph Size is not Expected seize , Got"+mergedgraph.size(), mergedgraph.size() == 400);

	}

	/*
	 * Add triples to datastore using multiple threads and same interface,
	 * Should add 5 sets of 100 triples to datastore
	 */
	@Test
	public void testMultiThreadAdd_Trx() throws InterruptedException{
		final Node uri = NodeFactory.createURI("http://multithreadgraph");  
		final int graphSize;

		class MyRunnable implements Runnable {
			MarkLogicDatasetGraph dataSetGraph = MarkLogicDatasetGraphFactory.createDatasetGraph(TEST_HOST, 8014, "rest-admin", "x",
					Authentication.DIGEST);
			@Override
			public void run(){
				try {
					dataSetGraph.begin(ReadWrite.WRITE);
					dataSetGraph.getGraph(uri);
					for (int j =0 ;j < 100; j++){

						Node subject = NodeFactory.createURI("testNamewithID/"+Thread.currentThread().getId()+"/"+"#lastName");
						Node predicate = NodeFactory.createURI("testNamewithID/"+Thread.currentThread().getId()+"/"+"#firstName");
						Node object = NodeFactory.createLiteral(Thread.currentThread().getId()+ "-" + j +"-" +"Object");
						dataSetGraph.add(uri, subject, predicate, object);
					}
					dataSetGraph.sync();
					dataSetGraph.commit();

				} catch (Exception e) {
					e.printStackTrace();
				}
				finally{
					try {
						if(dataSetGraph.isInTransaction())
							dataSetGraph.end();
					} catch (Exception e) {
						e.printStackTrace();
					}
					try {
						if(dataSetGraph.getDatabaseClient() != null)
							dataSetGraph.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

		}  

		Thread t1,t2,t3,t4,t5;
		t1 = new Thread(new MyRunnable());
		t1.setName("T1");

		t2 = new Thread(new MyRunnable());
		t2.setName("T2");

		t3 = new Thread(new MyRunnable());
		t3.setName("T3");

		t4 = new Thread(new MyRunnable());
		t4.setName("T4");

		t5 = new Thread(new MyRunnable());
		t5.setName("T5");

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();

		Graph mergedgraph = markLogicDatasetGraphAdmin.getGraph(uri);
		System.out.println("FINAL SIZE++++++====="+mergedgraph.size());
		assertTrue("Graph Size is not Expected seize , Got"+mergedgraph.size(), mergedgraph.size() == 500);


	}

	/*
	 * Add same triples set to datastore using multiple threads and same interface,
	 * should only add three set's of 100 triples to datastore but query should only return 100 triples 
	 * eliminating the duplicates
	 */
	@Test
	public void testMultiThreadAddDuplicate_Trx() throws InterruptedException{
		final Node uri = NodeFactory.createURI("http://multithreadgraph");  
		final int graphSize;

		class MyRunnable implements Runnable {
			MarkLogicDatasetGraph dataSetGraph = MarkLogicDatasetGraphFactory.createDatasetGraph(TEST_HOST, 8014, "rest-admin", "x",
					Authentication.DIGEST);
			@Override
			public void run(){
				try {
					dataSetGraph.begin(ReadWrite.WRITE);
					dataSetGraph.getGraph(uri);
					for (int j =0 ;j < 100; j++){

						Node subject = NodeFactory.createURI("testNamewithID/#lastName");
						Node predicate = NodeFactory.createURI("testNamewithID/#firstName");
						Node object = NodeFactory.createLiteral(j +"-" +"Object");
						dataSetGraph.add(uri, subject, predicate, object);
					}
					dataSetGraph.sync();
					dataSetGraph.commit();

				} catch (Exception e) {
					e.printStackTrace();
				}
				finally{
					try {
						if(dataSetGraph.isInTransaction())
							dataSetGraph.end();
					} catch (Exception e) {
						e.printStackTrace();
					}
					try {
						if(dataSetGraph.getDatabaseClient() != null)
							dataSetGraph.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

		}  

		Thread t1,t2,t3;
		t1 = new Thread(new MyRunnable());
		t1.setName("T1");

		t2 = new Thread(new MyRunnable());
		t2.setName("T2");

		t3 = new Thread(new MyRunnable());
		t3.setName("T3");

		t1.start();
		t2.start();
		t3.start();

		t1.join();
		t2.join();
		t3.join();

		Graph mergedgraph = markLogicDatasetGraphAdmin.getGraph(uri);
		System.out.println("FINAL SIZE++++++====="+mergedgraph.size());
		assertTrue("Graph Size is not Expected seize , Got"+mergedgraph.size(), mergedgraph.size() == 100);

	}
	
	
}
