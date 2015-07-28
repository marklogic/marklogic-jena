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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;

public class JenaGraphTests extends ConnectedRESTQA {
	private static GraphManager gmWriter;
	private static GraphManager gmReader;
	private static GraphManager gmAdmin;
	private static String dbName = "Jena-JavaAPI-Functional";
	private static String[] fNames = { "Jena-JavaAPI-Functional-1" };
	private static String restServerName = "REST-Java-Client-JenaAPI-Server";
	private static int restPort = 8014;
	private static int uberPort = 8000;
	private static DatabaseClient client;
	private DatabaseClient adminClient = null;
	private DatabaseClient writerClient = null;
	private DatabaseClient readerClient = null;
	private DatabaseClient evalClient = null;
	private static String datasource = "src/test/resources/data/semantics/";
	private static DatasetGraph markLogicDatasetGraphWriter;
	private static DatasetGraph markLogicDatasetGraphReader;
	private static DatasetGraph markLogicDatasetGraphAdmin;


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
    
	@Test
	public void testWrite_writeUser() {
		Graph g1 = markLogicDatasetGraphWriter.getDefaultGraph();
		Triple triple = new Triple(NodeFactory.createURI("s5"),NodeFactory.createURI("p5"),NodeFactory.createURI("o5"));
		g1.add(triple);
		Node n1 = NodeFactory.createURI("http://example.org/jenaAdd");
		markLogicDatasetGraphWriter.addGraph(n1, g1);
		Graph g2 =	markLogicDatasetGraphWriter.getGraph(n1);
		assertTrue("did not match Triples",g2.contains(NodeFactory.createURI("s5"), NodeFactory.createURI("p5"), NodeFactory.createURI("o5")));//g2.equals(triple.getPredicate().equals("@p5"));
		System.out.println(g2.toString());
	}
	
}
