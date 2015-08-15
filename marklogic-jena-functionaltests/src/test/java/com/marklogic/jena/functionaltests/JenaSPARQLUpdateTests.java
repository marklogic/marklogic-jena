package com.marklogic.jena.functionaltests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.jena.riot.RDFDataMgr;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.QuerySolutionMap;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateRequest;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.semantics.Capability;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.SPARQLBindings;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;
import com.sun.org.apache.xerces.internal.util.URI;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JenaSPARQLUpdateTests extends ConnectedRESTQA {
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
	private static MarkLogicDatasetGraph markLogicDatasetGraph;
	private static MarkLogicDatasetGraph markLogicDatasetGraphReader;
	private static MarkLogicDatasetGraph markLogicDatasetGraphAdmin;
	private static Dataset dataSet;
	private static QueryManager queryManager;
	
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
		markLogicDatasetGraph = MarkLogicDatasetGraphFactory.createDatasetGraph(writerClient);
		markLogicDatasetGraphReader = MarkLogicDatasetGraphFactory.createDatasetGraph(readerClient);
		markLogicDatasetGraphAdmin = MarkLogicDatasetGraphFactory.createDatasetGraph(adminClient);
		queryManager = writerClient.newQueryManager();
	}


	
//TODO :	 'pagination' i mean just testing queries with LIMIT and OFFSET
	
	
	@Test
	public void testStringAskQuery() {
		String file = datasource + "tigers.ttl";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraph, file);
		markLogicDatasetGraph.sync();
		dataSet = DatasetFactory.create(markLogicDatasetGraph);

		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				" ASK "+
				" WHERE"+ 
				" {"+
				" ?id bb:lastname  ?name ."+
				" FILTER  EXISTS { ?id bb:country ?countryname }"+
				" }";
		 QueryExecution queryExec = QueryExecutionFactory.create(query1, dataSet);
		 Boolean result = 	queryExec.execAsk();
		 assertFalse(result);
		 
		 
		 String query2 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
					"PREFIX  r: <http://marklogic.com/baseball/rules#>"+
					" ASK WHERE"+ 
					" {"+
					 " ?id bb:team r:Tigers."+
					    " ?id bb:position \"pitcher\"."+
					" }";
		 queryExec = QueryExecutionFactory.create(query2, dataSet);
		 assertTrue(queryExec.execAsk());

	}
	
	@Test
	public void testStringAskQuery2() {
		

		String file = datasource + "tigers.ttl";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraph, file);
		markLogicDatasetGraph.sync();
		Graph g = markLogicDatasetGraph.getDefaultGraph();
		Node newgraph = NodeFactory.createURI("http://marklogic.com/Graph1");
		markLogicDatasetGraph.addGraph(newgraph, g);
		dataSet = DatasetFactory.create(markLogicDatasetGraph);
		
		String query1 = "ASK FROM <http://marklogic.com/Graph1>"+
				" WHERE"+ 
				" {"+
				 " ?player ?team <#Tigers>."+
				 " }";
		 QueryExecution queryExec = QueryExecutionFactory.create(query1,dataSet);
		 Boolean result = 	queryExec.execAsk();
		 assertFalse(result);
		 
		 
		 String query2 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
					"PREFIX  r: <http://marklogic.com/baseball/rules#>"+
					" ASK WHERE"+ 
					" {"+
					 " ?id bb:team r:Tigers."+
					    " ?id bb:position \"pitcher\"."+
					" }";
		 queryExec = QueryExecutionFactory.create(query2, dataSet);
		 assertTrue(queryExec.execAsk());

	}
	
	@Test
	public void testStringAskQuery_withbinding() {

		String file = datasource + "tigers.ttl";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraph, file);
		markLogicDatasetGraph.sync();
		Graph g = markLogicDatasetGraph.getDefaultGraph();
		Node newgraph = NodeFactory.createURI("http://marklogic.com/Graph1");
		markLogicDatasetGraph.addGraph(newgraph, g);
		dataSet = DatasetFactory.create(markLogicDatasetGraph);
		 
		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				"ASK "+
				"WHERE"+ 
				"{"+
				" ?s bb:position ?o."+
				 "}";
		 QuerySolutionMap binding = new QuerySolutionMap();
	     binding.add("o", ResourceFactory.createResource("coach"));
		 QueryExecution queryExec = QueryExecutionFactory.create(query1, dataSet, binding);
      	 System.out.println(queryExec.toString());
      	 System.out.println(queryExec.execAsk());
		 assertTrue(queryExec.execAsk());
		 
	}
	

	@Test
	public void testConstrainQuery(){
		String file = datasource + "tigers.ttl";
			// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraph, file);
		markLogicDatasetGraph.sync();
		dataSet = DatasetFactory.create(markLogicDatasetGraph);
	

		String query1 = "ASK WHERE {?s ?p \"Nathan\" .}";
        String query2 = "SELECT ?s ?p ?o  WHERE {?s ?p ?o .}";

        // case one, rawcombined
        String combinedQuery =
                "{\"search\":" +
                        "{\"qtext\":\"lastname\"}}";
        String negCombinedQuery =
                "{\"search\":" +
                        "{\"qtext\":\"Alex\"}}";

        
        RawCombinedQueryDefinition rawCombined = queryManager.newRawCombinedQueryDefinition(new StringHandle().with(combinedQuery).withFormat(Format.JSON));
        RawCombinedQueryDefinition negRawCombined = queryManager.newRawCombinedQueryDefinition(new StringHandle().with(negCombinedQuery).withFormat(Format.JSON));

        markLogicDatasetGraph.setConstrainingQueryDefinition(rawCombined);
        QueryExecution queryExec = QueryExecutionFactory.create(query1, dataSet);
        System.out.println(queryExec);
        assertTrue(queryExec.execAsk());

           
        markLogicDatasetGraph.setConstrainingQueryDefinition(negRawCombined);
        queryExec = QueryExecutionFactory.create(query2, dataSet);

        ResultSet results = queryExec.execSelect();
			assertNotNull(results);
			assertTrue(results.hasNext());
			while (results.hasNext()) {
				QuerySolution qs = results.next();
				System.out.println(qs.toString());
				 assertTrue(qs.contains("s"));  
		            assertTrue(qs.contains("p"));
		            assertTrue(qs.contains("o"));
			System.out.println(	qs.get("o").asLiteral().getValue());
			assertTrue("Expecting Literal Alex but received::"+qs.get("o").asLiteral().getValue(),qs.get("o").asLiteral().getValue().equals("Alex"));
		}
	}
	
	@Test
	public void test001SPARQLUpdate_add(){
		String file = datasource + "tigers.ttl";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraph, file);
		GraphStore graphstore = markLogicDatasetGraph;
		markLogicDatasetGraph.sync();
		
		dataSet = DatasetFactory.create(markLogicDatasetGraph);
	
		UpdateRequest update = new UpdateRequest();
		update.add("INSERT DATA { <s1> <p1> <o1> }");
		update.add("DROP ALL")
	       .add("CREATE GRAPH <http://example/update1>")
	       .add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/update2> { <s1> <p1> <o1>  } }")
	       .add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/update3> { <s1> <p1> <o1>  } }") ;
		UpdateAction.execute(update,  graphstore);
		QueryExecution askQuery = QueryExecutionFactory.create("BASE <http://example.org/> ASK WHERE { GRAPH <update3> { <s1> <p1> <o1>  }}", graphstore.toDataset());
		assertTrue("update action must update database.", askQuery.execAsk());

		// Returns no results
		QueryExecution askQuery1 = QueryExecutionFactory.create("BASE <http://example.org/> ASK WHERE { GRAPH <update3> { <s0> <p0> <o0>  }}", graphstore.toDataset());
		assertFalse("update action must update database.", askQuery1.execAsk());
	}
	
	
	@Test
	public void testSPARQLUpdate_move(){
		String file = datasource + "tigers.ttl";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraph, file);
		GraphStore graphstore = markLogicDatasetGraph;
		markLogicDatasetGraph.sync();
		
		dataSet = DatasetFactory.create(markLogicDatasetGraph);
	
		UpdateRequest update = new UpdateRequest();
		update.add("INSERT DATA { <s1> <p1> <o1> }");
		
		update.add("DROP ALL")
	       .add("CREATE GRAPH <http://example/update1>")
	       .add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/update2> { <s1> <p1> <o1>  } }")
	       .add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/update3> { <s1> <p1> <o1>  } }") ;
		UpdateAction.execute(update,  graphstore);
		QueryExecution askQuery = QueryExecutionFactory.create("BASE <http://example.org/> ASK WHERE { GRAPH <update3> { <s1> <p1> <o1>  }}", graphstore.toDataset());
		assertTrue("update action must update database.", askQuery.execAsk());

		// Returns no results
		QueryExecution askQuery1 = QueryExecutionFactory.create("BASE <http://example.org/> ASK WHERE { GRAPH <update3> { <s0> <p0> <o0>  }}", graphstore.toDataset());
		assertFalse("update action must update database.", askQuery1.execAsk());
	}
	
	
	//Ask, select, construct and describe ...
	// RuleSets ..
	//Pagenation, , quires with limit and offset
	//Bindings ..?
	
	
	public void teststubs(){
	QueryDefinition constrainingQueryDefinition = null;
	markLogicDatasetGraph.setConstrainingQueryDefinition(constrainingQueryDefinition);
	SPARQLRuleset rulesets = null;
	markLogicDatasetGraph.setRulesets(rulesets);
	markLogicDatasetGraph.getRulesets();
	GraphPermissions permissions = null;
	markLogicDatasetGraph.setSPARQLUpdatePermissions(permissions);
	markLogicDatasetGraph.getSPARQLUpdatePermissions();
	markLogicDatasetGraph.startRequest();
	markLogicDatasetGraph.end();
	markLogicDatasetGraph.finishRequest();
	markLogicDatasetGraph.getConstrainingQueryDefinition();
	markLogicDatasetGraph.getDatabaseClient();
	markLogicDatasetGraph.toDataset();
	markLogicDatasetGraph.withRulesets(rulesets);
	SPARQLQueryDefinition qdef = null;
	String variableName = null;
	Node objectNode = null;
	markLogicDatasetGraph.bindObject(qdef, variableName, objectNode);
	}
}
