package com.marklogic.jena.functionaltests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;

import org.apache.jena.riot.Lang;
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
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.QuerySolutionMap;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateRequest;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.semantics.Capability;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;

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
		deleteRESTUser("perm-user");
		deleteUserRole("test-perm");
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

	@Test
	public void testStringAskQuery() {
		String file = datasource + "tigers.ttl";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraph, file);
		markLogicDatasetGraph.sync();
		dataSet = DatasetFactory.create(markLogicDatasetGraph);

		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>" + " ASK " + " WHERE" + " {" + " ?id bb:lastname  ?name ."
				+ " FILTER  EXISTS { ?id bb:country ?countryname }" + " }";
		QueryExecution queryExec = QueryExecutionFactory.create(query1, dataSet);
		Boolean result = queryExec.execAsk();
		assertFalse(result);

		String query2 = "PREFIX  bb: <http://marklogic.com/baseball/players#>" + "PREFIX  r: <http://marklogic.com/baseball/rules#>"
				+ " ASK WHERE" + " {" + " ?id bb:team r:Tigers." + " ?id bb:position \"pitcher\"." + " }";
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

		String query1 = "ASK FROM <http://marklogic.com/Graph1>" + " WHERE" + " {" + " ?player ?team <#Tigers>." + " }";
		QueryExecution queryExec = QueryExecutionFactory.create(query1, dataSet);
		Boolean result = queryExec.execAsk();
		assertFalse(result);

		String query2 = "PREFIX  bb: <http://marklogic.com/baseball/players#>" + "PREFIX  r: <http://marklogic.com/baseball/rules#>"
				+ " ASK WHERE" + " {" + " ?id bb:team r:Tigers." + " ?id bb:position \"pitcher\"." + " }";
		queryExec = QueryExecutionFactory.create(query2, dataSet);
		assertTrue(queryExec.execAsk());

	}

	@Test
	public void test002DescribeQuery_withbinding() {
		markLogicDatasetGraph.clear();
		Node newgraph = NodeFactory.createURI("http://marklogic.com/graph1");
		dataSet = DatasetFactory.create(markLogicDatasetGraph);
		markLogicDatasetGraph.add(newgraph, NodeFactory.createURI("john"), NodeFactory.createURI("fname"),
				NodeFactory.createURI("johnfname"));
		markLogicDatasetGraph.add(newgraph, NodeFactory.createURI("john"), NodeFactory.createURI("lname"),
				NodeFactory.createURI("johnlname"));
		markLogicDatasetGraph.add(newgraph, NodeFactory.createURI("john"), NodeFactory.createURI("homeTel"),
				NodeFactory.createURI("111111111D"));
		markLogicDatasetGraph.add(newgraph, NodeFactory.createURI("john"), NodeFactory.createURI("email"),
				NodeFactory.createURI("john@email.com"));

		markLogicDatasetGraph
				.add(newgraph, NodeFactory.createURI("Joe"), NodeFactory.createURI("fname"), NodeFactory.createURI("Joefname"));
		markLogicDatasetGraph
				.add(newgraph, NodeFactory.createURI("Joe"), NodeFactory.createURI("lname"), NodeFactory.createURI("Joelname"));
		markLogicDatasetGraph.add(newgraph, NodeFactory.createURI("Joe"), NodeFactory.createURI("homeTel"),
				NodeFactory.createURI("222222222D"));
		markLogicDatasetGraph.add(newgraph, NodeFactory.createURI("Joe"), NodeFactory.createURI("email"),
				NodeFactory.createURI("joe@email.com"));

		markLogicDatasetGraph.add(newgraph, NodeFactory.createURI("jerry"), NodeFactory.createURI("fname"),
				NodeFactory.createURI("jerryfname"));
		markLogicDatasetGraph.add(newgraph, NodeFactory.createURI("jerry"), NodeFactory.createURI("lname"),
				NodeFactory.createURI("jerrylname"));
		markLogicDatasetGraph.add(newgraph, NodeFactory.createURI("jerry"), NodeFactory.createURI("homeTel"),
				NodeFactory.createURI("333333333D"));
		markLogicDatasetGraph.add(newgraph, NodeFactory.createURI("jerry"), NodeFactory.createURI("email"),
				NodeFactory.createURI("jerry@email.com"));

		// TODO:: query to get everything related to subject / predicate
		String query1 = "CONSTRUCT{ ?person <homeTel> ?o .}  FROM <http://marklogic.com/graph1> WHERE {"
				+ "  ?person <homeTel> ?o .  ?person <fname> ?firstname .} ";
		QuerySolutionMap binding = new QuerySolutionMap();
		binding.add("firstname", ResourceFactory.createResource("Joefname"));
		QueryExecution queryExec = QueryExecutionFactory.create(query1, dataSet, binding);
		Model results = queryExec.execConstruct();
		assertTrue(results.getGraph().size() == 1);
		assertTrue(results.getGraph().contains(Node.ANY, Node.ANY, NodeFactory.createURI("222222222D")));

	}

	@Test
	public void testStringQuery_withbinding() {
		markLogicDatasetGraph.clear();
		String file = datasource + "tigers.ttl";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraph, file);
		markLogicDatasetGraph.sync();
		Graph g = markLogicDatasetGraph.getDefaultGraph();
		Node newgraph = NodeFactory.createURI("http://marklogic.com/Graph1");
		markLogicDatasetGraph.addGraph(newgraph, g);
		dataSet = DatasetFactory.create(markLogicDatasetGraph);

		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#> ASK FROM <http://marklogic.com/Graph1> WHERE {"
				+ " ?s bb:position ?o." + "}";

		String query2 = "PREFIX  bb: <http://marklogic.com/baseball/players#>  SELECT  ?o FROM <http://marklogic.com/Graph1> WHERE"
				+ "{ ?s bb:position ?o.} LIMIT 2";

		String query3 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"
				+ "CONSTRUCT{ ?ID bb:position ?o .}  FROM <http://marklogic.com/Graph1> WHERE { ?ID bb:position ?o ."
				+ "?ID bb:firstname ?firstname ." + "}  order by $ID  ?o ";

		String query4 = "PREFIX  bb: <http://marklogic.com/baseball/players#> CONSTRUCT { ?ID ?p \"coach\" .}  FROM <http://marklogic.com/Graph1>"
				+ "WHERE { ?ID ?p \"coach\" . ?ID bb:firstname ?firstname . Values ?firstname {\"Gene\"}}";

		// TODO:: confirm Query and add assert
		// PREFIX bb: <http://marklogic.com/baseball/players#>
		// CONSTRUCT { ?ID bb:position ?o .} FROM <http://marklogic.com/Graph1>
		// WHERE { ?ID bb:position ?o .
		// bb:120 bb:position ?o .}

		String query6 = "DESCRIBE <http://marklogic.com/baseball/players#107>";

		// ASK
		QuerySolutionMap binding = new QuerySolutionMap();
		binding.add("s", ResourceFactory.createResource("http://marklogic.com/baseball/players#120"));
		QueryExecution queryExec = QueryExecutionFactory.create(query1, dataSet, binding);
		assertTrue(queryExec.execAsk());

		// Select
		QueryExecution queryExec2 = QueryExecutionFactory.create(query2, dataSet, binding);
		ResultSet results2 = queryExec2.execSelect();

		while (results2.hasNext()) {
			QuerySolution qs = results2.next();
			System.out.println(qs.toString());
			assertTrue(qs.contains("o"));
			String obtained = qs.get("o").toString();
			System.out.println(obtained);
			assertTrue("Expecting Object node to be::catcher ", obtained.equals("catcher^^http://www.w3.org/2001/XMLSchema#string"));
		}

		// Construct with Literal Binding
		QuerySolutionMap binding3 = new QuerySolutionMap();
		binding3.add("firstname", ResourceFactory.createPlainLiteral("Bryan"));
		QueryExecution queryExec3 = QueryExecutionFactory.create(query3, dataSet, binding3);
		Model model = queryExec3.execConstruct();
		assertTrue(model.getGraph().size() == 1);
		assertTrue(model.getGraph().contains(Node.ANY, Node.ANY, NodeFactory.createLiteral("catcher")));

		// Construct with Values passed into query
		QueryExecution queryExec4 = QueryExecutionFactory.create(query4, dataSet);
		Model model4 = queryExec4.execConstruct();
		assertTrue(model4.getGraph().size() == 1);
		assertTrue(model4.getGraph().contains(NodeFactory.createURI("http://marklogic.com/baseball/players#158"),
				NodeFactory.createURI("http://marklogic.com/baseball/players#position"), NodeFactory.createLiteral("coach")));

		// Describe
		QueryExecution queryExec6 = QueryExecutionFactory.create(query6, dataSet);
		Model model6 = queryExec6.execDescribe();
		System.out.println(model6.getGraph().size() == 12);
		assertTrue(model6.getGraph().size() == 12);

	}

	@Test
	public void testSelectQuery_withbinding() {

		markLogicDatasetGraph.clear();
		String file = datasource + "property-paths.ttl";
		RDFDataMgr.read(markLogicDatasetGraph, file);
		markLogicDatasetGraph.sync();
		dataSet = DatasetFactory.create(markLogicDatasetGraph);

		StringBuilder queryBuilder = new StringBuilder(128);
		queryBuilder.append(" prefix : <http://learningsparql.com/ns/papers#> ");
		queryBuilder.append(" prefix c: <http://learningsparql.com/ns/citations#>");
		queryBuilder.append(" SELECT ?s");
		queryBuilder.append(" WHERE {  ");
		queryBuilder.append(" ?s ^c:cites :paperK2 . ");
		queryBuilder.append(" FILTER (?s != :paperK2)");
		queryBuilder.append(" } ");
		queryBuilder.append(" ORDER BY ?s ");

		String query = queryBuilder.toString();
		QuerySolutionMap binding = new QuerySolutionMap();
		binding.add("whatcode", ResourceFactory.createPlainLiteral("33333"));
		QueryExecution queryExec = QueryExecutionFactory.create(query, dataSet, binding);

		ResultSet results = queryExec.execSelect();
		assertNotNull(results);
		assertTrue(results.hasNext());
		while (results.hasNext()) {
			QuerySolution qs = results.next();
			System.out.println(qs.toString());
			assertTrue(qs.contains("s"));
			String obtained = qs.get("s").asNode().getURI();
			System.out.println(obtained);
			assertTrue("Expecting subject node to be::http://learningsparql.com/ns/papers#paperJ ",
					qs.get("s").asNode().getURI().equals("http://learningsparql.com/ns/papers#paperJ"));
		}
	}

	/*
	 * Constrain Query returns the complete document satisfying the search
	 */

	@Test
	public void testRawCombinedQueryQuery() {
		markLogicDatasetGraph.clear();
		String file = datasource + "tigers.ttl";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraph, file);
		markLogicDatasetGraph.sync();
		dataSet = DatasetFactory.create(markLogicDatasetGraph);
		queryManager = writerClient.newQueryManager();

		String query1 = "ASK WHERE {?s ?p \"Nathan\" .}";
		String query2 = "SELECT ?s ?p ?o  WHERE {?s ?p ?o .}";

		// case one, rawcombined
		String combinedQuery = "{\"search\":" + "{\"qtext\":\"lastname\"}}";
		String combinedQuery2 = "{\"search\":" + "{\"qtext\":\"Alex\"}}";

		RawCombinedQueryDefinition rawCombined = queryManager.newRawCombinedQueryDefinition(new StringHandle().with(combinedQuery)
				.withFormat(Format.JSON));
		RawCombinedQueryDefinition rawombined2 = queryManager.newRawCombinedQueryDefinition(new StringHandle().with(combinedQuery2)
				.withFormat(Format.JSON));

		markLogicDatasetGraph.setConstrainingQueryDefinition(rawCombined);
		QueryExecution queryExec = QueryExecutionFactory.create(query1, dataSet);
		System.out.println(queryExec);
		assertTrue(queryExec.execAsk());

		markLogicDatasetGraph.setConstrainingQueryDefinition(rawombined2);
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
		}
	}

	/*
	 * Constrain Query returns the complete document satisfying the search
	 */
	@Test
	public void testStringCriteriaQuery() {
		markLogicDatasetGraph.clear();
		String file = datasource + "tigers.ttl";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraph, file);
		markLogicDatasetGraph.sync();
		dataSet = DatasetFactory.create(markLogicDatasetGraph);
		queryManager = writerClient.newQueryManager();

		String query1 = "ASK WHERE {?s ?p \"Nathan\" .}";
		String query2 = "SELECT ?s ?p ?o WHERE {?s ?p ?o .}";

		StringQueryDefinition stringDef = queryManager.newStringDefinition().withCriteria("lastname");
		markLogicDatasetGraph.setConstrainingQueryDefinition(stringDef);
		QueryExecution queryExec = QueryExecutionFactory.create(query1, dataSet);
		System.out.println(queryExec);
		assertTrue(queryExec.execAsk());

		StringQueryDefinition stringDef1 = queryManager.newStringDefinition().withCriteria("Alex");
		markLogicDatasetGraph.setConstrainingQueryDefinition(stringDef1);
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
		}
	}

	@Test
	public void testSPARQLUpdate_add() {
		String file = datasource + "tigers.ttl";
		// Read triples into dataset
		RDFDataMgr.read(markLogicDatasetGraph, file);
		GraphStore graphstore = markLogicDatasetGraph;
		markLogicDatasetGraph.sync();

		dataSet = DatasetFactory.create(markLogicDatasetGraph);

		UpdateRequest update = new UpdateRequest();
		update.add("INSERT DATA { <s1> <p1> <o1> }");
		update.add("DROP ALL").add("CREATE GRAPH <http://example/update1>")
				.add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/update2> { <s1> <p1> <o1>  } }")
				.add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/update3> { <s1> <p1> <o1>  } }");
		UpdateAction.execute(update, graphstore);
		QueryExecution askQuery = QueryExecutionFactory.create(
				"BASE <http://example.org/> ASK WHERE { GRAPH <update3> { <s1> <p1> <o1>  }}", graphstore.toDataset());
		assertTrue("update action must update database.", askQuery.execAsk());

		// Returns no results
		QueryExecution askQuery1 = QueryExecutionFactory.create(
				"BASE <http://example.org/> ASK WHERE { GRAPH <update3> { <s0> <p0> <o0>  }}", graphstore.toDataset());
		assertFalse("update action must update database.", askQuery1.execAsk());
	}

	/*
	 * SPARQL UPDATE operations
	 */

	@Test
	public void testSPARQLUpdate() {
		GraphStore graphstore = markLogicDatasetGraph;
		UpdateRequest update = new UpdateRequest();
		// Clear all data and Insert two graphs
		update.add("DROP ALL").add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/copy1> { <s1> <p1> <o1>  } }")
				.add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/copy2> { <s2> <p2> <o2>  } }");
		UpdateAction.execute(update, graphstore);

		// Copy Graph1 to Graph 2 and validate
		update = new UpdateRequest();
		update.add("COPY <http://example.org/copy1> TO <http://example.org/copy2>");
		UpdateAction.execute(update, graphstore);

		QueryExecution askQuery = QueryExecutionFactory.create(
				"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy2> { <s1> <p1> <o1>  }}", graphstore.toDataset());
		assertTrue("Triples from copy1 should be copied to copy2", askQuery.execAsk());

		QueryExecution askQuery2 = QueryExecutionFactory.create(
				"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy2> { <s2> <p2> <o2>  }}", graphstore.toDataset());
		assertFalse("Triples from copy2 should be replaced by copy1", askQuery2.execAsk());

		// Perform INSERT WHERE on Graph 1 and validate
		update = new UpdateRequest();
		update.add("BASE <http://example.org/> INSERT  {GRAPH <copy1> {<s11> <p11> <o11>}}  WHERE {GRAPH <copy1> {?s ?p <o1>}}");
		UpdateAction.execute(update, graphstore);

		QueryExecution askQuery3 = QueryExecutionFactory.create(
				"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy1> { <s11> <p11> <o11>  }}", graphstore.toDataset());
		assertTrue("Triples Should be inserted into copy1", askQuery3.execAsk());

		// Perform DELETE WHERE on Graph 1 and validate
		update = new UpdateRequest();
		update.add("BASE <http://example.org/> DELETE  {GRAPH <copy1> {<s1> <p1> <o1>}}  WHERE {GRAPH <copy1> {?s ?p <o11>}}");
		UpdateAction.execute(update, graphstore);

		QueryExecution askQuery4 = QueryExecutionFactory.create(
				"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy1> { <s1> <p1> <o1>  }}", graphstore.toDataset());
		assertFalse("Triples Should be deleted from copy1", askQuery4.execAsk());

		// Move contents of graph 1 to graph 2 and validate
		update = new UpdateRequest();
		update.add("BASE <http://example.org/> MOVE <copy1> to <copy2>");
		UpdateAction.execute(update, graphstore);

		QueryExecution askQuery5 = QueryExecutionFactory.create(
				"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy1> { <s11> <p11> <o11>  }}", graphstore.toDataset());
		assertFalse("Triples Should be deleted from copy1", askQuery5.execAsk());

		QueryExecution askQuery6 = QueryExecutionFactory.create(
				"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy2> { <s11> <p11> <o11>  }}", graphstore.toDataset());
		assertTrue("Triples Should be deleted from copy1", askQuery6.execAsk());

		QueryExecution askQuery7 = QueryExecutionFactory.create(
				"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy2> { <s1> <p1> <o1>  }}", graphstore.toDataset());
		assertFalse("Triples Should be deleted from copy1", askQuery7.execAsk());

	}

	// TODO:: UPDATE test after the fix for Git issue #7
	@Test
	public void testSPARQLUpdate_Permissions() {

		createUserRolesWithPrevilages("test-perm");
		createRESTUser("perm-user", "x", "test-perm");

		UpdateRequest update = new UpdateRequest();
		update.add("DROP ALL").add("CREATE GRAPH <http://test.perm/>");
		UpdateAction.execute(update, markLogicDatasetGraph);
		GraphPermissions graphPermissions = markLogicDatasetGraph.getPermissions(NodeFactory.createURI("http://test.perm/"));

		markLogicDatasetGraph.setSPARQLUpdatePermissions(graphPermissions.permission("test-perm", Capability.READ));
		update = new UpdateRequest();
		// Clear all data and Insert two graphs with READ capability for
		// test-perm Role
		update.add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/copy1> { <s1> <p1> <o1>  } }").add(
				"BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/copy2> { <s2> <p2> <o2>  } }");
		UpdateAction.execute(update, markLogicDatasetGraph);

		// Validate test-perm role has READ capability on the graph
		graphPermissions = markLogicDatasetGraph.getPermissions(NodeFactory.createURI("http://example.org/copy1"));
		System.out.println("Should have READ::" + graphPermissions.get("test-perm"));
		assertTrue(graphPermissions.get("test-perm").contains(Capability.READ));

		// Set Update Capability for the test-perm role
		markLogicDatasetGraph.setSPARQLUpdatePermissions(graphPermissions.permission("test-perm", Capability.UPDATE));

		// Perform INSERT WHERE on Graph 1 and validate with BOTH update and
		// read capabilities for test-perm role
		update = new UpdateRequest();
		update.add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/copy1> { <s11> <p11> <o11>  } }");
		UpdateAction.execute(update, markLogicDatasetGraph);
		graphPermissions = markLogicDatasetGraph.getPermissions(NodeFactory.createURI("http://example.org/copy1"));
		assertTrue(graphPermissions.get("test-perm").contains(Capability.READ));

		QueryExecution askQuery3 = QueryExecutionFactory.create(
				"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy1> { <s11> <p11> <o11>  }}",
				markLogicDatasetGraph.toDataset());
		assertTrue("Triples Should be inserted into copy1", askQuery3.execAsk());

		// Perform DELETE WHERE on Graph 1 and validate BOTH READ and UPDATE
		// capabilities exist on the Graph
		update = new UpdateRequest();
		update.add("BASE <http://example.org/> DELETE  {GRAPH <copy1> {<s1> <p1> <o1>}}  WHERE {GRAPH <copy1> {?s ?p <o11>}}");
		UpdateAction.execute(update, markLogicDatasetGraph);

		graphPermissions = markLogicDatasetGraph.getPermissions(NodeFactory.createURI("http://example.org/copy1"));
		assertTrue(graphPermissions.get("test-perm").contains(Capability.READ));

		QueryExecution askQuery4 = QueryExecutionFactory.create(
				"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy1> { <s1> <p1> <o1>  }}",
				markLogicDatasetGraph.toDataset());
		assertFalse("Triples Should be deleted from copy1", askQuery4.execAsk());

	}

	@Test
	public void test001SPARQLUpdate_withTrx_Permissions() throws Exception {
		createUserRolesWithPrevilages("test-perm");
		createRESTUser("perm-user", "x", "test-perm");
		Exception exp = null;
		  
		try {
			try{
				markLogicDatasetGraph.end();
			}catch(Exception e){
				System.out.println(e);
				exp =e;
			}
			assertTrue(exp.toString().contains("No open transaction"));
			
			// Start Insert data within Trx, abort and validate
			markLogicDatasetGraph.begin(ReadWrite.WRITE);
			UpdateRequest update = new UpdateRequest();
			update.add("DROP ALL").add("BASE <http://example.org/> INSERT DATA { GRAPH <copy1> { <s1> <p1> <o1>  } }")
					.add("BASE <http://example.org/> INSERT DATA { GRAPH <copy2> { <s2> <p2> <o2>  } }");
			UpdateAction.execute(update, markLogicDatasetGraph);
			markLogicDatasetGraph.abort();
			QueryExecution askQuery = QueryExecutionFactory.create(
					"BASE <http://example.org/> ASK WHERE { GRAPH <copy1> { <s1> <p1> <o1>  }}", markLogicDatasetGraph.toDataset());
			assertFalse("Triples from copy1 should be copied to copy2", askQuery.execAsk());

			// Insert & copy within trx , commit and validate outside trx
			markLogicDatasetGraph.begin(ReadWrite.WRITE);
			update = new UpdateRequest();
			update.add("DROP ALL").add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/copy1> { <s1> <p1> <o1>  } }")
					.add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/copy2> { <s2> <p2> <o2>  } }");
			update.add("COPY <http://example.org/copy1> TO <http://example.org/copy2>");
			UpdateAction.execute(update, markLogicDatasetGraph);
			markLogicDatasetGraph.commit();

			QueryExecution askQuery1 = QueryExecutionFactory.create(
					"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy2> { <s1> <p1> <o1>  }}",
					markLogicDatasetGraph.toDataset());
			assertTrue("Triples from copy1 should be copied to copy2", askQuery1.execAsk());

			// Insert & Delete where .within trx and validate
			markLogicDatasetGraph.begin(ReadWrite.WRITE);
			update = new UpdateRequest();
			update.add("BASE <http://example.org/> INSERT  {GRAPH <copy1> {<s11> <p11> <o11>}}  WHERE {GRAPH <copy1> {?s ?p <o1>}}");
			update.add("BASE <http://example.org/> DELETE  {GRAPH <copy1> {<s1> <p1> <o1>}}  WHERE {GRAPH <copy1> {?s ?p <o11>}}");
			UpdateAction.execute(update, markLogicDatasetGraph);
			markLogicDatasetGraph.commit();

			QueryExecution askQuery2 = QueryExecutionFactory.create(
					"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy1> { <s11> <p11> <o11>  }}",
					markLogicDatasetGraph.toDataset());
			assertTrue("Triple Should be inserted into copy1", askQuery2.execAsk());
			QueryExecution askQuery3 = QueryExecutionFactory.create(
					"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy1> { <s1> <p1> <o1>  }}",
					markLogicDatasetGraph.toDataset());
			assertFalse("Triple Should be deleted from copy1", askQuery3.execAsk());

			// Delete within trx , end trx and validate graph and triples exist
			markLogicDatasetGraph.begin(ReadWrite.WRITE);
			update = new UpdateRequest();
			update.add("DROP ALL");
			UpdateAction.execute(update, markLogicDatasetGraph);
			markLogicDatasetGraph.end();
			askQuery2 = QueryExecutionFactory.create(
					"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy1> { <s11> <p11> <o11>  }}",
					markLogicDatasetGraph.toDataset());
			assertTrue("Triples & Graph copy1 Should exist ", askQuery2.execAsk());
			askQuery1 = QueryExecutionFactory.create(
					"BASE <http://example.org/> ASK WHERE { GRAPH <http://example.org/copy2> { <s1> <p1> <o1>  }}",
					markLogicDatasetGraph.toDataset());
			assertTrue("Triples & Graph copy2 Should exist ", askQuery1.execAsk());

			// set UPdate to role test-perm within transaction and validate
			GraphPermissions permissions = markLogicDatasetGraph.getPermissions(NodeFactory.createURI("http://example.org/copy1"));
			markLogicDatasetGraph.begin(ReadWrite.WRITE);
			markLogicDatasetGraph.setSPARQLUpdatePermissions(permissions.permission("test-perm", Capability.UPDATE));
			update = new UpdateRequest();
			update.add("BASE <http://example.org/> INSERT DATA {GRAPH <copy3> {<s3> <p3> <o3>}}");
			UpdateAction.execute(update, markLogicDatasetGraph);
			markLogicDatasetGraph.commit();
			permissions = markLogicDatasetGraph.getPermissions(NodeFactory.createURI("http://example.org/copy3"));
			assertTrue(permissions.get("test-perm").contains(Capability.UPDATE));

			// set READ to role test-perm within transaction and validate
			markLogicDatasetGraph.begin(ReadWrite.WRITE);
			markLogicDatasetGraph.setSPARQLUpdatePermissions(permissions.permission("test-perm", Capability.READ));
			update = new UpdateRequest();
			update.add("BASE <http://example.org/> INSERT  {GRAPH <copy3> {<s33> <p33> <o33>}}  WHERE {GRAPH <copy1> {?s ?p <o3>}}");
			UpdateAction.execute(update, markLogicDatasetGraph);
			markLogicDatasetGraph.commit();
			permissions = markLogicDatasetGraph.getPermissions(NodeFactory.createURI("http://example.org/copy3"));
			assertTrue(permissions.get("test-perm").contains(Capability.UPDATE));

			// set UPDATE to role test-perm within transaction, abort
			// transaction and validate
			markLogicDatasetGraph.begin(ReadWrite.WRITE);
			markLogicDatasetGraph.setSPARQLUpdatePermissions(permissions.permission("test-perm", Capability.READ));
			update = new UpdateRequest();
			update.add("BASE <http://example.org/> INSERT DATA {GRAPH <copy4> {<s4> <p4> <o4>}}");
			UpdateAction.execute(update, markLogicDatasetGraph);
			markLogicDatasetGraph.abort();
			 exp = null;
			try {
				permissions = markLogicDatasetGraph.getPermissions(NodeFactory.createURI("http://example.org/copy4"));
				assertFalse(permissions.get("test-perm").contains(Capability.READ));
			} catch (Exception e) {
				exp = e;
			}
			assertTrue(exp.toString().contains("Could not read resource at graphs") && exp != null);
		} finally {
			if (markLogicDatasetGraph.isInTransaction())
				markLogicDatasetGraph.commit();
		}

	}

	/*
	 * Pagination with different limits and Offeset
	 */

	@Test
	public void testPagination() {

		markLogicDatasetGraph.clear();
		String file = datasource + "tigers.ttl";
		RDFDataMgr.read(markLogicDatasetGraph, file);
		markLogicDatasetGraph.sync();
		dataSet = DatasetFactory.create(markLogicDatasetGraph);

		Query query = QueryFactory.create("PREFIX  bb: <http://marklogic.com/baseball/players#>  SELECT ?o  WHERE"
				+ "{ ?s bb:position ?o.}");

		// Query with limit 5
		query.setLimit(2);
		QueryExecution queryExec = QueryExecutionFactory.create(query, dataSet);

		ResultSet results = queryExec.execSelect();
		assertNotNull(results);
		assertTrue(results.hasNext());
		int count = 0;
		while (results.hasNext()) {
			QuerySolution qs = results.next();
			System.out.println(qs.toString());
			assertTrue(qs.contains("o"));
			count++;
		}
		assertTrue(count == 2);

		// Query with Offset 2
		query.setOffset(2);
		System.out.println(query.getLimit());
		queryExec = QueryExecutionFactory.create(query, dataSet);
		results = queryExec.execSelect();
		assertNotNull(results);
		assertTrue(results.hasNext());
		count = 0;
		while (results.hasNext()) {
			QuerySolution qs = results.next();
			System.out.println(qs.toString());
			assertTrue(qs.contains("o"));
			count++;
		}
		// TODO confirm LIMIT reset to the query git issue #6
		assertTrue("Should return 9 results but returned (UPdate after GIT issue #6 is resolved)::" + count, count == 9);

		// Query with limit 20 and offset 0
		query.setLimit(20);
		query.setOffset(0);
		queryExec = QueryExecutionFactory.create(query, dataSet);
		results = queryExec.execSelect();
		assertNotNull(results);
		assertTrue(results.hasNext());
		count = 0;
		while (results.hasNext()) {
			QuerySolution qs = results.next();
			System.out.println(qs.toString());
			assertTrue(qs.contains("o"));
			count++;
		}
		assertTrue(count == 11);

		// Query with limit 0 and offset 2
		query.setLimit(0);
		query.setOffset(2);
		queryExec = QueryExecutionFactory.create(query, dataSet);
		results = queryExec.execSelect();
		assertFalse(results.hasNext());

		// Query with limit -1 and offset 2
		Exception exp = null;
		try {
			query.setLimit(-1);
			query.setOffset(2);
			queryExec = QueryExecutionFactory.create(query, dataSet);
			results = queryExec.execSelect();
			assertFalse(results.hasNext());

		} catch (Exception e) {
			System.out.println(e);
			exp = e;
		}
		assertTrue("Should receive:: java.lang.IllegalArgumentException: pageLength must be 0 or greater ",
				exp.toString().contains("pageLength must be 0 or greater"));

		// Query with limit 10 and offset -1
		exp = null;
		try {
			query.setLimit(10);
			query.setOffset(-1);
			queryExec = QueryExecutionFactory.create(query, dataSet);
			results = queryExec.execSelect();
			assertFalse(results.hasNext());
		} catch (Exception e) {
			System.out.println(e);
			exp = e;
		}
		assertTrue("Should receive:: java.lang.IllegalArgumentException: start must be 1 or greater ",
				exp.toString().contains("start must be 1 or greater"));

		// Query with limit -1 and offset -1
		exp = null;
		try {
			query.setLimit(-1);
			query.setOffset(-1);
			queryExec = QueryExecutionFactory.create(query, dataSet);
			results = queryExec.execSelect();
			assertFalse(results.hasNext());
		} catch (Exception e) {
			System.out.println(e);
			exp = e;
		}
		assertTrue("Should receive:: java.lang.IllegalArgumentException: pageLength must be 0 or greater ",
				exp.toString().contains("pageLength must be 0 or greater"));
	}

	@Test
	public void testJenaRuleset() {
		// Build custom data for Ruleset and Inference tests
		String newline = System.getProperty("line.separator");
		StringBuffer inferdata = new StringBuffer();
		inferdata.append("prefix ad: <http://marklogicsparql.com/addressbook#>");
		inferdata.append(newline);
		inferdata.append("prefix id:  <http://marklogicsparql.com/id#>");
		inferdata.append(newline);
		inferdata.append("prefix rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>");
		inferdata.append(newline);
		inferdata.append("prefix rdfs:    <http://www.w3.org/2000/01/rdf-schema#>");
		inferdata.append(newline);
		inferdata.append("prefix ml: <http://marklogicsparql.com/>");
		inferdata.append(newline);

		inferdata.append("id:1111 ad:firstName \"John\" .");
		inferdata.append(newline);
		inferdata.append("id:1111 ad:lastName  \"Snelson\" .");
		inferdata.append(newline);
		inferdata.append("id:1111 ml:writeFuncSpecOf ml:Inference .");
		inferdata.append(newline);
		inferdata.append("id:1111 ml:worksFor ml:MarkLogic .");
		inferdata.append(newline);
		inferdata.append("id:1111 a ml:LeadEngineer .");
		inferdata.append(newline);

		inferdata.append("id:2222 ad:firstName \"Aries\" .");
		inferdata.append(newline);
		inferdata.append("id:2222 ad:lastName  \"Li\" .");
		inferdata.append(newline);
		inferdata.append("id:2222 ml:writeFuncSpecOf ml:SparqlUpdate .");
		inferdata.append(newline);
		inferdata.append("id:2222 ml:worksFor ml:MarkLogic .");
		inferdata.append(newline);
		inferdata.append("id:2222 a ml:SeniorEngineer .");
		inferdata.append(newline);

		inferdata.append("ml:LeadEngineer rdfs:subClassOf  ml:Engineer .");
		inferdata.append(newline);
		inferdata.append("ml:SeniorEngineer rdfs:subClassOf  ml:Engineer .");
		inferdata.append(newline);
		inferdata.append("ml:Engineer rdfs:subClassOf ml:Employee .");
		inferdata.append(newline);
		inferdata.append("ml:Employee rdfs:subClassOf ml:Person .");
		inferdata.append(newline);

		inferdata.append("ml:writeFuncSpecOf rdfs:subPropertyOf ml:design .");
		inferdata.append(newline);
		inferdata.append("ml:developPrototypeOf rdfs:subPropertyOf ml:design .");
		inferdata.append(newline);
		inferdata.append("ml:design rdfs:subPropertyOf ml:develop .");
		inferdata.append(newline);
		inferdata.append("ml:develop rdfs:subPropertyOf ml:worksOn .");

		// Read triples into data-set

		RDFDataMgr.read(markLogicDatasetGraph, new StringReader(inferdata.toString()), "JenaRuleSetTest", Lang.TURTLE);
		markLogicDatasetGraph.sync();
		Graph g = markLogicDatasetGraph.getDefaultGraph();
		Node newgraph = NodeFactory.createURI("http://marklogic.com/JenaRuleSetTest");
		markLogicDatasetGraph.addGraph(newgraph, g);
		dataSet = DatasetFactory.create(markLogicDatasetGraph);

		String query1 = "SELECT ?s ?p ?o  from <http://marklogic.com/JenaRuleSetTest> WHERE {?s ?p ?o .}";
		QueryExecution queryExec = QueryExecutionFactory.create(query1, dataSet);

		ResultSet results18 = queryExec.execSelect();
		// There should be 18 results, without any inference triples.
		int resCount = 0;
		while (results18.hasNext()) {
			QuerySolution qs = results18.next();
			System.out.println(qs.toString());
			resCount++;
		}
		System.out.println("Count of triples without inferences is " + resCount);
		assertEquals("Number of triples without inferences is incorrect ", 18, resCount);

		// Enable one rule set.

		// Multiple execution of a QueryExecution object throws exception.
		// Attempt to use the iterator twice.
		QueryExecution queryExec31 = QueryExecutionFactory.create(query1, dataSet);
		markLogicDatasetGraph.setRulesets(SPARQLRuleset.SUBCLASS_OF);

		ResultSet results31 = queryExec31.execSelect();
		resCount = 0;
		while (results31.hasNext()) {
			QuerySolution qs = results31.next();
			System.out.println(qs.toString());
			resCount++;
		}
		System.out.println("Count of triples with SUBCLASS_OF ruleset is " + resCount);
		assertEquals("Number of triples with SUBCLASS_OF ruleset, inferences is incorrect ", 31, resCount);

		// Enable two rule sets.
		queryExec = QueryExecutionFactory.create(query1, dataSet);
		markLogicDatasetGraph.setRulesets(SPARQLRuleset.SUBCLASS_OF, SPARQLRuleset.SUBPROPERTY_OF);

		ResultSet results44 = queryExec.execSelect();
		resCount = 0;
		while (results44.hasNext()) {
			QuerySolution qs = results44.next();
			System.out.println(qs.toString());
			resCount++;
		}
		System.out.println("Count of triples with SUBCLASS_OF and SUBPROPERTY_OF ruleset is  " + resCount);
		assertEquals("Number of triples with with SUBCLASS_OF and SUBPROPERTY_OF ruleset, inferences inferences is incorrect ", 44,
				resCount);
	}

	// RuleSets ..Transactions

	public void teststubs() {
		QueryDefinition constrainingQueryDefinition = null;
		markLogicDatasetGraph.setConstrainingQueryDefinition(constrainingQueryDefinition);
		SPARQLRuleset rulesets = null;
		markLogicDatasetGraph.setRulesets(rulesets);
		markLogicDatasetGraph.getRulesets();
		markLogicDatasetGraph.withRulesets(rulesets);

		GraphPermissions permissions = null;
		markLogicDatasetGraph.setSPARQLUpdatePermissions(permissions);
		markLogicDatasetGraph.getSPARQLUpdatePermissions();
		markLogicDatasetGraph.startRequest();
		markLogicDatasetGraph.end();
		markLogicDatasetGraph.finishRequest();
		markLogicDatasetGraph.getConstrainingQueryDefinition();
		markLogicDatasetGraph.getDatabaseClient();
		markLogicDatasetGraph.toDataset();
		SPARQLQueryDefinition qdef = null;
		String variableName = null;
		Node objectNode = null;
		markLogicDatasetGraph.bindObject(qdef, variableName, objectNode);
	}
}
