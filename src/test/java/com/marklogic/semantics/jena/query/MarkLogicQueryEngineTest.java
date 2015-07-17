package com.marklogic.semantics.jena.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.marklogic.semantics.jena.JenaTestBase;

public class MarkLogicQueryEngineTest extends JenaTestBase {

	private Dataset ds;
	
	@Before
	public void setupDataset() {
		ds = DatasetFactory.create(getMarkLogicDatasetGraph("testData.trig"));
     //   ds = DatasetFactory.create(getJenaDatasetGraph("testData.trig"));
	}
	
    @Test
    public void testSelect() {
        QueryExecution qe = QueryExecutionFactory.create("select ?s ?p ?o where { ?s ?p ?o}", ds );
        ResultSet results = qe.execSelect();
        //System.out.println(ResultSetFormatter.asText(results));
        while (results.hasNext()) {
            QuerySolution qs = results.next();
            assertTrue(qs.contains("s"));
            assertTrue(qs.contains("p"));
            assertTrue(qs.contains("o"));
        }
    }

    @Test
    public void testAsk() {
    	QueryExecution queryExec = QueryExecutionFactory.create("ASK WHERE { ?s ?p ?o }",
    			ds);
        assertTrue("ExecAsk true", queryExec.execAsk());
 	    queryExec = QueryExecutionFactory.create("ASK WHERE { <this> <isnt> <here> }",
   			ds);
 	    assertFalse("ExecAsk false", queryExec.execAsk());
    }
    
    @Test
    public void testDescribe() {
    	QueryExecution queryExec = QueryExecutionFactory.create("DESCRIBE <http://example.org/r1>",
    			ds);
        Model solution = queryExec.execDescribe();
        assertTrue("Got a solution with more than zero triples", solution.getGraph().size() > 0);
        
        queryExec = QueryExecutionFactory.create("DESCRIBE <http://example.org/g1333>",
    			ds);
        solution = queryExec.execDescribe();
        assertEquals("Got a solution with zero triples", 0, solution.getGraph().size());
    }

    @Test
    public void testConstruct() {
      	QueryExecution queryExec = QueryExecutionFactory.create("PREFIX : <http://example.org/> CONSTRUCT { :r100 ?p ?o } WHERE { :r1 ?p ?o }",
    			ds);
        Model solution = queryExec.execConstruct();
        System.out.println(solution.getGraph().size());
        assertTrue("Got a solution with four triples", solution.getGraph().size() > 0);
    }
    
    @Test
    @Ignore
    public void testPagination() {
    	
    }
    
    @Test
    @Ignore
    public void testTransactions() {
    	
    }
    
    // customizations, obtainable with MarkLogicQueryExecution directly? TODO
    @Test
    @Ignore
    public void testBindings() {
    	
    }
    
    @Test
    @Ignore
    public void testRulesets() {
    	
    }
    
    @Test
    @Ignore
    public void testCombinationQuery() {
    	
    }
}
