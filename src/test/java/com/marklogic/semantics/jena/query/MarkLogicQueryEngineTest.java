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
package com.marklogic.semantics.jena.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.hp.hpl.jena.graph.Graph;
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
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.marklogic.client.ResourceNotFoundException;
import com.marklogic.semantics.jena.JenaTestBase;

public class MarkLogicQueryEngineTest extends JenaTestBase {

    private Dataset ds;

    @Before
    public void setupDataset() {
        ds = DatasetFactory
                .create(getMarkLogicDatasetGraph("testdata/testData.trig"));
        // ds =
        // DatasetFactory.create(getJenaDatasetGraph("testdata/testData.trig"));
    }

    @Test
    public void testSelect() {
        QueryExecution qe = QueryExecutionFactory.create(
                "select ?s ?p ?o where { ?s ?p ?o}", ds);
        ResultSet results = qe.execSelect();
        // System.out.println(ResultSetFormatter.asText(results));
        while (results.hasNext()) {
            QuerySolution qs = results.next();
            assertTrue(qs.contains("s"));
            assertTrue(qs.contains("p"));
            assertTrue(qs.contains("o"));
        }
    }

    @Test
    public void testAsk() {
        QueryExecution queryExec = QueryExecutionFactory.create(
                "ASK WHERE { ?s ?p ?o }", ds);
        assertTrue("ExecAsk true", queryExec.execAsk());
        queryExec = QueryExecutionFactory.create(
                "ASK WHERE { <this> <isnt> <here> }", ds);
        assertFalse("ExecAsk false", queryExec.execAsk());
    }

    @Test
    public void testDescribe() {
        QueryExecution queryExec = QueryExecutionFactory.create(
                "DESCRIBE <http://example.org/r1>", ds);
        Model solution = queryExec.execDescribe();
        assertTrue("Got a solution with more than zero triples", solution
                .getGraph().size() > 0);

        queryExec = QueryExecutionFactory.create(
                "DESCRIBE <http://example.org/g1333>", ds);
        solution = queryExec.execDescribe();
        assertEquals("Got a solution with zero triples", 0, solution.getGraph()
                .size());
    }

    @Test
    public void testConstruct() {
        QueryExecution queryExec = QueryExecutionFactory
                .create("PREFIX : <http://example.org/> CONSTRUCT { :r100 ?p ?o } WHERE { :r1 ?p ?o }",
                        ds);
        Model solution = queryExec.execConstruct();
        assertTrue("Got a solution with four triples", solution.getGraph()
                .size() > 0);
    }

    @Test
    @Ignore
    public void testPagination() {

    }

    @Test
    public void testTransactions() {
        // add a graph during transaction
        try {
            ds.begin(ReadWrite.WRITE);
            Triple triple = new Triple(NodeFactory.createURI("s529"),
                    NodeFactory.createURI("p104"), NodeFactory.createURI("o22"));
            Graph transGraph = GraphFactory.createGraphMem();
            transGraph.add(triple);
            Model model = ModelFactory.createModelForGraph(transGraph);
            ds.addNamedModel("createdDuringTransaction", model);

            QueryExecution qe = QueryExecutionFactory.create(
                    "select ?o where { <s529> ?p ?o}", ds);
            ResultSet results = qe.execSelect();
            // System.out.println(ResultSetFormatter.asText(results));
            QuerySolution qs = results.next();
            assertEquals("Query worked during transaction", "o22", qs.get("o").toString());
            ds.abort();

            qe = QueryExecutionFactory.create(
                    "select ?o where { <s529> ?p ?o}", ds);
            results = qe.execSelect();
            assertFalse("Query should not execute against rolled back data",
                    results.hasNext());

            ds.begin(ReadWrite.WRITE);
            ds.addNamedModel("createdDuringTransaction", model);

            qe = QueryExecutionFactory.create(
                    "select ?o where { <s529> ?p ?o}", ds);
            results = qe.execSelect();
            // System.out.println(ResultSetFormatter.asText(results));
            qs = results.next();
            assertEquals("Query worked during transaction", "o22", qs.get("o").toString());
            ds.commit();

            //setupDataset();
            qe = QueryExecutionFactory.create("select ?o where {<s529> ?p ?o}", ds);
            results = qe.execSelect();
            assertTrue("Query should execute against committed data",
                    results.hasNext());

            ds.removeNamedModel("createdDuringTransaction");
        } finally {
            if (ds.isInTransaction())
                ds.abort();
            try {
                ds.removeNamedModel("createdDuringTransaction");
            } catch (ResourceNotFoundException ex) {
                //pass
            }
        }
    }

    @Test
    public void testBindings() {
        String query = "SELECT ?s ?o where { ?s <http://example.org/p1> ?o }";
        QuerySolutionMap binding = new QuerySolutionMap();
        binding.add("s", ResourceFactory.createResource("http://example.org/r1"));
        QueryExecution exec = QueryExecutionFactory.create(query, ds, binding);
        ResultSet results = exec.execSelect();
        QuerySolution result = results.next();
        assertFalse("Only one result for bound query", results.hasNext());
        assertEquals("string value 0", result.get("o").asLiteral().getValue());
        
        binding = new QuerySolutionMap();
        binding.add("s", ResourceFactory.createResource("http://example.org/r2"));
        exec = QueryExecutionFactory.create(query, ds, binding);
        results = exec.execSelect();
        result = results.next();
        assertFalse("Only one result for bound query", results.hasNext());
        assertEquals( "string value 2", result.get("o").asLiteral().getValue());

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
