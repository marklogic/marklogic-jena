/*
 * Copyright 2016-2017 MarkLogic Corporation
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
package com.marklogic.semantics.jena.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.QuerySolutionMap;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.graph.GraphFactory;

import com.marklogic.client.ResourceNotFoundException;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.jena.JenaTestBase;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;

public class MarkLogicQueryEngineTest extends JenaTestBase {

    private Dataset ds;

    @Before
    public void setupDataset() {
        ds = DatasetFactory
                .wrap(getMarkLogicDatasetGraph("testdata/testData.trig"));
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
    public void testPagination() {
        Query query = QueryFactory
                .create("prefix : <http://example.org/> select ?p ?o where { :r1 ?p ?o}");
        query.setLimit(5);
        query.setOffset(0);
        QueryExecution queryExec = QueryExecutionFactory.create(query, ds);
        ResultSet results = queryExec.execSelect();
        int i;
        for (i = 0; results.hasNext(); i++) {
            results.next();
        }
        assertEquals("Got proper start and offset for query", 4, i);

        query.setLimit(5);
        query.setOffset(1);
        queryExec = QueryExecutionFactory.create(query, ds);
        results = queryExec.execSelect();
        for (i = 0; results.hasNext(); i++) {
            results.next();
        }
        assertEquals("Got proper start and offset for query", 3, i);
        query.setLimit(2);
        query.setOffset(0);
        queryExec = QueryExecutionFactory.create(query, ds);
        results = queryExec.execSelect();
        for (i = 0; results.hasNext(); i++) {
            @SuppressWarnings("unused")
            QuerySolution qs = results.next();
        }
        assertEquals("Got proper start and offset for query", 2, i);

    }

    @Test
    public void testTransactions() {
        // add a graph during transaction
        try {
            ds.begin(ReadWrite.WRITE);
            Triple triple = new Triple(NodeFactory.createURI("http://s529"),
                    NodeFactory.createURI("http://p104"),
                    NodeFactory.createURI("http://o22"));
            Graph transGraph = GraphFactory.createGraphMem();
            transGraph.add(triple);
            Model model = ModelFactory.createModelForGraph(transGraph);
            ds.addNamedModel("createdDuringTransaction", model);

            QueryExecution qe = QueryExecutionFactory.create(
                    "select ?o where { <http://s529> ?p ?o}", ds);
            ResultSet results = qe.execSelect();
            // System.out.println(ResultSetFormatter.asText(results));
            QuerySolution qs = results.next();
            assertEquals("Query worked during transaction", "http://o22", qs
                    .get("o").toString());
            ds.abort();

            qe = QueryExecutionFactory.create(
                    "select ?o where { <http://s529> ?p ?o}", ds);
            results = qe.execSelect();
            assertFalse("Query should not execute against rolled back data",
                    results.hasNext());

            ds.begin(ReadWrite.WRITE);
            ds.addNamedModel("createdDuringTransaction", model);

            qe = QueryExecutionFactory.create(
                    "select ?o where { <http://s529> ?p ?o}", ds);
            results = qe.execSelect();
            // System.out.println(ResultSetFormatter.asText(results));
            qs = results.next();
            assertEquals("Query worked during transaction", "http://o22", qs
                    .get("o").toString());
            ds.commit();

            // setupDataset();
            qe = QueryExecutionFactory.create(
                    "select ?o where {<http://s529> ?p ?o}", ds);
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
                // pass
            }
        }
    }

    @Test
    public void testBindings() {
        String query = "SELECT ?s ?o where { ?s <http://example.org/p1> ?o }";
        QuerySolutionMap binding = new QuerySolutionMap();
        binding.add("s",
                ResourceFactory.createResource("http://example.org/r1"));
        QueryExecution exec = QueryExecutionFactory.create(query, ds, binding);
        ResultSet results = exec.execSelect();
        QuerySolution result = results.next();
        assertFalse("Only one result for bound query", results.hasNext());
        assertEquals("string value 0", result.get("o").asLiteral().getValue());

        binding = new QuerySolutionMap();
        binding.add("s",
                ResourceFactory.createResource("http://example.org/r2"));
        exec = QueryExecutionFactory.create(query, ds, binding);
        results = exec.execSelect();
        result = results.next();
        assertFalse("Only one result for bound query", results.hasNext());
        assertEquals("string value 2", result.get("o").asLiteral().getValue());

    }

    private List<String> project(ResultSet results, String key) {
        List<String> strings = new ArrayList<String>();
        while (results.hasNext()) {
            QuerySolution qs = results.next();
            strings.add((String) qs.get(key).asNode().getURI());
        }
        return strings;
    }

    @Test
    public void testRulesets() {
        MarkLogicDatasetGraph infTestDsg = getMarkLogicDatasetGraph();
        Dataset ds = DatasetFactory.wrap(infTestDsg);
        String query = "prefix : <http://example.org/> select ?o where { :r3 a ?o }";
        QueryExecution exec = QueryExecutionFactory.create(query, ds);
        ResultSet results = exec.execSelect();

        List<String> subjects = project(results, "o");
        assertEquals("No inference, got back list of size 1", 1,
                subjects.size());

        infTestDsg.withRulesets(SPARQLRuleset.RDFS);
        // MarkLogicQuery inferringQuery = MarkLogicQuery.create(query);
        // inferringQuery.setRulesets(SPARQLRuleset.RDFS);
        exec = QueryExecutionFactory.create(query, ds);
        results = exec.execSelect();
        subjects = project(results, "o");
        System.out.println(subjects);
        assertEquals("Using RDFs got back two class assertions", 2,
                subjects.size());

    }

    @Test
    public void testBaseUri() {

        MarkLogicDatasetGraph baseTest = getMarkLogicDatasetGraph();
        Dataset ds = DatasetFactory.wrap(baseTest);
        String query = "select ?o where { <r3> a ?o }";
        QueryExecution exec = QueryExecutionFactory.create(query, ds);
        ResultSet results = exec.execSelect();
        List<String> subjects = project(results, "o");
        assertEquals("No base, got back list of size 0", 0, subjects.size());

        Query q = QueryFactory.create(query, "http://example.org/");
        exec = QueryExecutionFactory.create(q, ds);
        results = exec.execSelect();
        subjects = project(results, "o");
        assertEquals("No base, got back list of size 1", 1, subjects.size());

    }

}
