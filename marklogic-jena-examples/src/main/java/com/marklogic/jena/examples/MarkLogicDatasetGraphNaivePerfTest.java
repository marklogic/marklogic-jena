package com.marklogic.jena.examples;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.jena.riot.RDFDataMgr;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;

@State(value = Scope.Thread)
/**
 * This is a benchmark that simply loads some data and then runs
 * a naive query over the data.
 */
public class MarkLogicDatasetGraphNaivePerfTest {

    private static Logger log = LoggerFactory.getLogger(MarkLogicDatasetGraphNaivePerfTest.class);
	private MarkLogicDatasetGraph markLogicDatasetGraph;
	private static Dataset dataset;
	
	@Setup
	public void configure() {
	    markLogicDatasetGraph = ExampleUtils.loadPropsAndInit();
        // markLogicDatasetGraph.clear();
        dataset = DatasetFactory.create(markLogicDatasetGraph);
	}
	
    @Benchmark
    public void perfNaiveQuery1()
            throws Exception {

        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 50";
        QueryExecution eq = QueryExecutionFactory.create(queryString, dataset);
        ResultSet results = eq.execSelect();
        while(results.hasNext()) {
            QuerySolution qs = results.next();
            log.debug("Got result from query with subject " + qs.get("s"));
        }
        ResultSetFormatter.out(System.out, results);
    }

}
