package com.marklogic.semantics.jena.benchmarks;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.jena.riot.RDFDataMgr;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.semantics.jena.graph.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.graph.MarkLogicDatasetGraphFactory;

@State(value = Scope.Thread)
public class MarkLogicDatasetGraphNaivePerfTest {

	private MarkLogicDatasetGraph markLogicDatasetGraph;
	
	Dataset dataset;
	
	@Setup
	public void loadData() {
		 Properties props = new Properties();
	        try {
	            props.load(new FileInputStream("gradle.properties"));
	        } catch (IOException e) {
	            System.err.println("problem loading properties file.");
	            System.exit(1);
	        }
	        String host = props.getProperty("mlHost");
	        int port = Integer.parseInt(props.getProperty("mlRestPort"));
	        String user = props.getProperty("writerUser");
	        String pass = props.getProperty("writerPassword");
	        String fileName = props.getProperty("jmhTestData");
	        
	        DatabaseClient client = DatabaseClientFactory.newClient(host,  port, user, pass, Authentication.DIGEST);
	        DatasetGraph dg = MarkLogicDatasetGraphFactory.createDatasetGraph(client);
	        Dataset ds = DatasetFactory.create(dg);

	       
		markLogicDatasetGraph = MarkLogicDatasetGraphFactory.createDatasetGraph(client);
        //markLogicDatasetGraph.clear();
        RDFDataMgr.read(markLogicDatasetGraph,  fileName);
	}
	
    @Benchmark
    public void perfNaiveQuery1()
            throws Exception {

        Properties props = new Properties();
        try {
            props.load(new FileInputStream("gradle.properties"));
        } catch (IOException e) {
            System.err.println("problem loading properties file.");
            System.exit(1);
        }
        String host = props.getProperty("mlHost");
        int port = Integer.parseInt(props.getProperty("mlRestPort"));
        String user = props.getProperty("adminUser");
        String pass = props.getProperty("adminPassword");

        DatabaseClient client = DatabaseClientFactory.newClient(host,  port, user, pass, Authentication.DIGEST);
        DatasetGraph dg = MarkLogicDatasetGraphFactory.createDatasetGraph(client);
        Dataset ds = DatasetFactory.create(dg);

        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 50";
        QueryExecution eq = QueryExecutionFactory.create(queryString, ds);
        ResultSet results = eq.execSelect();

       //esultSetFormatter.out(System.out, results);
    }

}
