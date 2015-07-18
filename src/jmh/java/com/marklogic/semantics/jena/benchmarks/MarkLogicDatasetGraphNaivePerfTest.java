package com.marklogic.semantics.jena.benchmarks;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.openjdk.jmh.annotations.Benchmark;

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
import com.marklogic.semantics.jena.graph.MarkLogicDatasetGraphFactory;

public class MarkLogicDatasetGraphNaivePerfTest {

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
        // extrude to semantics.utils

        DatabaseClient client = DatabaseClientFactory.newClient(host,  port, user, pass, Authentication.DIGEST);
        DatasetGraph dg = MarkLogicDatasetGraphFactory.createDatasetGraph(client);
        Dataset ds = DatasetFactory.create(dg);
        
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 50";
        QueryExecution eq = QueryExecutionFactory.create(queryString, ds);
        ResultSet results = eq.execSelect();

        ResultSetFormatter.out(System.out, results);
    }

}
