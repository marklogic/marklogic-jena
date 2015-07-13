package com.marklogic.semantics.jena;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.semantics.jena.graph.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.graph.MarkLogicDatasetGraphFactory;
import com.marklogic.semantics.jena.query.MarkLogicQueryEngine;

public class JenaTestBase {
    public static DatabaseClient readerClient;
    public static DatabaseClient writerClient;
    public static DatabaseClient adminClient;
    
    static {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("gradle.properties"));
        } catch (IOException e) {
            System.err.println("Properties file not loaded.");
            System.exit(1);
        }
        String host = props.getProperty("mlHost");
        int port = Integer.parseInt(props.getProperty("mlRestPort"));
        String adminUser = props.getProperty("adminUser");
        String adminPassword = props.getProperty("adminPassword");
        String writerUser = props.getProperty("writerUser");
        String writerPassword = props.getProperty("writerPassword");
        String readerUser = props.getProperty("readerUser");
        String readerPassword = props.getProperty("readerPassword");

        adminClient = DatabaseClientFactory.newClient(host, port, adminUser, adminPassword, Authentication.DIGEST);
        writerClient = DatabaseClientFactory.newClient(host, port, writerUser, writerPassword, Authentication.DIGEST);
        readerClient = DatabaseClientFactory.newClient(host, port, readerUser, readerPassword, Authentication.DIGEST);
    
        MarkLogicQueryEngine.register(readerClient);
    }
    
    protected static DatasetGraph getJenaDatasetGraph(String fileName) {
        return RDFDataMgr.loadDatasetGraph(fileName);
    }
    
    protected static MarkLogicDatasetGraph getMarkLogicDatasetGraph() {
    	return getMarkLogicDatasetGraph(null);
    }
    protected static MarkLogicDatasetGraph getMarkLogicDatasetGraph(String fileName) {
        MarkLogicDatasetGraph markLogicDatasetGraph = MarkLogicDatasetGraphFactory.createDatasetGraph(writerClient);
        markLogicDatasetGraph.clear();
        if (fileName != null) {
        	RDFDataMgr.read(markLogicDatasetGraph,  fileName);
        }
        return markLogicDatasetGraph;
    }
}
