package com.marklogic.jena.examples;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;

public class ExampleUtils {


    public static MarkLogicDatasetGraph loadPropsAndInit() {
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
        
        DatabaseClient client = DatabaseClientFactory.newClient(host, port,
                user, pass, Authentication.DIGEST);
        MarkLogicDatasetGraph dg = MarkLogicDatasetGraphFactory
                .createDatasetGraph(client);
        return dg;
    }
}
