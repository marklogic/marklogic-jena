/*
 * Copyright 2016-2018 MarkLogic Corporation
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
package com.marklogic.semantics.jena;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.jena.riot.RDFDataMgr;

import org.apache.jena.sparql.core.DatasetGraph;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

import javax.net.ssl.SSLContext;

public class JenaTestBase {
    protected static DatabaseClient readerClient;
    protected static DatabaseClient writerClient;
    protected static DatabaseClient adminClient;

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

        adminClient = DatabaseClientFactory.newClient(host, port,
                new DatabaseClientFactory.DigestAuthContext(adminUser, adminPassword));
        writerClient = DatabaseClientFactory.newClient(host, port,
                new DatabaseClientFactory.DigestAuthContext(writerUser, writerPassword));
        readerClient = DatabaseClientFactory.newClient(host, port,
                new DatabaseClientFactory.DigestAuthContext(readerUser, readerPassword));
    }

    protected static DatasetGraph getJenaDatasetGraph(String fileName) {
        return RDFDataMgr.loadDatasetGraph(fileName);
    }

    protected static MarkLogicDatasetGraph getMarkLogicDatasetGraph() {
        return getMarkLogicDatasetGraph(null);
    }

    protected static MarkLogicDatasetGraph getMarkLogicDatasetGraph(
            String fileName) {
        MarkLogicDatasetGraph markLogicDatasetGraph = MarkLogicDatasetGraphFactory
                .createDatasetGraph(writerClient);
        // markLogicDatasetGraph.clear();
        if (fileName != null) {
            RDFDataMgr.read(markLogicDatasetGraph, fileName);
        }
        // wait for read op to finish
        markLogicDatasetGraph.sync();
        return markLogicDatasetGraph;
    }
}
