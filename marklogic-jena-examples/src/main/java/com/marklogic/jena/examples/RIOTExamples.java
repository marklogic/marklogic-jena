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
package com.marklogic.jena.examples;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;

public class RIOTExamples {

    protected DatabaseClient client;
    private MarkLogicDatasetGraph dsg;

    public RIOTExamples() {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("gradle.properties"));
        } catch (IOException e) {
            System.err.println("Properties file not loaded.");
            System.exit(1);
        }
        String host = props.getProperty("mlHost");
        int port = Integer.parseInt(props.getProperty("mlRestPort"));
        String writerUser = props.getProperty("writerUser");
        String writerPassword = props.getProperty("writerPassword");

        client = DatabaseClientFactory.newClient(host, port, writerUser, writerPassword, Authentication.DIGEST);
    }

    public void loadFromFile() {
        dsg = MarkLogicDatasetGraphFactory.createDatasetGraph(client);
        RDFDataMgr.read(dsg, "file.nt", Lang.NTRIPLES);
    }

    public static void main(String... args) {
       RIOTExamples examples = new RIOTExamples();
       examples.loadFromFile();
    }
}
