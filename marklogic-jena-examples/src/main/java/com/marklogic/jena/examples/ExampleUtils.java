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
package com.marklogic.jena.examples;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;
import org.apache.commons.io.FileUtils;

public class ExampleUtils {


    public static MarkLogicDatasetGraph loadPropsAndInit() {
        Properties props = new Properties();
        // two attempts to load
        try {
            props.load(new FileInputStream("marklogic-jena-examples/gradle.properties"));
        } catch (IOException e) {
            // gradle prefers this path.
            try {
                props.load(new FileInputStream("gradle.properties"));
            } catch (IOException e2) {
                System.err.println("problem loading properties file.");
                System.exit(1);
            }
        }
        String host = props.getProperty("mlHost");
        int port = Integer.parseInt(props.getProperty("mlRestPort"));
        String user = props.getProperty("writerUser");
        String pass = props.getProperty("writerPassword");
        
        DatabaseClient client = DatabaseClientFactory.newClient(host, port,
                new DatabaseClientFactory.DigestAuthContext(user, pass));
        MarkLogicDatasetGraph dg = MarkLogicDatasetGraphFactory
                .createDatasetGraph(client);
        return dg;
    }
}
