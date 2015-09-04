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
package com.marklogic.semantics.jena;

import org.apache.jena.riot.RDFWriterRegistry;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.semantics.jena.client.JenaDatabaseClient;
import com.marklogic.semantics.jena.engine.MarkLogicQueryEngine;
import com.marklogic.semantics.jena.engine.MarkLogicUpdateEngine;

/**
 * Contains static methods for creating a MarkLogicDatasetGraph, which is a DatabsetGraph
 * that is backed by a MarkLogic database and REST API.
 */
public class MarkLogicDatasetGraphFactory {

    /**
     * Creates a MarkLogicDatasetGraph from an existing {@link com.marklogic.client.DatabaseClient}.
     * @param client An instance of DatabaseClient.
     * @return A MarkLogicDatasetGraph instance wrapping MarkLogic.
     */
	public static MarkLogicDatasetGraph createDatasetGraph(DatabaseClient client) {
	    JenaDatabaseClient jenaClient = new JenaDatabaseClient(client);
	    MarkLogicDatasetGraph datasetGraph = new MarkLogicDatasetGraph(jenaClient);
		MarkLogicQueryEngine.unregister();
		MarkLogicQueryEngine.register();
		MarkLogicUpdateEngine.unregister();
		MarkLogicUpdateEngine.register();
		return datasetGraph;
	}

	/**
     * Creates MarkLogicDatasetGraph from access parameters to a REST MarkLogic server.
     * 
     * @param host  the host with the REST server
     * @param port  the port for the REST server
     * @param user  the user with read, write, or administrative privileges
     * @param password  the password for the user
     * @param type  the type of authentication applied to the request
     * @return A MarkLogicDatasetGraph instance wrapping MarkLogic.
     */
    static public MarkLogicDatasetGraph createDatasetGraph(String host, int port, String user, String password, Authentication type) {
        DatabaseClient client = DatabaseClientFactory.newClient(host, port, user, password, type);
        return MarkLogicDatasetGraphFactory.createDatasetGraph(client);
    }
}
