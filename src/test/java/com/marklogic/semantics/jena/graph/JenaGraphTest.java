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
package com.marklogic.semantics.jena.graph;

import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Test;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;

public class JenaGraphTest {

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
	}
	
	public static DatasetGraph getJenaDatasetGraph(String fileName) {
		return RDFDataMgr.loadDatasetGraph(fileName);
	}
	
	public static DatasetGraph getMarkLogicDatasetGraph(String fileName) {
		DatasetGraph markLogicDatasetGraph = MarkLogicDatasetGraphFactory.createDatasetGraph(writerClient);
		markLogicDatasetGraph.clear();
		RDFDataMgr.read(markLogicDatasetGraph,  fileName);
		return markLogicDatasetGraph;
	}
	
	@Test
	public void testFirstRead() {
		DatasetGraph datasetGraph = getJenaDatasetGraph("testData.trig");
		DatasetGraph markLogicDatasetGraph = getMarkLogicDatasetGraph("testData.trig");

		Iterator<Node> jenaGraphs = datasetGraph.listGraphNodes();
		Iterator<Node> markLogicGraphs = markLogicDatasetGraph.listGraphNodes();

		while (jenaGraphs.hasNext()) {
			Node jenaGraphNode = jenaGraphs.next();
			assertTrue(markLogicGraphs.hasNext());

			// list must be at least as long as jena's
			Node markLogicNode = markLogicGraphs.next();

			Graph jenaGraph = datasetGraph.getGraph(jenaGraphNode);
			Graph markLogicGraph = markLogicDatasetGraph.getGraph(jenaGraphNode);

			RDFDataMgr.write(System.out, jenaGraph, Lang.TURTLE);
			RDFDataMgr.write(System.out, markLogicGraph, Lang.TURTLE);
			
			assertTrue(jenaGraph.isIsomorphicWith(markLogicGraph));


		}
	}
}
