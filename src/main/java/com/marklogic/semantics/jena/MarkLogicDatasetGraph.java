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

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Locale;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.WriterGraphRIOT;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.sparql.util.Context;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ResourceNotFoundException;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.OutputStreamHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.SPARQLBindings;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;

/**
 * A representation of MarkLogic's triple store as a DatasetGraph,
 * plus a few extra MarkLogic-specific features.
 *
 */
public class MarkLogicDatasetGraph implements DatasetGraph {

	private static Log log = LogFactory.getLog(MarkLogicDatasetGraph.class);
	
	private DatabaseClient client;
	private GraphManager graphManager;
	private SPARQLQueryManager sparqlQueryManager;
	
	/**
	 * Creates a new MarkLogicDatasetGraph using the supplied DatabaseClient.  If this client can write to
	 * the database, then the DatasetGraph is initialized with a default graph.
	 * @param client specifies the connection to the MarkLogic server.  Obtain from DatabaseClientFactory.
	 */
	public MarkLogicDatasetGraph(DatabaseClient client) {
		this.client = client;
		this.graphManager = client.newGraphManager();
		this.sparqlQueryManager = client.newSPARQLQueryManager();
	}
	
	private void initializeDefaultGraph() {
		String query = "INSERT DATA { }";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		sparqlQueryManager.executeUpdate(qdef);
	}
	
	private Graph toJenaGraph(InputStreamHandle handle) {
		Graph graph = GraphFactory.createDefaultGraph();
		InputStream is = handle.get();
		if (is != null) {
			RDFDataMgr.read(graph, is, Lang.NTRIPLES);
		}
		return graph;
	}
	@Override
	public Graph getDefaultGraph() {
		InputStreamHandle handle = new InputStreamHandle().withMimetype("application/n-triples");
		try {
			graphManager.read(GraphManager.DEFAULT_GRAPH, handle);
		} catch (ResourceNotFoundException e) {
			// no default graph in this database.  Initialize if allowed.
			log.info("No default graph found in triple store.  Initializing empty default graph.");
			initializeDefaultGraph();
		}
		return toJenaGraph(handle);
	}

	@Override
	public Graph getGraph(Node graphNode) {
		InputStreamHandle handle = new InputStreamHandle().withMimetype("application/n-triples");
		graphManager.read(graphNode.getURI(), handle);
		return toJenaGraph(handle);
	}

	@Override
	public boolean containsGraph(Node graphNode) {
		// TODO not implemented
		// return graphManager.contains(graphNode.getURI());
		return false;
	}

	@Override
	public void setDefaultGraph(Graph graph) {
		OutputStreamHandle handle = makeHandle(graph);
		graphManager.write(GraphManager.DEFAULT_GRAPH, handle);
	}

	private OutputStreamHandle makeHandle(Graph graph) {
		WriterGraphRIOT writer = RDFDataMgr.createGraphWriter(RDFFormat.NTRIPLES);
		OutputStreamRIOTSender riptSender = new OutputStreamRIOTSender(writer);
		OutputStreamHandle handle = new OutputStreamHandle(riptSender).withMimetype(Lang.NTRIPLES.getHeaderString());
		riptSender.setGraph(graph);
		return handle;
	}
	@Override
	public void addGraph(Node graphName, Graph graph) {
		log.debug("Adding graph to triple store");
		OutputStreamHandle handle = makeHandle(graph);
		graphManager.write(graphName.getURI(), handle);
	}

	@Override
	public void removeGraph(Node graphName) {
		// TODO Auto-generated method stub

		log.debug("removing graph from triple store");

	}

	@Override
	public Iterator<Node> listGraphNodes() {
		log.debug("listing graphs ");
		Iterator<String> graphNames = graphManager.listGraphUris();
		return new WrappingIterator(graphNames);
	}

	@Override
	public void add(Quad quad) {
		// TODO Auto-generated method stub
		// TODO transactions.
		log.debug("adding quad." + quad.toString());

		// naive, slow
		String insertQuad = "INSERT DATA { GRAPH ?g { ?s ?p ?o } }";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(insertQuad);
		SPARQLBindings bindings = qdef.getBindings();
		bindings.bind("g", quad.getGraph().getURI());
		bindings.bind("s", quad.getSubject().getURI());
		bindings.bind("p", quad.getPredicate().getURI());
		qdef.setBindings(bindings);

		Node objectNode = quad.getObject();
		if (objectNode.isURI()) {
			bindings.bind("o", objectNode.getURI());
		}
		else if (objectNode.isLiteral()) {
			if ( objectNode.getLiteralDatatype() != null) {
				try {
					String xsdType = objectNode.getLiteralDatatypeURI();
					String fragment = new URI(xsdType).getFragment();
					bindings.bind("o", objectNode.getLiteralLexicalForm(), fragment);
					log.debug("found " + xsdType);
				} catch (URISyntaxException e) {
					log.info("Is this an error");
				}
			} else if (!objectNode.getLiteralLanguage().equals("")) {
				String languageTag = objectNode.getLiteralLanguage();
				bindings.bind("o", objectNode.getLiteralLexicalForm(), Locale.forLanguageTag(languageTag));
			} else {
				// is this a hole, no type string?
				bindings.bind("o", objectNode.getLiteralLexicalForm(), "string");
			}
		}

		sparqlQueryManager.executeUpdate(qdef);

	}

	@Override
	public void delete(Quad quad) {
		// TODO Auto-generated method stub
		log.debug("deleting quad.");

	}

	@Override
	public void add(Node g, Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		log.debug("adding node. " + g.getURI() + ":" + s.getURI() + ":" + p.getURI() + ":" + o.toString());

	}

	@Override
	public void delete(Node g, Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		log.debug("deleting node.");

	}

	@Override
	public void deleteAny(Node g, Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		log.debug("deleting any node.");

	}

	@Override
	public Iterator<Quad> find() {
		// TODO Auto-generated method stub
		log.debug("find quad.");

		return null;
	}

	@Override
	public Iterator<Quad> find(Quad quad) {
		// TODO Auto-generated method stub
		log.debug("find quad.");

		
		return null;
	}

	@Override
	public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		log.debug("find node.");

		return null;
	}

	@Override
	public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		log.debug("find NG.");

		return null;
	}

	@Override
	public boolean contains(Node g, Node s, Node p, Node o) {
		// TODO Auto-generated method stub
		log.debug("contains node.");
		
		return false;
	}

	@Override
	public boolean contains(Quad quad) {
		// TODO Auto-generated method stub
		log.debug("contains quad.");

		return false;
	}

	@Override
	public void clear() {
		String query = "DROP SILENT ALL";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		sparqlQueryManager.executeUpdate(qdef);
	}

	@Override
	public boolean isEmpty() {
		String query = "ASK WHERE { ?s ?p ?o }";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		return sparqlQueryManager.executeAsk(qdef);
	}

	@Override
	public Lock getLock() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Context getContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long size() {
		Iterator<?> iterator = graphManager.listGraphUris();
		long size = 0;
		while (iterator.hasNext()) {
			iterator.next();
			size++;
		}
		return size;
	}

	@Override
	public void close() {
		graphManager = null;
		sparqlQueryManager = null;
		client.release();
	}

}
