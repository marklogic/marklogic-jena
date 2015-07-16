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

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Locale;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphCaching;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Transactional;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.sparql.resultset.JSONInput;
import com.hp.hpl.jena.update.GraphStore;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.client.semantics.SPARQLBindings;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.semantics.jena.util.WrappingIterator;

/**
 * A representation of MarkLogic's triple store as a DatasetGraph,
 * plus a few extra MarkLogic-specific features.
 *
 */
public class MarkLogicDatasetGraph extends DatasetGraphCaching implements DatasetGraph, GraphStore, Transactional {

	public class QuadsIterator implements Iterator<Quad> {

		private ResultSet results;
		private String graphName = null;

		public QuadsIterator(InputStream inputStream) {
			results = JSONInput.fromJSON(inputStream);
		}
		
		public QuadsIterator(String graphName, InputStream inputStream) {
			this.graphName = graphName;
			results = JSONInput.fromJSON(inputStream);
		}

		@Override
		public boolean hasNext() {
			return results.hasNext();
		}

		@Override
		public Quad next() {
			QuerySolution solution = results.next();
			Node s = solution.get("s").asNode();
			Node p = solution.get("p").asNode();
			Node o = solution.get("o").asNode();
			Node g = null;
			if (solution.get("g") != null) {
				g = solution.get("g").asNode();
			} else {
				if (graphName != null) {
					g = NodeFactory.createURI(graphName);
				}
			}
			Quad quad = new Quad(g, s, p, o);
			return quad;
		}

		@Override
		public void remove() {
			results.remove();
		}
	}

	private static final String DEFAULT_GRAPH_URI = "http://marklogic.com/semantics#default-graph";
	
	private static Log log = LogFactory.getLog(MarkLogicDatasetGraph.class);
	
	private GraphManager graphManager;
	private SPARQLQueryManager sparqlQueryManager;
	
	/**
	 * Creates a new MarkLogicDatasetGraph using the supplied DatabaseClient.  If this client can write to
	 * the database, then the DatasetGraph is initialized with a default graph.
	 * @param client specifies the connection to the MarkLogic server.  Obtain from DatabaseClientFactory.
	 */
	public MarkLogicDatasetGraph(DatabaseClient client) {
		this.graphManager = client.newGraphManager();
		this.sparqlQueryManager = client.newSPARQLQueryManager();
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
	public Iterator<Node> listGraphNodes() {
		log.debug("listing graphs ");
		Iterator<String> graphNames = graphManager.listGraphUris();
		return new WrappingIterator(graphNames);
	}

	@Override
	public void clear() {
		super.clear();
		String query = "DROP SILENT ALL";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		sparqlQueryManager.executeUpdate(qdef);
	}
	
	@Override
	protected void _close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Graph _createNamedGraph(Node graphNode) {
		// can't bind graph name, so this is a surrogate for
	    // CREATE GRAPH ?g
	    String query = "CONSTRUCT {?s ?p ?o} WHERE { ?s ?p ?o }";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		qdef.setDefaultGraphUris(graphNode.getURI());
		InputStreamHandle handle = sparqlQueryManager.executeConstruct(qdef, new InputStreamHandle().withMimetype(RDFMimeTypes.NTRIPLES));
		return toJenaGraph(handle);
	}

	@Override
	protected Graph _createDefaultGraph() {
		String query = "CONSTRUCT {?s ?p ?o}  WHERE { ?s ?p ?o }";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		qdef.setDefaultGraphUris(DEFAULT_GRAPH_URI);
		InputStreamHandle handle = sparqlQueryManager.executeConstruct(qdef, new InputStreamHandle().withMimetype(RDFMimeTypes.NTRIPLES));
		return toJenaGraph(handle);
	}

	@Override
	protected boolean _containsGraph(Node graphNode) {
		String query = "ASK WHERE { ?s ?p ?o  }";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		qdef.setDefaultGraphUris(graphNode.getURI());
        return sparqlQueryManager.executeAsk(qdef);
	}

	private SPARQLQueryDefinition bindObject(SPARQLQueryDefinition qdef, String nodeName, Node objectNode) {
		SPARQLBindings bindings = qdef.getBindings();
		if (objectNode.isURI()) {
			bindings.bind(nodeName, objectNode.getURI());
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
				bindings.bind(nodeName, objectNode.getLiteralLexicalForm(), Locale.forLanguageTag(languageTag));
			} else {
				// is this a hole, no type string?
				bindings.bind(nodeName, objectNode.getLiteralLexicalForm(), "string");
			}
		}
		qdef.setBindings(bindings);
		return qdef;
	}

	@Override
	protected void addToDftGraph(Node s, Node p, Node o) {
		String query = "INSERT DATA { ?s ?p ?o }";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		qdef.withBinding("s", s.getURI());
		qdef.withBinding("p", p.getURI());
		qdef = bindObject(qdef, "o", o);
		sparqlQueryManager.executeUpdate(qdef);
	}

	@Override
	protected void addToNamedGraph(Node g, Node s, Node p, Node o) {
		// workaround -- use graph inline. insecure.
		String query = "INSERT DATA { GRAPH <" + g.getURI() + "> { ?s ?p ?o } }";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		//qdef.withBinding("g", g.getURI());
		qdef.withBinding("s", s.getURI());
		qdef.withBinding("p", p.getURI());
		qdef = bindObject(qdef, "o", o);
		sparqlQueryManager.executeUpdate(qdef);
	}

	@Override
	protected void deleteFromDftGraph(Node s, Node p, Node o) {
		String query = "DELETE  WHERE { ?s ?p ?o }";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		qdef.withBinding("s", s.getURI());
		qdef.withBinding("p", p.getURI());
		qdef = bindObject(qdef, "o", o);
		sparqlQueryManager.executeUpdate(qdef);
	}

	@Override
	protected void deleteFromNamedGraph(Node g, Node s, Node p, Node o) {
		// workaround -- use graph inline. insecure.
		String gString = "?g";
		if (g != null) {
			gString = g.getURI();
		}
		String query = "DELETE WHERE { GRAPH <" + gString + "> { ?s ?p ?o } }";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		//qdef.withBinding("g", g.getURI());
		qdef.withBinding("s", s.getURI());
		qdef.withBinding("p", p.getURI());
		qdef = bindObject(qdef, "o", o);
		sparqlQueryManager.executeUpdate(qdef);
	}

	private InputStream selectTriplesInGraph(String graphName, Node s, Node p, Node o) {
		String query = "SELECT ?s ?p ?o where { ?s ?p ?o }";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		if (s != Node.ANY) { qdef.withBinding("s", s.getURI()); }
		if (p != Node.ANY) { qdef.withBinding("p", p.getURI()); }
		if (o != Node.ANY) { qdef = bindObject(qdef, "o", o); }
		// bug 33167 prevents binding ?g 
		// qdef.withBinding("g", DEFAULT_GRAPH_URI);
		qdef.setDefaultGraphUris(DEFAULT_GRAPH_URI);
		InputStreamHandle results = sparqlQueryManager.executeSelect(qdef, new InputStreamHandle());
		return results.get();
	}
	@Override
	protected Iterator<Quad> findInDftGraph(Node s, Node p, Node o) {
		InputStream results = selectTriplesInGraph(DEFAULT_GRAPH_URI, s,p,o);
		return new QuadsIterator(results);
	}
	

	@Override
	protected Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p,
			Node o) {
		InputStream results = selectTriplesInGraph(g.getURI(), s,p,o);
		return new QuadsIterator(g.getURI(), results);
	}

	@Override
	protected Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
		String query = "SELECT ?g ?s ?p ?o where {GRAPH ?g { ?s ?p ?o }}";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		if (s != Node.ANY) { qdef.withBinding("s", s.getURI()); }
		if (p != Node.ANY) { qdef.withBinding("p", p.getURI()); }
		if (o != Node.ANY) { qdef = bindObject(qdef, "o", o); }
		InputStreamHandle results = sparqlQueryManager.executeSelect(qdef, new InputStreamHandle());
		return new QuadsIterator(results.get());
	}
	
	@Override
    public void setDefaultGraph(Graph g)
    {
	    this.addGraph(NodeFactory.createURI(DEFAULT_GRAPH_URI), g);
	}

	@Override
	public void begin(ReadWrite readWrite) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void abort() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isInTransaction() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void end() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Dataset toDataset() {
		return DatasetFactory.create(this);
	}

	@Override
	public void startRequest() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void finishRequest() {
		// TODO Auto-generated method stub
		
	}

	public SPARQLQueryManager getSPARQLQueryManager() {
		return this.sparqlQueryManager;
	}
}
