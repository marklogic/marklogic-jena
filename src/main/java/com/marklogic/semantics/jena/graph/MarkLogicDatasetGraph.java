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
import java.util.Timer;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.WriterGraphRIOT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphTriplesQuads;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Transactional;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.update.GraphStore;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ResourceNotFoundException;
import com.marklogic.client.Transaction;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.OutputStreamHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.client.semantics.SPARQLBindings;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.semantics.jena.MarkLogicTransactionException;
import com.marklogic.semantics.jena.util.OutputStreamRIOTSender;
import com.marklogic.semantics.jena.util.QuadsIterator;
import com.marklogic.semantics.jena.util.WrappingIterator;
import com.marklogic.semantics.jena.util.WriteCacheTimerTask;

/**
 * A representation of MarkLogic's triple store as a DatasetGraph,
 * plus a few extra MarkLogic-specific features.
 *
 */
public class MarkLogicDatasetGraph extends DatasetGraphTriplesQuads implements DatasetGraph, GraphStore, Transactional {

	
	public static final String DEFAULT_GRAPH_URI = "http://marklogic.com/semantics#default-graph";
	static Logger log = LoggerFactory.getLogger(MarkLogicDatasetGraph.class);
	
	private GraphManager graphManager;
	private SPARQLQueryManager sparqlQueryManager;
	private DatabaseClient client;
	private WriteCacheTimerTask cache;

	private Transaction currentTransaction;
	
	/**
	 * Creates a new MarkLogicDatasetGraph using the supplied DatabaseClient.  If this client can write to
	 * the database, then the DatasetGraph is initialized with a default graph.
	 * @param client specifies the connection to the MarkLogic server.  Obtain from DatabaseClientFactory.
	 */
	public MarkLogicDatasetGraph(DatabaseClient client) {
		this.client = client;
		this.graphManager = client.newGraphManager();
		graphManager.setDefaultMimetype(RDFMimeTypes.NTRIPLES);
		this.sparqlQueryManager = client.newSPARQLQueryManager();
		this.cache = new WriteCacheTimerTask(this);
		Timer timer = new Timer();
        timer.scheduleAtFixedRate(cache, 1000, 1000);
	}

	@Override
	public Iterator<Node> listGraphNodes() {
		log.debug("listing graphs ");
		Iterator<String> graphNames = graphManager.listGraphUris();
		return new WrappingIterator(graphNames);
	}

	@Override
	public void clear() {
		String query = "DROP SILENT ALL";
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		sparqlQueryManager.executeUpdate(qdef);
	}
	
	public static SPARQLQueryDefinition bindObject(SPARQLQueryDefinition qdef, String nodeName, Node objectNode) {
		SPARQLBindings bindings = qdef.getBindings();
		if (objectNode.isURI()) {
			bindings.bind(nodeName, objectNode.getURI());
		}
		else if (objectNode.isLiteral()) {
			if ( objectNode.getLiteralDatatype() != null) {
				try {
					String xsdType = objectNode.getLiteralDatatypeURI();
					String fragment = new URI(xsdType).getFragment();
					bindings.bind(nodeName, objectNode.getLiteralLexicalForm(), fragment);
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
        s = skolemize(s);
        p = skolemize(p);
        o = skolemize(o);
	    cache.add(null, s, p, o);
//		String query = "INSERT DATA { ?s ?p ?o }";
//		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
//		log.debug(s.toString());
//		qdef.withBinding("s", s.getURI());
//		qdef.withBinding("p", p.getURI());
//		qdef = bindObject(qdef, "o", o);
//		sparqlQueryManager.executeUpdate(qdef);
	}

	private Node skolemize(Node s) {
		if (s.isBlank()) {
			return NodeFactory.createURI("http://marklogic.com/semantics/blank/" + s.toString());
		} else {
			return s;
		}
	}

	@Override
	protected void addToNamedGraph(Node g, Node s, Node p, Node o) {
		// workaround -- use graph inline. insecure.
		s = skolemize(s);
		p = skolemize(p);
		o = skolemize(o);
		cache.add(g, s, p, o);
//		String query = "INSERT DATA { GRAPH <" + g.getURI() + "> { ?s ?p ?o } }";
//		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
//		//qdef.withBinding("g", g.getURI());
//		qdef.withBinding("s", s.getURI());
//		qdef.withBinding("p", p.getURI());
//		qdef = bindObject(qdef, "o", o);
//		sparqlQueryManager.executeUpdate(qdef);
	}

	@Override
	protected void deleteFromDftGraph(Node s, Node p, Node o) {
	   // cache.run();
		String query = "DELETE  WHERE { ?s ?p ?o }";
	    sync();
		s = skolemize(s);
		p = skolemize(p);
		o = skolemize(o);
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition(query);
		qdef.withBinding("s", s.getURI());
		qdef.withBinding("p", p.getURI());
		qdef = bindObject(qdef, "o", o);
		sparqlQueryManager.executeUpdate(qdef);
	}

	@Override
	protected void deleteFromNamedGraph(Node g, Node s, Node p, Node o) {
	    sync();
		// workaround -- use graph inline. insecure.
		s = skolemize(s);
		p = skolemize(p);
		o = skolemize(o);
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
		s = skolemize(s);
		p = skolemize(p);
		o = skolemize(o);
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition("");
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT ?s ?p ?o where { ?s ?p ?o .");
		if (s != Node.ANY) { 
			qdef.withBinding("a", s.getURI());
			sb.append("FILTER (?s = ?a) ");
		}
		if (p != Node.ANY) { 
			qdef.withBinding("b", p.getURI()); 
			sb.append("FILTER (?p = ?b) ");
		}
		if (o != Node.ANY) { 
			qdef = bindObject(qdef, "c", o);
			sb.append("FILTER (?o = ?c) ");
		}
		sb.append("}");
		qdef.setSparql(sb.toString());
		qdef.setDefaultGraphUris(graphName);
		InputStreamHandle results = sparqlQueryManager.executeSelect(qdef, new InputStreamHandle());
		return results.get();
	}
	@Override
	protected Iterator<Quad> findInDftGraph(Node s, Node p, Node o) {
	    sync();
		InputStream results = selectTriplesInGraph(DEFAULT_GRAPH_URI, s,p,o);
		return new QuadsIterator(results);
	}
	

	@Override
	protected Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p,
			Node o) {
	    sync();
		InputStream results = selectTriplesInGraph(g.getURI(), s,p,o);
		return new QuadsIterator(g.getURI(), results);
	}

	@Override
	protected Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
	    sync();
	    s =  s != null ? s : Node.ANY;
		p =  p != null ? p : Node.ANY;
		o =  o != null ? o : Node.ANY;
		
		SPARQLQueryDefinition qdef = sparqlQueryManager.newQueryDefinition("");
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT ?g ?s ?p ?o where {GRAPH ?g { ?s ?p ?o }");
		if (s != Node.ANY) { 
			qdef.withBinding("a", s.getURI());
			sb.append("FILTER (?s = ?a) ");
		}
		if (p != Node.ANY) { 
			qdef.withBinding("b", p.getURI()); 
			sb.append("FILTER (?p = ?b) ");
		}
		if (o != Node.ANY) { 
			qdef = bindObject(qdef, "c", o);
			sb.append("FILTER (?o = ?c) ");
		}
		sb.append("}");
		qdef.setSparql(sb.toString());
		InputStreamHandle results = sparqlQueryManager.executeSelect(qdef, new InputStreamHandle());
		return new QuadsIterator(results.get());
	}
	
	@Override
    public void setDefaultGraph(Graph g)
    {
	    this.addGraph(NodeFactory.createURI(DEFAULT_GRAPH_URI), g);
	}

	private void checkCurrentTransaction() {
		if (this.currentTransaction == null) {
			throw new MarkLogicTransactionException("No open transaction");
		}
	}
	
	@Override
	public void begin(ReadWrite readWrite) {
		if (readWrite == ReadWrite.READ) {
			throw new MarkLogicTransactionException("MarkLogic only supports write transactions");
		} else {
			if (this.currentTransaction != null) {
				throw new MarkLogicTransactionException("Only one open transaction per MarkLogicDatasetGraph instance.");
			}
			this.currentTransaction = client.openTransaction();
		}
	}

	@Override
	public void commit() {
		checkCurrentTransaction();
		this.currentTransaction.commit();
		this.currentTransaction = null;
	}

	@Override
	public void abort() {
		checkCurrentTransaction();
		this.currentTransaction.rollback();
		this.currentTransaction = null;
	}

	@Override
	public boolean isInTransaction() {
		return (this.currentTransaction != null);
	}

	@Override
	public void end() {
		abort();
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
	
	@Override
	public Graph getDefaultGraph() {
		InputStreamHandle handle = new InputStreamHandle();
		Graph graph = GraphFactory.createDefaultGraph();
		try {
			graphManager.read(DEFAULT_GRAPH_URI, handle, this.currentTransaction);
			RDFDataMgr.read(graph, handle.get(), Lang.NTRIPLES);
		} catch (ResourceNotFoundException e) {
			// empty or non-existent.
		}
		return graph;
	}

	@Override
	public Graph getGraph(Node graphNode) {
		InputStreamHandle handle = new InputStreamHandle();
		graphManager.read(graphNode.getURI(), handle, currentTransaction);
		Graph graph = GraphFactory.createDefaultGraph();
		try {
			RDFDataMgr.read(graph, handle.get(), Lang.NTRIPLES);
		} catch (NullPointerException e) {
			
		}
		return graph;
	}

	@Override
	public void addGraph(Node graphName, Graph graph) {
	    WriterGraphRIOT writer = RDFDataMgr.createGraphWriter(Lang.NTRIPLES);
		OutputStreamRIOTSender sender = new OutputStreamRIOTSender(writer);
		sender.setGraph(graph);
		OutputStreamHandle handle = new OutputStreamHandle(sender);
		graphManager.merge(graphName.getURI(), handle, currentTransaction);
	}
	
	// NOT override
	public void mergeGraph(Node graphName, Graph graph) {
        WriterGraphRIOT writer = RDFDataMgr.createGraphWriter(Lang.NTRIPLES);
        OutputStreamRIOTSender sender = new OutputStreamRIOTSender(writer);
        sender.setGraph(graph);
        OutputStreamHandle handle = new OutputStreamHandle(sender);
        graphManager.merge(graphName.getURI(), handle, currentTransaction);
    }

	@Override
	public void removeGraph(Node graphName) {
		graphManager.delete(graphName.getURI(), currentTransaction);
	}

	public GraphPermissions getPermissions(Node g1) {
		return graphManager.getPermissions(g1.getURI());
	}

	public void addPermissions(Node g1, GraphPermissions permissions) {
		graphManager.mergePermissions(g1.getURI(), permissions);
	}
	
	public void clearPermissions(Node g1) {
		//graphManager.deletePermissions(uri);
	}

	public Transaction getCurrentTransaction() {
		return this.currentTransaction;
	}
	
	public void sync() {
	    cache.forceRun();
	}
}
