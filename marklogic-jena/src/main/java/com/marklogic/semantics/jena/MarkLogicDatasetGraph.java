/*
 * Copyright 2016 MarkLogic Corporation
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.shared.LockNone;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphTriplesQuads;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Transactional;
import com.hp.hpl.jena.update.GraphStore;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.RDFTypes;
import com.marklogic.client.semantics.SPARQLBindings;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.jena.client.JenaDatabaseClient;
import com.marklogic.semantics.jena.client.QuadsIterator;
import com.marklogic.semantics.jena.client.WrappingIterator;

/**
 * A representation of MarkLogic's triple store as a DatasetGraph, plus a few
 * extra MarkLogic-specific features. Use this class as you would any other
 * DatasetGraph in a jena-based project. If you know you are using a
 * MarkLogicDatasetGraph, simply cast it in order to use the MarkLogic-specific
 * capabilities.
 */
public class MarkLogicDatasetGraph extends DatasetGraphTriplesQuads implements
        DatasetGraph, GraphStore, Transactional {

    public static final String DEFAULT_GRAPH_URI = "http://marklogic.com/semantics#default-graph";
    private static Logger log = LoggerFactory
            .getLogger(MarkLogicDatasetGraph.class);

    private JenaDatabaseClient client;

    /*
     * An inferencing datasetGraph will add ruleset config to each query
     */
    private SPARQLRuleset[] rulesets;

    /*
     * Used to constrain of corpus of documents against which to make a Dataset
     */
    private QueryDefinition constrainingQueryDefinition;
    private GraphPermissions updatePermissions;

    /**
     * Creates a new MarkLogicDatasetGraph using the supplied DatabaseClient. If
     * this client can write to the database, then the DatasetGraph is
     * initialized with a default graph.
     * 
     * @param jenaClient
     *            specifies the connection to the MarkLogic server. Obtain from
     *            DatabaseClientFactory.
     */
    public MarkLogicDatasetGraph(JenaDatabaseClient jenaClient) {
        this.client = jenaClient;
    }

    /**
     * @see org.apache.jena.sparql.core.DatasetGraph
     */
    @Override
    public Iterator<Node> listGraphNodes() {
        log.debug("listing graphs ");
        checkIsOpen();
        sync();
        Iterator<String> graphNames = client.listGraphUris();
        return new WrappingIterator(graphNames);
    }

    /**
     * MarkLogicDatasetGraph does not make use of locks.
     * 
     * @return An instance of org.apache.jena.shared.LockNone
     */
    @Override
    public Lock getLock() {
        return new LockNone();
    }

    /**
     * @see org.apache.jena.sparql.core.DatasetGraph
     */
    @Override
    public void clear() {
        checkIsOpen();
        String query = "DROP SILENT ALL";
        SPARQLQueryDefinition qdef = client.newQueryDefinition(query);
        client.executeUpdate(qdef);
    }

    /**
     * Maps Jena bindings defined by a variable name and a
     * {@link org.apache.jena.graph.Node} to MarkLogic
     * {@link com.marklogic.client.semantics.SPARQLQueryDefinition} bindings.
     * 
     * @param qdef
     *            A {@link com.marklogic.client.semantics.SPARQLQueryDefinition}
     *            to decorate with a binding.
     * @param variableName
     *            Name of the variable
     * @param objectNode
     *            An RDF node with the value of variableName.
     * @return The query definition, with binding set.
     */
    public static SPARQLQueryDefinition bindObject(SPARQLQueryDefinition qdef,
            String variableName, Node objectNode) {
        SPARQLBindings bindings = qdef.getBindings();
        if (objectNode.isURI()) {
            bindings.bind(variableName, objectNode.getURI());
        } else if (objectNode.isLiteral()) {
            if (objectNode.getLiteralDatatype() != null) {
                try {
                    String xsdType = objectNode.getLiteralDatatypeURI();
                    String fragment = new URI(xsdType).getFragment();
                    bindings.bind(variableName,
                            objectNode.getLiteralLexicalForm(),
                            RDFTypes.valueOf(fragment.toUpperCase()));
                    log.debug("found " + xsdType);
                } catch (URISyntaxException e) {
                    throw new MarkLogicJenaException(
                            "Unrecognized binding type.  Use XSD only.", e);
                }
            } else if (! "".equals(objectNode.getLiteralLanguage())) {
                String languageTag = objectNode.getLiteralLanguage();
                bindings.bind(variableName, objectNode.getLiteralLexicalForm(),
                        Locale.forLanguageTag(languageTag));
            } else {
                // is this a hole, no type string?
                bindings.bind(variableName, objectNode.getLiteralLexicalForm(),
                        RDFTypes.STRING);
            }
        }
        qdef.setBindings(bindings);
        return qdef;
    }

    /*
     * @see org.apache.jena.sparql.core.DatasetGraphTriplesQuads Internally uses
     * a write cache to hold batches of quad updates. @see sync()
     */
    @Override
    protected void addToDftGraph(Node s, Node p, Node o) {
        checkIsOpen();
        Node s1 = skolemize(s);
        Node p1 = skolemize(p);
        Node o1 = skolemize(o);
        client.sinkQuad(null, s1, p1, o1);
    }

    private Node skolemize(Node s) {
        if (s.isBlank()) {
            return NodeFactory
                    .createURI("http://marklogic.com/semantics/blank/"
                            + s.toString());
        } else {
            return s;
        }
    }

    /**
     * @see org.apache.jena.sparql.core.DatasetGraphTriplesQuads Internally uses
     *      a write cache to hold batches of quad updates. @see sync()
     */
    @Override
    protected void addToNamedGraph(Node g, Node s, Node p, Node o) {
        checkIsOpen();
        Node s1 = skolemize(s);
        Node p1 = skolemize(p);
        Node o1 = skolemize(o);
        client.sinkQuad(g, s1, p1, o1);
    }

    /**
     * @see org.apache.jena.sparql.core.DatasetGraphTriplesQuads
     */
    @Override
    protected void deleteFromDftGraph(Node s, Node p, Node o) {
        checkIsOpen();
        sync();
        String query = "DELETE  WHERE { ?s ?p ?o }";
        Node s1 = skolemize(s);
        Node p1 = skolemize(p);
        Node o1 = skolemize(o);
        SPARQLQueryDefinition qdef = client.newQueryDefinition(query);
        qdef.withBinding("s", s1.getURI());
        qdef.withBinding("p", p1.getURI());
        qdef = bindObject(qdef, "o", o1);
        client.executeUpdate(qdef);
    }

    /**
     * @see org.apache.jena.sparql.core.DatasetGraphTriplesQuads
     */
    @Override
    protected void deleteFromNamedGraph(Node g, Node s, Node p, Node o) {
        checkIsOpen();
        sync();
        Node s1 = skolemize(s);
        Node p1 = skolemize(p);
        Node o1 = skolemize(o);
        String query = "DELETE WHERE { GRAPH ?g { ?s ?p ?o } }";
        SPARQLQueryDefinition qdef = client.newQueryDefinition(query);
        if (g != null) {
            qdef.withBinding("g", g.getURI());
        }
        qdef.withBinding("s", s1.getURI());
        qdef.withBinding("p", p1.getURI());
        qdef = bindObject(qdef, "o", o1);
        client.executeUpdate(qdef);
    }

    private InputStream selectTriplesInGraph(String graphName, Node s, Node p,
            Node o) {
        checkIsOpen();
        Node s1 = s != null ? s : Node.ANY;
        Node p1 = p != null ? p : Node.ANY;
        Node o1 = o != null ? o : Node.ANY;
        Node s2 = skolemize(s1);
        Node p2 = skolemize(p1);
        Node o2 = skolemize(o1);
        SPARQLQueryDefinition qdef = client.newQueryDefinition("");
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ?s ?p ?o where { ?s ?p ?o .");
        if (s2 != Node.ANY) {
            qdef.withBinding("a", s2.getURI());
            sb.append("FILTER (?s = ?a) ");
        }
        if (p2 != Node.ANY) {
            qdef.withBinding("b", p2.getURI());
            sb.append("FILTER (?p = ?b) ");
        }
        if (o2 != Node.ANY) {
            qdef = bindObject(qdef, "c", o2);
            sb.append("FILTER (?o = ?c) ");
        }
        sb.append("}");
        qdef.setSparql(sb.toString());
        qdef.setDefaultGraphUris(graphName);
        InputStreamHandle results = client.executeSelect(qdef,
                new InputStreamHandle());
        return results.get();
    }

    /**
     * @see org.apache.jena.sparql.core.DatasetGraphBaseFind
     */
    @Override
    protected Iterator<Quad> findInDftGraph(Node s, Node p, Node o) {
        checkIsOpen();
        sync();
        InputStream results = selectTriplesInGraph(DEFAULT_GRAPH_URI, s, p, o);
        return new QuadsIterator(results);
    }

    /**
     * @see org.apache.jena.sparql.core.DatasetGraphBaseFind
     */
    @Override
    protected Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p,
            Node o) {
        checkIsOpen();
        sync();
        InputStream results = selectTriplesInGraph(g.getURI(), s, p, o);
        return new QuadsIterator(g.getURI(), results);
    }

    /**
     * @see org.apache.jena.sparql.core.DatasetGraphBaseFind
     */
    @Override
    protected Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
        checkIsOpen();
        sync();
        Node s1 = s != null ? s : Node.ANY;
        Node p1 = p != null ? p : Node.ANY;
        Node o1 = o != null ? o : Node.ANY;

        SPARQLQueryDefinition qdef = client.newQueryDefinition("");
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ?g ?s ?p ?o where {GRAPH ?g { ?s ?p ?o }");
        if (s1 != Node.ANY) {
            qdef.withBinding("a", s1.getURI());
            sb.append("FILTER (?s = ?a) ");
        }
        if (p1 != Node.ANY) {
            qdef.withBinding("b", p1.getURI());
            sb.append("FILTER (?p = ?b) ");
        }
        if (o1 != Node.ANY) {
            qdef = bindObject(qdef, "c", o1);
            sb.append("FILTER (?o = ?c) ");
        }
        sb.append("}");
        qdef.setSparql(sb.toString());
        InputStreamHandle results = client.executeSelect(qdef,
                new InputStreamHandle());
        return new QuadsIterator(results.get());
    }

    @Override
    /**
     * @see org.apache.jena.sparql.core.DatasetGraph
     */
    public void setDefaultGraph(Graph g) {
        this.addGraph(NodeFactory.createURI(DEFAULT_GRAPH_URI), g);
    }

    @Override
    /**
     * Start a transaction.
     */
    public void begin(ReadWrite readWrite) {
        checkIsOpen();
        sync();
        client.begin(readWrite);
    }

    @Override
    /**
     * Commit the current transaction.
     */
    public void commit() {
        checkIsOpen();
        sync();
        client.commit();
    }

    @Override
    /**
     * Abort the current transaction with a rollback operation.
     */
    public void abort() {
        checkIsOpen();
        sync();
        client.abort();
    }

    @Override
    /**
     * @return true if there is a multi-statement transaction in play.
     */
    public boolean isInTransaction() {
        checkIsOpen();
        return client.isInTransaction();
    }

    @Override
    /**
     * Synonymous with abort();
     */
    public void end() {
        abort();
    }

    /**
     * Gets a view of the DatasetGraph as a Dataset, which is used to back
     * queries.
     */
    public Dataset toDataset() {
        checkIsOpen();
        return DatasetFactory.create(this);
    }

    /**
     * @see org.apache.jena.sparql.core.DatasetGraph
     */
    @Override
    public Graph getDefaultGraph() {
        checkIsOpen();
        sync();
        return client.readDefaultGraph();
    }

    /**
     * @see org.apache.jena.sparql.core.DatasetGraph
     */
    @Override
    public Graph getGraph(Node graphNode) {
        checkIsOpen();
        sync();
        return client.readGraph(graphNode.getURI());
    }

    /**
     * @see org.apache.jena.sparql.core.DatasetGraph
     */
    @Override
    public void addGraph(Node graphName, Graph graph) {
        checkIsOpen();
        sync();
        client.writeGraph(graphName.getURI(), graph);
    }

    /**
     * Merges triples into a graph on the MarkLogic server. mergeGraph() is NOT
     * part of Jena's DatasetGraph interface.
     * 
     * @param graphName
     *            The graph to merge with server state.
     * @param graph
     *            The graph data.
     */
    public void mergeGraph(Node graphName, Graph graph) {
        checkIsOpen();
        sync();
        client.mergeGraph(graphName.getURI(), graph);
    }

    /**
     * @see org.apache.jena.sparql.core.DatasetGraph
     */
    @Override
    public void removeGraph(Node graphName) {
        checkIsOpen();
        sync();
        client.deleteGraph(graphName.getURI());
    }

    @Override
    /**
     * Not supported by MarkLogicDatasetGraph.
     */
    public long size() {
        throw new UnsupportedOperationException("Dataset.size()");
    }

    /**
     * Gets the permissions associated with this graph.
     * 
     * @param graphName
     *            the node with the graph's name.
     * @return A {@link com.marklogic.client.semantics.GraphPermissions} object
     *         holding the graph's permissions.
     */
    public GraphPermissions getPermissions(Node graphName) {
        checkIsOpen();
        return client.getGraphPermissions(graphName.getURI());
    }

    /**
     * Adds permissions to a graph.
     * 
     * @param graphName
     *            the node with the graph's name.
     * @param permissions
     *            A {@link com.marklogic.client.semantics.GraphPermissions}
     *            object holding the graph's permissions.
     */
    public void addPermissions(Node graphName, GraphPermissions permissions) {
        checkIsOpen();
        client.mergeGraphPermissions(graphName.getURI(), permissions);
    }

    /**
     * Removes all but the default permissions from a graph.
     * 
     * @param graphName
     *            the node with the graph's name.
     */
    public void clearPermissions(Node graphName) {
        checkIsOpen();
        client.deletePermissions(graphName.getURI());
    }

    /**
     * Sets the permissions on a graph.
     * 
     * @param graphName
     *            the node with the graph's name.
     * @param permissions
     *            A {@link com.marklogic.client.semantics.GraphPermissions}
     *            object holding the graph's permissions.
     */
    public void writePermissions(Node graphName, GraphPermissions permissions) {
        checkIsOpen();
        client.writeGraphPermissions(graphName.getURI(), permissions);
    }

    /**
     * Forces the quads in the write cache to flush to the server.
     */
    public void sync() {
        client.sync();
    }

    /**
     * Specifies a set of inferencing rulesets to apply to a query. These
     * rulesets either come with MarkLogic server or were installed by an
     * administrator.
     * 
     * @param rulesets
     *            Zero-or-more rulesets to apply to queries.
     */
    public void setRulesets(SPARQLRuleset... rulesets) {
        this.rulesets = rulesets;
    }

    /**
     * Returns the array or rulesets currently used for SPARQL queries.
     * 
     * @return An array of SPARQLRulesets.
     */
    public SPARQLRuleset[] getRulesets() {
        return this.rulesets;
    }

    /**
     * Fluent setter for rulesets.
     * 
     * @param rulesets
     *            Zero-or-more rulesets to apply to queries.
     * @return The MarkLogicDatasetGraph, with rulesets set.
     */
    public MarkLogicDatasetGraph withRulesets(SPARQLRuleset... rulesets) {
        if (this.rulesets == null) {
            this.rulesets = rulesets;
        } else {
            Collection<SPARQLRuleset> collection = new ArrayList<SPARQLRuleset>();
            collection.addAll(Arrays.asList(this.rulesets));
            collection.addAll(Arrays.asList(rulesets));
            this.rulesets = collection.toArray(new SPARQLRuleset[] {});
        }
        return this;
    }

    /**
     * Sets a MarkLogic Java API QueryDefinition that is applied to SPARQL
     * queries to restrict documents upon which queries are run.
     * 
     * @param constrainingQueryDefinition
     *            A query definition. Use raw query definitions or QueryBuilder.
     */
    public void setConstrainingQueryDefinition(
            QueryDefinition constrainingQueryDefinition) {
        this.constrainingQueryDefinition = constrainingQueryDefinition;
    }

    /**
     * Return the query defintion currently associated with SPARQL Queries
     * against this DatasetGraph.
     * 
     * @return the QueryDefinition.
     */
    public QueryDefinition getConstrainingQueryDefinition() {
        return constrainingQueryDefinition;
    }

    /**
     * Return the com.marklogic.semantics.jena.client.JenaDatabaseClient
     * 
     * @return THe active client associated with this graph.
     */
    public JenaDatabaseClient getDatabaseClient() {
        return this.client;
    }

    /**
     * Set the permissions for graphs created by this DatasetGraph during SPARQL
     * update operations. Set to null for default permissions.
     * 
     * @param permissions
     *            One or more permissions to add to graphs created during SPARQL
     *            updates.
     */
    public void setSPARQLUpdatePermissions(GraphPermissions permissions) {
        this.updatePermissions = permissions;
    }

    /**
     * Get the permissions that are to be written to new graphs during SPARQL
     * update.
     * 
     * @return the permissions associated with updates.
     */
    public GraphPermissions getSPARQLUpdatePermissions() {
        return this.updatePermissions;
    }

    /**
     * Closes the connection to the database. After closing the DatabaseGraph,
     * it can no longer be used for query or update.
     */
    @Override
    public void close() {
        checkIsOpen();
        this.client.close();
        this.client = null;
    }

    private void checkIsOpen() {
        if (client == null) {
            throw new MarkLogicJenaException("DatabaseGraph is closed");
        }
    }

    @Override
    public void startRequest() {
        // noop
    }

    @Override
    public void finishRequest() {
        // noop
    }
}
