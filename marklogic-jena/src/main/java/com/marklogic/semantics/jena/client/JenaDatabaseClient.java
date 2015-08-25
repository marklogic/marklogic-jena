package com.marklogic.semantics.jena.client;

import java.util.Iterator;
import java.util.Timer;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.WriterGraphRIOT;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ResourceNotFoundException;
import com.marklogic.client.Transaction;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.OutputStreamHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicTransactionException;

/**
 * A class to encapsulate access to the Java API's DatabaseClient for Jena users.
 * Access the underlying Java API client with getClient();
 */
public class JenaDatabaseClient {

    private GraphManager graphManager;
    private SPARQLQueryManager sparqlQueryManager;
    private WriteCacheTimerTask cache;
    private DatabaseClient client;
    private Transaction currentTransaction;
    
    public JenaDatabaseClient(DatabaseClient client) {
        this.client = client;
        this.graphManager = client.newGraphManager();
        this.graphManager.setDefaultMimetype(RDFMimeTypes.NTRIPLES);
        this.sparqlQueryManager = client.newSPARQLQueryManager();
        this.cache = new WriteCacheTimerTask(this);
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(cache, 1000, 1000);
    }

    /**
     * Create a new {@link com.marklogic.client.semantics.SPARQLQueryDefinition} from
     * a query String.  You can use the resulting object to configure various
     * aspects of the query or set binding variables.
     * @param queryString A SPARQL Query or SPARQL Update.
     * @return A new {@link com.marklogic.client.semantics.SPARQLQueryDefinition}
     */
    public SPARQLQueryDefinition newQueryDefinition(String queryString) {
        return this.sparqlQueryManager.newQueryDefinition(queryString);
    }

    public void executeUpdate(SPARQLQueryDefinition qdef) {
        this.sparqlQueryManager.executeUpdate(qdef, currentTransaction);
    }

    public boolean executeAsk(SPARQLQueryDefinition qdef) {
        return this.sparqlQueryManager.executeAsk(qdef, currentTransaction);
    }

    public InputStreamHandle executeConstruct(SPARQLQueryDefinition qdef,
            InputStreamHandle handle) {
        return this.sparqlQueryManager.executeConstruct(qdef, handle, currentTransaction);
    }

    public InputStreamHandle executeDescribe(SPARQLQueryDefinition qdef,
            InputStreamHandle handle) {
        return this.sparqlQueryManager.executeDescribe(qdef, handle, currentTransaction);
    }

    public InputStreamHandle executeSelect(SPARQLQueryDefinition qdef,
            InputStreamHandle handle, Long offset, Long limit) {
        if (limit == null) {
            this.sparqlQueryManager.clearPageLength();
        } else {
            this.sparqlQueryManager.setPageLength(limit);
        }
        if (offset != null) {
            return this.sparqlQueryManager.executeSelect(qdef, handle, offset, currentTransaction);
        }
        else {
            return this.sparqlQueryManager.executeSelect(qdef, handle, currentTransaction);
        }
    }

    public InputStreamHandle executeSelect(SPARQLQueryDefinition qdef,
            InputStreamHandle handle) {
        return executeSelect(qdef, handle, null, null);
    }

    public Iterator<String> listGraphUris() {
        return this.graphManager.listGraphUris();
    }

    public void mergeGraph(String uri, Graph graph) {
        WriterGraphRIOT writer = RDFDataMgr.createGraphWriter(Lang.NTRIPLES);
        OutputStreamRIOTSender sender = new OutputStreamRIOTSender(writer);
        sender.setGraph(graph);
        OutputStreamHandle handle = new OutputStreamHandle(sender);
        this.graphManager.merge(uri, handle, currentTransaction);
    }

    public void deleteGraph(String uri) {
        this.graphManager.delete(uri, currentTransaction);
    }

    public GraphPermissions getGraphPermissions(String uri) {
        return this.graphManager.getPermissions(uri, currentTransaction);
    }

    public void mergeGraphPermissions(String uri, GraphPermissions permissions) {
        this.graphManager.mergePermissions(uri, permissions, currentTransaction);
    }

    public void deletePermissions(String uri) {
        this.graphManager.deletePermissions(uri, currentTransaction);
    }

    public void writeGraphPermissions(String uri, GraphPermissions permissions) {
        this.graphManager.writePermissions(uri, permissions, currentTransaction);
    }

    public Transaction openTransaction() {
        return this.client.openTransaction();
    }


    public Graph readDefaultGraph() {
        return readGraph(MarkLogicDatasetGraph.DEFAULT_GRAPH_URI);
    }

    public Graph readGraph(String uri) {
        InputStreamHandle handle = new InputStreamHandle();
        Graph graph = GraphFactory.createDefaultGraph();
        try {
            this.graphManager.read(uri, handle, currentTransaction);
            RDFDataMgr.read(graph, handle.get(), Lang.NTRIPLES);
        } catch (NullPointerException e) {
            
        } catch (ResourceNotFoundException e) {
            
        }
        // close handle?
        return graph;
    }

    public void writeGraph(String uri, Graph graph) {
        WriterGraphRIOT writer = RDFDataMgr.createGraphWriter(Lang.NTRIPLES);
        OutputStreamRIOTSender sender = new OutputStreamRIOTSender(writer);
        sender.setGraph(graph);
        OutputStreamHandle handle = new OutputStreamHandle(sender);
        this.graphManager.write(uri, handle, currentTransaction);
    }

    /**
     * Puts a quad into the cache, which is periodically sent to MarkLogic
     * @param g Graph node.
     * @param s Subject node
     * @param p Property node.
     * @param o Object Node.
     */
    public void sinkQuad(Node g, Node s, Node p, Node o) {
        cache.add(g, s, p, o);
    }

    /**
     * Flushes the write cache, ensuring consistent server state
     * before query/delete.
     */
    public void sync() {
        cache.forceRun();
    }

    

    private void checkCurrentTransaction() {
        if (this.currentTransaction == null) {
            throw new MarkLogicTransactionException("No open transaction");
        }
    }
    
    public void begin(ReadWrite readWrite) {
        if (readWrite == ReadWrite.READ) {
            throw new MarkLogicTransactionException("MarkLogic only supports write transactions");
        } else {
            if (this.currentTransaction != null) {
                throw new MarkLogicTransactionException("Only one open transaction per MarkLogicDatasetGraph instance.");
            }
            this.currentTransaction = openTransaction();
        }
    }

    public void commit() {
        checkCurrentTransaction();
        this.currentTransaction.commit();
        this.currentTransaction = null;
    }

    public void abort() {
        checkCurrentTransaction();
        this.currentTransaction.rollback();
        this.currentTransaction = null;
    }

    public boolean isInTransaction() {
        return (this.currentTransaction != null);
    }

}
