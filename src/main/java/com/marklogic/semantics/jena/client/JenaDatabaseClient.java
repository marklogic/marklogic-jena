package com.marklogic.semantics.jena.client;

import java.util.Iterator;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.Transaction;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.OutputStreamHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;

/**
 * A class to encapsulate access to the Java API's DatabaseClient for Jena users.
 * Access the underlying Java API client with getClient();
 */
public class JenaDatabaseClient {

    private GraphManager graphManager;
    private SPARQLQueryManager sparqlQueryManager;
    
    private DatabaseClient client;
    
    public JenaDatabaseClient(DatabaseClient client) {
        this.client = client;
        this.graphManager = client.newGraphManager();
        this.graphManager.setDefaultMimetype(RDFMimeTypes.NTRIPLES);
        this.sparqlQueryManager = client.newSPARQLQueryManager();
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

    public void executeUpdate(SPARQLQueryDefinition qdef, Transaction tx) {
        this.sparqlQueryManager.executeUpdate(qdef, tx);
    }

    public boolean executeAsk(SPARQLQueryDefinition qdef, Transaction tx) {
        return this.sparqlQueryManager.executeAsk(qdef, tx);
    }

    public InputStreamHandle executeConstruct(SPARQLQueryDefinition qdef,
            InputStreamHandle handle, Transaction tx) {
        return this.sparqlQueryManager.executeConstruct(qdef, handle, tx);
    }

    public InputStreamHandle executeDescribe(SPARQLQueryDefinition qdef,
            InputStreamHandle handle, Transaction tx) {
        return this.sparqlQueryManager.executeDescribe(qdef, handle, tx);
    }

    public InputStreamHandle executeSelect(SPARQLQueryDefinition qdef,
            InputStreamHandle handle, long offset, long limit, Transaction tx) {
        return this.sparqlQueryManager.executeSelect(qdef, handle, offset, limit, tx);
    }

    public InputStreamHandle executeSelect(SPARQLQueryDefinition qdef,
            InputStreamHandle handle, Transaction tx) {
        return executeSelect(qdef, handle, -1, -1, tx);
    }

    public Iterator<String> listGraphUris() {
        return this.graphManager.listGraphUris();
    }

    public void mergeGraph(String uri, OutputStreamHandle handle,
            Transaction tx) {
        this.graphManager.merge(uri, handle, tx);
    }

    public void deleteGraph(String uri, Transaction tx) {
        this.graphManager.delete(uri, tx);
    }

    public GraphPermissions getGraphPermissions(String uri) {
        return this.graphManager.getPermissions(uri);
    }

    public void mergeGraphPermissions(String uri, GraphPermissions permissions) {
        this.graphManager.mergePermissions(uri, permissions);
    }

    public Transaction openTransaction() {
        return this.client.openTransaction();
    }

    public InputStreamHandle readGraph(String uri, InputStreamHandle handle,
            Transaction tx) {
        return this.graphManager.read(uri, handle, tx);
    }

}
