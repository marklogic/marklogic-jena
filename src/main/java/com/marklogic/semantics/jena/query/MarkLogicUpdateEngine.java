package com.marklogic.semantics.jena.query;

import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.modify.UpdateEngine;
import com.hp.hpl.jena.sparql.modify.UpdateEngineFactory;
import com.hp.hpl.jena.sparql.modify.UpdateEngineMain;
import com.hp.hpl.jena.sparql.modify.UpdateEngineRegistry;
import com.hp.hpl.jena.sparql.modify.request.UpdateVisitor;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.update.GraphStore;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.semantics.jena.graph.MarkLogicDatasetGraph;

public class MarkLogicUpdateEngine extends UpdateEngineMain {

    private static UpdateEngineFactory factory = new MarkLogicUpdateEngineFactory();
    private static SPARQLQueryManager sparqlQueryManager;
    
    public MarkLogicUpdateEngine(GraphStore graphStore,
            Binding inputBinding, Context context) {
        super(graphStore, inputBinding, context);
    }

    @Override
    protected UpdateVisitor prepareWorker() {
        return new MarkLogicUpdateEngineWorker(graphStore, inputBinding, context, sparqlQueryManager) ;
    }
    
    public static void unregister() {
        UpdateEngineRegistry.removeFactory(factory);
    }

    public static void register(DatabaseClient client) {
        UpdateEngineRegistry.addFactory(factory);
        sparqlQueryManager = client.newSPARQLQueryManager();
    }
    
    public static class MarkLogicUpdateEngineFactory implements UpdateEngineFactory {

        @Override
        public boolean accept(GraphStore graphStore, Context context) {
            return (graphStore instanceof MarkLogicDatasetGraph);
        }

        @Override
        public UpdateEngine create(GraphStore graphStore, Binding inputBinding,
                Context context) {
            MarkLogicUpdateEngine engine = new MarkLogicUpdateEngine(graphStore, inputBinding, context);
            return engine;
        }


    }
}
