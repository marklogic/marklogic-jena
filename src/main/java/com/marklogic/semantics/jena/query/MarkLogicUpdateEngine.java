package com.marklogic.semantics.jena.query;

import org.apache.jena.atlas.lib.Sink;

import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.modify.UpdateEngine;
import com.hp.hpl.jena.sparql.modify.UpdateEngineFactory;
import com.hp.hpl.jena.sparql.modify.UpdateEngineMain;
import com.hp.hpl.jena.sparql.modify.UpdateEngineRegistry;
import com.hp.hpl.jena.sparql.modify.request.UpdateAdd;
import com.hp.hpl.jena.sparql.modify.request.UpdateClear;
import com.hp.hpl.jena.sparql.modify.request.UpdateCopy;
import com.hp.hpl.jena.sparql.modify.request.UpdateCreate;
import com.hp.hpl.jena.sparql.modify.request.UpdateDataDelete;
import com.hp.hpl.jena.sparql.modify.request.UpdateDataInsert;
import com.hp.hpl.jena.sparql.modify.request.UpdateDeleteWhere;
import com.hp.hpl.jena.sparql.modify.request.UpdateDrop;
import com.hp.hpl.jena.sparql.modify.request.UpdateLoad;
import com.hp.hpl.jena.sparql.modify.request.UpdateModify;
import com.hp.hpl.jena.sparql.modify.request.UpdateMove;
import com.hp.hpl.jena.sparql.modify.request.UpdateVisitor;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.update.Update;
import com.marklogic.client.Transaction;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.semantics.jena.MarkLogicJenaException;
import com.marklogic.semantics.jena.client.JenaDatabaseClient;
import com.marklogic.semantics.jena.graph.MarkLogicDatasetGraph;

public class MarkLogicUpdateEngine extends UpdateEngineMain {

    private static UpdateEngineFactory factory = new MarkLogicUpdateEngineFactory();

    public MarkLogicUpdateEngine(GraphStore graphStore, Binding inputBinding,
            Context context) {
        super(graphStore, inputBinding, context);
    }

    @Override
    protected UpdateVisitor prepareWorker() {
        return new MarkLogicUpdateEngineWorker(graphStore, inputBinding,
                context);
    }

    public static void unregister() {
        UpdateEngineRegistry.removeFactory(factory);
    }

    public static void register() {
        UpdateEngineRegistry.addFactory(factory);
    }

    public static class MarkLogicUpdateEngineFactory implements
            UpdateEngineFactory {

        @Override
        public boolean accept(GraphStore graphStore, Context context) {
            return (graphStore instanceof MarkLogicDatasetGraph);
        }

        @Override
        public UpdateEngine create(GraphStore graphStore, Binding inputBinding,
                Context context) {
            MarkLogicUpdateEngine engine = new MarkLogicUpdateEngine(
                    graphStore, inputBinding, context);
            return engine;
        }

    }

    public class MarkLogicUpdateEngineWorker implements UpdateVisitor {

        private MarkLogicDatasetGraph markLogicDatasetGraph;
        private JenaDatabaseClient client;
        private Binding initial;

        public MarkLogicUpdateEngineWorker(GraphStore graphStore,
                Binding inputBinding, Context context) {
            if (!(graphStore instanceof MarkLogicDatasetGraph)) {
                throw new MarkLogicJenaException(
                        "UpdateVisitor created with incorrect GraphStore implementation");
            } else {
                this.markLogicDatasetGraph = (MarkLogicDatasetGraph) graphStore;
                this.initial = inputBinding;
                this.client = markLogicDatasetGraph.getDatabaseClient();
            }
        }

        private void exec(Update update) {
            SPARQLQueryDefinition qdef = client.newQueryDefinition(update
                    .toString());
            if (markLogicDatasetGraph.getRulesets() != null) {
                qdef.setRulesets(markLogicDatasetGraph.getRulesets());
            }
            MarkLogicQueryEngine.bindVariables(qdef, this.initial,
                    markLogicDatasetGraph);
            Transaction tx = markLogicDatasetGraph.getCurrentTransaction();

            client.executeUpdate(qdef, tx);
        }

        @Override
        public void visit(UpdateDrop update) {
            exec(update);
        }

        @Override
        public void visit(UpdateClear update) {
            exec(update);
        }

        @Override
        public void visit(UpdateCreate update) {
            exec(update);
        }

        @Override
        public void visit(UpdateLoad update) {
            exec(update);
        }

        @Override
        public void visit(UpdateAdd update) {
            exec(update);
        }

        @Override
        public void visit(UpdateCopy update) {
            exec(update);
        }

        @Override
        public void visit(UpdateMove update) {
            exec(update);
        }

        @Override
        public void visit(UpdateDataInsert update) {
            exec(update);
        }

        @Override
        public void visit(UpdateDataDelete update) {
            exec(update);
        }

        @Override
        public void visit(UpdateDeleteWhere update) {
            exec(update);
        }

        @Override
        public void visit(UpdateModify update) {
            exec(update);
        }

        /**
         * Not required by this implementation.
         */
        @Override
        public Sink<Quad> createInsertDataSink() {
            return null;
        }

        /**
         * Not required by this implementation.
         */
        @Override
        public Sink<Quad> createDeleteDataSink() {
            return null;
        }

    }
}
