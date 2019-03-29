/*
 * Copyright 2016-2019 MarkLogic Corporation
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
package com.marklogic.semantics.jena.engine;

import org.apache.jena.atlas.lib.Sink;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.modify.UpdateEngine;
import org.apache.jena.sparql.modify.UpdateEngineFactory;
import org.apache.jena.sparql.modify.UpdateEngineMain;
import org.apache.jena.sparql.modify.UpdateEngineRegistry;
import org.apache.jena.sparql.modify.request.UpdateAdd;
import org.apache.jena.sparql.modify.request.UpdateClear;
import org.apache.jena.sparql.modify.request.UpdateCopy;
import org.apache.jena.sparql.modify.request.UpdateCreate;
import org.apache.jena.sparql.modify.request.UpdateDataDelete;
import org.apache.jena.sparql.modify.request.UpdateDataInsert;
import org.apache.jena.sparql.modify.request.UpdateDeleteWhere;
import org.apache.jena.sparql.modify.request.UpdateDrop;
import org.apache.jena.sparql.modify.request.UpdateLoad;
import org.apache.jena.sparql.modify.request.UpdateModify;
import org.apache.jena.sparql.modify.request.UpdateMove;
import org.apache.jena.sparql.modify.request.UpdateVisitor;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.update.Update;

import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicJenaException;
import com.marklogic.semantics.jena.client.JenaDatabaseClient;

/**
 * Provides a connection between Jena's update mechanism and SPARQL
 * UPDATE on MarkLogic.
 */
public class MarkLogicUpdateEngine extends UpdateEngineMain {

    private static UpdateEngineFactory factory = new MarkLogicUpdateEngineFactory();

    public MarkLogicUpdateEngine(DatasetGraph graphStore, Binding inputBinding,
            Context context) {
        super(graphStore, inputBinding, context);
    }

    @Override
    protected UpdateVisitor prepareWorker() {
        return new MarkLogicUpdateEngineWorker(datasetGraph, inputBinding,
                context);
    }

    /**
     * UnRegisters the factory from Jena's UpdateEngineRegistry.
     */
    public static void unregister() {
        UpdateEngineRegistry.removeFactory(factory);
    }

    /**
     * Registers the factory with Jena's UpdateEngineRegistry.
     */
    public static void register() {
        UpdateEngineRegistry.addFactory(factory);
    }

    /**
     * Creates the UpdateEngine for integration into Jena.
     */
    public static class MarkLogicUpdateEngineFactory implements
            UpdateEngineFactory {

        @Override
        public boolean accept(DatasetGraph graphStore, Context context) {
            return (graphStore instanceof MarkLogicDatasetGraph);
        }

        @Override
        public UpdateEngine create(DatasetGraph datasetGraph, Binding inputBinding,
                Context context) {
            MarkLogicUpdateEngine engine = new MarkLogicUpdateEngine(
                    datasetGraph, inputBinding, context);
            return engine;
        }

    }

    /**
     * Code that implements sending update queries to the MarkLogic
     * SPARQL UPDATE endpoint.
     */
    public class MarkLogicUpdateEngineWorker implements UpdateVisitor {

        private MarkLogicDatasetGraph markLogicDatasetGraph;
        private JenaDatabaseClient client;
        private Binding initial;

        public MarkLogicUpdateEngineWorker(DatasetGraph graphStore,
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
            if (markLogicDatasetGraph.getSPARQLUpdatePermissions() != null) {
                qdef.setUpdatePermissions(markLogicDatasetGraph.getSPARQLUpdatePermissions());
            }
            client.executeUpdate(qdef);
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
