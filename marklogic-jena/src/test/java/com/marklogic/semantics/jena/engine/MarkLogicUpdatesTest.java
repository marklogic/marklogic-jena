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
package com.marklogic.semantics.jena.engine;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateRequest;
import com.marklogic.client.semantics.Capability;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.semantics.jena.JenaTestBase;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;

public class MarkLogicUpdatesTest extends JenaTestBase {

    @Test
    public void testUpdateAction() {
        DatasetGraph gs = getMarkLogicDatasetGraph();
        UpdateRequest update = new UpdateRequest();
        update.add("INSERT DATA { <s2> <p1> <o1> }");
        update.add("DROP ALL")
                .add("CREATE GRAPH <http://example/update1>")
                .add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/update2> { <s1> <p1> <o1>  } }")
                .add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/update3> { <s1> <p1> <o1>  } }");

        UpdateAction.execute(update, gs);

        QueryExecution askQuery = QueryExecutionFactory
                .create("BASE <http://example.org/> ASK WHERE { GRAPH <update3> { <s1> <p1> <o1>  }}",
                        DatasetFactory.create(gs));
        assertTrue("update action must update database.", askQuery.execAsk());

    }

    @Test
    public void testUpdateTransactions() {
        MarkLogicDatasetGraph dsg = getMarkLogicDatasetGraph();

        UpdateRequest update = new UpdateRequest();
        update.add("BASE <http://example.org/> INSERT DATA { GRAPH <transact3> { <s4882> <p132> <o12321> } }");

        // insert a graph within a transaction, rollback
        assertFalse(dsg.isInTransaction());
        dsg.begin(ReadWrite.WRITE);
        assertTrue(dsg.isInTransaction());
        UpdateAction.execute(update, dsg);
        dsg.abort();
        assertFalse(dsg.isInTransaction());
        QueryExecution queryExec = QueryExecutionFactory
                .create("ASK WHERE { graph <http://example.org/transact3> { ?s ?p ?o }}",
                        dsg.toDataset());
        assertFalse("transact3 graph must not exist after rollback",
                queryExec.execAsk());

        dsg.begin(ReadWrite.WRITE);
        assertTrue(dsg.isInTransaction());
        UpdateAction.execute(update, dsg);
        dsg.commit();
        assertFalse(dsg.isInTransaction());

        queryExec = QueryExecutionFactory
                .create("BASE <http://example.org/>  ASK WHERE {  GRAPH <transact3> { ?s ?p ?o }}",
                        dsg.toDataset());
        assertTrue("transact3 graph must exist after commit",
                queryExec.execAsk());
    }

    @Test
    public void testSparqlUpdatePermissions() {
        MarkLogicDatasetGraph dsg = getMarkLogicDatasetGraph();

        UpdateRequest update = new UpdateRequest();
        update.add("BASE <http://example.org/> INSERT DATA { GRAPH <gp1> { <s4882> <p132> <o12321> } }");
        UpdateAction.execute(update, dsg);

        GraphPermissions graphPermissions = dsg.getPermissions(NodeFactory
                .createURI("http://example.org/gp1"));
        dsg.setSPARQLUpdatePermissions(graphPermissions.permission(
                "semantics-peon-role", Capability.READ));

        update = new UpdateRequest();
        update.add("BASE <http://example.org/> INSERT DATA { GRAPH <gp2> { <s4882> <p132> <o12321> } }");
        UpdateAction.execute(update, dsg);

        GraphPermissions gp2 = dsg.getPermissions(NodeFactory
                .createURI("http://example.org/gp2"));
        assertTrue("SPARQL Update permissions assigned",
                gp2.get("semantics-peon-role").contains(Capability.READ));

        update = new UpdateRequest();
        update.add("BASE <http://example.org/> DROP GRAPH <gp2>");
        UpdateAction.execute(update, dsg);

        dsg.setSPARQLUpdatePermissions(null);

        update = new UpdateRequest();
        update.add("BASE <http://example.org/> INSERT DATA { GRAPH <gp2> { <s4882> <p132> <o12321> } }");
        UpdateAction.execute(update, dsg);

        gp2 = dsg.getPermissions(NodeFactory
                .createURI("http://example.org/gp2"));
        assertNull("SPARQL Update permissions assigned",
                gp2.get("semantics-peon-role"));

    }

    @After
    public void dropTransactGraph() {
        MarkLogicDatasetGraph dsg = getMarkLogicDatasetGraph();

        UpdateRequest update = new UpdateRequest();
        update.add("BASE <http://example.org/> DROP SILENT GRAPH <transact3>")
                .add("DROP SILENT GRAPH <http://example.org/update2>")
                .add("DROP SILENT GRAPH <http://example.org/update3>")
                .add("DROP SILENT GRAPH <http://example.org/gp2>");
        UpdateAction.execute(update, dsg);
    }

}
