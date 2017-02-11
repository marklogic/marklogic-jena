/*
 * Copyright 2016-2017 MarkLogic Corporation
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
package com.marklogic.jena.examples;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;

/**
 * How to run queries.
 *
 */
public class SPARQLUpdateExample {
    
    private MarkLogicDatasetGraph dsg;

    public SPARQLUpdateExample() {
        dsg = ExampleUtils.loadPropsAndInit();
    }

    private void run() {
        dsg.clear();
        
        String insertData = "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
                + "PREFIX : <http://example.org/> "
                +"INSERT DATA {GRAPH :g1 {"
                + ":charles a foaf:Person ; "
                + "        foaf:name \"Charles\" ;"
                + "        foaf:knows :jim ."
                + ":jim    a foaf:Person ;"
                + "        foaf:name \"Jim\" ;"
                + "        foaf:knows :charles ."
                + "} }";
        
        System.out.println("Running SPARQL update");
        
        UpdateRequest update = UpdateFactory.create(insertData);
        UpdateProcessor processor = UpdateExecutionFactory.create(update, dsg);
        processor.execute();
        
        System.out.println("Examine the data as JSON-LD");
        RDFDataMgr.write(System.out, dsg.getGraph(NodeFactory.createURI("http://example.org/g1")), RDFFormat.JSONLD_PRETTY);
        
        System.out.println("Remove it.");
        
        update = UpdateFactory.create("PREFIX : <http://example.org/> DROP GRAPH :g1");
        processor = UpdateExecutionFactory.create(update, dsg);
        processor.execute();
        dsg.close();
    }

    public static void main(String... args) {
        SPARQLUpdateExample example = new SPARQLUpdateExample();
        example.run();
     }

}
