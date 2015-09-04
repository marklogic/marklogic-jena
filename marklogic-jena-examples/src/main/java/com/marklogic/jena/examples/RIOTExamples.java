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
package com.marklogic.jena.examples;

import java.util.Iterator;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.sparql.core.Quad;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;

public class RIOTExamples {

    private MarkLogicDatasetGraph dsg;

    public RIOTExamples() {
        dsg = ExampleUtils.loadPropsAndInit();
    }
    
    public void run() {
        System.out.println("Loading triples from NT files to MarkLogic");
        RDFDataMgr.read(dsg, "src/main/resources/dbpedia60k.nt", Lang.NTRIPLES);
        
        System.out.println("Loading more from RDF/XML file to MarkLogic");
        RDFDataMgr.read(dsg, "src/main/resources/test.owl", Lang.RDFXML);

        System.out.println("Write the entire database to System.out");
        // this does not work, see Issue #16
        // RDFDataMgr.write(System.out, dsg.toDataset(), Lang.NTRIPLES);
        int i=0;
        for (Iterator<Quad> quads = dsg.find(); quads.hasNext(); i++) {
            Quad quad = quads.next();
            System.out.println(quad.toString());
        }
        dsg.close();
    }

    public static void main(String... args) {
       RIOTExamples example = new RIOTExamples();
       example.run();
    }
}
