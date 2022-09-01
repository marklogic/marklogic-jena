/*
 * Copyright 2015-2019 MarkLogic Corporation
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

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import com.marklogic.semantics.jena.MarkLogicDatasetGraph;

/**
 * Using the RDFDataMgr to import/export triples into/out of MarkLogic
 */
public class RIOTExample {

    private MarkLogicDatasetGraph dsg;

    public RIOTExample() {
        dsg = ExampleUtils.loadPropsAndInit();
    }
    
    private void run() {
        System.out.println("Loading triples from NT files to MarkLogic");
        RDFDataMgr.read(dsg, "src/main/resources/dbpedia60k.nt", Lang.NTRIPLES);
        
        System.out.println("Loading more from RDF/XML file to MarkLogic");
        RDFDataMgr.read(dsg, "src/main/resources/test.owl", Lang.RDFXML);

        System.out.println("Write the entire database to System.out as NQUADS");
        RDFDataMgr.write(System.out, dsg.toDataset(), RDFFormat.NQUADS_UTF8);
        
        dsg.close();
    }

    public static void main(String... args) {
       RIOTExample example = new RIOTExample();
       example.run();
    }
}
