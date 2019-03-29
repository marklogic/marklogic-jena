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
package com.marklogic.jena.examples;

import java.io.StringReader;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;

/**
 * How to run queries.
 *
 */
public class SPARQLQueryExample {
    
    private MarkLogicDatasetGraph dsg;

    public SPARQLQueryExample() {
        dsg = ExampleUtils.loadPropsAndInit();
    }

    private void run() {
        // Make some triples
        dsg.clear();
        
        String turtle = "@prefix foaf: <http://xmlns.com/foaf/0.1/> ."
                + "@prefix : <http://example.org/> ."
                +":charles a foaf:Person ; "
                + "        foaf:name \"Charles\" ;"
                + "        foaf:knows :jim ."
                + ":jim    a foaf:Person ;"
                + "        foaf:name \"Jim\" ;"
                + "        foaf:knows :charles .";
        System.out.println("Loading triples");
        
        RDFDataMgr.read(dsg,  new StringReader(turtle), "", Lang.TURTLE);
        
        QueryExecution execution = QueryExecutionFactory.create(
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
                +"select ?aname ?bname where { ?a foaf:knows ?b ."
                + "                    ?a foaf:name ?aname ."
                + "                    ?b foaf:name ?bname }", dsg.toDataset());
        int n = 1;
        for (ResultSet results = execution.execSelect();
                results.hasNext();
                n++) {
            QuerySolution solution = results.next();
            System.out.println(
                    "Solution #" + n + ": "
                    + solution.get("aname").asLiteral().getString()
                    +" knows " 
                    + solution.get("bname").asLiteral().getString());
        }
        dsg.close();
    }

    public static void main(String... args) {
        SPARQLQueryExample example = new SPARQLQueryExample();
        example.run();
     }

}
