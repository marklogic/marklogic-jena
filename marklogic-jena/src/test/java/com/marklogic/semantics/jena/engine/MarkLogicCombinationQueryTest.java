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
package com.marklogic.semantics.jena.engine;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.semantics.jena.JenaTestBase;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;

public class MarkLogicCombinationQueryTest extends JenaTestBase {

    private Dataset ds;
    private MarkLogicDatasetGraph dsg;
    private QueryManager qmgr;
    
    @Before
    public void setupDataset() {
        dsg = getMarkLogicDatasetGraph("testdata/testData.trig");

        ds = DatasetFactory
                .create(dsg);

        String tripleDocOne = 

                "<semantic-document>\n" +
                "<title>First Title</title>\n" +
                "<size>100</size>\n" +
                "<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">" +
                "<sem:triple><sem:subject>http://example.org/r9928</sem:subject>" +
                "<sem:predicate>http://example.org/p3</sem:predicate>" +
                "<sem:object datatype=\"http://www.w3.org/2001/XMLSchema#int\">1</sem:object></sem:triple>" +
                "</sem:triples>\n" +
                "</semantic-document>";

        String tripleDocTwo = 

                "<semantic-document>\n" +
                "<title>Second Title</title>\n" +
                "<size>500</size>\n" +
                "<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">" +
                "<sem:triple><sem:subject>http://example.org/r9929</sem:subject>" +
                "<sem:predicate>http://example.org/p3</sem:predicate>" +
                "<sem:object datatype=\"http://www.w3.org/2001/XMLSchema#int\">2</sem:object></sem:triple>" +
                "</sem:triples>\n" +
                "</semantic-document>";

        XMLDocumentManager docMgr = writerClient.newXMLDocumentManager();
        docMgr.write("/directory1/doc1.xml", new StringHandle().with(tripleDocOne));
        docMgr.write("/directory2/doc2.xml", new StringHandle().with(tripleDocTwo));
        qmgr = writerClient.newQueryManager();

    }

    @Test
    public void testCombinationQuery() {
        
        String query1 = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";
        String query2 = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";
        
        // case one, rawcombined
        String combinedQuery = 
            "{\"search\":" +
            "{\"qtext\":\"First Title\"}}";
        String negCombinedQuery = 
                "{\"search\":" +
                "{\"qtext\":\"Second Title\"}}";
        
        RawCombinedQueryDefinition rawCombined = qmgr.newRawCombinedQueryDefinition(new StringHandle().with(combinedQuery).withFormat(Format.JSON));
        RawCombinedQueryDefinition negRawCombined = qmgr.newRawCombinedQueryDefinition(new StringHandle().with(negCombinedQuery).withFormat(Format.JSON));

        dsg.setConstrainingQueryDefinition(rawCombined);
        
        QueryExecution queryExec = QueryExecutionFactory.create(query1, ds);
        
        assertTrue(queryExec.execAsk());
        
        queryExec = QueryExecutionFactory.create(query2, ds);
        
        assertFalse(queryExec.execAsk());
        
        dsg.setConstrainingQueryDefinition(negRawCombined);
        queryExec = QueryExecutionFactory.create(query1, ds);
        
        assertFalse(queryExec.execAsk());
        
        queryExec = QueryExecutionFactory.create(query2, ds);
        
        assertTrue(queryExec.execAsk());
        dsg.setConstrainingQueryDefinition(null);
    }
    
    @Test
    public void testStringQuery() {
        StringQueryDefinition stringDef = qmgr.newStringDefinition().withCriteria("First");
        dsg.setConstrainingQueryDefinition(stringDef);
        
        String posQuery = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";
        String negQuery = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";
        QueryExecution queryExec = QueryExecutionFactory.create(posQuery, ds);
        assertTrue(queryExec.execAsk());
        
        queryExec = QueryExecutionFactory.create(negQuery, ds);
        assertFalse(queryExec.execAsk());
        
        // set to null
        dsg.setConstrainingQueryDefinition(null);
        queryExec = QueryExecutionFactory.create(posQuery, ds);
        assertTrue(queryExec.execAsk());
        queryExec = QueryExecutionFactory.create(negQuery, ds);
        assertTrue(queryExec.execAsk());
       
    }
    
    @Test
    public void testStructuredQuery() {
        // reversing neg and pos for diversity.
        StructuredQueryBuilder qb = new StructuredQueryBuilder();
        QueryDefinition structuredDef = qb.build(qb.term("Second"));
        dsg.setConstrainingQueryDefinition(structuredDef);
        
        String posQuery = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";
        String negQuery = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";
       
        QueryExecution queryExec = QueryExecutionFactory.create(posQuery, ds);
        assertTrue(queryExec.execAsk());
        
        queryExec = QueryExecutionFactory.create(negQuery, ds);
        assertFalse(queryExec.execAsk());
        
        // set to null
        dsg.setConstrainingQueryDefinition(null);
        queryExec = QueryExecutionFactory.create(posQuery, ds);
        assertTrue(queryExec.execAsk());
        queryExec = QueryExecutionFactory.create(negQuery, ds);
        assertTrue(queryExec.execAsk());
       
    }
    
    @After
    public void cleanupDocs() {
        XMLDocumentManager docMgr = writerClient.newXMLDocumentManager();
        
        docMgr.delete("/directory1/doc1.xml");
        docMgr.delete("/directory2/doc2.xml");

    }
}
