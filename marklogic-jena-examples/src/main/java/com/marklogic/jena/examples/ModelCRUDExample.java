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

import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.graph.GraphFactory;

import java.io.StringReader;

public class ModelCRUDExample {

    private MarkLogicDatasetGraph dsg;

    public ModelCRUDExample() {
        dsg = ExampleUtils.loadPropsAndInit();
    }

    private void run() {
        // Make some triples
        dsg.clear();
        Dataset dataset = dsg.toDataset();
        String modelName = "http://example.org/graphs/charles";
        
        String turtle = "@prefix foaf: <http://xmlns.com/foaf/0.1/> ."
                + "@prefix : <http://example.org/> ."
                +":charles a foaf:Person ; "
                + "        foaf:name \"Charles\" ;"
                + "        foaf:knows :jim ."
                + ":jim    a foaf:Person ;"
                + "        foaf:name \"Jim\" ;"
                + "        foaf:knows :charles .";
        
        System.out.println("Make a model and load the turtle into it (client-side)");
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model,  new StringReader(turtle), "", Lang.TURTLE);

        System.out.println("Store the model in MarkLogic.");
        dataset.addNamedModel(modelName, model);

        System.out.println("Make a triple by hand.");
        Model moreTriples = ModelFactory.createDefaultModel();
        Statement statement = ResourceFactory.createStatement(
            ResourceFactory.createResource("http://example.org/charles"),
            ResourceFactory.createProperty("http://example.org/hasDog"),
            ResourceFactory.createResource("http://example.org/vashko")
        );
        moreTriples.add( statement );

        System.out.println("Combine models and save");
        model.add(moreTriples);
        dataset.addNamedModel(modelName, model);

        System.out.println("Get it back into a new model (union of two original ones)");
        Model retrievedModel = dataset.getNamedModel(modelName);
        
        System.out.println("Remove model from MarkLogic");
        dataset.removeNamedModel(modelName);
    }
    
    public static void main(String... args) {
        ModelCRUDExample example = new ModelCRUDExample();
        example.run();
     }

}
