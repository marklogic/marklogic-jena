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
package com.marklogic.semantics.jena.graph;

import org.junit.Test;

import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateRequest;
import com.marklogic.semantics.jena.JenaTestBase;

public class MarkLogicUpdatesTest extends JenaTestBase {

	@Test
	public void testUpdateAction() {
		GraphStore gs = getMarkLogicDatasetGraph();
		UpdateRequest update = new UpdateRequest();
		update.add("INSERT DATA { <s2> <p1> <o1> }");
		update.add("DROP ALL")
	       .add("CREATE GRAPH <http://example/update1>")
	       .add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/update2> { <s1> <p1> <o1>  } }")
	       .add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example.org/update3> { <s1> <p1> <o1>  } }") ;


		UpdateAction.execute(update,  gs);
		//UpdateProcessor updateExec = UpdateExecutionFactory.create(update,  gs);
		//updateExec.execute();
	}
	
}
