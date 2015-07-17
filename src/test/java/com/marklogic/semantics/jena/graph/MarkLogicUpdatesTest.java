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
	       .add("CREATE GRAPH <http://example/g2>")
	       .add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example/g3> { <s1> <p1> <o1>  } }")
	       .add("BASE <http://example.org/> INSERT DATA { GRAPH <http://example/g2> { <s1> <p1> <o1>  } }") ;


		UpdateAction.execute(update,  gs);
		//UpdateProcessor updateExec = UpdateExecutionFactory.create(update,  gs);
		//updateExec.execute();
	}
	
}
