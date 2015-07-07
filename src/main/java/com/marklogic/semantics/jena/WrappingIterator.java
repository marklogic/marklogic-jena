package com.marklogic.semantics.jena;

import java.util.Iterator;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;

public class WrappingIterator implements Iterator<Node> {

	private Iterator<String> iterator;
	
	public WrappingIterator(Iterator<String> innerIterator) {
		this.iterator = innerIterator;
	}
	@Override
	public boolean hasNext() {
		return this.iterator.hasNext();
	}

	@Override
	public Node next() {
		return NodeFactory.createURI(iterator.next());
	}

}
