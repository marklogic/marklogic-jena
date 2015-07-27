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
package com.marklogic.semantics.jena.client;

import java.util.Iterator;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;

/**
 * Adapting iterator from MarkLogic's Iterator<String>
 * to Jena's Iterator<Node>
 */
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
	@Override
	public void remove() {
		iterator.remove();
	}

}
