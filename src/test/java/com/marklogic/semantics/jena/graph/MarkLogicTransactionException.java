package com.marklogic.semantics.jena.graph;

import com.marklogic.semantics.jena.MarkLogicJenaException;

@SuppressWarnings("serial")
public class MarkLogicTransactionException extends MarkLogicJenaException {

	public MarkLogicTransactionException(String message) {
		super(message);
	}

}
