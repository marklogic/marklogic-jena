package com.marklogic.semantics.jena.query;

import com.hp.hpl.jena.query.Query;
import com.marklogic.client.semantics.SPARQLRuleset;

/**
 * Extends the capabilities of a Jena Query object with
 * support for query-time inference configuration.
 */
public class MarkLogicQuery extends Query {

	public static MarkLogicQuery asMarkLogicQuery(Query q) {
		MarkLogicQuery mlQuery = new MarkLogicQuery();
		mlQuery.setBaseURI(q.getBaseURI());
		// TODO copy whole query object.
		return mlQuery;
	}
	
	public MarkLogicQuery() {
		super();
	}
	
	private SPARQLRuleset[] rulesets;
	
	public void setRulesets(SPARQLRuleset... rulesets) {
		this.rulesets = rulesets;
	}
	public SPARQLRuleset[] getRulesets() {
		return this.rulesets;
	}
}
