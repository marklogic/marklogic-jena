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
