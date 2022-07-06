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
package com.marklogic.semantics.jena.engine;

import java.io.ByteArrayInputStream;
import java.util.Iterator;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RiotException;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIter1;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.iterator.QueryIteratorCheck;
import org.apache.jena.sparql.engine.iterator.QueryIteratorResultSet;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.syntax.Template;
import org.apache.jena.sparql.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicJenaException;
import com.marklogic.semantics.jena.client.JenaDatabaseClient;

/**
 * Implements a hook into the Jena query execution framework
 * for working with MarkLogic.
 * Applications need not use this class.
 * it accessed via the MarkLogicDatasetGraph.
 */
public class MarkLogicQueryEngine extends QueryEngineMain {

	@SuppressWarnings("unused")
	private final static Logger log = LoggerFactory.getLogger(MarkLogicQueryEngine.class);
	private BasicPattern bgp = null;
	private Template template = null;
	private MarkLogicDatasetGraph markLogicDatasetGraph;
	private Binding initial;

	/**
	 * gets the factory for making MarkLogicQueryEngine.
	 * @return a QueryEngineFactory that makes MarkLogicQueryEngine instances.
	 */
	static public QueryEngineFactory getFactory() { return factory ; }

	/**
	 * Registers the factory with Jena's QueryEngineRegistry.
	 */
    public static void register() {
        QueryEngineRegistry.addFactory(factory) ;
    }

    /**
     * Removes the factory from Jena's QueryEngineRegistry.
     */
    static public void unregister()     {
        QueryEngineRegistry.removeFactory(factory);
    }

    /**
     * Constructor.
     * @param query A Jena Query.  This engine does not use all parts of the Query hierarchy.
     * @param datasetGraph The MarkLogic instance viewed through Jena. Must be a MarkLogicDatasetGraph.
     * @param initial Bindings for the query.
     * @param context
     */
    private MarkLogicQueryEngine(Query query, DatasetGraph datasetGraph,
            Binding initial, Context context) {
        super(query, datasetGraph, initial, context);
        bgp = new BasicPattern();
        bgp.add(new Triple(
                Var.alloc("s"),
                Var.alloc("p"),
                Var.alloc("o")));
        template = new Template(bgp);
        this.markLogicDatasetGraph = (MarkLogicDatasetGraph) datasetGraph;
        this.initial = initial;
    }

    /**
     * Constructor.
     * @param query A Jena Query.  This engine does not use all parts of the Query hierarchy.
     * @param dataset The MarkLogic instance viewed through Jena. Must be a MarkLogicDatasetGraph.
     */
    private MarkLogicQueryEngine(Query query, DatasetGraph dataset) {
        this(query, dataset, null, null);
    }

    private SPARQLQueryDefinition prepareQueryDefinition(Query query) {
        JenaDatabaseClient client = markLogicDatasetGraph.getDatabaseClient();
        SPARQLQueryDefinition qdef = client.newQueryDefinition(query.toString());
        if (query.getBaseURI() != null) {
            qdef.setBaseUri(query.getBaseURI());
        } else {
            query.setBaseURI((String) null);
        }
        if (markLogicDatasetGraph.getRulesets() != null) {
            qdef.setRulesets(markLogicDatasetGraph.getRulesets());
        }
        bindVariables(qdef, this.initial, markLogicDatasetGraph);
        QueryDefinition constrainingQueryDefinition = markLogicDatasetGraph.getConstrainingQueryDefinition();

        qdef.setConstrainingQueryDefinition(constrainingQueryDefinition);
        return qdef;
    }

    static void bindVariables(SPARQLQueryDefinition qdef, Binding initial, MarkLogicDatasetGraph markLogicDatasetGraph) {
        if (initial == null) {
        }
        else {
            Iterator<Var> varsIterator = initial.vars();
            while (initial != null && varsIterator.hasNext()) {
                Var v = varsIterator.next();
                Node bindingValue = initial.get(v);
                MarkLogicDatasetGraph.bindObject(qdef, v.getName(), bindingValue);
            }
        }
    }

    @Override
    /**
     * Evaluation hook for all queries.
     */
    public QueryIterator eval(Op op, DatasetGraph dsg, Binding initial, Context context)
    {
      // see if this can be null
      ExecutionContext execCxt = new ExecutionContext(context, null, dsg, null);
      MarkLogicDatasetGraph markLogicDatasetGraph = (MarkLogicDatasetGraph) originalDataset;
      JenaDatabaseClient client = markLogicDatasetGraph.getDatabaseClient();
      markLogicDatasetGraph.sync();
      markLogicDatasetGraph.syncAdds();
      QueryIterator qIter = null;

      Query query = (Query)context.get(ARQConstants.sysCurrentQuery);

      Long limit = null;
      Long offset = null;
      if (query.hasLimit()) {
          limit = query.getLimit();
          query.setLimit(Query.NOLIMIT);
      }
      if (query.hasOffset()) {
          // offset is off-by-one from 'start'
          offset = query.getOffset() + 1;
          query.setOffset(Query.NOLIMIT);
      }

      SPARQLQueryDefinition qdef = prepareQueryDefinition(query);

      InputStreamHandle handle = new InputStreamHandle();

      if (query.isAskType()) {
        boolean answer = client.executeAsk(qdef);
        QueryIterator qIter1 = QueryIterRoot.create(initial, execCxt) ;
        qIter = new BooleanQueryIterator(qIter1, execCxt, answer);
      } else if (query.isConstructType() || query.isDescribeType()) {
        // what I need to create here is a QueryIterator that contains
        // bindings of s, p, and o to every triple.
        if (query.isConstructType()) client.executeConstruct(qdef, handle);
        if (query.isDescribeType()) client.executeDescribe(qdef, handle);
        Iterator<Triple> triples = null;
        if(handle.get() != null) {
          triples = RDFDataMgr.createIteratorTriples(handle.get(), Lang.NTRIPLES, null);
        } else {
          log.info("Got null result from CONSTRUCT, constructing alternate iterator");
          triples = RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(".".getBytes()), Lang.NTRIPLES, null);
        }
        QueryIterator qIter1 = QueryIterRoot.create(initial, execCxt) ;
          qIter = new TripleQueryIterator(qIter1, execCxt, triples);
        query.setConstructTemplate(template);
        //throw new MarkLogicJenaException("Construct Type Supported by Engine Layer");
      } else if (query.isSelectType()) {
        client.executeSelect(qdef, handle, offset, limit);
        ResultSet results = ResultSetFactory.fromJSON(handle.get());
        qIter = new QueryIteratorResultSet(results);
      } else {
        handle.close();
        throw new MarkLogicJenaException("Unrecognized Query Type");
      }
      // Wrap with something to check for closed iterators.
      qIter = QueryIteratorCheck.check(qIter, execCxt) ;
      return qIter;
    }

    // ---- Factory
    protected static QueryEngineFactory factory = new MarkLogicQueryEngineFactory() ;


    /**
     * For internal use.
     */
    protected static class MarkLogicQueryEngineFactory implements QueryEngineFactory {

        @Override
        public boolean accept(Query query, DatasetGraph datasetGraph, Context context) {
            return (datasetGraph instanceof MarkLogicDatasetGraph);
        }

        @Override
        public Plan create(Query query, DatasetGraph dataset, Binding inputBinding,
                Context context) {
            MarkLogicQueryEngine engine = new MarkLogicQueryEngine(query, dataset, inputBinding, context) ;
            return engine.getPlan();
        }

        @Override
        public boolean accept(Op op, DatasetGraph dataset, Context context) {
            return false;
        }

        @Override
        public Plan create(Op op, DatasetGraph dataset, Binding inputBinding,
                Context context) {
            return null;
        }

    }


    /**
     * Wraps a boolean result into an iterator implementation (with one item)
     */
    class BooleanQueryIterator extends QueryIter1 implements QueryIterator {
    	private boolean answer;

		public BooleanQueryIterator(QueryIterator input, ExecutionContext ctx, Boolean answer) {
			super(input, ctx);
			this.answer = answer;
		}

		@Override
		protected void requestSubCancel() {
			//pass
		}

		@Override
		protected void closeSubIterator() {
			//pass
		}

		@Override
		protected boolean hasNextBinding() {
			return answer;
		}

		@Override
		protected Binding moveToNextBinding() {
			return BindingFactory.binding();
		}
    }

    /**
     * Wraps the results of a CONSTRUCT query into a new binding
     * which is understood by Jena's internals.
     */
    class TripleQueryIterator extends QueryIter1 {

    	private Iterator<Triple> triples;

		public TripleQueryIterator(QueryIterator input, ExecutionContext execCxt, Iterator<Triple> triples) {
			super(input, execCxt);
			this.triples = triples;
		}

		@Override
		protected boolean hasNextBinding() {
			return triples.hasNext();
		}

		@Override
		protected Binding moveToNextBinding() {
			// we need a binding that's ?s ?p ?o
			try {
				Triple triple = triples.next();
				BindingBuilder binding = BindingFactory.builder();
				binding.add(Var.alloc("s"), triple.getSubject());
				binding.add(Var.alloc("p"), triple.getPredicate());
				binding.add(Var.alloc("o"), triple.getObject());
				return binding.build();
			} catch (RiotException e) {
				// bug in empty results for describe.  this is
				// a workaround. TODO
			}
			return BindingFactory.binding();
		}

		@Override
		protected void requestSubCancel() {
			// not needed?
		}

		@Override
		protected void closeSubIterator() {
			// not needed?
		}

	}
}
