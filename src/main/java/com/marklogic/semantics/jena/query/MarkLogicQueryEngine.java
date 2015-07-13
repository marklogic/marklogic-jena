package com.marklogic.semantics.jena.query;

import java.util.Iterator;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.RiotReader;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.ARQConstants;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.Plan;
import com.hp.hpl.jena.sparql.engine.QueryEngineFactory;
import com.hp.hpl.jena.sparql.engine.QueryEngineRegistry;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter1;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRoot;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIteratorCheck;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIteratorResultSet;
import com.hp.hpl.jena.sparql.engine.main.QueryEngineMain;
import com.hp.hpl.jena.sparql.resultset.JSONInput;
import com.hp.hpl.jena.sparql.syntax.Template;
import com.hp.hpl.jena.sparql.util.Context;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.semantics.jena.MarkLogicJenaException;
import com.marklogic.semantics.jena.graph.MarkLogicDatasetGraph;

public class MarkLogicQueryEngine extends QueryEngineMain {



	static public QueryEngineFactory getFactory() { return factory ; } 
    static public void register()       { QueryEngineRegistry.addFactory(factory) ; }
    static public void unregister()     { QueryEngineRegistry.removeFactory(factory) ; }
    static public SPARQLQueryManager sparqlManager;
    
    public MarkLogicQueryEngine(Query query, DatasetGraph dataset,
            Binding initial, Context context) {
        super(query, dataset, initial, context);
    }
    
    public MarkLogicQueryEngine(Query query, DatasetGraph dataset) {
        super(query, dataset, null, null);
    }
   
    @Override
    public QueryIterator eval(Op op, DatasetGraph dsg, Binding initial, Context context)
    {
    	// see if this can be null
    	ExecutionContext execCxt = new ExecutionContext(context, dsg.getDefaultGraph(), dsg, null);
        QueryIterator qIter1 = QueryIterRoot.create(initial, execCxt) ;
        QueryIterator qIter = null;
        
        Query query = (Query)context.get(ARQConstants.sysCurrentQuery);
        
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(query.toString());
        
        InputStreamHandle handle = new InputStreamHandle();
        
        if (query.isAskType()) {
			qIter = new BooleanQueryIterator(qIter1, execCxt, sparqlManager.executeAsk(qdef));
        } else if (query.isConstructType() || query.isDescribeType()) {
        	// what I need to create here is a QueryIterator that contains
        	// bindings of s, p, and o to every triple.
        	if (query.isConstructType()) sparqlManager.executeConstruct(qdef, handle);
        	if (query.isDescribeType()) sparqlManager.executeDescribe(qdef, handle);
        	Iterator<Triple> triples = RiotReader.createIteratorTriples(handle.get(), Lang.NTRIPLES, null);
        	qIter = new TripleQueryIterator(qIter1, execCxt, triples);
        	BasicPattern bgp = new BasicPattern();
        	bgp.add(new Triple(
        			Var.alloc("s"), 
        			Var.alloc("p"), 
        			Var.alloc("o")));
        	query.setConstructTemplate(new Template(bgp));
        	//throw new MarkLogicJenaException("Construct Type Supported by Engine Layer");
        } else if (query.isSelectType()) {
        	sparqlManager.executeSelect(qdef, handle);
            ResultSet results = JSONInput.fromJSON(handle.get());
            qIter = new QueryIteratorResultSet(results);
        } else {
        	throw new MarkLogicJenaException("Unrecognized Query Type");
        }
     // Wrap with something to check for closed iterators.
        qIter = QueryIteratorCheck.check(qIter, execCxt) ;
        return qIter;
    }
	// ---- Factory
    protected static QueryEngineFactory factory = new MarkLogicQueryEngineFactory() ;


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


    public static void register(DatabaseClient client) {
        register();
        sparqlManager = client.newSPARQLQueryManager();
    }
    
    public class BooleanQueryIterator extends QueryIter1 implements QueryIterator {
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
			return null;
		}
    }

    public class TripleQueryIterator extends QueryIter1 {

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
				BindingMap binding = BindingFactory.create();
				binding.add(Var.alloc("s"), triple.getSubject());
				binding.add(Var.alloc("p"), triple.getPredicate());
				binding.add(Var.alloc("o"), triple.getObject());
				return binding;
			} catch (RiotException e) {
				// bug in empty results for describe.  this is
				// a workaround. TODO
			}
			return BindingFactory.binding();
		}

		@Override
		protected void requestSubCancel() {
			// TODO Auto-generated method stub
			
		}

		@Override
		protected void closeSubIterator() {
			// TODO Auto-generated method stub
			
		}

	}
}
