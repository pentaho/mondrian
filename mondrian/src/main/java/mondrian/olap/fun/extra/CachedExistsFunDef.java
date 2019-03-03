/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2019 Hitachi Vantara..  All rights reserved.
*/

package mondrian.olap.fun.extra;

import java.util.HashMap;
import java.util.List;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.StringCalc;
import mondrian.calc.TupleCalc;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.fun.FunDefBase;

/**
 * CachedExistsFunDef is a replacement for the Exists MDX function that Analyzer uses for projecting tuples for
 * subtotals, Top10 and other calculations.
 * 
 * The MDX Analyzer generates uses Exists on the same named set many times within the same query. This can be very
 * computationally expensive because every call to Exists will scan the input set. After generating a cache, the tuples
 * that match the Exists criteria can be looked up without any scans.
 * 
 * @author Benny Chow
 *
 */
public class CachedExistsFunDef extends FunDefBase {
  public static final CachedExistsFunDef instance = new CachedExistsFunDef();

  CachedExistsFunDef() {
    super( "CachedExists",
        "Returns tuples from a non-dynamic <Set> that exists in the specified <Tuple>.  This function will build a query level cache named <String> based on the <Tuple> type.",
        "fxxtS" );
  }

  public Calc compileCall( ResolvedFunCall call, ExpCompiler compiler ) {
    final ListCalc listCalc1 = compiler.compileList( call.getArg( 0 ) );
    final TupleCalc tupleCalc1 = compiler.compileTuple( call.getArg( 1 ) );
    final StringCalc stringCalc = compiler.compileString( call.getArg( 2 ) );

    return new AbstractListCalc( call, new Calc[] { listCalc1, tupleCalc1, stringCalc } ) {
      public TupleList evaluateList( Evaluator evaluator ) {

        Member[] subtotal = tupleCalc1.evaluateTuple( evaluator );
        String namedSetName = stringCalc.evaluateString( evaluator );

        Object cacheObj = evaluator.getQuery().getEvalCache( makeSetCacheKey( namedSetName, subtotal ) );
        if ( cacheObj != null ) {
          HashMap<String, TupleList> setCache = (HashMap<String, TupleList>) cacheObj;
          TupleList tuples = setCache.get( makeSubtotalKey( subtotal ) );
          if ( tuples == null ) {
            tuples = TupleCollections.emptyList( listCalc1.getType().getArity() );
          }
          return tuples;
        }

        // Build subtotal cache
        HashMap<String, TupleList> setCache = new HashMap<String, TupleList>();
        TupleList setToCache = listCalc1.evaluateList( evaluator );
        for ( List<Member> tuple : setToCache ) {
          String subtotalKey = makeSubtotalKey( tuple, subtotal );
          TupleList tupleCache = setCache.get( subtotalKey );
          if ( tupleCache == null ) {
            tupleCache = TupleCollections.createList( listCalc1.getType().getArity() );
            setCache.put( subtotalKey, tupleCache );
          }
          tupleCache.add( tuple );
        }
        evaluator.getQuery().putEvalCache( makeSetCacheKey( namedSetName, subtotal ), setCache );

        TupleList tuples = setCache.get( makeSubtotalKey( subtotal ) );
        if ( tuples == null ) {
          tuples = TupleCollections.emptyList( listCalc1.getType().getArity() );
        }
        return tuples;
      }
    };
  }

  private String makeSubtotalKey( List<Member> tuple, Member[] subtotal ) {
    StringBuilder builder = new StringBuilder();
    for ( int i = 0; i < subtotal.length; i++ ) {
      Member subtotalMember = subtotal[i];
      Member tupleMember = tuple.get( i );
      int parentLevels = tupleMember.getDepth() - subtotalMember.getDepth();
      while ( parentLevels-- > 0 ) {
        tupleMember = tupleMember.getParentMember();
      }
      builder.append( tupleMember.getUniqueName() );
    }
    return builder.toString();
  }

  private String makeSetCacheKey( String setName, Member[] members ) {
    StringBuilder builder = new StringBuilder();
    builder.append( setName );
    for ( Member m : members ) {
      builder.append( m.getLevel().getUniqueName() );
    }
    return builder.toString();
  }

  private String makeSubtotalKey( Member[] members ) {
    StringBuilder builder = new StringBuilder();
    for ( Member m : members ) {
      builder.append( m.getUniqueName() );
    }
    return builder.toString();
  }

}
