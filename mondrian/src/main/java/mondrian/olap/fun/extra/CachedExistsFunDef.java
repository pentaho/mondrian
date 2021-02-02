/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2021 Hitachi Vantara.  All rights reserved.
*/

package mondrian.olap.fun.extra;

import java.util.ArrayList;
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
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.fun.FunDefBase;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.Type;

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

  private static final String TIMING_NAME = CachedExistsFunDef.class.getSimpleName();

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
        evaluator.getTiming().markStart( TIMING_NAME );
        try {

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

          // Build a mapping from subtotal tuple types to the input set's tuple types
          List<Hierarchy> listHiers = getHierarchies( listCalc1.getType() );
          List<Hierarchy> subtotalHiers = getHierarchies( tupleCalc1.getType() );
          int[] subtotalToListIndex = new int[subtotalHiers.size()];
          for ( int i = 0; i < subtotalToListIndex.length; i++ ) {
            Hierarchy subtotalHier = subtotalHiers.get( i );
            boolean found = false;
            for ( int j = 0; j < listHiers.size(); j++ ) {
              if ( listHiers.get( j ) == subtotalHier ) {
                subtotalToListIndex[i] = j;
                found = true;
                break;
              }
            }
            if ( !found ) {
              throw new IllegalArgumentException( "Hierarchy in <Tuple> not present in <Set>" );
            }
          }

          // Build subtotal cache
          HashMap<String, TupleList> setCache = new HashMap<String, TupleList>();
          TupleList setToCache = listCalc1.evaluateList( evaluator );
          for ( List<Member> tuple : setToCache ) {
            String subtotalKey = makeSubtotalKey( subtotalToListIndex, tuple, subtotal );
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
        } finally {
          evaluator.getTiming().markEnd( TIMING_NAME );
        }
      }
    };
  }

  /**
   * Returns a list of hierarchies used by the input type.
   * 
   * If an input type is a dimension instead of a hierarchy, then return the dimension's default hierarchy. See
   * MONDRIAN-2704
   * 
   * @param t
   * @return
   */
  private List<Hierarchy> getHierarchies( Type t ) {
    List<Hierarchy> hiers = new ArrayList<Hierarchy>();
    if ( t instanceof MemberType ) {
      hiers.add( getHierarchy( t ) );
    } else if ( t instanceof TupleType ) {
      TupleType tupleType = (TupleType) t;
      for ( Type elementType : tupleType.elementTypes ) {
        hiers.add( getHierarchy( elementType ) );
      }
    } else if ( t instanceof SetType ) {
      SetType setType = (SetType) t;
      if ( setType.getElementType() instanceof MemberType ) {
        hiers.add( getHierarchy( setType.getElementType() ) );
      } else if ( setType.getElementType() instanceof TupleType ) {
        TupleType tupleTypes = (TupleType) setType.getElementType();
        for ( Type elementType : tupleTypes.elementTypes ) {
          hiers.add( getHierarchy( elementType ) );
        }
      }
    }
    return hiers;
  }

  private Hierarchy getHierarchy( Type t ) {
    if ( t.getHierarchy() != null ) {
      return t.getHierarchy();
    }
    return t.getDimension().getHierarchy();
  }

  /**
   * Generates a subtotal key for the input tuple based on the type of the input subtotal tuple.
   * 
   * For example, if the subtotal tuple contained:
   * 
   * ([Product].[Food], [Time].[1998])
   * 
   * then the type of this subtotal tuple is:
   * 
   * ([Product].[Family], [Time].[Year])
   * 
   * The subtotal key would need to contain the same types from the input tuple.
   * 
   * So if a sample input tuple contained:
   * 
   * ([Gender].[M], [Product].[Drink].[Dairy], [Time].[1997].[Q1])
   * 
   * The subtotal key for this tuple would be:
   * 
   * ([Product].[Drink], [Time].[1997])
   * 
   * 
   * @param subtotalToListIndex
   * @param tuple
   * @param subtotal
   * @return
   */
  private String makeSubtotalKey( int[] subtotalToListIndex, List<Member> tuple, Member[] subtotal ) {
    StringBuilder builder = new StringBuilder();
    for ( int i = 0; i < subtotal.length; i++ ) {
      Member subtotalMember = subtotal[i];
      Member tupleMember = tuple.get( subtotalToListIndex[i] );
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
