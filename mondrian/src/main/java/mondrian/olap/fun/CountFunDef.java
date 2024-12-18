/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractIntegerCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>Count</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class CountFunDef extends AbstractAggregateFunDef {
  static final String[] ReservedWords = new String[] { "INCLUDEEMPTY", "EXCLUDEEMPTY" };

  static final ReflectiveMultiResolver Resolver =
      new ReflectiveMultiResolver( "Count", "Count(<Set>[, EXCLUDEEMPTY | INCLUDEEMPTY])",
          "Returns the number of tuples in a set, empty cells included unless the optional EXCLUDEEMPTY flag is used.",
          new String[] { "fnx", "fnxy" }, CountFunDef.class, ReservedWords );
  private static final String TIMING_NAME = CountFunDef.class.getSimpleName();

  public CountFunDef( FunDef dummyFunDef ) {
    super( dummyFunDef );
  }

  public Calc compileCall( ResolvedFunCall call, ExpCompiler compiler ) {
    final Calc calc = compiler.compileAs( call.getArg( 0 ), null, ResultStyle.ITERABLE_ANY );
    final boolean includeEmpty =
        call.getArgCount() < 2 || ( (Literal) call.getArg( 1 ) ).getValue().equals( "INCLUDEEMPTY" );
    return new AbstractIntegerCalc( call, new Calc[] { calc } ) {
      public int evaluateInteger( Evaluator evaluator ) {
        evaluator.getTiming().markStart( TIMING_NAME );
        final int savepoint = evaluator.savepoint();
        try {
          evaluator.setNonEmpty( false );
          final int count;
          if ( calc instanceof IterCalc ) {
            IterCalc iterCalc = (IterCalc) calc;
            TupleIterable iterable = evaluateCurrentIterable( iterCalc, evaluator );
            count = count( evaluator, iterable, includeEmpty );
          } else {
            // must be ListCalc
            ListCalc listCalc = (ListCalc) calc;
            TupleList list = evaluateCurrentList( listCalc, evaluator );
            count = count( evaluator, list, includeEmpty );
          }
          return count;
        } finally {
          evaluator.restore( savepoint );
          evaluator.getTiming().markEnd( TIMING_NAME );
        }
      }

      public boolean dependsOn( Hierarchy hierarchy ) {
        // COUNT(<set>, INCLUDEEMPTY) is straightforward -- it
        // depends only on the dimensions that <Set> depends
        // on.
        if ( super.dependsOn( hierarchy ) ) {
          return true;
        }
        if ( includeEmpty ) {
          return false;
        }
        // COUNT(<set>, EXCLUDEEMPTY) depends only on the
        // dimensions that <Set> depends on, plus all
        // dimensions not masked by the set.
        return !calc.getType().usesHierarchy( hierarchy, true );
      }
    };
  }
}

// End CountFunDef.java
