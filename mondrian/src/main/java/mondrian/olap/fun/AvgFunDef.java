/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.calc.impl.ValueCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Definition of the <code>Avg</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class AvgFunDef extends AbstractAggregateFunDef {
  static final ReflectiveMultiResolver Resolver =
      new ReflectiveMultiResolver( "Avg", "Avg(<Set>[, <Numeric Expression>])",
          "Returns the average value of a numeric expression evaluated over a set.", new String[] { "fnx", "fnxn" },
          AvgFunDef.class );

  private static final String TIMING_NAME = AvgFunDef.class.getSimpleName();

  public AvgFunDef( FunDef dummyFunDef ) {
    super( dummyFunDef );
  }

  public Calc compileCall( ResolvedFunCall call, ExpCompiler compiler ) {
    final ListCalc listCalc = compiler.compileList( call.getArg( 0 ) );
    final Calc calc =
        call.getArgCount() > 1 ? compiler.compileScalar( call.getArg( 1 ), true ) : new ValueCalc( call );
    return new AbstractDoubleCalc( call, new Calc[] { listCalc, calc } ) {
      public double evaluateDouble( Evaluator evaluator ) {
        evaluator.getTiming().markStart( TIMING_NAME );
        final int savepoint = evaluator.savepoint();
        try {
          TupleList memberList = evaluateCurrentList( listCalc, evaluator );
          evaluator.setNonEmpty( false );
          return (Double) avg( evaluator, memberList, calc );
        } finally {
          evaluator.restore( savepoint );
          evaluator.getTiming().markEnd( TIMING_NAME );
        }
      }

      public boolean dependsOn( Hierarchy hierarchy ) {
        return anyDependsButFirst( getCalcs(), hierarchy );
      }
    };
  }
}

// End AvgFunDef.java
