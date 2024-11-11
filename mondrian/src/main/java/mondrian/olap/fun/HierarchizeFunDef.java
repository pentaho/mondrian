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

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.FunDef;
import mondrian.olap.fun.sort.Sorter;

/**
 * Definition of the <code>Hierarchize</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class HierarchizeFunDef extends FunDefBase {
  static final String[] prePost = { "PRE", "POST" };
  static final ReflectiveMultiResolver Resolver =
    new ReflectiveMultiResolver(
      "Hierarchize",
      "Hierarchize(<Set>[, POST])",
      "Orders the members of a set in a hierarchy.",
      new String[] { "fxx", "fxxy" },
      HierarchizeFunDef.class,
      prePost );

  public HierarchizeFunDef( FunDef dummyFunDef ) {
    super( dummyFunDef );
  }

  public Calc compileCall( ResolvedFunCall call, ExpCompiler compiler ) {
    final ListCalc listCalc =
      compiler.compileList( call.getArg( 0 ), true );
    String order = getLiteralArg( call, 1, "PRE", prePost );
    final boolean post = order.equals( "POST" );
    return new AbstractListCalc( call, new Calc[] { listCalc } ) {
      public TupleList evaluateList( Evaluator evaluator ) {
        TupleList list = listCalc.evaluateList( evaluator );
        return Sorter.hierarchizeTupleList( list, post );
      }
    };
  }
}

// End HierarchizeFunDef.java
