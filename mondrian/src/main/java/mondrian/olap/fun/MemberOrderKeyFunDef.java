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

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.MemberCalc;
import mondrian.calc.impl.AbstractCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.fun.sort.OrderKey;

/**
 * Definition of the <code>&lt;Member&gt;.OrderKey</code> MDX builtin function.
 *
 * <p>Syntax:
 * <blockquote><code>&lt;Member&gt;.OrderKey</code></blockquote>
 *
 * @author kvu
 * @since Nov 10, 2008
 */
public final class MemberOrderKeyFunDef extends FunDefBase {
  static final MemberOrderKeyFunDef instance =
    new MemberOrderKeyFunDef();

  /**
   * Creates the singleton MemberOrderKeyFunDef.
   */
  private MemberOrderKeyFunDef() {
    super(
      "OrderKey", "Returns the member order key.", "pvm" );
  }

  public Calc compileCall( ResolvedFunCall call, ExpCompiler compiler ) {
    final MemberCalc memberCalc =
      compiler.compileMember( call.getArg( 0 ) );
    return new CalcImpl( call, memberCalc );
  }

  public static class CalcImpl extends AbstractCalc {
    private final MemberCalc memberCalc;

    /**
     * Creates a Calc
     *
     * @param exp        Source expression
     * @param memberCalc Compiled expression to calculate member
     */
    public CalcImpl( Exp exp, MemberCalc memberCalc ) {
      super( exp, new Calc[] { memberCalc } );
      this.memberCalc = memberCalc;
    }

    public OrderKey evaluate( Evaluator evaluator ) {
      return new OrderKey( memberCalc.evaluateMember( evaluator ) );
    }

    protected String getName() {
      return "OrderKey";
    }
  }
}

// End MemberOrderKeyFunDef.java
