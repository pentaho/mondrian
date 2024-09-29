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
import mondrian.calc.impl.AbstractBooleanCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.rolap.RolapMember;
import mondrian.rolap.RolapUtil;

/**
 * Definition of the <code>IS NULL</code> MDX function.
 *
 * @author medstat
 * @since Aug 21, 2006
 */
class IsNullFunDef extends FunDefBase {
    /**
     * Resolves calls to the <code>IS NULL</code> postfix operator.
     */
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "IS NULL",
            "<Expression> IS NULL",
            "Returns whether an object is null",
            new String[]{"Qbm", "Qbl", "Qbh", "Qbd"},
            IsNullFunDef.class);

    public IsNullFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        assert call.getArgCount() == 1;
        final MemberCalc memberCalc = compiler.compileMember(call.getArg(0));
        return new AbstractBooleanCalc(call, new Calc[]{memberCalc}) {
            public boolean evaluateBoolean(Evaluator evaluator) {
                Member member = memberCalc.evaluateMember(evaluator);
                return member.isNull()
                   || nonAllWithNullKey((RolapMember) member);
      }
    };
  }

  /**
   * Dimension members with a null value are treated as the null member.
   */
  private boolean nonAllWithNullKey(RolapMember member) {
    return !member.isAll() && member.getKey() == RolapUtil.sqlNullValue;
  }
}

// End IsNullFunDef.java
