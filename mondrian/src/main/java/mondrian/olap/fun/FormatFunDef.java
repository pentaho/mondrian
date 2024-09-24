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
import mondrian.calc.impl.AbstractStringCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.util.Format;

import java.util.Locale;

/**
 * Definition of the <code>Format</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class FormatFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Format",
            "Format(<Expression>, <String Expression>)",
            "Formats a number or date to a string.",
            new String[] { "fSmS", "fSnS", "fSDS" },
            FormatFunDef.class);

    public FormatFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp[] args = call.getArgs();
        final Calc calc = compiler.compileScalar(call.getArg(0), true);
        final Locale locale = compiler.getEvaluator().getConnectionLocale();
        if (args[1] instanceof Literal) {
            // Constant string expression: optimize by
            // compiling format string.
            String formatString = (String) ((Literal) args[1]).getValue();
            final Format format = new Format(formatString, locale);
            return new AbstractStringCalc(call, new Calc[] {calc}) {
                public String evaluateString(Evaluator evaluator) {
                    final Object o = calc.evaluate(evaluator);
                    return format.format(o);
                }
            };
        } else {
            // Variable string expression
            final StringCalc stringCalc =
                    compiler.compileString(call.getArg(1));
            return new AbstractStringCalc(call, new Calc[] {calc, stringCalc}) {
                public String evaluateString(Evaluator evaluator) {
                    final Object o = calc.evaluate(evaluator);
                    final String formatString =
                            stringCalc.evaluateString(evaluator);
                    final Format format =
                            new Format(formatString, locale);
                    return format.format(o);
                }
            };
        }
    }
}

// End FormatFunDef.java
