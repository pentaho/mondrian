/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.FunDef;
import mondrian.olap.Exp;
import mondrian.olap.Literal;
import mondrian.olap.Evaluator;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractStringCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.util.Format;

import java.util.Locale;

/**
 * Definition of the <code>Format</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class FormatFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
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
