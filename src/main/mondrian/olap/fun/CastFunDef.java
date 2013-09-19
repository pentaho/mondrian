/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.GenericCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.TypeUtil;
import mondrian.resource.MondrianResource;

import java.util.List;

/**
 * Definition of the <code>CAST</code> MDX operator.
 *
 * <p><code>CAST</code> is a mondrian-specific extension to MDX, because the MDX
 * standard does not define how values are to be converted from one
 * type to another. Microsoft Analysis Services, for Resolver, uses the Visual
 * Basic functions <code>CStr</code>, <code>CInt</code>, etc.
 * The syntax for this operator was inspired by the <code>CAST</code> operator
 * in the SQL standard.
 *
 * <p>Examples:<ul>
 * <li><code>CAST(1 + 2 AS STRING)</code></li>
 * <li><code>CAST('12.' || '56' AS NUMERIC)</code></li>
 * <li><code>CAST('tr' || 'ue' AS BOOLEAN)</code></li>
 * </ul>
 *
 * @author jhyde
 * @since Sep 3, 2006
 */
public class CastFunDef extends FunDefBase {
    static final ResolverBase Resolver = new ResolverImpl();

    private CastFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Type targetType = call.getType();
        final Exp arg = call.getArg(0);
        final Calc calc = compiler.compileScalar(arg, false);
        return new CalcImpl(arg, calc, targetType);
    }

    private static RuntimeException cannotConvert(
        Object o,
        final Type targetType)
    {
        return Util.newInternal(
            "cannot convert value '" + o
            + "' to targetType '" + targetType
            + "'");
    }

    public static int toInt(
        Object o,
        final Type targetType)
    {
        if (o == null) {
            return FunUtil.IntegerNull;
        }
        if (o instanceof String) {
            return Integer.parseInt((String) o);
        }
        if (o instanceof Number) {
            return ((Number) o).intValue();
        }
        throw cannotConvert(o, targetType);
    }

    private static double toDouble(Object o, final Type targetType) {
        if (o == null) {
            return FunUtil.DoubleNull;
        }
        if (o instanceof String) {
            return Double.valueOf((String) o);
        }
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        }
        throw cannotConvert(o, targetType);
    }

    public static boolean toBoolean(Object o, final Type targetType) {
        if (o == null) {
            return FunUtil.BooleanNull;
        }
        if (o instanceof Boolean) {
            return (Boolean) o;
        }
        if (o instanceof String) {
            return Boolean.valueOf((String) o);
        }
        if (o instanceof Number) {
            return ((Number) o).doubleValue() > 0;
        }
        throw cannotConvert(o, targetType);
    }

    /**
     * Resolves calls to the CAST operator.
     */
    private static class ResolverImpl extends ResolverBase {

        public ResolverImpl() {
            super(
                "Cast", "Cast(<Expression> AS <Type>)",
                "Converts values to another type.", Syntax.Cast);
        }

        public FunDef resolve(
            Exp[] args, Validator validator, List<Conversion> conversions)
        {
            if (args.length != 2) {
                return null;
            }
            if (!(args[1] instanceof Literal)) {
                return null;
            }
            Literal literal = (Literal) args[1];
            String typeName = (String) literal.getValue();
            int returnCategory;
            if (typeName.equalsIgnoreCase("String")) {
                returnCategory = Category.String;
            } else if (typeName.equalsIgnoreCase("Numeric")) {
                returnCategory = Category.Numeric;
            } else if (typeName.equalsIgnoreCase("Boolean")) {
                returnCategory = Category.Logical;
            } else if (typeName.equalsIgnoreCase("Integer")) {
                returnCategory = Category.Integer;
            } else {
                throw MondrianResource.instance().CastInvalidType.ex(typeName);
            }
            final FunDef dummyFunDef =
                createDummyFunDef(this, returnCategory, args);
            return new CastFunDef(dummyFunDef);
        }
    }

    private static class CalcImpl extends GenericCalc {
        private final Calc calc;
        private final Type targetType;
        private final int targetCategory;

        public CalcImpl(Exp arg, Calc calc, Type targetType) {
            super(arg);
            this.calc = calc;
            this.targetType = targetType;
            this.targetCategory = TypeUtil.typeToCategory(targetType);
        }

        public Calc[] getCalcs() {
            return new Calc[] {calc};
        }

        public Object evaluate(Evaluator evaluator) {
            switch (targetCategory) {
            case Category.String:
                return evaluateString(evaluator);
            case Category.Integer:
                return FunUtil.box(evaluateInteger(evaluator));
            case Category.Numeric:
                return FunUtil.box(evaluateDouble(evaluator));
            case Category.DateTime:
                return evaluateDateTime(evaluator);
            case Category.Logical:
                return evaluateBoolean(evaluator);
            default:
                throw Util.newInternal("category " + targetCategory);
            }
        }

        public String evaluateString(Evaluator evaluator) {
            final Object o = calc.evaluate(evaluator);
            if (o == null) {
                return null;
            }
            return String.valueOf(o);
        }

        public int evaluateInteger(Evaluator evaluator) {
            final Object o = calc.evaluate(evaluator);
            return toInt(o, targetType);
        }

        public double evaluateDouble(Evaluator evaluator) {
            final Object o = calc.evaluate(evaluator);
            return toDouble(o, targetType);
        }

        public boolean evaluateBoolean(Evaluator evaluator) {
            final Object o = calc.evaluate(evaluator);
            return toBoolean(o, targetType);
        }
    }
}

// End CastFunDef.java
