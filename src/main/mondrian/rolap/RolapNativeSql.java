/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2013 Pentaho
// All Rights Reserved.
*/

package mondrian.rolap;

import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.StringType;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.Dialect;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates SQL from parse tree nodes. Currently it creates the SQL that
 * accesses a measure for the ORDER BY that is generated for a TopCount.<p/>
 *
 * @author av
 * @since Nov 17, 2005
  */
public class RolapNativeSql {

    private SqlQuery sqlQuery;
    private Dialect dialect;

    CompositeSqlCompiler numericCompiler;
    CompositeSqlCompiler booleanCompiler;

    RolapStoredMeasure storedMeasure;
    final AggStar aggStar;
    final Evaluator evaluator;
    final RolapLevel rolapLevel;

    /**
     * We remember one of the measures so we can generate
     * the constraints from RolapAggregationManager. Also
     * make sure all measures live in the same star.
     *
     * @return false if one or more saved measures are not
     * from the same star (or aggStar if defined), true otherwise.
     *
     * @see RolapAggregationManager#makeRequest(RolapEvaluator)
     */
    private boolean saveStoredMeasure(RolapStoredMeasure m) {
        if (aggStar != null && !storedMeasureIsPresentOnAggStar(m)) {
            return false;
        }
        if (storedMeasure != null) {
            RolapStar star1 = getStar(storedMeasure);
            RolapStar star2 = getStar(m);
            if (star1 != star2) {
                return false;
            }
        }
        this.storedMeasure = m;
        return true;
    }

    private boolean storedMeasureIsPresentOnAggStar(RolapStoredMeasure m) {
        RolapStar.Column column =
            (RolapStar.Column) m.getStarMeasure();
        int bitPos = column.getBitPosition();
        return  aggStar.lookupColumn(bitPos) != null;
    }

    private RolapStar getStar(RolapStoredMeasure m) {
        return ((RolapStar.Measure) m.getStarMeasure()).getStar();
    }

    /**
     * Translates an expression into SQL
     */
    interface SqlCompiler {
        /**
         * Returns SQL. If <code>exp</code> can not be compiled into SQL,
         * returns null.
         *
         * @param exp Expression
         * @return SQL, or null if cannot be converted into SQL
         */
        String compile(Exp exp);
    }

    /**
     * Implementation of {@link SqlCompiler} that uses chain of responsibility
     * to find a matching sql compiler.
     */
    static class CompositeSqlCompiler implements SqlCompiler {
        List<SqlCompiler> compilers = new ArrayList<SqlCompiler>();

        public void add(SqlCompiler compiler) {
            compilers.add(compiler);
        }

        public String compile(Exp exp) {
            for (SqlCompiler compiler : compilers) {
                String s = compiler.compile(exp);
                if (s != null) {
                    return s;
                }
            }
            return null;
        }

        public String toString() {
            return compilers.toString();
        }
    }

    /**
     * Compiles a numeric literal to SQL.
     */
    class NumberSqlCompiler implements SqlCompiler {
        public String compile(Exp exp) {
            if (!(exp instanceof Literal)) {
                return null;
            }
            if ((exp.getCategory() & Category.Numeric) == 0) {
                return null;
            }
            Literal literal = (Literal) exp;
            String expr = String.valueOf(literal.getValue());
            if (dialect.getDatabaseProduct().getFamily()
                == Dialect.DatabaseProduct.DB2)
            {
                expr = "FLOAT(" + expr + ")";
            }
            return expr;
        }

        public String toString() {
            return "NumberSqlCompiler";
        }
    }

    /**
     * Base class to remove MemberScalarExp.
     */
    abstract class MemberSqlCompiler implements SqlCompiler {
        protected Exp unwind(Exp exp) {
            return exp;
        }
    }

    /**
     * Compiles a measure into SQL, the measure will be aggregated
     * like <code>sum(measure)</code>.
     */
    class StoredMeasureSqlCompiler extends MemberSqlCompiler {

        public String compile(Exp exp) {
            exp = unwind(exp);
            if (!(exp instanceof MemberExpr)) {
                return null;
            }
            final Member member = ((MemberExpr) exp).getMember();
            if (!(member instanceof RolapStoredMeasure)) {
                return null;
            }
            RolapStoredMeasure measure = (RolapStoredMeasure) member;
            if (measure.isCalculated()) {
                return null; // ??
            }
            if (!saveStoredMeasure(measure)) {
                return null;
            }

            String exprInner;
            // Use aggregate table to create condition if available
            if (aggStar != null
                && measure.getStarMeasure() instanceof RolapStar.Column)
            {
                RolapStar.Column column =
                    (RolapStar.Column) measure.getStarMeasure();
                int bitPos = column.getBitPosition();
                AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
                exprInner = aggColumn.generateExprString(sqlQuery);
            } else {
                exprInner =
                    measure.getMondrianDefExpression().getExpression(sqlQuery);
            }

            String expr = measure.getAggregator().getExpression(exprInner);
            if (dialect.getDatabaseProduct().getFamily()
                == Dialect.DatabaseProduct.DB2)
            {
                expr = "FLOAT(" + expr + ")";
            }
            return expr;
        }

        public String toString() {
            return "StoredMeasureSqlCompiler";
        }
    }

    /**
     * Compiles a MATCHES MDX operator into SQL regular
     * expression match.
     */
    class MatchingSqlCompiler extends FunCallSqlCompilerBase {

        protected MatchingSqlCompiler()
        {
            super(Category.Logical, "MATCHES", 2);
        }

        public String compile(Exp exp) {
            if (!match(exp)) {
                return null;
            }
            if (!dialect.allowsRegularExpressionInWhereClause()
                || !(exp instanceof ResolvedFunCall)
                || evaluator == null)
            {
                return null;
            }

            final Exp arg0 = ((ResolvedFunCall)exp).getArg(0);
            final Exp arg1 = ((ResolvedFunCall)exp).getArg(1);

            // Must finish by ".Caption" or ".Name"
            if (!(arg0 instanceof ResolvedFunCall)
                || ((ResolvedFunCall)arg0).getArgCount() != 1
                || !(arg0.getType() instanceof StringType)
                || (!((ResolvedFunCall)arg0).getFunName().equals("Name")
                    && !((ResolvedFunCall)arg0)
                            .getFunName().equals("Caption")))
            {
                return null;
            }

            final boolean useCaption;
            if (((ResolvedFunCall)arg0).getFunName().equals("Name")) {
                useCaption = false;
            } else {
                useCaption = true;
            }

            // Must be ".CurrentMember"
            final Exp currMemberExpr = ((ResolvedFunCall)arg0).getArg(0);
            if (!(currMemberExpr instanceof ResolvedFunCall)
                || ((ResolvedFunCall)currMemberExpr).getArgCount() != 1
                || !(currMemberExpr.getType() instanceof MemberType)
                || !((ResolvedFunCall)currMemberExpr)
                        .getFunName().equals("CurrentMember"))
            {
                return null;
            }

            // Must be a dimension, a hierarchy or a level.
            final RolapCubeDimension dimension;
            final Exp dimExpr = ((ResolvedFunCall)currMemberExpr).getArg(0);
            if (dimExpr instanceof DimensionExpr) {
                dimension =
                    (RolapCubeDimension) evaluator.getCachedResult(
                        new ExpCacheDescriptor(dimExpr, evaluator));
            } else if (dimExpr instanceof HierarchyExpr) {
                final RolapCubeHierarchy hierarchy =
                    (RolapCubeHierarchy) evaluator.getCachedResult(
                        new ExpCacheDescriptor(dimExpr, evaluator));
                dimension = (RolapCubeDimension) hierarchy.getDimension();
            } else if (dimExpr instanceof LevelExpr) {
                final RolapCubeLevel level =
                    (RolapCubeLevel) evaluator.getCachedResult(
                        new ExpCacheDescriptor(dimExpr, evaluator));
                dimension = (RolapCubeDimension) level.getDimension();
            } else {
                return null;
            }

            if (rolapLevel != null
                && dimension.equals(rolapLevel.getDimension()))
            {
                // We can't use the evaluator because the filter is filtering
                // a set which is uses same dimension as the predicate.
                // We must use, in order of priority,
                //  - caption requested: caption->name->key
                //  - name requested: name->key
                MondrianDef.Expression expression = useCaption
                ? rolapLevel.captionExp == null
                        ? rolapLevel.nameExp == null
                            ? rolapLevel.keyExp
                            : rolapLevel.nameExp
                        : rolapLevel.captionExp
                    : rolapLevel.nameExp == null
                        ? rolapLevel.keyExp
                        : rolapLevel.nameExp;
                 // If an aggregation table is used, it might be more efficient
                 // to use only the aggregate table and not the hierarchy table.
                 // Try to lookup the column bit key. If that fails, we will
                 // link the aggregate table to the hierarchy table. If no
                 // aggregate table is used, we can use the column expression
                 // directly.
                String sourceExp;
                if (aggStar != null
                    && rolapLevel instanceof RolapCubeLevel
                    && expression == rolapLevel.keyExp)
                {
                    int bitPos =
                        ((RolapCubeLevel)rolapLevel).getStarKeyColumn()
                            .getBitPosition();
                    mondrian.rolap.aggmatcher.AggStar.Table.Column col =
                        aggStar.lookupColumn(bitPos);
                    if (col != null) {
                        sourceExp = col.generateExprString(sqlQuery);
                    } else {
                        // Make sure the level table is part of the query.
                        rolapLevel.getHierarchy().addToFrom(
                            sqlQuery,
                            expression);
                        sourceExp = expression.getExpression(sqlQuery);
                    }
                } else if (aggStar != null) {
                    // Make sure the level table is part of the query.
                    rolapLevel.getHierarchy().addToFrom(sqlQuery, expression);
                    sourceExp = expression.getExpression(sqlQuery);
                } else {
                    sourceExp = expression.getExpression(sqlQuery);
                }

                // The dialect might require the use of the alias rather
                // then the column exp.
                if (dialect.requiresHavingAlias()) {
                    sourceExp = sqlQuery.getAlias(sourceExp);
                }
                return
                    dialect.generateRegularExpression(
                        sourceExp,
                        String.valueOf(
                            evaluator.getCachedResult(
                                new ExpCacheDescriptor(arg1, evaluator))));
            } else {
                return null;
            }
        }
        public String toString() {
            return "MatchingSqlCompiler";
        }
    }

    /**
     * Compiles the underlying expression of a calculated member.
     */
    class CalculatedMemberSqlCompiler extends MemberSqlCompiler {
        SqlCompiler compiler;

        CalculatedMemberSqlCompiler(SqlCompiler argumentCompiler) {
            this.compiler = argumentCompiler;
        }

        public String compile(Exp exp) {
            exp = unwind(exp);
            if (!(exp instanceof MemberExpr)) {
                return null;
            }
            final Member member = ((MemberExpr) exp).getMember();
            if (!(member instanceof RolapCalculatedMember)) {
                return null;
            }
            exp = member.getExpression();
            if (exp == null) {
                return null;
            }
            return compiler.compile(exp);
        }

        public String toString() {
            return "CalculatedMemberSqlCompiler";
        }
    }

    /**
     * Contains utility methods to compile FunCall expressions into SQL.
     */
    abstract class FunCallSqlCompilerBase implements SqlCompiler {
        int category;
        String mdx;
        int argCount;

        FunCallSqlCompilerBase(int category, String mdx, int argCount) {
            this.category = category;
            this.mdx = mdx;
            this.argCount = argCount;
        }

        /**
         * @return true if exp is a matching FunCall
         */
        protected boolean match(Exp exp) {
            if ((exp.getCategory() & category) == 0) {
                return false;
            }
            if (!(exp instanceof FunCall)) {
                return false;
            }
            FunCall fc = (FunCall) exp;
            if (!mdx.equalsIgnoreCase(fc.getFunName())) {
                return false;
            }
            Exp[] args = fc.getArgs();
            if (args.length != argCount) {
                return false;
            }
            return true;
        }

        /**
         * compiles the arguments of a FunCall
         *
         * @return array of expressions or null if either exp does not match or
         * any argument could not be compiled.
         */
        protected String[] compileArgs(Exp exp, SqlCompiler compiler) {
            if (!match(exp)) {
                return null;
            }
            Exp[] args = ((FunCall) exp).getArgs();
            String[] sqls = new String[args.length];
            for (int i = 0; i < args.length; i++) {
                sqls[i] = compiler.compile(args[i]);
                if (sqls[i] == null) {
                    return null;
                }
            }
            return sqls;
        }
    }

    /**
     * Compiles a funcall, e.g. foo(a, b, c).
     */
    class FunCallSqlCompiler extends FunCallSqlCompilerBase {
        SqlCompiler compiler;
        String sql;

        protected FunCallSqlCompiler(
            int category, String mdx, String sql,
            int argCount, SqlCompiler argumentCompiler)
        {
            super(category, mdx, argCount);
            this.sql = sql;
            this.compiler = argumentCompiler;
        }

        public String compile(Exp exp) {
            String[] args = compileArgs(exp, compiler);
            if (args == null) {
                return null;
            }
            StringBuilder buf = new StringBuilder();
            buf.append(sql);
            buf.append("(");
            for (int i = 0; i < args.length; i++) {
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append(args[i]);
            }
            buf.append(") ");
            return buf.toString();
        }

        public String toString() {
            return "FunCallSqlCompiler[" + mdx + "]";
        }
    }

    /**
     * Shortcut for an unary operator like NOT(a).
     */
    class UnaryOpSqlCompiler extends FunCallSqlCompiler {
        protected UnaryOpSqlCompiler(
            int category,
            String mdx,
            String sql,
            SqlCompiler argumentCompiler)
        {
            super(category, mdx, sql, 1, argumentCompiler);
        }
    }

    /**
     * Shortcut for ().
     */
    class ParenthesisSqlCompiler extends FunCallSqlCompiler {
        protected ParenthesisSqlCompiler(
            int category,
            SqlCompiler argumentCompiler)
        {
            super(category, "()", "", 1, argumentCompiler);
        }

        public String toString() {
            return "ParenthesisSqlCompiler";
        }
    }

    /**
     * Compiles an infix operator like addition into SQL like <code>(a
     * + b)</code>.
     */
    class InfixOpSqlCompiler extends FunCallSqlCompilerBase {
        private final String sql;
        private final SqlCompiler compiler;

        protected InfixOpSqlCompiler(
            int category,
            String mdx,
            String sql,
            SqlCompiler argumentCompiler)
        {
            super(category, mdx, 2);
            this.sql = sql;
            this.compiler = argumentCompiler;
        }

        public String compile(Exp exp) {
            String[] args = compileArgs(exp, compiler);
            if (args == null) {
                return null;
            }
            return "(" + args[0] + " " + sql + " " + args[1] + ")";
        }

        public String toString() {
            return "InfixSqlCompiler[" + mdx + "]";
        }
    }

    /**
     * Compiles an <code>IsEmpty(measure)</code>
     * expression into SQL <code>measure is null</code>.
     */
    class IsEmptySqlCompiler extends FunCallSqlCompilerBase {
        private final SqlCompiler compiler;

        protected IsEmptySqlCompiler(
            int category, String mdx,
            SqlCompiler argumentCompiler)
        {
            super(category, mdx, 1);
            this.compiler = argumentCompiler;
        }

        public String compile(Exp exp) {
            String[] args = compileArgs(exp, compiler);
            if (args == null) {
                return null;
            }
            return "(" + args[0] + " is null" + ")";
        }

        public String toString() {
            return "IsEmptySqlCompiler[" + mdx + "]";
        }
    }

    /**
     * Compiles an <code>IIF(cond, val1, val2)</code> expression into SQL
     * <code>CASE WHEN cond THEN val1 ELSE val2 END</code>.
     */
    class IifSqlCompiler extends FunCallSqlCompilerBase {

        SqlCompiler valueCompiler;

        IifSqlCompiler(int category, SqlCompiler valueCompiler) {
            super(category, "iif", 3);
            this.valueCompiler = valueCompiler;
        }

        public String compile(Exp exp) {
            if (!match(exp)) {
                return null;
            }
            Exp[] args = ((FunCall) exp).getArgs();
            String cond = booleanCompiler.compile(args[0]);
            String val1 = valueCompiler.compile(args[1]);
            String val2 = valueCompiler.compile(args[2]);
            if (cond == null || val1 == null || val2 == null) {
                return null;
            }
            return sqlQuery.getDialect().caseWhenElse(cond, val1, val2);
        }
    }

    /**
     * Creates a RolapNativeSql.
     *
     * @param sqlQuery the query which is needed for different SQL dialects -
     * it is not modified
     */
    public RolapNativeSql(
        SqlQuery sqlQuery,
        AggStar aggStar,
        Evaluator evaluator,
        RolapLevel rolapLevel)
    {
        this.sqlQuery = sqlQuery;
        this.rolapLevel = rolapLevel;
        this.evaluator = evaluator;
        this.dialect = sqlQuery.getDialect();
        this.aggStar = aggStar;

        numericCompiler = new CompositeSqlCompiler();
        booleanCompiler = new CompositeSqlCompiler();

        numericCompiler.add(new NumberSqlCompiler());
        numericCompiler.add(new StoredMeasureSqlCompiler());
        numericCompiler.add(new CalculatedMemberSqlCompiler(numericCompiler));
        numericCompiler.add(
            new ParenthesisSqlCompiler(Category.Numeric, numericCompiler));
        numericCompiler.add(
            new InfixOpSqlCompiler(
                Category.Numeric, "+", "+", numericCompiler));
        numericCompiler.add(
            new InfixOpSqlCompiler(
                Category.Numeric, "-", "-", numericCompiler));
        numericCompiler.add(
            new InfixOpSqlCompiler(
                Category.Numeric, "/", "/", numericCompiler));
        numericCompiler.add(
            new InfixOpSqlCompiler(
                Category.Numeric, "*", "*", numericCompiler));
        numericCompiler.add(
            new IifSqlCompiler(Category.Numeric, numericCompiler));

        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, "<", "<", numericCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, "<=", "<=", numericCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, ">", ">", numericCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, ">=", ">=", numericCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, "=", "=", numericCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, "<>", "<>", numericCompiler));
        booleanCompiler.add(
            new IsEmptySqlCompiler(
                Category.Logical, "IsEmpty", numericCompiler));

        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, "and", "AND", booleanCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, "or", "OR", booleanCompiler));
        booleanCompiler.add(
            new UnaryOpSqlCompiler(
                Category.Logical, "not", "NOT", booleanCompiler));
        booleanCompiler.add(
            new MatchingSqlCompiler());
        booleanCompiler.add(
            new ParenthesisSqlCompiler(Category.Logical, booleanCompiler));
        booleanCompiler.add(
            new IifSqlCompiler(Category.Logical, booleanCompiler));
    }

    /**
     * Generates an aggregate of a measure, e.g. "sum(Store_Sales)" for
     * TopCount. The returned expr will be added to the select list and to the
     * order by clause.
     */
    public String generateTopCountOrderBy(Exp exp) {
        return numericCompiler.compile(exp);
    }

    public String generateFilterCondition(Exp exp) {
        return booleanCompiler.compile(exp);
    }

    public RolapStoredMeasure getStoredMeasure() {
        return storedMeasure;
    }

}

// End RolapNativeSql.java
