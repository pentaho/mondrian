/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 February, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.Type;
import mondrian.util.Format;

import java.util.*;

/**
 * <code>BuiltinFunTable</code> contains a list of all built-in MDX functions.
 *
 * <p>Note: Boolean expressions return {@link Boolean#TRUE},
 * {@link Boolean#FALSE} or null. null is returned if the expression can not be
 * evaluated because some values have not been loaded from database yet.</p>
 *
 * @author jhyde
 * @since 26 February, 2002
 * @version $Id$
 **/
public class BuiltinFunTable extends FunTable {
    /**
     * Maps the upper-case name of a function plus its {@link Syntax} to an
     * array of {@link Validator} objects for that name.
     */
    private HashMap mapNameToResolvers;

    private static final Resolver[] emptyResolvers = new Resolver[0];
    private final Validator dummyResolver = Util.createSimpleResolver(this);

    private Exp valueFunCall;
    private final HashSet reservedWords = new HashSet();
    private static final Resolver[] emptyResolverArray = new Resolver[0];

    /**
     * Creates a <code>BuiltinFunTable</code>. This method should only be
     * called from {@link FunTable#instance}.
     **/
    public BuiltinFunTable() {
        init();
        valueFunCall = new FunCall("_Value", Syntax.Function, new Exp[0])
            .accept(dummyResolver);
        LinReg.valueFunCall = valueFunCall;
    }

    private static String makeResolverKey(String name, Syntax syntax) {
        return name.toUpperCase() + "$" + syntax;
    }

    /**
     * Calls {@link #defineFunctions} to load function definitions into a
     * List, then indexes that collection.
     **/
    private void init() {
        defineFunctions();
        Collections.sort(this.funInfoList);
        // Map upper-case function names to resolvers.
        mapNameToResolvers = new HashMap();
        for (int i = 0, n = resolvers.size(); i < n; i++) {
            Resolver resolver = (Resolver) resolvers.get(i);
            String key = makeResolverKey(resolver.getName(), resolver.getSyntax());
            List v2 = (List) mapNameToResolvers.get(key);
            if (v2 == null) {
                v2 = new ArrayList();
                mapNameToResolvers.put(key, v2);
            }
            v2.add(resolver);
        }
        // Convert the Lists into arrays.
        for (Iterator keys = mapNameToResolvers.keySet().iterator(); keys.hasNext();) {
            String key = (String) keys.next();
            List v2 = (List) mapNameToResolvers.get(key);
            mapNameToResolvers.put(key, v2.toArray(emptyResolverArray));
        }
    }

    protected void define(FunDef funDef) {
        define(new SimpleResolver(funDef));
    }

    protected void define(Resolver resolver) {
        addFunInfo(resolver);

        resolvers.add(resolver);
        final String[] reservedWords = resolver.getReservedWords();
        for (int i = 0; i < reservedWords.length; i++) {
            String reservedWord = reservedWords[i];
            defineReserved(reservedWord);
        }
    }

    protected void addFunInfo(Resolver resolver) {
        this.funInfoList.add(FunInfo.make(resolver));
    }

    /**
     * Converts an argument to a parameter type.
     */
    public Exp convert(Exp fromExp, int to, Validator resolver) {
        Exp exp = convert_(fromExp, to);
        if (exp == null) {
            throw Util.newInternal("cannot convert " + fromExp + " to " + to);
        }
        return resolver.validate(exp);
    }

    private static Exp convert_(Exp fromExp, int to) {
        int from = fromExp.getCategory();
        if (from == to) {
            return fromExp;
        }
        switch (from) {
        case Category.Array:
            return null;
        case Category.Dimension:
            // Seems funny that you can 'downcast' from a dimension, doesn't
            // it? But we add an implicit 'CurrentMember', for example,
            // '[Time].PrevMember' actually means
            // '[Time].CurrentMember.PrevMember'.
            switch (to) {
            case Category.Hierarchy:
                // "<Dimension>.CurrentMember.Hierarchy"
                return new FunCall(
                        "Hierarchy", Syntax.Property, new Exp[]{
                        new FunCall(
                                "CurrentMember",
                                Syntax.Property, new Exp[]{fromExp}
                        )}
                );
            case Category.Level:
                // "<Dimension>.CurrentMember.Level"
                return new FunCall(
                        "Level", Syntax.Property, new Exp[]{
                        new FunCall(
                                "CurrentMember",
                                Syntax.Property, new Exp[]{fromExp}
                        )}
                );
            case Category.Member:
            case Category.Tuple:
                // "<Dimension>.CurrentMember"
                return new FunCall(
                        "CurrentMember",
                        Syntax.Property,
                        new Exp[]{fromExp});
            default:
                return null;
            }
        case Category.Hierarchy:
            switch (to) {
            case Category.Dimension:
                // "<Hierarchy>.Dimension"
                return new FunCall(
                        "Dimension",
                        Syntax.Property,
                        new Exp[]{fromExp});
            default:
                return null;
            }
        case Category.Level:
            switch (to) {
            case Category.Dimension:
                // "<Level>.Dimension"
                return new FunCall(
                        "Dimension",
                        Syntax.Property,
                        new Exp[] {fromExp});
            case Category.Hierarchy:
                // "<Level>.Hierarchy"
                return new FunCall(
                        "Hierarchy",
                        Syntax.Property,
                        new Exp[] {fromExp});
            default:
                return null;
            }
        case Category.Logical:
            return null;
        case Category.Member:
            switch (to) {
            case Category.Dimension:
                // "<Member>.Dimension"
                return new FunCall(
                        "Dimension",
                        Syntax.Property,
                        new Exp[] {fromExp});
            case Category.Hierarchy:
                // "<Member>.Hierarchy"
                return new FunCall(
                        "Hierarchy",
                        Syntax.Property,
                        new Exp[]{fromExp});
            case Category.Level:
                // "<Member>.Level"
                return new FunCall(
                        "Level",
                        Syntax.Property,
                        new Exp[]{fromExp});
            case Category.Tuple:
                // Conversion to tuple is trivial: a member is a
                // one-dimensional tuple already.
                return fromExp;
            case Category.Numeric | Category.Constant:
            case Category.String | Category.Constant: //todo: assert is a string member
                // "<Member>.Value"
                return new FunCall(
                        "Value",
                        Syntax.Property,
                        new Exp[]{fromExp});
            case Category.Value:
            case Category.Numeric:
            case Category.String:
                return fromExp;
            default:
                return null;
            }
        case Category.Numeric | Category.Constant:
            switch (to) {
            case Category.Value:
            case Category.Numeric:
                return fromExp;
            default:
                return null;
            }
        case Category.Numeric:
            switch (to) {
            case Category.Value:
                return fromExp;
            case Category.Numeric | Category.Constant:
                return new FunCall(
                        "_Value",
                        Syntax.Function,
                        new Exp[] {fromExp});
            default:
                return null;
            }
        case Category.Set:
            return null;
        case Category.String | Category.Constant:
            switch (to) {
            case Category.Value:
            case Category.String:
                return fromExp;
            default:
                return null;
            }
        case Category.String:
            switch (to) {
            case Category.Value:
                return fromExp;
            case Category.String | Category.Constant:
                return new FunCall(
                        "_Value",
                        Syntax.Function,
                        new Exp[] {fromExp});
            default:
                return null;
            }
        case Category.Tuple:
            switch (to) {
            case Category.Value:
                return fromExp;
            case Category.Numeric:
            case Category.String:
                return new FunCall(
                        "_Value",
                        Syntax.Function,
                        new Exp[] {fromExp});
            default:
                return null;
            }
        case Category.Value:
            return null;
        case Category.Symbol:
            return null;
        default:
            throw Util.newInternal("unknown category " + from);
        }
    }

    /**
     * Returns whether we can convert an argument to a parameter tyoe.
     * @param fromExp argument type
     * @param to   parameter type
     * @param conversionCount in/out count of number of conversions performed;
     *             is incremented if the conversion is non-trivial (for
     *             example, converting a member to a level).
     *
     * @see FunTable#convert
     */
    static boolean canConvert(Exp fromExp, int to, int[] conversionCount) {
        int from = fromExp.getCategory();
        if (from == to) {
            return true;
        }
        switch (from) {
        case Category.Array:
            return false;
        case Category.Dimension:
            // Seems funny that you can 'downcast' from a dimension, doesn't
            // it? But we add an implicit 'CurrentMember', for example,
            // '[Time].PrevMember' actually means
            // '[Time].CurrentMember.PrevMember'.
            if (to == Category.Hierarchy ||
                    to == Category.Level ||
                    to == Category.Member ||
                    to == Category.Tuple) {
                conversionCount[0]++;
                return true;
            } else {
                return false;
            }
        case Category.Hierarchy:
            if (to == Category.Dimension) {
                conversionCount[0]++;
                return true;
            } else {
                return false;
            }
        case Category.Level:
            if (to == Category.Dimension ||
                    to == Category.Hierarchy) {
                conversionCount[0]++;
                return true;
            } else {
                return false;
            }
        case Category.Logical:
            return false;
        case Category.Member:
            if (to == Category.Dimension ||
                    to == Category.Hierarchy ||
                    to == Category.Level ||
                    to == Category.Tuple) {
                conversionCount[0]++;
                return true;
            } else if (to == (Category.Numeric | Category.Expression)) {
                // We assume that members are numeric, so a cast to a numeric
                // expression is less expensive than a conversion to a string
                // expression.
                conversionCount[0]++;
                return true;
            } else if (to == Category.Value ||
                    to == (Category.String | Category.Expression)) {
                conversionCount[0] += 2;
                return true;
            } else {
                return false;
            }
        case Category.Numeric | Category.Constant:
            return to == Category.Value ||
                to == Category.Numeric;
        case Category.Numeric:
            return to == Category.Value ||
                to == (Category.Numeric | Category.Constant);
        case Category.Set:
            return false;
        case Category.String | Category.Constant:
            return to == Category.Value ||
                to == Category.String;
        case Category.String:
            return to == Category.Value ||
                to == (Category.String | Category.Constant);
        case Category.Tuple:
            return to == Category.Value ||
                to == Category.Numeric;
        case Category.Value:
            return false;
        case Category.Symbol:
            return false;
        default:
            throw Util.newInternal("unknown category " + from);
        }
    }

    public FunDef getDef(FunCall call, Validator resolver) {
        String key = makeResolverKey(call.getFunName(), call.getSyntax());

        // Resolve function by its upper-case name first.  If there is only one
        // function with that name, stop immediately.  If there is more than
        // function, use some custom method, which generally involves looking
        // at the type of one of its arguments.
        String signature = call.getSyntax().getSignature(call.getFunName(),
                Category.Unknown, ExpBase.getTypes(call.getArgs()));
        Resolver[] resolvers = (Resolver[]) mapNameToResolvers.get(key);
        if (resolvers == null) {
            resolvers = emptyResolvers;
        }

        int[] conversionCount = new int[1];
        int minConversions = Integer.MAX_VALUE;
        int matchCount = 0;
        FunDef matchDef = null;
        for (int i = 0; i < resolvers.length; i++) {
            conversionCount[0] = 0;
            FunDef def = resolvers[i].resolve(call.getArgs(), conversionCount);
            if (def != null) {
                if (def.getReturnCategory() == Category.Set &&
                        resolver.requiresExpression()) {
                    continue;
                }
                int conversions = conversionCount[0];
                if (conversions < minConversions) {
                    minConversions = conversions;
                    matchCount = 1;
                    matchDef = def;
                } else if (conversions == minConversions) {
                    matchCount++;
                } else {
                    // ignore this match -- it required more coercions than
                    // other overloadings we've seen
                }
            }
        }
        switch (matchCount) {
        case 0:
            throw MondrianResource.instance().newNoFunctionMatchesSignature(
                    signature);
        case 1:
            final String matchKey = makeResolverKey(matchDef.getName(),
                    matchDef.getSyntax());
            Util.assertTrue(matchKey.equals(key), matchKey);
            return matchDef;
        default:
            throw MondrianResource.instance()
                    .newMoreThanOneFunctionMatchesSignature(signature);
        }
    }

    public boolean requiresExpression(FunCall call, int k,
            Validator resolver) {
        final FunDef funDef = call.getFunDef();
        if (funDef != null) {
            final int[] parameterTypes = funDef.getParameterTypes();
            return parameterTypes[k] != Category.Set;
        }
        // The function call has not been resolved yet. In fact, this method
        // may have been invoked while resolving the child. Consider this:
        //   CrossJoin([Measures].[Unit Sales] * [Measures].[Store Sales])
        //
        // In order to know whether to resolve '*' to the multiplication
        // operator (which returns a scalar) or the crossjoin operator (which
        // returns a set) we have to know what kind of expression is expected.
        String key = makeResolverKey(call.getFunName(), call.getSyntax());
        Resolver[] resolvers = (Resolver[]) mapNameToResolvers.get(key);
        if (resolvers == null) {
            resolvers = emptyResolvers;
        }
        for (int i = 0; i < resolvers.length; i++) {
            Resolver resolver2 = resolvers[i];
            if (!resolver2.requiresExpression(k)) {
                // This resolver accepts a set in this argument position,
                // therefore we don't REQUIRE a scalar expression.
                return false;
            }
        }
        return true;
    }

    public boolean isReserved(String s) {
        return reservedWords.contains(s.toUpperCase());
    }

    /**
     * Defines a reserved word.
     */
    public void defineReserved(String s) {
        reservedWords.add(s.toUpperCase());
    }

    /**
     * Derived class can override this method to add more functions.
     **/
    protected void defineFunctions() {
        defineReserved("NULL");

        // first char: p=Property, m=Method, i=Infix, P=Prefix
        // 2nd:

        // ARRAY FUNCTIONS
        if (false) define(new FunDefBase(
                "SetToArray",
                "SetToArray(<Set>[, <Set>]...[, <Numeric Expression>])",
                "Converts one or more sets to an array for use in a user-if (false) defined function.",
                "fa*"));
        //
        // DIMENSION FUNCTIONS
        define(new FunDefBase(
                "Dimension",
                "<Hierarchy>.Dimension",
                "Returns the dimension that contains a specified hierarchy.",
                "pdh") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
                return hierarchy.getDimension();
            }
        });

        //??Had to add this to get <Hierarchy>.Dimension to work?
        define(new FunDefBase(
                "Dimension",
                "<Dimension>.Dimension",
                "Returns the dimension that contains a specified hierarchy.",
                "pdd") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Dimension dimension = getDimensionArg(evaluator, args, 0, true);
                return dimension;
            }

        });

        define(new FunDefBase(
                "Dimension",
                "<Level>.Dimension",
                "Returns the dimension that contains a specified level.",
                "pdl") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Level level = getLevelArg(evaluator, args, 0, true);
                return level.getDimension();
            }
        });

        define(new FunDefBase(
                "Dimension",
                "<Member>.Dimension",
                "Returns the dimension that contains a specified member.",
                "pdm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                return member.getDimension();
            }
        });

        define(new FunDefBase(
                "Dimensions",
                "Dimensions(<Numeric Expression>)",
                "Returns the dimension whose zero-based position within the cube is specified by a numeric expression.",
                "fdn") {
            public Type getResultType(Validator validator, Exp[] args) {
                return new mondrian.olap.type.DimensionType(null);
            }

            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Cube cube = evaluator.getCube();
                Dimension[] dimensions = cube.getDimensions();
                int n = getIntArg(evaluator, args, 0);
                if ((n > dimensions.length) || (n < 1)) {
                    throw newEvalException(
                            this, "Index '" + n + "' out of bounds");
                }
                return dimensions[n - 1];
            }
        });
        define(new FunDefBase(
                "Dimensions",
                "Dimensions(<String Expression>)",
                "Returns the dimension whose name is specified by a string.",
                "fdS") {
            public Type getResultType(Validator validator, Exp[] args) {
                return new mondrian.olap.type.DimensionType(null);
            }

            public Object evaluate(Evaluator evaluator, Exp[] args) {
                String defValue = "Default Value";
                String s = getStringArg(evaluator, args, 0, defValue);
                if (s.indexOf("[") == -1) {
                    s = Util.quoteMdxIdentifier(s);
                }
                OlapElement o = evaluator.getSchemaReader().lookupCompound(
                        evaluator.getCube(),
                        explode(s),
                        false,
                        Category.Dimension);
                if (o instanceof Dimension) {
                    return (Dimension) o;
                } else if (o == null) {
                    throw newEvalException(this, "Dimension '" + s + "' not found");
                } else {
                    throw newEvalException(this, "Dimensions(" + s + ") found " + o);
                }
            }
        });

        //
        // HIERARCHY FUNCTIONS
        define(new FunDefBase(
                "Hierarchy",
                "<Level>.Hierarchy",
                "Returns a level's hierarchy.",
                "phl") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Level level = getLevelArg(evaluator, args, 0, true);
                return level.getHierarchy();
            }
        });
        define(new FunDefBase(
                "Hierarchy",
                "<Member>.Hierarchy",
                "Returns a member's hierarchy.",
                "phm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                return member.getHierarchy();
            }
        });

        //
        // LEVEL FUNCTIONS
        define(new FunDefBase(
                "Level",
                "<Member>.Level",
                "Returns a member's level.",
                "plm") {
            public Type getResultType(Validator validator, Exp[] args) {
                final Type argType = args[0].getTypeX();
                return new mondrian.olap.type.LevelType(argType.getHierarchy(),
                        argType.getLevel());
            }

            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                return member.getLevel();
            }
        });

        define(new FunDefBase(
                "Levels",
                "<Hierarchy>.Levels(<Numeric Expression>)",
                "Returns the level whose position in a hierarchy is specified by a numeric expression.",
                "mlhn") {
            public Type getResultType(Validator validator, Exp[] args) {
                final Type argType = args[0].getTypeX();
                return new mondrian.olap.type.LevelType(argType.getHierarchy(),
                        argType.getLevel());
            }

            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
                Level[] levels = hierarchy.getLevels();

                int n = getIntArg(evaluator, args, 1);
                if ((n >= levels.length) || (n < 0)) {
                    throw newEvalException(
                            this, "Index '" + n + "' out of bounds");
                }
                return levels[n];
            }
        });

        define(new FunDefBase(
                "Levels",
                "Levels(<String Expression>)",
                "Returns the level whose name is specified by a string expression.",
                "flS") {
            public Type getResultType(Validator validator, Exp[] args) {
                final Type argType = args[0].getTypeX();
                return new mondrian.olap.type.LevelType(argType.getHierarchy(),
                        argType.getLevel());
            }
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                String s = getStringArg(evaluator, args, 0, null);
                Cube cube = evaluator.getCube();
                OlapElement o = (s.startsWith("[")) ?
                        evaluator.getSchemaReader().lookupCompound(
                                cube,
                                explode(s),
                                false,
                                Category.Level) :
                        // lookupCompound barfs if "s" doesn't have matching
                        // brackets, so don't even try
                        null;

                if (o instanceof Level) {
                    return (Level) o;
                } else if (o == null) {
                    throw newEvalException(this, "Level '" + s + "' not found");
                } else {
                    throw newEvalException(this, "Levels('" + s + "') found " + o);
                }
            }
        });

        //
        // LOGICAL FUNCTIONS
        define(new MultiResolver(
                "IsEmpty",
                "IsEmpty(<Value Expression>)",
                "Determines if an expression evaluates to the empty cell value.",
                new String[] {"fbS", "fbn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        Object o = getScalarArg(evaluator, args, 0);
                        return Boolean.valueOf(o == Util.nullValue);
                    }
                };
            }
        });

        //
        // MEMBER FUNCTIONS
        define(new MultiResolver(
                "Ancestor",
                "Ancestor(<Member>, {<Level>|<Numeric Expression>})",
                "Returns the ancestor of a member at a specified level.",
                new String[] {"fmml", "fmmn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {

                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        Member member = getMemberArg(evaluator, args, 0, false);
                        Object arg2 = getArg(evaluator, args, 1);

                        Level level = null;
                        int distance;
                        if (arg2 instanceof Level) {
                            level = (Level) arg2;
                            distance = member.getLevel().getDepth() - level.getDepth();
                        } else {
                            distance = ((Number)arg2).intValue();
                        }

                        return ancestor(evaluator, member, distance, level);
                    }
                };
            }
        });

        define(new FunDefBase(
                "Cousin",
                "Cousin(<member>, <ancestor member>)",
                "Returns the member with the same relative position under <ancestor member> as the member specified.",
                "fmmm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                Member ancestorMember = getMemberArg(evaluator, args, 1, true);
                Member cousin = cousin(
                        evaluator.getSchemaReader(),
                        member,
                        ancestorMember);
                return cousin;
            }
        });

        define(new FunDefBase(
                "CurrentMember",
                "<Dimension>.CurrentMember",
                "Returns the current member along a dimension during an iteration.",
                "pmd") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Dimension dimension = getDimensionArg(evaluator, args, 0, true);
                return evaluator.getContext(dimension);
            }
        });

        define(new FunDefBase(
                "DataMember",
                "<Member>.DataMember",
                "Returns the system-generated data member that is associated with a nonleaf member of a dimension.",
                "pmm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                return member.getDataMember();
            }
        });

        define(new FunDefBase(
                "DefaultMember",
                "<Dimension>.DefaultMember",
                "Returns the default member of a dimension.",
                "pmd") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Dimension dimension = getDimensionArg(evaluator, args, 0, true);
                return evaluator.getSchemaReader().getHierarchyDefaultMember(
                        dimension.getHierarchy());
            }
        });

        define(new FunDefBase(
                "FirstChild",
                "<Member>.FirstChild",
                "Returns the first child of a member.",
                "pmm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                Member[] children = evaluator.getSchemaReader().getMemberChildren(member);
                return (children.length == 0)
                        ? member.getHierarchy().getNullMember()
                        : children[0];
            }
        });

        define(new FunDefBase(
                "FirstSibling",
                "<Member>.FirstSibling",
                "Returns the first child of the parent of a member.",
                "pmm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                Member parent = member.getParentMember();
                Member[] children;
                if (parent == null) {
                    if (member.isNull()) {
                        return member;
                    }
                    children = evaluator.getSchemaReader().getHierarchyRootMembers(member.getHierarchy());
                } else {
                    children = evaluator.getSchemaReader().getMemberChildren(parent);
                }
                return children[0];
            }
        });

        if (false) define(new FunDefBase(
                "Item",
                "<Tuple>.Item(<Numeric Expression>)",
                "Returns a member from a tuple.",
                "mm*"));

        define(new MultiResolver(
                "Lag",
                "<Member>.Lag(<Numeric Expression>)",
                "Returns a member further along the specified member's dimension.",
                new String[]{"mmmn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        Member member = getMemberArg(evaluator, args, 0, true);
                        int n = getIntArg(evaluator, args, 1);
                        return evaluator.getSchemaReader().getLeadMember(member, -n);
                    }
                };
            }
        });

        define(new FunDefBase(
                "LastChild",
                "<Member>.LastChild",
                "Returns the last child of a member.",
                "pmm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                Member[] children =
                        evaluator.getSchemaReader().getMemberChildren(member);
                return (children.length == 0)
                        ? member.getHierarchy().getNullMember()
                        : children[children.length - 1];
            }

        });

        define(new FunDefBase(
                "LastSibling",
                "<Member>.LastSibling",
                "Returns the last child of the parent of a member.",
                "pmm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                Member parent = member.getParentMember();
                Member[] children;
                final SchemaReader schemaReader = evaluator.getSchemaReader();
                if (parent == null) {
                    if (member.isNull()) {
                        return member;
                    }
                    children = schemaReader.getHierarchyRootMembers(
                            member.getHierarchy());
                } else {
                    children = schemaReader.getMemberChildren(parent);
                }
                return children[children.length - 1];
            }
        });

        define(new MultiResolver(
                "Lead",
                "<Member>.Lead(<Numeric Expression>)",
                "Returns a member further along the specified member's dimension.",
                new String[]{"mmmn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        Member member = getMemberArg(evaluator, args, 0, true);
                        int n = getIntArg(evaluator, args, 1);
                        return evaluator.getSchemaReader().getLeadMember(member, n);
                    }
                };
            }});

        define(new FunDefBase(
                "Members",
                "Members(<String Expression>)",
                "Returns the member whose name is specified by a string expression.",
                "fmS"));

        define(new FunDefBase(
                "NextMember",
                "<Member>.NextMember",
                "Returns the next member in the level that contains a specified member.",
                "pmm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                return evaluator.getSchemaReader().getLeadMember(member, +1);
            }
        });

        define(new MultiResolver(
                "OpeningPeriod",
                "OpeningPeriod([<Level>[, <Member>]])",
                "Returns the first descendant of a member at a level.",
                new String[] {"fm", "fml", "fmlm"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Type getResultType(Validator validator, Exp[] args) {
                        if (args.length == 0) {
                            // With no args, the default implementation cannot
                            // guess the hierarchy, so we supply the Time
                            // dimension.
                            Hierarchy hierarchy = validator.getQuery()
                                    .getCube().getTimeDimension()
                                    .getHierarchy();
                            return new MemberType(hierarchy, null, null);
                        }
                        return super.getResultType(validator, args);
                    }
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        return openingClosingPeriod(evaluator, args, true);
                    }
                };
            }
        });

        define(new MultiResolver(
                "ClosingPeriod",
                "ClosingPeriod([<Level>[, <Member>]])",
                "Returns the last descendant of a member at a level.",
                new String[] {"fm", "fml", "fmlm", "fmm"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Type getResultType(Validator validator, Exp[] args) {
                        if (args.length == 0) {
                            // With no args, the default implementation cannot
                            // guess the hierarchy, so we supply the Time
                            // dimension.
                            Hierarchy hierarchy = validator.getQuery()
                                    .getCube().getTimeDimension()
                                    .getHierarchy();
                            return new MemberType(hierarchy, null, null);
                        }
                        return super.getResultType(validator, args);
                    }

                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        return openingClosingPeriod(evaluator, args, false);
                    }
                };
            }
        });

        define(new MultiResolver(
                "ParallelPeriod",
                "ParallelPeriod([<Level>[, <Numeric Expression>[, <Member>]]])",
                "Returns a member from a prior period in the same relative position as a specified member.",
                new String[] {"fm", "fml", "fmln", "fmlnm"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Type getResultType(Validator validator, Exp[] args) {
                        if (args.length == 0) {
                            // With no args, the default implementation cannot
                            // guess the hierarchy, so we supply the Time
                            // dimension.
                            Hierarchy hierarchy = validator.getQuery()
                                    .getCube().getTimeDimension()
                                    .getHierarchy();
                            return new MemberType(hierarchy, null, null);
                        }
                        return super.getResultType(validator, args);
                    }

                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        // Member defaults to [Time].currentmember
                        Member member = (args.length == 3)
                            ? getMemberArg(evaluator, args, 2, true)
                            : evaluator.getContext(
                                evaluator.getCube().getTimeDimension());

                        // Numeric Expression defaults to 1.
                        int lagValue = (args.length >= 2)
                            ? getIntArg(evaluator, args, 1)
                            : 1;

                        Level ancestorLevel;
                        if (args.length >= 1) {
                            ancestorLevel = getLevelArg(evaluator, args, 0, true);
                        } else {
                            Member parent = member.getParentMember();

                            if (parent == null ||
                                    parent.getCategory() != Category.Member) {
                                //
                                // The parent isn't a member (it's probably a hierarchy),
                                // so there is no parallelperiod.
                                //
                                return member.getHierarchy().getNullMember();
                            }

                            ancestorLevel = parent.getLevel();
                        }

                        //
                        // Now do some error checking.
                        // The ancestorLevel and the member must be from the
                        // same hierarchy.
                        //
                        if (member.getHierarchy() != ancestorLevel.getHierarchy()) {
                            MondrianResource.instance().newFunctionMbrAndLevelHierarchyMismatch(
                                    "ParallelPeriod", ancestorLevel.getHierarchy().getUniqueName(),
                                    member.getHierarchy().getUniqueName()
                            );
                        }

                        int distance = member.getLevel().getDepth() - ancestorLevel.getDepth();

                        Member ancestor = ancestor(evaluator, member, distance, ancestorLevel);

                        Member inLaw = evaluator.getSchemaReader().getLeadMember(ancestor, -lagValue);

                        return cousin(evaluator.getSchemaReader(), member, inLaw);
                    }
                };
            }
        });

        define(new FunDefBase(
                "Parent",
                "<Member>.Parent",
                "Returns the parent of a member.",
                "pmm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                Member parent = evaluator.getSchemaReader().getMemberParent(member);
                if (parent == null) {
                    parent = member.getHierarchy().getNullMember();
                }
                return parent;
            }
        });

        define(new FunDefBase(
                "PrevMember",
                "<Member>.PrevMember",
                "Returns the previous member in the level that contains a specified member.",
                "pmm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                return evaluator.getSchemaReader().getLeadMember(member, -1);
            }
        });

        define(new FunDefBase(
                "StrToMember",
                "StrToMember(<String Expression>)",
                "Returns a member from a unique name String in MDX format.",
                "fmS") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                String mname = getStringArg(evaluator, args, 0, null);
                Cube cube = evaluator.getCube();
                SchemaReader schemaReader = evaluator.getSchemaReader();
                String[] uniqueNameParts = Util.explode(mname);
                Member member = (Member) schemaReader.lookupCompound(cube,
                        uniqueNameParts, true, Category.Member);
                // Member member = schemaReader.getMemberByUniqueName(uniqueNameParts, false);
                return member;
            }
        });

        if (false) define(new FunDefBase(
                "ValidMeasure",
                "ValidMeasure(<Tuple>)",
                "Returns a valid measure in a virtual cube by forcing inapplicable dimensions to their top level.",
                "fm*"));
        //
        // NUMERIC FUNCTIONS
        define(new MultiResolver(
                "Aggregate",
                "Aggregate(<Set>[, <Numeric Expression>])",
                "Returns a calculated value using the appropriate aggregate function, based on the context of the query.",
                new String[] {"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        // compute members only if the context has changed
                        List members = (List) evaluator.getCachedResult(args[0]);
                        if (members == null) {
                            members = (List) getArg(evaluator, args, 0);
                            evaluator.setCachedResult(args[0], members);
                        }

                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        Aggregator aggregator = (Aggregator) evaluator.getProperty(Property.PROPERTY_AGGREGATION_TYPE);
                        if (aggregator == null) {
                            throw newEvalException(null, "Could not find an aggregator in the current evaluation context");
                        }
                        Aggregator rollup = aggregator.getRollup();
                        if (rollup == null) {
                            throw newEvalException(null, "Don't know how to rollup aggregator '" + aggregator + "'");
                        }
                        return rollup.aggregate(evaluator.push(), members, exp);
                    }
                };
            }
        });

        define(new MultiResolver(
                "$AggregateChildren",
                "$AggregateChildren(<Hierarchy>)",
                "Equivalent to 'Aggregate(<Hierarchy>.CurrentMember.Children); for internal use.",
                new String[] {"Inh"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
                        Member member = evaluator.getParent().getContext(hierarchy.getDimension());
                        List members = (List) member.getPropertyValue(Property.PROPERTY_CONTRIBUTING_CHILDREN);
                        Aggregator aggregator = (Aggregator) evaluator.getProperty(Property.PROPERTY_AGGREGATION_TYPE);
                        if (aggregator == null) {
                            throw newEvalException(null, "Could not find an aggregator in the current evaluation context");
                        }
                        Aggregator rollup = aggregator.getRollup();
                        if (rollup == null) {
                            throw newEvalException(null, "Don't know how to rollup aggregator '" + aggregator + "'");
                        }
                        return rollup.aggregate(evaluator.push(), members, valueFunCall);
                    }
                };
            }
        });

        define(new MultiResolver(
                "Avg",
                "Avg(<Set>[, <Numeric Expression>])",
                "Returns the average value of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        return avg(evaluator.push(), members, exp);
                    }
                };
            }
        });

        define(new MultiResolver(
                "Correlation",
                "Correlation(<Set>, <Numeric Expression>[, <Numeric Expression>])",
                "Returns the correlation of two series evaluated over a set.",
                new String[]{"fnxn","fnxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp1 = (ExpBase) getArgNoEval(args, 1);
                        ExpBase exp2 = (ExpBase) getArgNoEval(args, 2, valueFunCall);
                        return correlation(evaluator.push(), members, exp1, exp2);
                    }
                };
            }
        });

        final String[] resWords = {"INCLUDEEMPTY", "EXCLUDEEMPTY"};
        define(new MultiResolver(
                "Count",
                "Count(<Set>[, EXCLUDEEMPTY | INCLUDEEMPTY])",
                "Returns the number of tuples in a set, empty cells included unless the optional EXCLUDEEMPTY flag is used.",
            new String[]{"fnx", "fnxy"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        String empties = getLiteralArg(args, 1, "INCLUDEEMPTY", resWords, null);
                        final boolean includeEmpty = empties.equals("INCLUDEEMPTY");
                        return count(evaluator, members, includeEmpty);
                    }
                };
            }
            public String[] getReservedWords() {
                return resWords;
            }
        });

        define(new FunDefBase(
                "Count",
                "<Set>.Count",
                "Returns the number of tuples in a set including empty cells.",
                "pnx") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                List members = (List) getArg(evaluator, args, 0);
                return count(evaluator, members, true);
            }
        });

        define(new MultiResolver(
                "Covariance",
                "Covariance(<Set>, <Numeric Expression>[, <Numeric Expression>])",
                "Returns the covariance of two series evaluated over a set (biased).",
                new String[]{"fnxn","fnxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp1 = (ExpBase) getArgNoEval(args, 1);
                        ExpBase exp2 = (ExpBase) getArgNoEval(args, 2);
                        return covariance(evaluator.push(), members, exp1, exp2, true);
                    }
                };
            }
        });

        define(new MultiResolver(
                "CovarianceN",
                "CovarianceN(<Set>, <Numeric Expression>[, <Numeric Expression>])",
                "Returns the covariance of two series evaluated over a set (unbiased).",
                new String[]{"fnxn","fnxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp1 = (ExpBase) getArgNoEval(args, 1);
                        ExpBase exp2 = (ExpBase) getArgNoEval(args, 2, valueFunCall);
                        return covariance(evaluator.push(), members, exp1, exp2, false);
                    }
                };
            }
        });

        define(new FunDefBase(
                "IIf",
                "IIf(<Logical Expression>, <Numeric Expression1>, <Numeric Expression2>)",
                "Returns one of two numeric values determined by a logical test.",
                "fnbnn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Boolean b = getBooleanArg(evaluator, args, 0);
                if (b == null) {
                    // The result of the logical expression is not known,
                    // probably because some necessary value is not in the
                    // cache yet. Evaluate both expressions so that the cache
                    // gets populated as soon as possible.
                    getDoubleArg(evaluator, args, 1, null);
                    getDoubleArg(evaluator, args, 2, null);
                    return new Double(Double.NaN);
                }
                Object o = (b.booleanValue())
                        ? getDoubleArg(evaluator, args, 1, null)
                        : getDoubleArg(evaluator, args, 2, null);
                return o;
            }
        });


        define(new FunkResolver(
                "LinRegIntercept",
                "LinRegIntercept(<Set>, <Numeric Expression>[, <Numeric Expression>])",
                "Calculates the linear regression of a set and returns the value of b in the regression line y = ax + b.",
                new String[]{"fnxn","fnxnn"},
                new LinReg.Intercept()));

        define(new FunkResolver(
                "LinRegPoint",
                "LinRegPoint(<Numeric Expression>, <Set>, <Numeric Expression>[, <Numeric Expression>])",
                "Calculates the linear regression of a set and returns the value of y in the regression line y = ax + b.",
                new String[]{"fnnxn","fnnxnn"},
                new LinReg.Point()));

        define(new FunkResolver(
                "LinRegR2",
                "LinRegR2(<Set>, <Numeric Expression>[, <Numeric Expression>])",
                "Calculates the linear regression of a set and returns R2 (the coefficient of determination).",
                new String[]{"fnxn","fnxnn"},
                new LinReg.R2()));

        define(new FunkResolver(
                "LinRegSlope",
                "LinRegSlope(<Set>, <Numeric Expression>[, <Numeric Expression>])",
                "Calculates the linear regression of a set and returns the value of a in the regression line y = ax + b.",
                new String[]{"fnxn","fnxnn"},
                new LinReg.Slope()));

        define(new FunkResolver(
                "LinRegVariance",
                "LinRegVariance(<Set>, <Numeric Expression>[, <Numeric Expression>])",
                "Calculates the linear regression of a set and returns the variance associated with the regression line y = ax + b.",
                new String[]{"fnxn","fnxnn"},
                new LinReg.Variance()));

        define(new MultiResolver(
                "Max",
                "Max(<Set>[, <Numeric Expression>])",
                "Returns the maximum value of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        return max(evaluator.push(), members, exp);
                    }
            };
            }
        });

        define(new MultiResolver(
                "Median",
                "Median(<Set>[, <Numeric Expression>])",
                "Returns the median value of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        //todo: ignore nulls, do we need to ignore the List?
                        return median(evaluator.push(), members, exp);
                }
            };
            }
        });

        define(new MultiResolver(
                "Min",
                "Min(<Set>[, <Numeric Expression>])",
                "Returns the minimum value of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        return min(evaluator.push(), members, exp);
                    }
                };
            }
        });

        define(new FunDefBase(
                "Ordinal",
                "<Level>.Ordinal",
                "Returns the zero-based ordinal value associated with a level.",
                "pnl") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Level level = getLevelArg(evaluator, args, 0, false);

                return new Double(level.getDepth());
            }
        });

        define(new FunkResolver(
                "Rank",
                "Rank(<Tuple>, <Set> [, <Calc Expression>])",
                "Returns the one-based rank of a tuple in a set.",
                new String[]{"fntx","fntxn", "fnmx", "fnmxn"},
                new RankFunDef()));

        define(new MultiResolver(
                "Stddev",
                "Stddev(<Set>[, <Numeric Expression>])",
                "Alias for Stdev.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        return stdev(evaluator.push(), members, exp, false);
                    }
                };
            }
        });

        define(new MultiResolver(
                "Stdev",
                "Stdev(<Set>[, <Numeric Expression>])",
                "Returns the standard deviation of a numeric expression evaluated over a set (unbiased).",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        return stdev(evaluator.push(), members, exp, false);
                    }
                };
            }
        });

        define(new MultiResolver(
                "StddevP",
                "StddevP(<Set>[, <Numeric Expression>])",
                "Alias for StdevP.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        return stdev(evaluator.push(), members, exp, true);
                    }
                };
            }
        });

        define(new MultiResolver(
                "StdevP",
                "StdevP(<Set>[, <Numeric Expression>])",
                "Returns the standard deviation of a numeric expression evaluated over a set (biased).",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        return stdev(evaluator.push(), members, exp, true);
                    }
                };
            }
        });

        define(new MultiResolver(
                "Sum", "Sum(<Set>[, <Numeric Expression>])", "Returns the sum of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        return sum(evaluator.push(), members, exp);
                    }
                };
            }
        });

        define(new FunDefBase(
                "Value",
                "<Measure>.Value",
                "Returns the value of a measure.",
                "pnm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                return member.evaluateScalar(evaluator);
            }
        });

        define(new FunDefBase(
                "_Value",
                "_Value(<Tuple>)",
                "Returns the value of the current measure within the context of a tuple.",
                "fnt") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member[] members = getTupleArg(evaluator, args, 0);
                Evaluator evaluator2 = evaluator.push(members);
                return evaluator2.evaluateCurrent();
            }
        });

        define(new FunDefBase(
                "_Value",
                "_Value()",
                "Returns the value of the current measure.",
                "fn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                return evaluator.evaluateCurrent();
            }
        });

        // _Value is a pseudo-function which evaluates a tuple to a number.
        // It needs a custom resolver.
        if (false) define(new ResolverBase(
                "_Value",
                null,
                null,
                Syntax.Parentheses) {
            public FunDef resolve(Exp[] args, int[] conversionCount) {
                if (args.length == 1 &&
                        args[0].getCategory() == Category.Tuple) {
                    return new ValueFunDef(new int[] {Category.Tuple});
                }
                for (int i = 0; i < args.length; i++) {
                    Exp arg = args[i];
                    if (!canConvert(arg, Category.Member,  conversionCount)) {
                        return null;
                    }
                }
                int[] argTypes = new int[args.length];
                for (int i = 0; i < argTypes.length; i++) {
                    argTypes[i] = Category.Member;
                }
                return new ValueFunDef(argTypes);
            }
        });

        define(new MultiResolver(
                "Var",
                "Var(<Set>[, <Numeric Expression>])",
                "Returns the variance of a numeric expression evaluated over a set (unbiased).",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        return var(evaluator.push(), members, exp, false);
                    }
                };
            }
        });

        define(new MultiResolver(
                "Variance", "Variance(<Set>[, <Numeric Expression>])",
                "Alias for Var.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        return var(evaluator.push(), members, exp, false);
                    }
                };
            }
        });

        define(new MultiResolver(
                "VarianceP",
                "VarianceP(<Set>[, <Numeric Expression>])",
                "Alias for VarP.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        return var(evaluator.push(), members, exp, true);
                    }
                };
            }
        });

        define(new MultiResolver(
                "VarP",
                "VarP(<Set>[, <Numeric Expression>])",
                "Returns the variance of a numeric expression evaluated over a set (biased).",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
                        return var(evaluator.push(), members, exp, true);
                    }
                };
            }
        });

        //
        // SET FUNCTIONS
        if (false) define(new FunDefBase(
                "AddCalculatedMembers",
                "AddCalculatedMembers(<Set>)",
                "Adds calculated members to a set.",
                "fx*"));

        define(new FunDefBase(
                "Ascendants",
                "Ascendants(<Member>)",
                "Returns the set of the ascendants of a specified member.",
                "fxm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, false);
                if (member.isNull()) {
                    return new ArrayList();
                }
                Member[] members = member.getAncestorMembers();
                final List result = new ArrayList(members.length + 1);
                result.add(member);
                addAll(result, members);
                return result;
            }
        });

        define(new MultiResolver(
                "BottomCount",
                "BottomCount(<Set>, <Count>[, <Numeric Expression>])",
                "Returns a specified number of items from the bottom of a set, optionally ordering the set first.",
                new String[]{"fxxnn", "fxxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List list = (List) getArg(evaluator, args, 0);
                        int n = getIntArg(evaluator, args, 1);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 2, null);
                        if (exp != null) {
                            boolean desc = false, brk = true;
                            sort(evaluator, list, exp, desc, brk);
                        }
                        if (n < list.size()) {
                            list = list.subList(0, n);
                        }
                        return list;
                    }
                };
            }
        });

        define(new MultiResolver(
                "BottomPercent",
                "BottomPercent(<Set>, <Percentage>, <Numeric Expression>)",
                "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified percentage.",
                new String[]{"fxxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 2);
                        Double n = getDoubleArg(evaluator, args, 1);
                        return topOrBottom(evaluator.push(), members, exp, false, true, n.doubleValue());
                    }
                };
            }
        });

        define(new MultiResolver(
                "BottomSum",
                "BottomSum(<Set>, <Value>, <Numeric Expression>)",
                "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified value.",
                new String[]{"fxxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 2);
                        Double n = getDoubleArg(evaluator, args, 1);
                        return topOrBottom(evaluator.push(), members, exp, false, false, n.doubleValue());
                    }
                };
            }
        });

        define(new FunDefBase(
                "Children",
                "<Member>.Children",
                "Returns the children of a member.",
                "pxm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                Member[] children = evaluator.getSchemaReader().getMemberChildren(member);
                return Arrays.asList(children);
            }
        });

        define(new MultiResolver(
                "Crossjoin",
                "Crossjoin(<Set1>, <Set2>)",
                "Returns the cross product of two sets.",
                new String[]{"fxxx"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new CrossJoinFunDef(dummyFunDef);
            }
        });

        define(new MultiResolver(
                "NonEmptyCrossJoin",
                "NonEmptyCrossJoin(<Set1>, <Set2>)",
                "Returns the cross product of two sets, excluding empty tuples and tuples without associated fact table data.",
                new String[]{"fxxx"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new NonEmptyCrossJoinFunDef(dummyFunDef);
            }
        });

        define(new MultiResolver(
                "*",
                "<Set1> * <Set2>",
                "Returns the cross product of two sets.",
                new String[]{"ixxx", "ixmx", "ixxm", "ixmm"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new CrossJoinFunDef(dummyFunDef);
            }
        });

        define(new DescendantsFunDef.Resolver());

        define(new FunDefBase(
                "Distinct",
                "Distinct(<Set>)",
                "Eliminates duplicate tuples from a set.",
                "fxx") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                List list = (List) getArg(evaluator, args, 0);
                HashSet hashSet = new HashSet(list.size());
                Iterator iter = list.iterator();
                List result = new ArrayList();

                while (iter.hasNext()) {
                    Object element = iter.next();
                    MemberHelper lookupObj = new MemberHelper(element);

                    if (hashSet.add(lookupObj)) {
                        result.add(element);
                    }
                }
                return result;
            }
        });

        define(new MultiResolver(
                "DrilldownLevel",
                "DrilldownLevel(<Set>[, <Level>]) or DrilldownLevel(<Set>, , <Index>)",
                "Drills down the members of a set, at a specified level, to one level below. Alternatively, drills down on a specified dimension in the set.",
                new String[]{"fxx", "fxxl"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        //todo add fssl functionality
                        List set0 = (List) getArg(evaluator, args, 0);

                        if (set0.size() == 0) {
                            return set0;
                        }

                        int searchDepth = -1;

                        Level level = getLevelArg(evaluator, args, 1, false);
                        if (level != null) {
                            searchDepth = level.getDepth();
                        }

                        if (searchDepth == -1) {
                            searchDepth = ((Member)set0.get(0)).getLevel().getDepth();

                            for (int i = 1, m = set0.size(); i < m; i++) {
                                Member member = (Member) set0.get(i);
                                int memberDepth = member.getLevel().getDepth();

                                if (memberDepth > searchDepth) {
                                    searchDepth = memberDepth;
                                }
                            }
                        }

                        List drilledSet = new ArrayList();

                        for (int i = 0, m = set0.size(); i < m; i++) {
                            Member member = (Member) set0.get(i);
                            drilledSet.add(member);

                            Member nextMember = i == m - 1 ? null : (Member) set0.get(i + 1);

                            //
                            // This member is drilled if it's at the correct depth
                            // and if it isn't drilled yet. A member is considered
                            // to be "drilled" if it is immediately followed by
                            // at least one descendant
                            //
                            if (member.getLevel().getDepth() == searchDepth
                                    && !isAncestorOf(member, nextMember, true)) {
                                Member[] childMembers = evaluator.getSchemaReader().getMemberChildren(member);
                                for (int j = 0; j < childMembers.length; j++) {
                                    drilledSet.add(childMembers[j]);
                                }
                            }
                        }
                        return drilledSet;
                    }
                };
            }
        });

        if (false) define(new FunDefBase(
                "DrilldownLevelBottom",
                "DrilldownLevelBottom(<Set>, <Count>[, [<Level>][, <Numeric Expression>]])",
                "Drills down the bottom N members of a set, at a specified level, to one level below.",
                "fx*"));

        if (false) define(new FunDefBase(
                "DrilldownLevelTop",
                "DrilldownLevelTop(<Set>, <Count>[, [<Level>][, <Numeric Expression>]])",
                "Drills down the top N members of a set, at a specified level, to one level below.",
                "fx*"));

        define(new DrilldownMemberFunDef.Resolver());

        if (false) define(new FunDefBase(
                "DrilldownMemberBottom",
                "DrilldownMemberBottom(<Set1>, <Set2>, <Count>[, [<Numeric Expression>][, RECURSIVE]])",
                "Like DrilldownMember except that it includes only the bottom N children.",
                "fx*"));

        if (false) define(new FunDefBase(
                "DrilldownMemberTop",
                "DrilldownMemberTop(<Set1>, <Set2>, <Count>[, [<Numeric Expression>][, RECURSIVE]])",
                "Like DrilldownMember except that it includes only the top N children.",
                "fx*"));

        if (false) define(new FunDefBase(
                "DrillupLevel",
                "DrillupLevel(<Set>[, <Level>])",
                "Drills up the members of a set that are below a specified level.",
                "fx*"));

        if (false) define(new FunDefBase(
                "DrillupMember",
                "DrillupMember(<Set1>, <Set2>)",
                "Drills up the members in a set that are present in a second specified set.",
                "fx*"));

        define(new MultiResolver(
                "Except",
                "Except(<Set1>, <Set2>[, ALL])",
                "Finds the difference between two sets, optionally retaining duplicates.",
                new String[]{"fxxx", "fxxxy"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        // todo: implement ALL
                        HashSet set = new HashSet();
                        set.addAll((List) getArg(evaluator, args, 1));
                        List set1 = (List) getArg(evaluator, args, 0);
                        List result = new ArrayList();
                        for (int i = 0, count = set1.size(); i < count; i++) {
                            Object o = set1.get(i);
                            if (!set.contains(o)) {
                                result.add(o);
                            }
                        }
                        return result;
                    }
                };
            }
        });

        if (false) define(new FunDefBase(
                "Extract",
                "Extract(<Set>, <Dimension>[, <Dimension>...])",
                "Returns a set of tuples from extracted dimension elements. The opposite of Crossjoin.",
                "fx*"));

        define(new FunDefBase(
                "Filter",
                "Filter(<Set>, <Search Condition>)",
                "Returns the set resulting from filtering a set based on a search condition.",
                "fxxb") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                List members = (List) getArg(evaluator, args, 0);
                Exp exp = args[1];
                List result = new ArrayList();
                Evaluator evaluator2 = evaluator.push();
                for (int i = 0, count = members.size(); i < count; i++) {
                    Object o = members.get(i);
                    if (o instanceof Member) {
                        evaluator2.setContext((Member) o);
                    } else if (o instanceof Member[]) {
                        evaluator2.setContext((Member[]) o);
                    } else {
                        throw Util.newInternal(
                                "unexpected type in set: " + o.getClass());
                    }
                    Boolean b = (Boolean) exp.evaluateScalar(evaluator2);
                    if (b != null && b.booleanValue()) {
                        result.add(o);
                    }
                }
                return result;
            }

            public boolean dependsOn(Exp[] args, Dimension dimension) {
                return dependsOnIntersection(args, dimension);
            }

        });

        define(new MultiResolver(
                "Generate",
                "Generate(<Set1>, <Set2>[, ALL])",
                "Applies a set to each member of another set and joins the resulting sets by union.",
                new String[] {"fxxx", "fxxxy"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                final boolean all = getLiteralArg(args, 2, "", new String[] {"ALL"}, dummyFunDef).equalsIgnoreCase("ALL");
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        List result = new ArrayList();
                        HashSet emitted = all ? null : new HashSet();
                        for (int i = 0; i < members.size(); i++) {
                            Object o = members.get(i);
                            if (o instanceof Member) {
                                evaluator.setContext((Member) o);
                            } else {
                                evaluator.setContext((Member[]) o);
                            }
                            final List result2 = (List) args[1].evaluate(evaluator);
                            if (all) {
                                result.addAll(result2);
                            } else {
                                for (int j = 0; j < result2.size(); j++) {
                                    Object row = result2.get(j);
                                    if (emitted.add(row)) {
                                        result.add(row);
                                    }
                                }
                            }
                        }
                        return result;
                    }
                };
            }
        });

        define(new MultiResolver(
                "Head",
                "Head(<Set>[, < Numeric Expression >])",
                "Returns the first specified number of elements in a set.",
                new String[] {"fxx", "fxxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        final int count = args.length < 2 ? 1 :
                                getIntArg(evaluator, args, 1);
                        if (count >= members.size()) {
                            return members;
                        }
                        if (count <= 0) {
                            return new ArrayList();
                        }
                        return members.subList(0, count);
                    }
                };
            }
        });

        final String[] prePost = {"PRE","POST"};
        define(new MultiResolver(
                "Hierarchize", "Hierarchize(<Set>[, POST])", "Orders the members of a set in a hierarchy.",
                new String[] {"fxx", "fxxy"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                String order = getLiteralArg(args, 1, "PRE", prePost, dummyFunDef);
                final boolean post = order.equals("POST");
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        hierarchize(members, post);
                        return members;
                    }
                };
            }

            public String[] getReservedWords() {
                return prePost;
            }
        });

        define(new MultiResolver(
                "Intersect",
                "Intersect(<Set1>, <Set2>[, ALL])",
                "Returns the intersection of two input sets, optionally retaining duplicates.",
                new String[] {"fxxxy", "fxxx"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                final boolean all = getLiteralArg(args, 2, "", new String[] {"ALL"}, dummyFunDef).equalsIgnoreCase("ALL");
                return new IntersectFunDef(dummyFunDef, all);
            }
        });

        if (false) define(new FunDefBase(
                "LastPeriods",
                "LastPeriods(<Index>[, <Member>])",
                "Returns a set of members prior to and including a specified member.",
                "fx*"));

        define(new FunDefBase(
                "Members",
                "<Dimension>.Members",
                "Returns the set of all members in a dimension.",
                "pxd") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Dimension dimension = (Dimension) getArg(evaluator, args, 0);
                Hierarchy hierarchy = dimension.getHierarchy();
                return addMembers(evaluator.getSchemaReader(), new ArrayList(), hierarchy);
            }
        });

        define(new FunDefBase(
                "Members",
                "<Hierarchy>.Members",
                "Returns the set of all members in a hierarchy.",
                "pxh") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Hierarchy hierarchy = (Hierarchy) getArg(evaluator, args, 0);
                return addMembers(evaluator.getSchemaReader(),
                    new ArrayList(), hierarchy);
            }
        });

        define(new FunDefBase(
                "Members",
                "<Level>.Members",
                "Returns the set of all members in a level.",
                "pxl") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Level level = (Level) getArg(evaluator, args, 0);
                return Arrays.asList(evaluator.getSchemaReader().getLevelMembers(level));
            }
        });
        define(new XtdFunDef.Resolver(
                "Mtd",
                "Mtd([<Member>])",
                "A shortcut function for the PeriodsToDate function that specifies the level to be Month.",
                new String[]{"fx", "fxm"},
                LevelType.TimeMonths));

        define(new OrderFunDef.OrderResolver());

        define(new MultiResolver(
                "PeriodsToDate",
                "PeriodsToDate([<Level>[, <Member>]])",
                "Returns a set of periods (members) from a specified level starting with the first period and ending with a specified member.",
                new String[]{"fx", "fxl", "fxlm"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Type getResultType(Validator validator, Exp[] args) {
                        if (args.length == 0) {
                            // With no args, the default implementation cannot
                            // guess the hierarchy.
                            Hierarchy hierarchy = validator.getQuery()
                                    .getCube().getTimeDimension()
                                    .getHierarchy();
                            return new SetType(
                                    new MemberType(hierarchy, null, null));
                        }
                        final Type type = args[0].getTypeX();
                        if (type.getHierarchy().getDimension()
                                .getDimensionType() !=
                                DimensionType.TimeDimension) {
                            throw MondrianResource.instance()
                                    .newTimeArgNeeded(getName());
                        }
                        return super.getResultType(validator, args);
                    }
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        Level level;
                        Member member;
                        if (args.length == 0) {
                            member = evaluator.getContext(
                                    evaluator.getCube().getTimeDimension());
                            level = member.getLevel().getParentLevel();
                        } else {
                            level = getLevelArg(evaluator, args, 0, false);
                            member = getMemberArg(evaluator, args, 1, false);
                        }
                        return periodsToDate(evaluator, level, member);
                    }
                };
            }
        });

        define(new XtdFunDef.Resolver(
                "Qtd",
                "Qtd([<Member>])",
                "A shortcut function for the PeriodsToDate function that specifies the level to be Quarter.",
                new String[]{"fx", "fxm"},
                LevelType.TimeQuarters));

        if (false) define(new FunDefBase(
                "StripCalculatedMembers",
                "StripCalculatedMembers(<Set>)",
                "Removes calculated members from a set.",
                "fx*"));

        // "Siblings" is not a standard MDX function.
        define(new FunDefBase(
                "Siblings",
                "<Member>.Siblings",
                "Returns the set of siblings of the specified member.",
                "pxm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                Member parent = member.getParentMember();
                final SchemaReader schemaReader = evaluator.getSchemaReader();
                Member[] siblings = (parent == null)
                    ? schemaReader.getHierarchyRootMembers(member.getHierarchy())
                    : schemaReader.getMemberChildren(parent);

                return Arrays.asList(siblings);
            }
        });

        define(new FunDefBase(
                "StrToSet",
                "StrToSet(<String Expression>)",
                "Constructs a set from a string expression.",
                "fxS") {
            public Exp validateCall(Validator validator, FunCall call) {
                final Exp[] args = call.getArgs();
                final int argCount = args.length;
                if (argCount <= 1) {
                    throw Util.getRes().newMdxFuncArgumentsNum(getName());
                }
                for (int i = 1; i < argCount; i++) {
                    final Exp arg = args[i];
                    if (arg instanceof Dimension) {
                        // if arg is a dimension, switch to dimension's default
                        // hierarchy
                        args[i] = ((Dimension) arg).getHierarchy();
                    } else if (arg instanceof Hierarchy) {
                        // nothing
                    } else {
                        throw Util.getRes().newMdxFuncNotHier(
                                new Integer(i + 1), getName());
                    }
                }
                return super.validateCall(validator, call);
            }

            public Type getResultType(Validator validator, Exp[] args) {
                if (args.length == 1) {
                    // This is a call to the standard version of StrToSet,
                    // which doesn't give us any hints about type.
                    return new SetType(null);
                } else {
                    // This is a call to Mondrian's extended version of
                    // StrToSet, of the form
                    //   StrToSet(s, <Hier1>, ... , <HierN>)
                    //
                    // The result is a set of tuples
                    //  (<Hier1>, ... ,  <HierN>)
                    final ArrayList list = new ArrayList();
                    for (int i = 1; i < args.length; i++) {
                        Exp arg = args[i];
                        final Type type = arg.getTypeX();
                        list.add(type);
                    }
                    final Type[] types =
                            (Type[]) list.toArray(new Type[list.size()]);
                    return new SetType(new TupleType(types));
                }
            }
        });

        define(new MultiResolver(
                "Subset",
                "Subset(<Set>, <Start>[, <Count>])",
                "Returns a subset of elements from a set.",
                new String[] {"fxxn", "fxxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        final int start = getIntArg(evaluator, args, 1);
                        final int end;
                        if (args.length < 3) {
                            end = members.size();
                        } else {
                            final int count = getIntArg(evaluator, args, 2);
                            end = start + count;
                        }
                        if (start >= end || start < 0) {
                            return new ArrayList();
                        }
                        if (start == 0 && end >= members.size()) {
                            return members;
                        }
                        return members.subList(start, end);
                    }
                };
            }
        });

        define(new MultiResolver(
                "Tail",
                "Tail(<Set>[, <Count>])",
                "Returns a subset from the end of a set.",
                new String[] {"fxx", "fxxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        final int count = args.length < 2 ? 1 :
                                getIntArg(evaluator, args, 1);
                        if (count >= members.size()) {
                            return members;
                        }
                        if (count <= 0) {
                            return new ArrayList();
                        }
                        return members.subList(members.size() - count, members.size());
                    }
                };
            }
        });

        define(new MultiResolver(
                "ToggleDrillState",
                "ToggleDrillState(<Set1>, <Set2>[, RECURSIVE])",
                "Toggles the drill state of members. This function is a combination of DrillupMember and DrilldownMember.",
                new String[]{"fxxx", "fxxxy"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List v0 = (List) getArg(evaluator, args, 0),
                                v1 = (List) getArg(evaluator, args, 1);
                        if (args.length > 2) {
                            throw MondrianResource.instance().newToggleDrillStateRecursiveNotSupported();
                        }
                        if (v1.isEmpty()) {
                            return v0;
                        }
                        if (v0.isEmpty()) {
                            return v0;
                        }
                        HashSet set = new HashSet();
                        set.addAll(v1);
                        HashSet set1 = set;
                        List result = new ArrayList();
                        int i = 0, n = v0.size();
                        while (i < n) {
                            Object o = v0.get(i++);
                            result.add(o);
                            Member m = null;
                            int k = -1;
                            if (o instanceof Member) {
                                if (!set1.contains(o)) {
                                    continue;
                                }
                                m = (Member) o;
                                k = -1;
                            } else {
                                Util.assertTrue(o instanceof Member[]);
                                Member[] members = (Member[]) o;
                                for (int j = 0; j < members.length; j++) {
                                    Member member = members[j];
                                    if (set1.contains(member)) {
                                        k = j;
                                        m = member;
                                        break;
                                    }
                                }
                                if (k == -1) {
                                    continue;
                                }
                            }
                            boolean isDrilledDown = false;
                            if (i < n) {
                                Object next = v0.get(i);
                                Member nextMember = (k < 0) ? (Member) next :
                                        ((Member[]) next)[k];
                                boolean strict = true;
                                if (isAncestorOf(m, nextMember, strict)) {
                                    isDrilledDown = true;
                                }
                            }
                            if (isDrilledDown) {
                                // skip descendants of this member
                                do {
                                    Object next = v0.get(i);
                                    Member nextMember = (k < 0) ? (Member) next :
                                            ((Member[]) next)[k];
                                    boolean strict = true;
                                    if (isAncestorOf(m, nextMember, strict)) {
                                        i++;
                                    } else {
                                        break;
                                    }
                                } while (i < n);
                            } else {
                                Member[] children = evaluator.getSchemaReader().getMemberChildren(m);
                                for (int j = 0; j < children.length; j++) {
                                    if (k < 0) {
                                        result.add(children[j]);
                                    } else {
                                        Member[] members = (Member[]) ((Member[]) o).clone();
                                        members[k] = children[j];
                                        result.add(members);
                                    }
                                }
                            }
                        }
                        return result;
                    }
                };
            }

            public String[] getReservedWords() {
                return new String[] {"RECURSIVE"};
            }
        });

        define(new MultiResolver(
                "TopCount",
                "TopCount(<Set>, <Count>[, <Numeric Expression>])",
                "Returns a specified number of items from the top of a set, optionally ordering the set first.",
                new String[]{"fxxnn", "fxxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List list = (List) getArg(evaluator, args, 0);
                        int n = getIntArg(evaluator, args, 1);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 2, null);
                        if (exp != null) {
                            boolean desc = true, brk = true;
                            sort(evaluator, list, exp, desc, brk);
                        }
                        if (n < list.size()) {
                            list = list.subList(0, n);
                        }
                        return list;
                    }
                };
            }
        });

        define(new MultiResolver(
                "TopPercent",
                "TopPercent(<Set>, <Percentage>, <Numeric Expression>)",
                "Sorts a set and returns the top N elements whose cumulative total is at least a specified percentage.",
                new String[]{"fxxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 2);
                        Double n = getDoubleArg(evaluator, args, 1);
                        return topOrBottom(evaluator.push(), members, exp, true, true, n.doubleValue());
                    }
                };
            }
        });

        define(new MultiResolver(
                "TopSum",
                "TopSum(<Set>, <Value>, <Numeric Expression>)",
                "Sorts a set and returns the top N elements whose cumulative total is at least a specified value.",
                new String[]{"fxxnn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArgNoEval(args, 2);
                        Double n = getDoubleArg(evaluator, args, 1);
                        return topOrBottom(evaluator.push(), members, exp, true, false, n.doubleValue());
                    }
                };
            }
        });

        final String[] allDistinct = new String[] {"ALL", "DISTINCT"};
        define(new MultiResolver(
                "Union",
                "Union(<Set1>, <Set2>[, ALL])",
                "Returns the union of two sets, optionally retaining duplicates.",
                new String[] {"fxxx", "fxxxy"}) {
            public String[] getReservedWords() {
                return allDistinct;
            }

            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                String allString = getLiteralArg(args, 2, "DISTINCT", allDistinct, dummyFunDef);
                final boolean all = allString.equalsIgnoreCase("ALL");
                checkCompatible(args[0], args[1], dummyFunDef);
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List left = (List) getArg(evaluator, args, 0);
                        List right = (List) getArg(evaluator, args, 1);
                        if (all) {
                            if ((left == null) || left.isEmpty()) {
                                return right;
                            }
                            left.addAll(right);
                            return left;
                        } else {
                            HashSet added = new HashSet();
                            List result = new ArrayList();
                            addUnique(result, left, added);
                            addUnique(result, right, added);
                            return result;
                        }
                    }
                };
            }
        });

        if (false) define(new FunDefBase(
                "VisualTotals",
                "VisualTotals(<Set>, <Pattern>)",
                "Dynamically totals child members specified in a set using a pattern for the total label in the result set.",
                "fx*"));

        define(new XtdFunDef.Resolver(
                "Wtd",
                "Wtd([<Member>])",
                "A shortcut function for the PeriodsToDate function that specifies the level to be Week.",
                new String[]{"fx", "fxm"},
                LevelType.TimeWeeks));

        define(new XtdFunDef.Resolver(
                "Ytd",
                "Ytd([<Member>])",
                "A shortcut function for the PeriodsToDate function that specifies the level to be Year.",
                new String[]{"fx", "fxm"},
                LevelType.TimeYears));

        define(new FunDefBase(
                ":",
                "<Member>:<Member>",
                "Infix colon operator returns the set of members between a given pair of members.",
                "ixmm") {
            // implement FunDef
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                final Member member0 = getMemberArg(evaluator, args, 0, true);
                final Member member1 = getMemberArg(evaluator, args, 1, true);
                if (member0.isNull() || member1.isNull()) {
                    return Collections.EMPTY_LIST;
                }
                if (member0.getLevel() != member1.getLevel()) {
                    throw newEvalException(this, "Members must belong to the same level");
                }
                return FunUtil.memberRange(evaluator, member0, member1);
            }
        });

        // special resolver for the "{...}" operator
        define(new ResolverBase(
                "{}",
                "{<Member> [, <Member>]...}",
                "Brace operator constructs a set.",
                Syntax.Braces) {
            public FunDef resolve(Exp[] args, int[] conversionCount) {
                int[] parameterTypes = new int[args.length];
                for (int i = 0; i < args.length; i++) {
                    if (canConvert(
                            args[i], Category.Member, conversionCount)) {
                        parameterTypes[i] = Category.Member;
                        continue;
                    }
                    if (canConvert(
                            args[i], Category.Set, conversionCount)) {
                        parameterTypes[i] = Category.Set;
                        continue;
                    }
                    if (canConvert(
                            args[i], Category.Tuple, conversionCount)) {
                        parameterTypes[i] = Category.Tuple;
                        continue;
                    }
                    return null;
                }
                return new SetFunDef(this, parameterTypes);
            }
        });

        //
        // STRING FUNCTIONS
        define(new MultiResolver(
                "Format",
                "Format(<Numeric Expression>, <String Expression>)",
                "Formats a number to string.",
                new String[] { "fSmS", "fSnS" }) {
            protected FunDef createFunDef(final Exp[] args,
                                          final FunDef dummyFunDef) {
                final Locale locale = Locale.getDefault(); // todo: use connection's locale
                if (args[1] instanceof Literal) {
                    // Constant string expression: optimize by compiling
                    // format string.
                    String formatString = (String) ((Literal) args[1]).getValue();
                    final Format format = new Format(formatString, locale);
                    return new FunDefBase(dummyFunDef) {
                        public Object evaluate(Evaluator evaluator, Exp[] args) {
                            Double o = getDoubleArg(evaluator, args, 0);
                            return format.format(o);
                        }
                    };
                } else {
                    // Variable string expression
                    return new FunDefBase(dummyFunDef) {
                        public Object evaluate(Evaluator evaluator, Exp[] args) {
                            Double o = getDoubleArg(evaluator, args, 0);
                            String formatString = getStringArg(evaluator, args, 1, null);
                            final Format format = new Format(formatString, locale);
                            return format.format(o);
                        }
                    };
                }
            }
        });

        define(new FunDefBase(
                "IIf",
                "IIf(<Logical Expression>, <String Expression1>, <String Expression2>)",
                "Returns one of two string values determined by a logical test.",
                "fSbSS") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Boolean b = getBooleanArg(evaluator, args, 0);
                if (b == null) {
                    // The result of the logical expression is not known,
                    // probably because some necessary value is not in the
                    // cache yet. Evaluate both expressions so that the cache
                    // gets populated as soon as possible.
                    getStringArg(evaluator, args, 1, null);
                    getStringArg(evaluator, args, 2, null);
                    return null;
                }
                Object o = (b.booleanValue())
                    ? getStringArg(evaluator, args, 1, null)
                    : getStringArg(evaluator, args, 2, null);
                return o;
            }
        });

        define(new FunDefBase(
                "Caption",
                "<Dimension>.Caption",
                "Returns the caption of a dimension.",
                "pSd") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Dimension dimension = getDimensionArg(evaluator, args, 0, true);
                return dimension.getCaption();
            }
        });

        define(new FunDefBase(
                "Caption",
                "<Hierarchy>.Caption",
                "Returns the caption of a hierarchy.",
                "pSh") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
                return hierarchy.getCaption();
            }
        });

        define(new FunDefBase(
                "Caption",
                "<Level>.Caption",
                "Returns the caption of a level.",
                "pSl") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Level level = getLevelArg(evaluator, args, 0, true);
                return level.getCaption();
            }
        });

        define(new FunDefBase(
                "Caption",
                "<Member>.Caption",
                "Returns the caption of a member.",
                "pSm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                return member.getCaption();
            }
        });

        define(new FunDefBase(
                "Name",
                "<Dimension>.Name",
                "Returns the name of a dimension.",
                "pSd") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Dimension dimension = getDimensionArg(evaluator, args, 0, true);
                return dimension.getName();
            }
        });

        define(new FunDefBase(
                "Name",
                "<Hierarchy>.Name",
                "Returns the name of a hierarchy.",
                "pSh") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
                return hierarchy.getName();
            }
        });

        define(new FunDefBase(
                "Name",
                "<Level>.Name",
                "Returns the name of a level.",
                "pSl") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Level level = getLevelArg(evaluator, args, 0, true);
                return level.getName();
            }
        });

        define(new FunDefBase(
                "Name",
                "<Member>.Name",
                "Returns the name of a member.",
                "pSm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                return member.getName();
            }
        });

        define(new FunDefBase(
                "SetToStr",
                "SetToStr(<Set>)",
                "Constructs a string from a set.",
                "fSx") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                List items = (List) getArg(evaluator, args, 0);
                StringBuffer buf = new StringBuffer();
                buf.append("{");
                for (int i = 0; i < items.size(); i++) {
                    if (i > 0) {
                        buf.append(", ");
                    }
                    final Object o = items.get(i);
                    appendMemberOrTuple(buf, o);
                }
                buf.append("}");
                return buf.toString();
            }
        });

        define(new FunDefBase(
                "TupleToStr",
                "TupleToStr(<Tuple>)",
                "Constructs a string from a tuple.",
                "fSt") {
            // implement FunDef
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Object o = getArg(evaluator, args, 0);
                StringBuffer buf = new StringBuffer();
                appendMemberOrTuple(buf, o);
                return buf.toString();
            }
        });

        define(new FunDefBase(
                "UniqueName",
                "<Dimension>.UniqueName",
                "Returns the unique name of a dimension.",
                "pSd") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Dimension dimension = getDimensionArg(evaluator, args, 0, true);
                return dimension.getUniqueName();
            }
        });

        define(new FunDefBase(
                "UniqueName",
                "<Hierarchy>.UniqueName",
                "Returns the unique name of a hierarchy.",
                "pSh") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
                return hierarchy.getUniqueName();
            }
        });

        define(new FunDefBase(
                "UniqueName",
                "<Level>.UniqueName",
                "Returns the unique name of a level.",
                "pSl") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Level level = getLevelArg(evaluator, args, 0, true);
                return level.getUniqueName();
            }
        });

        define(new FunDefBase(
                "UniqueName",
                "<Member>.UniqueName",
                "Returns the unique name of a member.",
                "pSm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                return member.getUniqueName();
            }
        });

        //
        // TUPLE FUNCTIONS
        define(new FunDefBase(
                "Current",
                "<Set>.Current",
                "Returns the current tuple from a set during an iteration.",
                "ptx"));

        // we do not support the <String expression> arguments
        if (false) define(new FunDefBase(
                "Item",
                "<Set>.Item(<String Expression>[, <String Expression>...] | <Index>)",
                "Returns a tuple from a set.",
                "mx*"));

        define(new MultiResolver(
                "Item",
                "<Set>.Item(<Index>)",
                "Returns the <Index>th element in the set, or ",
                new String[] {"mtxn", "mmtn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        Object arg0 = getArg(evaluator, args, 0);
                        int index = getIntArg(evaluator, args, 1);
                        int setSize;

                        if (arg0 instanceof List) {
                            List theSet = (List)arg0;

                            setSize = theSet.size();
                            if (index < setSize && index >= 0) {
                                return theSet.get(index);
                            }
                        } else {
                            //
                            // You'll get a member in the following case:
                            // {[member]}.item(0).item(0), even though the first invocation of
                            // item returned a tuple.
                            //
                            assert ((arg0 instanceof Member[]) || (arg0 instanceof Member));

                            if (arg0 instanceof Member) {
                                if (index == 0) {
                                    return arg0;
                                }
                                setSize = 0;
                            } else {
                                Member[] tuple = (Member[]) arg0;

                                if (index < tuple.length && index >= 0) {
                                    return tuple[index];
                                }

                                setSize = tuple.length;
                            }
                        }

                        throw MondrianResource.instance().newItemIndexOutOfBounds(Integer.toString(index), Integer.toString(setSize - 1));
                    }
                };
            }
        });

        define(new FunDefBase(
                "StrToTuple",
                "StrToTuple(<String Expression>)",
                "Constructs a tuple from a string.",
                "ftS") {
            public Exp validateCall(Validator validator, FunCall call) {
                final Exp[] args = call.getArgs();
                final int argCount = args.length;
                if (argCount <= 1) {
                    throw Util.getRes().newMdxFuncArgumentsNum(getName());
                }
                for (int i = 1; i < argCount; i++) {
                    final Exp arg = args[i];
                    if (arg instanceof Dimension) {
                        // if arg is a dimension, switch to dimension's default
                        // hierarchy
                        args[i] = ((Dimension) arg).getHierarchy();
                    } else if (arg instanceof Hierarchy) {
                        // nothing
                    } else {
                        throw Util.getRes().newMdxFuncNotHier(
                                new Integer(i + 1), getName());
                    }
                }
                return super.validateCall(validator, call);
            }

            public Type getResultType(Validator validator, Exp[] args) {
                if (args.length == 1) {
                    // This is a call to the standard version of StrToTuple,
                    // which doesn't give us any hints about type.
                    return new TupleType(null);
                } else {
                    // This is a call to Mondrian's extended version of
                    // StrToTuple, of the form
                    //   StrToTuple(s, <Hier1>, ... , <HierN>)
                    //
                    // The result is a tuple
                    //  (<Hier1>, ... ,  <HierN>)
                    final ArrayList list = new ArrayList();
                    for (int i = 1; i < args.length; i++) {
                        Exp arg = args[i];
                        final Type type = arg.getTypeX();
                        list.add(type);
                    }
                    final Type[] types =
                            (Type[]) list.toArray(new Type[list.size()]);
                    return new TupleType(types);
                }
            }
        });

        // special resolver for "()"
        define(new ResolverBase(
            "()",
            null,
            null,
            Syntax.Parentheses) {
            public FunDef resolve(Exp[] args, int[] conversionCount) {
                // Compare with TupleFunDef.getReturnCategory().  For example,
                //   ([Gender].members) is a set,
                //   ([Gender].[M]) is a member,
                //   (1 + 2) is a numeric,
                // but
                //   ([Gender].[M], [Marital Status].[S]) is a tuple.
                return (args.length == 1)
                    ? new ParenthesesFunDef(args[0].getCategory())
                    : (FunDef) new TupleFunDef(ExpBase.getTypes(args));
            }
        });

        //
        // GENERIC VALUE FUNCTIONS
        define(new ResolverBase(
                "CoalesceEmpty",
                "CoalesceEmpty(<Value Expression>[, <Value Expression>]...)",
                "Coalesces an empty cell value to a different value. All of the expressions must be of the same type (number or string).",
                Syntax.Function) {
            public FunDef resolve(Exp[] args, int[] conversionCount) {
                if (args.length < 1) {
                    return null;
                }
                final int[] types = {Category.Numeric, Category.String};
                for (int j = 0; j < types.length; j++) {
                    int type = types[j];
                    int matchingArgs = 0;
                    conversionCount[0] = 0;
                    for (int i = 0; i < args.length; i++) {
                        if (canConvert(args[i], type, conversionCount)) {
                            matchingArgs++;
                        }
                    }
                    if (matchingArgs == args.length) {
                        return new CoalesceEmptyFunDef(this, type, ExpBase.getTypes(args));
                    }
                }
                return null;
            }

            public boolean requiresExpression(int k) {
                return true;
            }
        });

        define(new ResolverBase(
                "_CaseTest",
                "Case When <Logical Expression> Then <Expression> [...] [Else <Expression>] End",
                "Evaluates various conditions, and returns the corresponding expression for the first which evaluates to true.",
                Syntax.Case) {
            public FunDef resolve(Exp[] args, int[] conversionCount) {
                if (args.length < 1) {
                    return null;
                }
                int j = 0;
                int clauseCount = args.length / 2;
                int mismatchingArgs = 0;
                int returnType = args[1].getCategory();
                for (int i = 0; i < clauseCount; i++) {
                    if (!canConvert(args[j++], Category.Logical, conversionCount)) {
                        mismatchingArgs++;
                    }
                    if (!canConvert(args[j++], returnType, conversionCount)) {
                        mismatchingArgs++;
                    }
                }
                if (j < args.length) {
                    if (!canConvert(args[j++], returnType, conversionCount)) {
                        mismatchingArgs++;
                    }
                }
                Util.assertTrue(j == args.length);
                if (mismatchingArgs == 0) {
                    return new FunDefBase(this, returnType, ExpBase.getTypes(args)) {
                        // implement FunDef
                        public Object evaluate(Evaluator evaluator, Exp[] args) {
                            return evaluateCaseTest(evaluator, args);
                        }
                    };
                } else {
                    return null;
                }
            }

            public boolean requiresExpression(int k) {
                return true;
            }

            Object evaluateCaseTest(Evaluator evaluator, Exp[] args) {
                int clauseCount = args.length / 2;
                int j = 0;
                for (int i = 0; i < clauseCount; i++) {
                    boolean logical = getBooleanArg(evaluator, args, j++, false);
                    if (logical) {
                        return getArg(evaluator, args, j);
                    } else {
                        j++;
                    }
                }
                return (j < args.length)
                    ? getArg(evaluator,  args, j)
                    : null;
            }
        });

        define(new ResolverBase(
                "_CaseMatch",
                "Case <Expression> When <Expression> Then <Expression> [...] [Else <Expression>] End",
                "Evaluates various expressions, and returns the corresponding expression for the first which matches a particular value.",
                Syntax.Case) {
            public FunDef resolve(Exp[] args, int[] conversionCount) {
                if (args.length < 3) {
                    return null;
                }
                int valueType = args[0].getCategory();
                int returnType = args[2].getCategory();
                int j = 0;
                int clauseCount = (args.length - 1) / 2;
                int mismatchingArgs = 0;
                if (!canConvert(args[j++], valueType, conversionCount)) {
                    mismatchingArgs++;
                }
                for (int i = 0; i < clauseCount; i++) {
                    if (!canConvert(args[j++], valueType, conversionCount)) {
                        mismatchingArgs++;
                    }
                    if (!canConvert(args[j++], returnType, conversionCount)) {
                        mismatchingArgs++;
                    }
                }
                if (j < args.length) {
                    if (!canConvert(args[j++], returnType, conversionCount)) {
                        mismatchingArgs++;
                    }
                }
                Util.assertTrue(j == args.length);
                if (mismatchingArgs == 0) {
                    return new FunDefBase(this, returnType, ExpBase.getTypes(args)) {
                        // implement FunDef
                        public Object evaluate(Evaluator evaluator, Exp[] args) {
                            return evaluateCaseMatch(evaluator, args);
                        }
                    };
                } else {
                    return null;
                }
            }

            public boolean requiresExpression(int k) {
                return true;
            }

            Object evaluateCaseMatch(Evaluator evaluator, Exp[] args) {
                int clauseCount = (args.length - 1)/ 2;
                int j = 0;
                Object value = getArg(evaluator, args, j++);
                for (int i = 0; i < clauseCount; i++) {
                    Object match = getArg(evaluator, args, j++);
                    if (match.equals(value)) {
                        return getArg(evaluator, args, j);
                    } else {
                        j++;
                    }
                }
                return (j < args.length)
                    ? getArg(evaluator, args, j)
                    : null;
            }
        });

        define(new PropertiesFunDef.Resolver());

        //
        // PARAMETER FUNCTIONS
        define(new ParameterFunDef.ParameterResolver());

        define(new ParameterFunDef.ParamRefResolver());

        //
        // OPERATORS
        define(new FunDefBase(
                "+",
                "<Numeric Expression> + <Numeric Expression>",
                "Adds two numbers.",
                "innn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Double o0 = getDoubleArg(evaluator, args, 0, null);
                Double o1 = getDoubleArg(evaluator, args, 1, null);
                if (o0 == null || o1 == null) {
                    return null;
                }
                return new Double(o0.doubleValue() + o1.doubleValue());
            }
        });

        define(new FunDefBase(
                "-",
                "<Numeric Expression> - <Numeric Expression>",
                "Subtracts two numbers.",
                "innn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Double o0 = getDoubleArg(evaluator, args, 0, null);
                Double o1 = getDoubleArg(evaluator, args, 1, null);
                if (o0 == null || o1 == null) {
                    return null;
                }
                return new Double(o0.doubleValue() - o1.doubleValue());
            }
        });

        define(new FunDefBase(
                "*",
                "<Numeric Expression> * <Numeric Expression>",
                "Multiplies two numbers.",
                "innn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Double o0 = getDoubleArg(evaluator, args, 0, null);
                Double o1 = getDoubleArg(evaluator, args, 1, null);
                if (o0 == null || o1 == null) {
                    return null;
                }
                return new Double(o0.doubleValue() * o1.doubleValue());
            }
        });

        define(new FunDefBase(
                "/",
                "<Numeric Expression> / <Numeric Expression>",
                "Divides two numbers.",
                "innn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Double o0 = getDoubleArg(evaluator, args, 0, null);
                Double o1 = getDoubleArg(evaluator, args, 1, null);
                if (o0 == null || o1 == null) {
                    return null;
                }
                return new Double(o0.doubleValue() / o1.doubleValue());
            }
            // todo: use this, via reflection
            public double evaluate(double d1, double d2) {
                return d1 / d2;
            }
        });

        define(new FunDefBase(
                "-",
                "- <Numeric Expression>",
                "Returns the negative of a number.",
                "Pnn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Double o0 = getDoubleArg(evaluator, args, 0, null);
                if (o0 == null) {
                    return null;
                }
                return new Double(- o0.doubleValue());
            }
        });

        define(new FunDefBase(
                "||",
                "<String Expression> || <String Expression>",
                "Concatenates two strings.",
                "iSSS") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                String o0 = getStringArg(evaluator, args, 0, null);
                String o1 = getStringArg(evaluator, args, 1, null);
                return o0 + o1;
            }
        });

        define(new FunDefBase(
                "AND",
                "<Logical Expression> AND <Logical Expression>",
                "Returns the conjunction of two conditions.",
                "ibbb") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                // if the first arg is known and false, we dont evaluate the second
                Boolean b1 = getBooleanArg(evaluator, args, 0);
                if ((b1 != null) && !b1.booleanValue()) {
                    return Boolean.FALSE;
                }
                Boolean b2 = getBooleanArg(evaluator, args, 1);
                if ((b2 != null) && !b2.booleanValue()) {
                    return Boolean.FALSE;
                }
                if (b1 == null || b2 == null) {
                    return null;
                }
                return Boolean.valueOf(b1.booleanValue() && b2.booleanValue());
            }
        });

        define(new FunDefBase(
                "OR",
                "<Logical Expression> OR <Logical Expression>",
                "Returns the disjunction of two conditions.",
                "ibbb") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                // if the first arg is known and true, we dont evaluate the second
                Boolean b1 = getBooleanArg(evaluator, args, 0);
                if ((b1 != null) && b1.booleanValue()) {
                    return Boolean.TRUE;
                }
                Boolean b2 = getBooleanArg(evaluator, args, 1);
                if ((b2 != null) && b2.booleanValue()) {
                    return Boolean.TRUE;
                }
                if (b1 == null || b2 == null) {
                    return null;
                }
                return Boolean.valueOf(b1.booleanValue() || b2.booleanValue());
            }
        });

        define(new FunDefBase(
                "XOR",
                "<Logical Expression> XOR <Logical Expression>",
                "Returns whether two conditions are mutually exclusive.",
                "ibbb") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Boolean b1 = getBooleanArg(evaluator, args, 0);
                Boolean b2 = getBooleanArg(evaluator, args, 1);
                if (b1 == null || b2 == null) {
                    return null;
                }
                return Boolean.valueOf(b1.booleanValue() != b2.booleanValue());
            }
        });

        define(new FunDefBase(
                "NOT",
                "NOT <Logical Expression>",
                "Returns the negation of a condition.",
                "Pbb") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Boolean b = getBooleanArg(evaluator, args, 0);
                return (b == null) ?
                        null :
                        b.booleanValue() ?
                        Boolean.FALSE :
                        Boolean.TRUE;
            }
        });

        define(new FunDefBase(
                "=",
                "<String Expression> = <String Expression>",
                "Returns whether two expressions are equal.",
                "ibSS") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                String o0 = getStringArg(evaluator, args, 0, null);
                String o1 = getStringArg(evaluator, args, 1, null);
                if (o0 == null || o1 == null) {
                    return null;
                }
                return Boolean.valueOf(o0.equals(o1));
            }
        });

        define(new FunDefBase(
                "=",
                "<Numeric Expression> = <Numeric Expression>",
                "Returns whether two expressions are equal.",
                "ibnn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Double o0 = getDoubleArg(evaluator, args, 0);
                Double o1 = getDoubleArg(evaluator, args, 1);
                if (o0.isNaN() || o1.isNaN()) {
                    return null;
                }
                return Boolean.valueOf(o0.equals(o1));
            }
        });

        define(new FunDefBase(
                "<>",
                "<String Expression> <> <String Expression>",
                "Returns whether two expressions are not equal.",
                "ibSS") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                String o0 = getStringArg(evaluator, args, 0, null);
                String o1 = getStringArg(evaluator, args, 1, null);
                if (o0 == null || o1 == null) {
                    return null;
                }
                return o0.equals(o1) ?
                        Boolean.FALSE :
                        Boolean.TRUE;
            }
        });

        define(new FunDefBase(
                "<>",
                "<Numeric Expression> <> <Numeric Expression>",
                "Returns whether two expressions are not equal.",
                "ibnn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Double o0 = getDoubleArg(evaluator, args, 0);
                Double o1 = getDoubleArg(evaluator, args, 1);
                if (o0.isNaN() || o1.isNaN()) {
                    return null;
                }
                return o0.equals(o1) ?
                        Boolean.FALSE :
                        Boolean.TRUE;
            }
        });

        define(new FunDefBase(
                "<",
                "<Numeric Expression> < <Numeric Expression>",
                "Returns whether an expression is less than another.",
                "ibnn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Double o0 = getDoubleArg(evaluator, args, 0);
                Double o1 = getDoubleArg(evaluator, args, 1);
                if (o0.isNaN() || o1.isNaN()) {
                    return null;
                }
                return Boolean.valueOf(o0.compareTo(o1) < 0);
            }
        });

        define(new FunDefBase(
                "<",
                "<String Expression> < <String Expression>",
                "Returns whether an expression is less than another.",
                "ibSS") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                String o0 = getStringArg(evaluator, args, 0, null);
                String o1 = getStringArg(evaluator, args, 1, null);
                if (o0 == null || o1 == null) {
                    return null;
                }
                return Boolean.valueOf(o0.compareTo(o1) < 0);
            }
        });

        define(new FunDefBase(
                "<=",
                "<Numeric Expression> <= <Numeric Expression>",
                "Returns whether an expression is less than or equal to another.",
                "ibnn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Double o0 = getDoubleArg(evaluator, args, 0);
                Double o1 = getDoubleArg(evaluator, args, 1);
                if (o0.isNaN() || o1.isNaN()) {
                    return null;
                }
                return Boolean.valueOf(o0.compareTo(o1) <= 0);
            }
        });

        define(new FunDefBase(
                "<=",
                "<String Expression> <= <String Expression>",
                "Returns whether an expression is less than or equal to another.",
                "ibSS") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                String o0 = getStringArg(evaluator, args, 0, null);
                String o1 = getStringArg(evaluator, args, 1, null);
                if (o0 == null || o1 == null) {
                    return null;
                }
                return Boolean.valueOf(o0.compareTo(o1) <= 0);
            }
        });

        define(new FunDefBase(
                ">",
                "<Numeric Expression> > <Numeric Expression>",
                "Returns whether an expression is greater than another.",
                "ibnn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Double o0 = getDoubleArg(evaluator, args, 0);
                Double o1 = getDoubleArg(evaluator, args, 1);
                if (o0.isNaN() || o1.isNaN()) {
                    return null;
                }
                return Boolean.valueOf(o0.compareTo(o1) > 0);
            }
        });

        define(new FunDefBase(
                ">",
                "<String Expression> > <String Expression>",
                "Returns whether an expression is greater than another.",
                "ibSS") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                String o0 = getStringArg(evaluator, args, 0, null);
                String o1 = getStringArg(evaluator, args, 1, null);
                if (o0 == null || o1 == null) {
                    return null;
                }
                return Boolean.valueOf(o0.compareTo(o1) > 0);
            }
        });

        define(new FunDefBase(
                ">=",
                "<Numeric Expression> >= <Numeric Expression>",
                "Returns whether an expression is greater than or equal to another.",
                "ibnn") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Double o0 = getDoubleArg(evaluator, args, 0);
                Double o1 = getDoubleArg(evaluator, args, 1);
                if (o0.isNaN() || o1.isNaN()) {
                    return null;
                }
                return Boolean.valueOf(o0.compareTo(o1) >= 0);
            }
        });

        define(new FunDefBase(
                ">=",
                "<String Expression> >= <String Expression>",
                "Returns whether an expression is greater than or equal to another.",
                "ibSS") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                String o0 = getStringArg(evaluator, args, 0, null);
                String o1 = getStringArg(evaluator, args, 1, null);
                if (o0 == null || o1 == null) {
                    return null;
                }
                return Boolean.valueOf(o0.compareTo(o1) >= 0);
            }
        });

        // NON-STANDARD FUNCTIONS

        define(new MultiResolver(
                "FirstQ",
                "FirstQ(<Set>[, <Numeric Expression>])",
                "Returns the 1st quartile value of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArg(evaluator, args, 1, valueFunCall);
                        //todo: ignore nulls, do we need to ignore the List?
                        return quartile(evaluator.push(), members, exp, 1);
                    }
                };
            }
        });

        define(new MultiResolver(
                "ThirdQ",
                "ThirdQ(<Set>[, <Numeric Expression>])",
                "Returns the 3rd quartile value of a numeric expression evaluated over a set.",
                new String[]{"fnx", "fnxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new FunDefBase(dummyFunDef) {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        List members = (List) getArg(evaluator, args, 0);
                        ExpBase exp = (ExpBase) getArg(evaluator, args, 1, valueFunCall);
                        //todo: ignore nulls, do we need to ignore the List?
                        return quartile(evaluator.push(), members, exp, 3);
                    }
                };
            }
        });
    }

    private static void appendMemberOrTuple(
            StringBuffer buf,
            Object memberOrTuple) {
        if (memberOrTuple instanceof Member) {
            Member member = (Member) memberOrTuple;
            buf.append(member.getUniqueName());
        } else {
            Member[] members = (Member[]) memberOrTuple;
            buf.append("(");
            for (int j = 0; j < members.length; j++) {
                if (j > 0) {
                    buf.append(", ");
                }
                Member member = members[j];
                buf.append(member.getUniqueName());
            }
            buf.append(")");
        }
    }

    /**
     * Returns a read-only version of the name-to-resolvers map. Used by the
     * testing framework.
     */
    protected static Map getNameToResolversMap() {
        return Collections.unmodifiableMap(((BuiltinFunTable)instance()).mapNameToResolvers);
    }
}

// End BuiltinFunTable.java
