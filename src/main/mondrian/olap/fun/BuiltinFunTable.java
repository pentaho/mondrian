/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 February, 2002
*/
package mondrian.olap.fun;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;
import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;
import mondrian.util.Format;

import java.util.*;

/**
 * <code>BuiltinFunTable</code> contains a list of all built-in MDX functions.
 *
 * @author jhyde
 * @since 26 February, 2002
 * @version $Id$
 **/
public class BuiltinFunTable extends FunTable {
	/** Maps the upper-case name of a function plus its {@link Syntax} to an
     * array of {@link Resolver}s for that name. **/
	private HashMap mapNameToResolvers;

	private static final Resolver[] emptyResolvers = new Resolver[0];
    private final Exp.Resolver dummyResolver = Util.createSimpleResolver(this);

    private Exp valueFunCall;
	private static final String months = "[Time].[1997].[Q1].[1]" + FunUtil.nl +
							"[Time].[1997].[Q1].[2]" + FunUtil.nl +
							"[Time].[1997].[Q1].[3]" + FunUtil.nl +
							"[Time].[1997].[Q2].[4]" + FunUtil.nl +
							"[Time].[1997].[Q2].[5]" + FunUtil.nl +
							"[Time].[1997].[Q2].[6]" + FunUtil.nl +
							"[Time].[1997].[Q3].[7]" + FunUtil.nl +
							"[Time].[1997].[Q3].[8]" + FunUtil.nl +
							"[Time].[1997].[Q3].[9]" + FunUtil.nl +
							"[Time].[1997].[Q4].[10]" + FunUtil.nl +
							"[Time].[1997].[Q4].[11]" + FunUtil.nl +
							"[Time].[1997].[Q4].[12]";
	private static final String quarters = "[Time].[1997].[Q1]" + FunUtil.nl +
							"[Time].[1997].[Q2]" + FunUtil.nl +
							"[Time].[1997].[Q3]" + FunUtil.nl +
							"[Time].[1997].[Q4]";
	private static final String year1997 = "[Time].[1997]";
	private final HashSet reservedWords = new HashSet();
	private static final Resolver[] emptyResolverArray = new Resolver[0];

    /**
	 * Creates a <code>BuiltinFunTable</code>. This method should only be
	 * called from {@link FunTable#instance}.
	 **/
	public BuiltinFunTable() {
		init();
        valueFunCall = new FunCall("_Value", Syntax.Function, new Exp[0])
                .resolve(dummyResolver);
	}

    private static String makeResolverKey(String name, Syntax syntax) {
        return name.toUpperCase() + "$" + syntax;
    }

	/** Calls {@link #defineFunctions} to load function definitions into a
	 * List, then indexes that collection. **/
	private void init() {
		resolvers = new ArrayList();
		defineFunctions();
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
		resolvers.add(resolver);
	}

	static Syntax decodeSyntacticType(String flags) {
		char c = flags.charAt(0);
		switch (c) {
		case 'p':
			return Syntax.Property;
		case 'f':
			return Syntax.Function;
		case 'm':
			return Syntax.Method;
		case 'i':
			return Syntax.Infix;
		case 'P':
			return Syntax.Prefix;
		case 'I':
			return Syntax.Internal;
		default:
			throw Util.newInternal(
					"unknown syntax code '" + c + "' in string '" + flags + "'");
		}
	}

	static int decodeReturnType(String flags) {
		final int returnType = decodeType(flags, 1);
		if ((returnType & Category.Mask) != returnType) {
			throw Util.newInternal("bad return code flag in flags '" + flags + "'");
		}
		return returnType;
	}

	static int decodeType(String flags, int offset) {
		char c = flags.charAt(offset);
		switch (c) {
		case 'a':
			return Category.Array;
		case 'd':
			return Category.Dimension;
		case 'h':
			return Category.Hierarchy;
		case 'l':
			return Category.Level;
		case 'b':
			return Category.Logical;
		case 'm':
			return Category.Member;
		case 'N':
			return Category.Numeric | Category.Constant;
		case 'n':
			return Category.Numeric;
		case 'x':
			return Category.Set;
		case '#':
			return Category.String | Category.Constant;
		case 'S':
			return Category.String;
		case 't':
			return Category.Tuple;
		case 'v':
			return Category.Value;
		case 'y':
			return Category.Symbol;
		default:
			throw Util.newInternal(
					"unknown type code '" + c + "' in string '" + flags + "'");
		}
	}

	/**
	 * Converts an argument to a parameter type.
	 */
	public Exp convert(Exp fromExp, int to, Exp.Resolver resolver) {
		Exp exp = convert_(fromExp, to);
		if (exp == null) {
			throw Util.newInternal("cannot convert " + fromExp + " to " + to);
		}
		return resolver.resolveChild(exp);
	}

	private static Exp convert_(Exp fromExp, int to) {
		int from = fromExp.getType();
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
				// "<Dimension>.CurrentMember"
				return new FunCall("CurrentMember", Syntax.Property, new Exp[]{fromExp});
			default:
				return null;
			}
		case Category.Hierarchy:
			switch (to) {
			case Category.Dimension:
				// "<Hierarchy>.Dimension"
				return new FunCall("Dimension", Syntax.Property, new Exp[]{fromExp});
			default:
				return null;
			}
		case Category.Level:
			switch (to) {
			case Category.Dimension:
				// "<Level>.Dimension"
				return new FunCall("Dimension", Syntax.Property, new Exp[]{fromExp});
			case Category.Hierarchy:
				// "<Level>.Hierarchy"
				return new FunCall("Hierarchy", Syntax.Property, new Exp[]{fromExp});
			default:
				return null;
			}
		case Category.Logical:
			return null;
		case Category.Member:
			switch (to) {
			case Category.Dimension:
				// "<Member>.Dimension"
				return new FunCall("Dimension", Syntax.Property, new Exp[]{fromExp});
			case Category.Hierarchy:
				// "<Member>.Hierarchy"
				return new FunCall("Hierarchy", Syntax.Property, new Exp[]{fromExp});
			case Category.Level:
				// "<Member>.Level"
				return new FunCall("Level", Syntax.Property, new Exp[]{fromExp});
			case Category.Numeric | Category.Constant:
			case Category.String | Category.Constant: //todo: assert is a string member
				// "<Member>.Value"
				return new FunCall("Value", Syntax.Property, new Exp[]{fromExp});
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
				return new FunCall("_Value", Syntax.Function, new Exp[] {fromExp});
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
				return new FunCall("_Value", Syntax.Function, new Exp[] {fromExp});
			default:
				return null;
			}
		case Category.Tuple:
			switch (to) {
			case Category.Value:
				return fromExp;
			case Category.Numeric:
			case Category.String:
				return new FunCall("_Value", Syntax.Function, new Exp[] {fromExp});
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
		int from = fromExp.getType();
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
					to == Category.Member) {
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
					to == Category.Numeric) {
				conversionCount[0]++;
				return true;
			} else if (to == Category.Value ||
					to == (Category.Numeric | Category.Expression) ||
					to == (Category.String | Category.Expression)) {
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

	static int[] decodeParameterTypes(String flags) {
		int[] parameterTypes = new int[flags.length() - 2];
		for (int i = 0; i < parameterTypes.length; i++) {
			parameterTypes[i] = decodeType(flags, i + 2);
		}
		return parameterTypes;
	}

	public FunDef getDef(FunCall call, Exp.Resolver resolver) {
        String key = makeResolverKey(call.getFunName(), call.getSyntax());

		// Resolve function by its upper-case name first.  If there is only one
		// function with that name, stop immediately.  If there is more than
		// function, use some custom method, which generally involves looking
		// at the type of one of its arguments.
        String signature = call.getSyntax().getSignature(call.getFunName(),
                Category.Unknown, ExpBase.getTypes(call.args));
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
			FunDef def = resolvers[i].resolve(call.args, conversionCount);
			if (def != null) {
                if (def.getReturnType() == Category.Set &&
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
			throw Util.newInternal("no function matches signature '" +
                    signature + "'");
		case 1:
            final String matchKey = makeResolverKey(matchDef.getName(),
                    matchDef.getSyntax());
            Util.assertTrue(matchKey.equals(key), matchKey);
			return matchDef;
		default:
			throw Util.newInternal(
                    "more than one function matches signature '" + signature +
					"'");
		}
	}

    public boolean requiresExpression(FunCall call, int k,
            Exp.Resolver resolver) {
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
	 * Defines a set of reserved words.
	 */
	public void defineReserved(String[] a) {
		for (int i = 0; i < a.length; i++) {
			defineReserved(a[i]);
		}
	}

	/**
	 * Defines every name in an enumeration as a reserved word.
	 */
	public void defineReserved(EnumeratedValues values) {
		defineReserved(values.getNames());
	}

	/**
	 * Derived class can override this method to add more functions.
	 **/
	protected void defineFunctions() {
		defineReserved("NULL");

		// first char: p=Property, m=Method, i=Infix, P=Prefix
		// 2nd:

		// ARRAY FUNCTIONS
		if (false) define(new FunDefBase("SetToArray", "SetToArray(<Set>[, <Set>]...[, <Numeric Expression>])", "Converts one or more sets to an array for use in a user-if (false) defined function.", "fa*"));
		//
		// DIMENSION FUNCTIONS
		define(new FunDefBase("Dimension", "<Hierarchy>.Dimension", "Returns the dimension that contains a specified hierarchy.", "pdh") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
				return hierarchy.getDimension();
			}
		});

		//??Had to add this to get <Hierarchy>.Dimension to work?
		define(new FunDefBase("Dimension", "<Dimension>.Dimension", "Returns the dimension that contains a specified hierarchy.", "pdd") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Dimension dimension = getDimensionArg(evaluator, args, 0, true);
				return dimension;
			}

			public void testDimensionHierarchy(FoodMartTestCase test) {
				String s = test.executeExpr("[Time].Dimension.Name");
                Assert.assertEquals("Time", s);
			}
		});

		define(new FunDefBase("Dimension", "<Level>.Dimension", "Returns the dimension that contains a specified level.", "pdl") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Level level = getLevelArg(evaluator, args, 0, true);
				return level.getDimension();
			}

			public void testLevelDimension(FoodMartTestCase test) {
				String s = test.executeExpr("[Time].[Year].Dimension");
                Assert.assertEquals("[Time]", s);
			}
		});

		define(new FunDefBase("Dimension", "<Member>.Dimension", "Returns the dimension that contains a specified member.", "pdm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return member.getDimension();
			}

			public void testMemberDimension(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Time].[1997].[Q2].Dimension");
                Assert.assertEquals("[Time]", s);
			}
		});

		define(new FunDefBase("Dimensions", "Dimensions(<Numeric Expression>)", "Returns the dimension whose zero-based position within the cube is specified by a numeric expression.", "fdn") {
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

			public void testDimensionsNumeric(FoodMartTestCase test) {
				String s = test.executeExpr("Dimensions(2).Name");
                Assert.assertEquals("Store", s);
			}
		});
		define(new FunDefBase("Dimensions", "Dimensions(<String Expression>)", "Returns the dimension whose name is specified by a string.", "fdS") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				String defValue = "Default Value";
				String s = getStringArg(evaluator, args, 0, defValue);
				if (s.indexOf("[") == -1) {
					s = Util.quoteMdxIdentifier(s);
				}
				OlapElement o = lookupCompound(evaluator.getSchemaReader(),
						evaluator.getCube(), explode(s), false, Category.Dimension);
				if (o instanceof Dimension) {
					return (Dimension) o;
				} else if (o == null) {
					throw newEvalException(this, "Dimension '" + s + "' not found");
				} else {
					throw newEvalException(this, "Dimensions(" + s + ") found " + o);
				}
			}

			public void testDimensionsString(FoodMartTestCase test) {
				String s = test.executeExpr(
						"Dimensions(\"Store\").UniqueName");
                Assert.assertEquals("[Store]", s);
			}
		});

		//
		// HIERARCHY FUNCTIONS
		define(new FunDefBase("Hierarchy", "<Level>.Hierarchy", "Returns a level's hierarchy.", "phl") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Level level = getLevelArg(evaluator, args, 0, true);
				return level.getHierarchy();
			}
		});
		define(new FunDefBase("Hierarchy", "<Member>.Hierarchy", "Returns a member's hierarchy.", "phm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return member.getHierarchy();
			}

			public void testTime(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Time].[1997].[Q1].[1].Hierarchy");
                Assert.assertEquals("[Time]", s);
			}

			public void testBasic9(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Gender].[All Gender].[F].Hierarchy");
                Assert.assertEquals("[Gender]", s);
			}

			public void testFirstInLevel9(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Education Level].[All Education Levels].[Bachelors Degree].Hierarchy");
                Assert.assertEquals("[Education Level]", s);
			}

			public void testHierarchyAll(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Gender].[All Gender].Hierarchy");
                Assert.assertEquals("[Gender]", s);
			}

			public void testHierarchyNull(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Gender].[All Gender].Parent.Hierarchy");
                Assert.assertEquals("[Gender]", s); // MSOLAP gives "#ERR"
			}
		});

		//
		// LEVEL FUNCTIONS
		define(new FunDefBase("Level", "<Member>.Level", "Returns a member's level.", "plm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return member.getLevel();
			}

			public void testMemberLevel(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Time].[1997].[Q1].[1].Level.UniqueName");
                Assert.assertEquals("[Time].[Month]", s);
			}
		});

		define(new FunDefBase("Levels", "<Hierarchy>.Levels(<Numeric Expression>)", "Returns the level whose position in a hierarchy is specified by a numeric expression.", "mlhn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
				Level[] levels = hierarchy.getLevels();

				int n = getIntArg(evaluator, args, 1);
				if ((n > levels.length) || (n < 1)) {
					throw newEvalException(
							this, "Index '" + n + "' out of bounds");
				}
				return levels[n - 1];
			}

			public void testLevelsNumeric(FoodMartTestCase test) {
				String s = test.executeExpr("[Time].Levels(2).Name");
                Assert.assertEquals("Quarter", s);
			}
			public void testLevelsTooSmall(FoodMartTestCase test) {
				test.assertExprThrows(
						"[Time].Levels(0).Name",
						"Index '0' out of bounds");
			}
			public void testLevelsTooLarge(FoodMartTestCase test) {
				test.assertExprThrows(
						"[Time].Levels(8).Name",
						"Index '8' out of bounds");
			}
		});

		define(new FunDefBase("Levels", "Levels(<String Expression>)", "Returns the level whose name is specified by a string expression.", "flS") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				String s = getStringArg(evaluator, args, 0, null);
				Cube cube = evaluator.getCube();
				OlapElement o = null;
				if (s.startsWith("[")) {
					o = lookupCompound(evaluator.getSchemaReader(), cube, explode(s), false, Category.Level);
				} else {
					// lookupCompound barfs if "s" doesn't have matching
					// brackets, so don't even try
					o = null;
				}
				if (o instanceof Level) {
					return (Level) o;
				} else if (o == null) {
					throw newEvalException(this, "Level '" + s + "' not found");
				} else {
					throw newEvalException(this, "Levels('" + s + "') found " + o);
				}
			}

			public void testLevelsString(FoodMartTestCase test) {
				String s = test.executeExpr(
						"Levels(\"[Time].[Year]\").UniqueName");
                Assert.assertEquals("[Time].[Year]", s);
			}

			public void testLevelsStringFail(FoodMartTestCase test) {
				test.assertExprThrows(
						"Levels(\"nonexistent\").UniqueName",
						"Level 'nonexistent' not found");
			}
		});

		//
		// LOGICAL FUNCTIONS
		define(new FunkResolver("IsEmpty", "IsEmpty(<Value Expression>)", "Determines if an expression evaluates to the empty cell value.",
				new String[] {"fbS", "fbn"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Object o = getScalarArg(evaluator, args, 0);
						if (o == Util.nullValue) {
							return Boolean.TRUE;
						} else {
							return Boolean.FALSE;
						}
					}
					public void testIsEmpty(FoodMartTestCase test) {
						test.runQueryCheckResult(
								"WITH MEMBER [Measures].[Foo] AS 'Iif(IsEmpty([Measures].[Unit Sales]), 5, [Measures].[Unit Sales])'" + nl +
								"SELECT {[Store].[USA].[WA].children} on columns" + nl +
								"FROM Sales" + nl +
								"WHERE ( [Time].[1997].[Q4].[12]," + nl +
								" [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer]," + nl +
								" [Measures].[Foo])",
								"Axis #0:" + nl +
								"{[Time].[1997].[Q4].[12], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer], [Measures].[Foo]}" + nl +
								"Axis #1:" + nl +
								"{[Store].[All Stores].[USA].[WA].[Bellingham]}" + nl +
								"{[Store].[All Stores].[USA].[WA].[Bremerton]}" + nl +
								"{[Store].[All Stores].[USA].[WA].[Seattle]}" + nl +
								"{[Store].[All Stores].[USA].[WA].[Spokane]}" + nl +
								"{[Store].[All Stores].[USA].[WA].[Tacoma]}" + nl +
								"{[Store].[All Stores].[USA].[WA].[Walla Walla]}" + nl +
								"{[Store].[All Stores].[USA].[WA].[Yakima]}" + nl +
								"Row #0: 5" + nl +
								"Row #0: 5" + nl +
								"Row #0: 2" + nl +
								"Row #0: 5" + nl +
								"Row #0: 11" + nl +
								"Row #0: 5" + nl +
								"Row #0: 4" + nl);
					}
				}));

		define(new FunDefBase("IsEmpty", "IsEmpty(<Value Expression>)", "Determines if an expression evaluates to the empty cell value.", "fbn"));
		//
		// MEMBER FUNCTIONS
		define(new FunDefBase("Ancestor", "Ancestor(<Member>, <Level>)", "Returns the ancestor of a member at a specified level.", "fmml") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, false);
				Level level = getLevelArg(evaluator, args, 1, false);
				if (member.getHierarchy() != level.getHierarchy()) {
					throw newEvalException(
							this,
							"member '" + member +
							"' is not in the same hierarchy as level '" +
							level + "'");
				}
				if (member.getLevel().equals(level)) {
					return member;
				}
				Member[] members = member.getAncestorMembers();
				for (int i = 0; i < members.length; i++) {
					if (members[i].getLevel().equals(level))
						return members[i];
				}
				return member.getHierarchy().getNullMember(); // not found
			}

			public void testAncestor(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Ancestor([Store].[USA].[CA].[Los Angeles],[Store Country])");
                Assert.assertEquals("USA", member.getName());
			}

			public void testAncestorHigher(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Ancestor([Store].[USA],[Store].[Store City])");
                Assert.assertNull(member); // MSOLAP returns null
			}

			public void testAncestorSameLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Ancestor([Store].[Canada],[Store].[Store Country])");
                Assert.assertEquals("Canada", member.getName());
			}

			public void testAncestorWrongHierarchy(FoodMartTestCase test) {
				// MSOLAP gives error "Formula error - dimensions are not
				// valid (they do not match) - in the Ancestor function"
				test.assertAxisThrows(
						"Ancestor([Gender].[M],[Store].[Store Country])",
						"member '[Gender].[All Gender].[M]' is not in the same hierarchy as level '[Store].[Store Country]'");
			}

			public void testAncestorAllLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Ancestor([Store].[USA].[CA],[Store].Levels(1))");
                Assert.assertTrue(member.isAll());
			}
		});

		define(new FunDefBase("ClosingPeriod", "ClosingPeriod([<Level>[, <Member>]])", "Returns the last sibling among the descendants of a member at a level.", "fm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Cube cube = evaluator.getCube();
				Dimension timeDimension = cube.getYearLevel().getDimension();
				Member member = evaluator.getContext(timeDimension);
				Level level = member.getLevel().getChildLevel();
				return openClosingPeriod(evaluator, this, member, level);
			}
			public void testClosingPeriodNoArgs(FoodMartTestCase test) {
				// MSOLAP returns [1997].[Q4], because [Time].CurrentMember =
				// [1997].
				Member member = test.executeAxis("ClosingPeriod()");
                Assert.assertEquals("[Time].[1997].[Q4]", member.getUniqueName());
			}
		});
		define(new FunDefBase("ClosingPeriod", "ClosingPeriod([<Level>[, <Member>]])", "Returns the last sibling among the descendants of a member at a level.", "fml") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Cube cube = evaluator.getCube();
				Dimension timeDimension = cube.getYearLevel().getDimension();
				Member member = evaluator.getContext(timeDimension);
				Level level = getLevelArg(evaluator, args, 0, true);
				return openClosingPeriod(evaluator, this, member, level);
			}
			public void testClosingPeriodLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"ClosingPeriod([Month])");
                Assert.assertEquals("[Time].[1997].[Q4].[12]", member.getUniqueName());
			}
			public void testClosingPeriodLevelNotInTimeFails(FoodMartTestCase test) {
				test.assertAxisThrows(
						"ClosingPeriod([Store].[Store City])",
						"member '[Time].[1997]' must be in same hierarchy as level '[Store].[Store City]'");
			}
		});
		define(new FunDefBase("ClosingPeriod", "ClosingPeriod([<Level>[, <Member>]])", "Returns the last sibling among the descendants of a member at a level.", "fmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				Level level = member.getLevel().getChildLevel();
				return openClosingPeriod(evaluator, this, member, level);
			}
			public void testClosingPeriodMember(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"ClosingPeriod([USA])");
                Assert.assertEquals("WA", member.getName());
			}
            public void testClosingPeriodMemberLeaf(FoodMartTestCase test) {
                Member member = test.executeAxis(
                        "ClosingPeriod([Time].[1997].[Q3].[8])");
                Assert.assertNull(member);
            }
		});
		define(new FunDefBase("ClosingPeriod", "ClosingPeriod([<Level>[, <Member>]])", "Returns the last sibling among the descendants of a member at a level.", "fmlm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Level level = getLevelArg(evaluator, args, 0, true);
				Member member = getMemberArg(evaluator, args, 1, true);
				return openClosingPeriod(evaluator, this, member, level);
			}
			public void testClosingPeriod(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"ClosingPeriod([Month],[1997])");
                Assert.assertEquals("[Time].[1997].[Q4].[12]", member.getUniqueName());
			}
			public void testClosingPeriodBelow(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"ClosingPeriod([Quarter],[1997].[Q3].[8])");
                Assert.assertNull(member);
			}
		});

		define(new FunDefBase("Cousin", "Cousin(<Member1>, <Member2>)", "Returns the member with the same relative position under a member as the member specified.", "fmmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member1 = getMemberArg(evaluator, args, 0, true);
				Member member2 = getMemberArg(evaluator, args, 1, true);
				Member cousin = cousin(evaluator.getSchemaReader(), member1, member2);
				if (cousin == null) {
					cousin = member1.getHierarchy().getNullMember();
				}
				return cousin;
			}
			private Member cousin(SchemaReader schemaReader, Member member1, Member member2) {
				if (member1.getHierarchy() != member2.getHierarchy()) {
					throw newEvalException(
							this,
							"Members '" + member1 + "' and '" + member2 +
							"' are not compatible as cousins");
				}
				if (member1.getLevel().getDepth() < member2.getLevel().getDepth()) {
					return null;
				}
				return cousin2(schemaReader, member1, member2);
			}
			private Member cousin2(SchemaReader schemaReader, Member member1, Member member2) {
				if (member1.getLevel() == member2.getLevel()) {
					return member2;
				}
				Member uncle = cousin2(schemaReader, member1.getParentMember(), member2);
				if (uncle == null) {
					return null;
				}
				int ordinal = Util.getMemberOrdinalInParent(schemaReader, member1);
				Member[] cousins = schemaReader.getMemberChildren(uncle);
				if (cousins.length <= ordinal) {
					return null;
				}
				return cousins[ordinal];
			}

			public void testCousin1(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Cousin([1997].[Q4],[1998])");
                Assert.assertEquals("[Time].[1998].[Q4]", member.getUniqueName());
			}

			public void testCousin2(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Cousin([1997].[Q4].[12],[1998].[Q1])");
                Assert.assertEquals("[Time].[1998].[Q1].[3]", member.getUniqueName());
			}

			public void testCousinOverrun(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Cousin([Customers].[USA].[CA].[San Jose], [Customers].[USA].[OR])");
				// CA has more cities than OR
                Assert.assertNull(member);
			}

			public void testCousinThreeDown(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Cousin([Customers].[USA].[CA].[Berkeley].[Alma Shelton], [Customers].[Mexico])");
				// Alma Shelton is the 3rd child
				// of the 4th child (Berkeley)
				// of the 1st child (CA)
				// of USA
                Assert.assertEquals("[Customers].[All Customers].[Mexico].[DF].[Tixapan].[Albert Clouse]", member.getUniqueName());
			}

			public void testCousinSameLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Cousin([Gender].[M], [Gender].[F])");
                Assert.assertEquals("F", member.getName());
			}

			public void testCousinHigherLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Cousin([Time].[1997], [Time].[1998].[Q1])");
                Assert.assertNull(member);
			}

			public void testCousinWrongHierarchy(FoodMartTestCase test) {
				test.assertAxisThrows(
						"Cousin([Time].[1997], [Gender].[M])",
						"Members '[Time].[1997]' and '[Gender].[All Gender].[M]' are not compatible as cousins");
			}
		});
		define(new FunDefBase("CurrentMember", "<Dimension>.CurrentMember", "Returns the current member along a dimension during an iteration.", "pmd") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Dimension dimension = getDimensionArg(evaluator, args, 0, true);
				return evaluator.getContext(dimension);
			}
            public void testCurrentMemberFromSlicer(FoodMartTestCase test) {
				Result result = test.runQuery(
						"with member [Measures].[Foo] as '[Gender].CurrentMember.Name'" + nl +
						"select {[Measures].[Foo]} on columns" + nl +
						"from Sales where ([Gender].[F])");
                Assert.assertEquals("F", result.getCell(new int[] {0}).getValue());
			}
			public void testCurrentMemberFromDefaultMember(FoodMartTestCase test) {
				Result result = test.runQuery(
						"with member [Measures].[Foo] as '[Time].CurrentMember.Name'" + nl +
						"select {[Measures].[Foo]} on columns" + nl +
						"from Sales");
                Assert.assertEquals("1997", result.getCell(new int[] {0}).getValue());
			}
			public void testCurrentMemberFromAxis(FoodMartTestCase test) {
				Result result = test.runQuery(
						"with member [Measures].[Foo] as '[Gender].CurrentMember.Name || [Marital Status].CurrentMember.Name'" + nl +
						"select {[Measures].[Foo]} on columns," + nl +
						" CrossJoin({[Gender].children}, {[Marital Status].children}) on rows" + nl +
						"from Sales");
                Assert.assertEquals("FM", result.getCell(new int[] {0,0}).getValue());
			}
			/**
			 * When evaluating a calculated member, MSOLAP regards that
			 * calculated member as the current member of that dimension, so it
			 * cycles in this case. But I disagree; it is the previous current
			 * member, before the calculated member was expanded.
			 */
			public void testCurrentMemberInCalcMember(FoodMartTestCase test) {
				Result result = test.runQuery(
						"with member [Measures].[Foo] as '[Measures].CurrentMember.Name'" + nl +
						"select {[Measures].[Foo]} on columns" + nl +
						"from Sales");
                Assert.assertEquals("Unit Sales", result.getCell(new int[] {0}).getValue());
			}
		});
		define(new FunDefBase("DefaultMember", "<Dimension>.DefaultMember", "Returns the default member of a dimension.", "pmd") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Dimension dimension = getDimensionArg(evaluator, args, 0, true);
				return evaluator.getSchemaReader().getHierarchyDefaultMember(
						dimension.getHierarchy());
			}

			public void testDimensionDefaultMember(FoodMartTestCase test) {
				Member member = test.executeAxis("[Measures].DefaultMember");
                Assert.assertEquals("Unit Sales", member.getName());
			}
		});

		define(new FunDefBase("FirstChild", "<Member>.FirstChild", "Returns the first child of a member.", "pmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				Member[] children = evaluator.getSchemaReader().getMemberChildren(member);
				if (children.length == 0) {
					return member.getHierarchy().getNullMember();
				} else {
					return children[0];
				}
			}

			public void testFirstChildFirstInLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Time].[1997].[Q4].FirstChild");
                Assert.assertEquals("10", member.getName());
			}

			public void testFirstChildAll(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[All Gender].FirstChild");
                Assert.assertEquals("F", member.getName());
			}

			public void testFirstChildOfChildless(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[All Gender].[F].FirstChild");
                Assert.assertNull(member);
			}
		});

		define(new FunDefBase("FirstSibling", "<Member>.FirstSibling", "Returns the first child of the parent of a member.", "pmm") {
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

			public void testFirstSiblingFirstInLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[F].FirstSibling");
                Assert.assertEquals("F", member.getName());
			}

			public void testFirstSiblingLastInLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Time].[1997].[Q4].FirstSibling");
                Assert.assertEquals("Q1", member.getName());
			}

			public void testFirstSiblingAll(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[All Gender].FirstSibling");
                Assert.assertTrue(member.isAll());
			}

			public void testFirstSiblingRoot(FoodMartTestCase test) {
				// The [Measures] hierarchy does not have an 'all' member, so
				// [Unit Sales] does not have a parent.
				Member member = test.executeAxis(
						"[Measures].[Store Sales].FirstSibling");
                Assert.assertEquals("Unit Sales", member.getName());
			}

			public void testFirstSiblingNull(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[F].FirstChild.FirstSibling");
                Assert.assertNull(member);
			}
		});

		if (false) define(new FunDefBase("Item", "<Tuple>.Item(<Numeric Expression>)", "Returns a member from a tuple.", "mm*"));

		define(new FunkResolver(
				"Lag", "<Member>.Lag(<Numeric Expression>)", "Returns a member further along the specified member's dimension.",
				new String[]{"mmmn"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Member member = getMemberArg(evaluator, args, 0, true);
						int n = getIntArg(evaluator, args, 1);
						return evaluator.getSchemaReader().getLeadMember(member, -n);
					}

					public void testLag(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Time].[1997].[Q4].[12].Lag(4)");
                        Assert.assertEquals("8", member.getName());
					}

					public void testLagFirstInLevel(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Gender].[F].Lag(1)");
						Assert.assertNull(member);
					}

					public void testLagAll(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Gender].DefaultMember.Lag(2)");
						Assert.assertNull(member);
					}

					public void testLagRoot(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Time].[1998].Lag(1)");
						Assert.assertEquals("1997", member.getName());
					}

					public void testLagRootTooFar(FoodMartTestCase test) {
						Member member = test.executeAxis("[Time].[1998].Lag(2)");
						Assert.assertNull(member);
					}
				}));

		define(new FunDefBase("LastChild", "<Member>.LastChild", "Returns the last child of a member.", "pmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				Member[] children = evaluator.getSchemaReader().getMemberChildren(member);
				if (children.length == 0) {
					return member.getHierarchy().getNullMember();
				} else {
					return children[children.length - 1];
				}
			}

			public void testLastChild(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].LastChild");
				Assert.assertEquals("M", member.getName());
			}

			public void testLastChildLastInLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Time].[1997].[Q4].LastChild");
				Assert.assertEquals("12", member.getName());
			}

			public void testLastChildAll(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[All Gender].LastChild");
				Assert.assertEquals("M", member.getName());
			}

			public void testLastChildOfChildless(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[M].LastChild");
				Assert.assertNull(member);
			}
		});

		define(new FunDefBase("LastSibling", "<Member>.LastSibling", "Returns the last child of the parent of a member.", "pmm") {
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
				return children[children.length - 1];
			}

			public void testLastSibling(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[F].LastSibling");
				Assert.assertEquals("M", member.getName());
			}

			public void testLastSiblingFirstInLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Time].[1997].[Q1].LastSibling");
				Assert.assertEquals("Q4", member.getName());
			}

			public void testLastSiblingAll(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[All Gender].LastSibling");
                Assert.assertTrue(member.isAll());
			}

			public void testLastSiblingRoot(FoodMartTestCase test) {
				// The [Time] hierarchy does not have an 'all' member, so
				// [1997], [1998] do not have parents.
				Member member = test.executeAxis(
						"[Time].[1998].LastSibling");
				Assert.assertEquals("1998", member.getName());
			}

			public void testLastSiblingNull(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[F].FirstChild.LastSibling");
				Assert.assertNull(member);
			}
		});

		define(new FunkResolver(
				"Lead", "<Member>.Lead(<Numeric Expression>)", "Returns a member further along the specified member's dimension.",
				new String[]{"mmmn"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Member member = getMemberArg(evaluator, args, 0, true);
						int n = getIntArg(evaluator, args, 1);
						return evaluator.getSchemaReader().getLeadMember(member, n);
					}

					public void testLead(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Time].[1997].[Q2].[4].Lead(4)");
						Assert.assertEquals("8", member.getName());
					}

					public void testLeadNegative(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Gender].[M].Lead(-1)");
						Assert.assertEquals("F", member.getName());
					}

					public void testLeadLastInLevel(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Gender].[M].Lead(3)");
						Assert.assertNull(member);
					}
				}));

		define(new FunDefBase("Members", "Members(<String Expression>)", "Returns the member whose name is specified by a string expression.", "fmS"));

		define(new FunDefBase(
				"NextMember", "<Member>.NextMember", "Returns the next member in the level that contains a specified member.", "pmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return evaluator.getSchemaReader().getLeadMember(member, +1);
			}

			public void testBasic2(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Gender].[F].NextMember} ON COLUMNS from Sales");
                Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("M"));
			}

			public void testFirstInLevel2(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Gender].[M].NextMember} ON COLUMNS from Sales");
                Assert.assertTrue(result.getAxes()[0].positions.length == 0);
			}

			public void testAll2(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Gender].PrevMember} ON COLUMNS from Sales");
				// previous to [Gender].[All] is null, so no members are returned
                Assert.assertTrue(result.getAxes()[0].positions.length == 0);
			}
		});

		if (false) define(new FunDefBase("OpeningPeriod", "OpeningPeriod([<Level>[, <Member>]])", "Returns the first sibling among the descendants of a member at a level.", "fm*"));
		if (false) define(new FunDefBase("ParallelPeriod", "ParallelPeriod([<Level>[, <Numeric Expression>[, <Member>]]])", "Returns a member from a prior period in the same relative position as a specified member.", "fm*"));

		define(new FunDefBase("Parent", "<Member>.Parent", "Returns the parent of a member.", "pmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				Member parent = member.getParentMember();
				if (parent == null ||
						!evaluator.getSchemaReader().getRole().canAccess(parent)) {
					parent = member.getHierarchy().getNullMember();
				}
				return parent;
			}

			public void testBasic5(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select{ [Product].[All Products].[Drink].Parent} on columns from Sales");
                Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("All Products"));
			}

			public void testFirstInLevel5(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Time].[1997].[Q2].[4].Parent} on columns,{[Gender].[M]} on rows from Sales");
                Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Q2"));
			}

			public void testAll5(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Time].[1997].[Q2].Parent} on columns,{[Gender].[M]} on rows from Sales");
				// previous to [Gender].[All] is null, so no members are returned
                Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("1997"));
			}
		});

		define(new FunDefBase("PrevMember", "<Member>.PrevMember", "Returns the previous member in the level that contains a specified member.", "pmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return evaluator.getSchemaReader().getLeadMember(member, -1);
			}

			public void testBasic(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Gender].[M].PrevMember} ON COLUMNS from Sales");
                Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("F"));
			}

			public void testFirstInLevel(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Gender].[F].PrevMember} ON COLUMNS from Sales");
                Assert.assertTrue(result.getAxes()[0].positions.length == 0);
			}

			public void testAll(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Gender].PrevMember} ON COLUMNS from Sales");
				// previous to [Gender].[All] is null, so no members are returned
                Assert.assertTrue(result.getAxes()[0].positions.length == 0);
			}
		});
		if (false) define(new FunDefBase("ValidMeasure", "ValidMeasure(<Tuple>)", "Returns a valid measure in a virtual cube by forcing inapplicable dimensions to their top level.", "fm*"));
		//
		// NUMERIC FUNCTIONS
		define(new FunkResolver("Aggregate", "Aggregate(<Set>[, <Numeric Expression>])", "Returns a calculated value using the appropriate aggregate function, based on the context of the query.",
				new String[] {"fnx", "fnxn"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						List members = (List) getArg(evaluator, args, 0);
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
					public void testAggregate(FoodMartTestCase test) {
						test.runQueryCheckResult(
								"WITH MEMBER [Store].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})'" + nl +
								"SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS," + nl +
								"      {[Store].[USA].[CA], [Store].[USA].[OR], [Store].[CA plus OR]} ON ROWS" + nl +
								"FROM Sales" + nl +
								"WHERE ([1997].[Q1])",
								"Axis #0:" + nl +
								"{[Time].[1997].[Q1]}" + nl +
								"Axis #1:" + nl +
								"{[Measures].[Unit Sales]}" + nl +
								"{[Measures].[Store Sales]}" + nl +
								"Axis #2:" + nl +
								"{[Store].[All Stores].[USA].[CA]}" + nl +
								"{[Store].[All Stores].[USA].[OR]}" + nl +
								"{[Store].[CA plus OR]}" + nl +
								"Row #0: 16,890" + nl +
								"Row #0: 36,175.20" + nl +
								"Row #1: 19,287" + nl +
								"Row #1: 40,170.29" + nl +
								"Row #2: 36,177" + nl +
								"Row #2: 76,345.49" + nl);
					}
					public void testAggregate2(FoodMartTestCase test) {
						test.runQueryCheckResult(
								"WITH" + nl +
								"  MEMBER [Time].[1st Half Sales] AS 'Aggregate({Time.[1997].[Q1], Time.[1997].[Q2]})'" + nl +
								"  MEMBER [Time].[2nd Half Sales] AS 'Aggregate({Time.[1997].[Q3], Time.[1997].[Q4]})'" + nl +
								"  MEMBER [Time].[Difference] AS 'Time.[2nd Half Sales] - Time.[1st Half Sales]'" + nl +
								"SELECT" + nl +
								"   { [Store].[Store State].Members} ON COLUMNS," + nl +
								"   { Time.[1st Half Sales], Time.[2nd Half Sales], Time.[Difference]} ON ROWS" + nl +
								"FROM Sales" + nl +
								"WHERE [Measures].[Store Sales]",
								"Axis #0:" + nl +
								"{[Measures].[Store Sales]}" + nl +
								"Axis #1:" + nl +
								"{[Store].[All Stores].[Canada].[BC]}" + nl +
								"{[Store].[All Stores].[Mexico].[DF]}" + nl +
								"{[Store].[All Stores].[Mexico].[Guerrero]}" + nl +
								"{[Store].[All Stores].[Mexico].[Jalisco]}" + nl +
								"{[Store].[All Stores].[Mexico].[Veracruz]}" + nl +
								"{[Store].[All Stores].[Mexico].[Yucatan]}" + nl +
								"{[Store].[All Stores].[Mexico].[Zacatecas]}" + nl +
								"{[Store].[All Stores].[USA].[CA]}" + nl +
								"{[Store].[All Stores].[USA].[OR]}" + nl +
								"{[Store].[All Stores].[USA].[WA]}" + nl +
								"Axis #2:" + nl +
								"{[Time].[1st Half Sales]}" + nl +
								"{[Time].[2nd Half Sales]}" + nl +
								"{[Time].[Difference]}" + nl +
								"Row #0: (null)" + nl +
								"Row #0: (null)" + nl +
								"Row #0: (null)" + nl +
								"Row #0: (null)" + nl +
								"Row #0: (null)" + nl +
								"Row #0: (null)" + nl +
								"Row #0: (null)" + nl +
								"Row #0: 74,571.95" + nl +
								"Row #0: 71,943.17" + nl +
								"Row #0: 125,779.50" + nl +
								"Row #1: (null)" + nl +
								"Row #1: (null)" + nl +
								"Row #1: (null)" + nl +
								"Row #1: (null)" + nl +
								"Row #1: (null)" + nl +
								"Row #1: (null)" + nl +
								"Row #1: (null)" + nl +
								"Row #1: 84,595.89" + nl +
								"Row #1: 70,333.90" + nl +
								"Row #1: 138,013.72" + nl +
								"Row #2: (null)" + nl +
								"Row #2: (null)" + nl +
								"Row #2: (null)" + nl +
								"Row #2: (null)" + nl +
								"Row #2: (null)" + nl +
								"Row #2: (null)" + nl +
								"Row #2: (null)" + nl +
								"Row #2: 10,023.94" + nl +
								"Row #2: -1,609.27" + nl +
								"Row #2: 12,234.22" + nl);
					}
					public void testAggregateToSimulateCompoundSlicer(FoodMartTestCase test) {
						test.runQueryCheckResult(
								"WITH MEMBER [Time].[1997 H1] as 'Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})'" + nl +
								"  MEMBER [Education Level].[College or higher] as 'Aggregate({[Education Level].[Bachelors Degree], [Education Level].[Graduate Degree]})'" + nl +
								"SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns," + nl +
								"  {[Product].children} on rows" + nl +
								"FROM [Sales]" + nl +
								"WHERE ([Time].[1997 H1], [Education Level].[College or higher], [Gender].[F])",
								"Axis #0:" + nl +
								"{[Time].[1997 H1], [Education Level].[College or higher], [Gender].[All Gender].[F]}" + nl +
								"Axis #1:" + nl +
								"{[Measures].[Unit Sales]}" + nl +
								"{[Measures].[Store Sales]}" + nl +
								"Axis #2:" + nl +
								"{[Product].[All Products].[Drink]}" + nl +
								"{[Product].[All Products].[Food]}" + nl +
								"{[Product].[All Products].[Non-Consumable]}" + nl +
								"Row #0: 1,797" + nl +
								"Row #0: 3,620.49" + nl +
								"Row #1: 15,002" + nl +
								"Row #1: 31,931.88" + nl +
								"Row #2: 3,845" + nl +
								"Row #2: 8,173.22" + nl);
					}
				}));
		define(new FunkResolver("$AggregateChildren", "$AggregateChildren(<Hierarchy>)", "Equivalent to 'Aggregate(<Hierarchy>.CurrentMember.Children); for internal use.",
				new String[] {"Inh"},
				new FunkBase() {
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
				}));
		define(new FunkResolver(
			"Avg", "Avg(<Set>[, <Numeric Expression>])", "Returns the average value of a numeric expression evaluated over a set.",
			new String[]{"fnx", "fnxn"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					List members = (List) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
					return avg(evaluator.push(), members, exp);
				}
				public void testAvg(FoodMartTestCase test) {
					String result = test.executeExpr(
							"AVG({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
					Assert.assertEquals("188,412.71", result);
				}
				//todo: testAvgWithNulls
			}));
		define(new FunkResolver(
			"Correlation", "Correlation(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Returns the correlation of two series evaluated over a set.",
			new String[]{"fnxn","fnxnn"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					List members = (List) getArg(evaluator, args, 0);
					ExpBase exp1 = (ExpBase) getArgNoEval(args, 1);
					ExpBase exp2 = (ExpBase) getArgNoEval(args, 2, valueFunCall);
					return correlation(evaluator.push(), members, exp1, exp2);
				}
				public void testCorrelation(FoodMartTestCase test) {
					String result = test.executeExpr("Correlation({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales]) * 1000000");
					Assert.assertEquals("999,906", result);
				}
			}));

		defineReserved(new String[] {"EXCLUDEEMPTY","INCLUDEEMPTY"});
		define(new FunkResolver(
			"Count", "Count(<Set>[, EXCLUDEEMPTY | INCLUDEEMPTY])", "Returns the number of tuples in a set, empty cells included unless the optional EXCLUDEEMPTY flag is used.",
			new String[]{"fnx", "fnxy"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					List members = (List) getArg(evaluator, args, 0);
					String empties = getLiteralArg(args, 1, "INCLUDEEMPTY", new String[] {"INCLUDEEMPTY", "EXCLUDEEMPTY"}, null);
					final boolean includeEmpty = empties.equals("INCLUDEEMPTY");
					return count(evaluator, members, includeEmpty);
				}
				public void testCount(FoodMartTestCase test) {
					String result = test.executeExpr(
							"count({[Promotion Media].[Media Type].members})");
					Assert.assertEquals("14", result);
				}
				public void testCountExcludeEmpty(FoodMartTestCase test) {
                    test.runQueryCheckResult(
                            "with member [Measures].[Promo Count] as " + nl +
                            " ' Count(Crossjoin({[Measures].[Unit Sales]}," + nl +
                            " {[Promotion Media].[Media Type].members}), EXCLUDEEMPTY)'" + nl +
                            "select {[Measures].[Unit Sales], [Measures].[Promo Count]} on columns," + nl +
                            " {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].children} on rows" + nl +
                            "from Sales",
                            "Axis #0:" + nl +
                            "{}" + nl +
                            "Axis #1:" + nl +
                            "{[Measures].[Unit Sales]}" + nl +
                            "{[Measures].[Promo Count]}" + nl +
                            "Axis #2:" + nl +
                            "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent]}" + nl +
                            "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous]}" + nl +
                            "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner]}" + nl +
                            "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token]}" + nl +
                            "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington]}" + nl +
                            "Row #0: 738" + nl +
                            "Row #0: 14" + nl +
                            "Row #1: 632" + nl +
                            "Row #1: 13" + nl +
                            "Row #2: 655" + nl +
                            "Row #2: 14" + nl +
                            "Row #3: 735" + nl +
                            "Row #3: 14" + nl +
                            "Row #4: 647" + nl +
                            "Row #4: 12" + nl);
				}
				//todo: testCountNull, testCountNoExp
			}));
		define(new FunkResolver(
			"Covariance", "Covariance(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Returns the covariance of two series evaluated over a set (biased).",
			new String[]{"fnxn","fnxnn"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					List members = (List) getArg(evaluator, args, 0);
					ExpBase exp1 = (ExpBase) getArgNoEval(args, 1);
					ExpBase exp2 = (ExpBase) getArgNoEval(args, 2);
					return covariance(evaluator.push(), members, exp1, exp2, true);
				}
				public void testCovariance(FoodMartTestCase test) {
					String result = test.executeExpr("Covariance({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])");
					Assert.assertEquals("1,355,761,899", result);
				}
			}));
		define(new FunkResolver(
			"CovarianceN", "CovarianceN(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Returns the covariance of two series evaluated over a set (unbiased).",
			new String[]{"fnxn","fnxnn"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					List members = (List) getArg(evaluator, args, 0);
					ExpBase exp1 = (ExpBase) getArgNoEval(args, 1);
					ExpBase exp2 = (ExpBase) getArgNoEval(args, 2, valueFunCall);
					return covariance(evaluator.push(), members, exp1, exp2, false);
				}
				public void testCovarianceN(FoodMartTestCase test) {
					String result = test.executeExpr("CovarianceN({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])");
					Assert.assertEquals("2,033,642,849", result);
				}
			}));
		define(new FunDefBase("IIf", "IIf(<Logical Expression>, <Numeric Expression1>, <Numeric Expression2>)", "Returns one of two numeric values determined by a logical test.", "fnbnn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				boolean logical = getBooleanArg(evaluator, args, 0);
				return getDoubleArg(evaluator, args, logical ? 1 : 2, null);
			}

			public void testIIfNumeric(FoodMartTestCase test) {
				String s = test.executeExpr(
						"IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, 45, 32)");
				Assert.assertEquals("45", s);
			}
		});
		if (false) define(new FunDefBase("LinRegIntercept", "LinRegIntercept(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns the value of b in the regression line y = ax + b.", "fn*"));
		if (false) define(new FunDefBase("LinRegPoint", "LinRegPoint(<Numeric Expression>, <Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns the value of y in the regression line y = ax + b.", "fn*"));
		if (false) define(new FunDefBase("LinRegR2", "LinRegR2(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns R2 (the coefficient of determination).", "fn*"));
		if (false) define(new FunDefBase("LinRegSlope", "LinRegSlope(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns the value of a in the regression line y = ax + b.", "fn*"));
		if (false) define(new FunDefBase("LinRegVariance", "LinRegVariance(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns the variance associated with the regression line y = ax + b.", "fn*"));
		define(new FunkResolver(
			"Max", "Max(<Set>[, <Numeric Expression>])", "Returns the maximum value of a numeric expression evaluated over a set.",
			new String[]{"fnx", "fnxn"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					List members = (List) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
					return max(evaluator.push(), members, exp);
				}
				public void testMax(FoodMartTestCase test) {
					String result = test.executeExpr(
							"MAX({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
					Assert.assertEquals("263,793.22", result);
				}
			}));
		define(new FunkResolver(
			"Median", "Median(<Set>[, <Numeric Expression>])", "Returns the median value of a numeric expression evaluated over a set.",
			new String[]{"fnx", "fnxn"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					List members = (List) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
					//todo: ignore nulls, do we need to ignore the List?
					return median(evaluator.push(), members, exp);

				}
				public void testMedian(FoodMartTestCase test) {
					String result = test.executeExpr(
							"MEDIAN({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
					Assert.assertEquals("159,167.84", result);
				}
				public void testMedian2(FoodMartTestCase test) {
					test.runQueryCheckResult(
							"WITH" + nl +
							"   MEMBER [Time].[1st Half Sales] AS 'Sum({[Time].[1997].[Q1], [Time].[1997].[Q2]})'" + nl +
							"   MEMBER [Time].[2nd Half Sales] AS 'Sum({[Time].[1997].[Q3], [Time].[1997].[Q4]})'" + nl +
							"   MEMBER [Time].[Median] AS 'Median(Time.Members)'" + nl +
							"SELECT" + nl +
							"   NON EMPTY { [Store].[Store Name].Members} ON COLUMNS," + nl +
							"   { [Time].[1st Half Sales], [Time].[2nd Half Sales], [Time].[Median]} ON ROWS" + nl +
							"FROM Sales" + nl +
							"WHERE [Measures].[Store Sales]",

							"Axis #0:" + nl +
							"{[Measures].[Store Sales]}" + nl +
							"Axis #1:" + nl +
							"{[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6]}" + nl +
							"{[Store].[All Stores].[USA].[CA].[Los Angeles].[Store 7]}" + nl +
							"{[Store].[All Stores].[USA].[CA].[San Diego].[Store 24]}" + nl +
							"{[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}" + nl +
							"{[Store].[All Stores].[USA].[OR].[Portland].[Store 11]}" + nl +
							"{[Store].[All Stores].[USA].[OR].[Salem].[Store 13]}" + nl +
							"{[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]}" + nl +
							"{[Store].[All Stores].[USA].[WA].[Bremerton].[Store 3]}" + nl +
							"{[Store].[All Stores].[USA].[WA].[Seattle].[Store 15]}" + nl +
							"{[Store].[All Stores].[USA].[WA].[Spokane].[Store 16]}" + nl +
							"{[Store].[All Stores].[USA].[WA].[Tacoma].[Store 17]}" + nl +
							"{[Store].[All Stores].[USA].[WA].[Walla Walla].[Store 22]}" + nl +
							"{[Store].[All Stores].[USA].[WA].[Yakima].[Store 23]}" + nl +
							"Axis #2:" + nl +
							"{[Time].[1st Half Sales]}" + nl +
							"{[Time].[2nd Half Sales]}" + nl +
							"{[Time].[Median]}" + nl +
							"Row #0: 20,801.04" + nl +
							"Row #0: 25,421.41" + nl +
							"Row #0: 26,275.11" + nl +
							"Row #0: 2,074.39" + nl +
							"Row #0: 28,519.18" + nl +
							"Row #0: 43,423.99" + nl +
							"Row #0: 2,140.99" + nl +
							"Row #0: 25,502.08" + nl +
							"Row #0: 25,293.50" + nl +
							"Row #0: 23,265.53" + nl +
							"Row #0: 34,926.91" + nl +
							"Row #0: 2,159.60" + nl +
							"Row #0: 12,490.89" + nl +
							"Row #1: 24,949.20" + nl +
							"Row #1: 29,123.87" + nl +
							"Row #1: 28,156.03" + nl +
							"Row #1: 2,366.79" + nl +
							"Row #1: 26,539.61" + nl +
							"Row #1: 43,794.29" + nl +
							"Row #1: 2,598.24" + nl +
							"Row #1: 27,394.22" + nl +
							"Row #1: 27,350.57" + nl +
							"Row #1: 26,368.93" + nl +
							"Row #1: 39,917.05" + nl +
							"Row #1: 2,546.37" + nl +
							"Row #1: 11,838.34" + nl +
							"Row #2: 4,577.35" + nl +
							"Row #2: 5,211.38" + nl +
							"Row #2: 4,722.87" + nl +
							"Row #2: 398.24" + nl +
							"Row #2: 5,039.50" + nl +
							"Row #2: 7,374.59" + nl +
							"Row #2: 410.22" + nl +
							"Row #2: 4,924.04" + nl +
							"Row #2: 4,569.13" + nl +
							"Row #2: 4,511.68" + nl +
							"Row #2: 6,630.91" + nl +
							"Row #2: 419.51" + nl +
							"Row #2: 2,169.48" + nl);
				}
			}));

		define(new FunkResolver(
			"Min", "Min(<Set>[, <Numeric Expression>])", "Returns the minimum value of a numeric expression evaluated over a set.",
			new String[]{"fnx", "fnxn"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					List members = (List) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
					return min(evaluator.push(), members, exp);
				}
				public void testMin(FoodMartTestCase test) {
					String result = test.executeExpr(
							"MIN({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
					Assert.assertEquals("142,277.07", result);
				}
				public void testMinTupel(FoodMartTestCase test) {
					String result = test.executeExpr(
						"Min([Customers].[All Customers].[USA].Children, ([Measures].[Unit Sales], [Gender].[All Gender].[F]))");
					Assert.assertEquals("33,036", result);
				}
			}));
		define(new FunDefBase("Ordinal", "<Level>.Ordinal", "Returns the zero-based ordinal value associated with a level.", "pnl"));
		if (false) define(new FunDefBase("Rank", "Rank(<Tuple>, <Set>)", "Returns the one-based rank of a tuple in a set.", "fn*"));
		define(new FunkResolver(
				"Stddev", "Stddev(<Set>[, <Numeric Expression>])", "Alias for Stdev.",
				new String[]{"fnx", "fnxn"},
				new FunkBase() {
						public Object evaluate(Evaluator evaluator, Exp[] args) {
							List members = (List) getArg(evaluator, args, 0);
							ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
							return stdev(evaluator.push(), members, exp, false);
						}
				}));
		define(new FunkResolver(
				"Stdev", "Stdev(<Set>[, <Numeric Expression>])", "Returns the standard deviation of a numeric expression evaluated over a set (unbiased).",
				new String[]{"fnx", "fnxn"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						List members = (List) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
						return stdev(evaluator.push(), members, exp, false);
					}
					public void testStdev(FoodMartTestCase test) {
						String result = test.executeExpr(
								"STDEV({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
						Assert.assertEquals("65,825.45", result);
					}
				}));
		define(new FunkResolver(
				"StddevP", "StddevP(<Set>[, <Numeric Expression>])", "Alias for StdevP.",
				new String[]{"fnx", "fnxn"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						List members = (List) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
						return stdev(evaluator.push(), members, exp, true);
					}
				}));
		define(new FunkResolver(
				"StdevP", "StdevP(<Set>[, <Numeric Expression>])", "Returns the standard deviation of a numeric expression evaluated over a set (biased).",
				new String[]{"fnx", "fnxn"},
				new FunkBase() {
						public Object evaluate(Evaluator evaluator, Exp[] args) {
							List members = (List) getArg(evaluator, args, 0);
							ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
							return stdev(evaluator.push(), members, exp, true);
						}
					public void testStdevP(FoodMartTestCase test) {
						String result = test.executeExpr(
								"STDEVP({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
						Assert.assertEquals("53,746.26", result);
					}
				}));
		define(new FunkResolver(
				"Sum", "Sum(<Set>[, <Numeric Expression>])", "Returns the sum of a numeric expression evaluated over a set.",
				new String[]{"fnx", "fnxn"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						List members = (List) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
						return sum(evaluator.push(), members, exp);
					}
					public void testSumNoExp(FoodMartTestCase test) {
						String result = test.executeExpr(
								"SUM({[Promotion Media].[Media Type].members})");
						Assert.assertEquals("266,773", result);
					}
				}));
		define(new FunDefBase("Value", "<Measure>.Value", "Returns the value of a measure.", "pnm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return member.evaluateScalar(evaluator);
			}
		});
		define(new FunDefBase("_Value", "_Value(<Tuple>)", "Returns the value of the current measure within the context of a tuple.", "fnt") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member[] members = getTupleArg(evaluator, args, 0);
				Evaluator evaluator2 = evaluator.push(members);
				return evaluator2.evaluateCurrent();
			}
		});
		define(new FunDefBase("_Value", "_Value()", "Returns the value of the current measure.", "fn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				return evaluator.evaluateCurrent();
			}
		});
		// _Value is a pseudo-function which evaluates a tuple to a number.
		// It needs a custom resolver.
		if (false)
		define(new ResolverBase("_Value", null, null, Syntax.Parentheses) {
			public FunDef resolve(Exp[] args, int[] conversionCount) {
				if (args.length == 1 &&
						args[0].getType() == Category.Tuple) {
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
		define(new FunkResolver(
				"Var", "Var(<Set>[, <Numeric Expression>])", "Returns the variance of a numeric expression evaluated over a set (unbiased).",
				new String[]{"fnx", "fnxn"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						List members = (List) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
						return var(evaluator.push(), members, exp, false);
					}
					public void testVar(FoodMartTestCase test) {
						String result = test.executeExpr(
								"VAR({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
						Assert.assertEquals("4,332,990,493.69", result);
					}
				}));
		define(new FunkResolver(
				"Variance", "Variance(<Set>[, <Numeric Expression>])", "Alias for Var.",
				new String[]{"fnx", "fnxn"},
				new FunkBase() {
						public Object evaluate(Evaluator evaluator, Exp[] args) {
							List members = (List) getArg(evaluator, args, 0);
							ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
							return var(evaluator.push(), members, exp, false);
						}
				}));
		define(new FunkResolver(
				"VarianceP", "VarianceP(<Set>[, <Numeric Expression>])", "Alias for VarP.",
				new String[]{"fnx", "fnxn"},
				new FunkBase() {
						public Object evaluate(Evaluator evaluator, Exp[] args) {
							List members = (List) getArg(evaluator, args, 0);
							ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
							return var(evaluator.push(), members, exp, true);
						}
				}));
		define(new FunkResolver(
				"VarP", "VarP(<Set>[, <Numeric Expression>])", "Returns the variance of a numeric expression evaluated over a set (biased).",
				new String[]{"fnx", "fnxn"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						List members = (List) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArgNoEval(args, 1, valueFunCall);
						return var(evaluator.push(), members, exp, true);
					}
					public void testVarP(FoodMartTestCase test) {
						String result = test.executeExpr(
								"VARP({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
						Assert.assertEquals("2,888,660,329.13", result);
					}
				}));

		//
		// SET FUNCTIONS
		if (false) define(new FunDefBase("AddCalculatedMembers", "AddCalculatedMembers(<Set>)", "Adds calculated members to a set.", "fx*"));

        define(new FunDefBase("Ascendants", "Ascendants(<Member>)", "Returns the set of the ascendants of a specified member.", "fxm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, false);
                if (member.isNull()) {
                    return new ArrayList();
                }
                Member[] members = member.getAncestorMembers();
                final ArrayList result = new ArrayList(members.length + 1);
                result.add(member);
                addAll(result, members);
                return result;
            }

            public void testAscendants(FoodMartTestCase test) {
                test.assertAxisReturns(
                        "Ascendants([Store].[USA].[CA])",
                        "[Store].[All Stores].[USA].[CA]" + nl +
                        "[Store].[All Stores].[USA]" + nl +
                        "[Store].[All Stores]");
            }

            public void testAscendantsAll(FoodMartTestCase test) {
                test.assertAxisReturns(
                        "Ascendants([Store].DefaultMember)",
                        "[Store].[All Stores]");
            }

            public void testAscendantsNull(FoodMartTestCase test) {
                test.assertAxisReturns(
                        "Ascendants([Gender].[F].PrevMember)",
                        "");
            }
        });

		define(new FunkResolver(
			"BottomCount",
			"BottomCount(<Set>, <Count>[, <Numeric Expression>])",
			"Returns a specified number of items from the bottom of a set, optionally ordering the set first.",
			new String[]{"fxxnn", "fxxn"},
			new FunkBase() {
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
				public void testBottomCount(FoodMartTestCase test) {
					test.assertAxisReturns("BottomCount({[Promotion Media].[Media Type].members}, 2, [Measures].[Unit Sales])",
							"[Promotion Media].[All Media].[Radio]" + nl +
							"[Promotion Media].[All Media].[Sunday Paper, Radio, TV]");
				}
				//todo: test unordered

			}));
		define(new FunkResolver(
			"BottomPercent", "BottomPercent(<Set>, <Percentage>, <Numeric Expression>)", "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified percentage.",
			new String[]{"fxxnn"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					List members = (List) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArgNoEval(args, 2);
					Double n = getDoubleArg(evaluator, args, 1);
					return topOrBottom(evaluator.push(), members, exp, false, true, n.doubleValue());

				}
				public void testBottomPercent(FoodMartTestCase test) {
					test.assertAxisReturns("BottomPercent({[Promotion Media].[Media Type].members}, 1, [Measures].[Unit Sales])",
							"[Promotion Media].[All Media].[Radio]" + nl +
							"[Promotion Media].[All Media].[Sunday Paper, Radio, TV]");
				}
				//todo: test precision
			}));

		define(new FunkResolver(
			"BottomSum", "BottomSum(<Set>, <Value>, <Numeric Expression>)", "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified value.",
			new String[]{"fxxnn"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					List members = (List) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArgNoEval(args, 2);
					Double n = getDoubleArg(evaluator, args, 1);
					return topOrBottom(evaluator.push(), members, exp, false, false, n.doubleValue());

				}
				public void testBottomSum(FoodMartTestCase test) {
					test.assertAxisReturns("BottomSum({[Promotion Media].[Media Type].members}, 5000, [Measures].[Unit Sales])",
							"[Promotion Media].[All Media].[Radio]" + nl +
							"[Promotion Media].[All Media].[Sunday Paper, Radio, TV]");
				}
			}));
		define(new FunDefBase("Children", "<Member>.Children", "Returns the children of a member.", "pxm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				Member[] children = evaluator.getSchemaReader().getMemberChildren(member);
				return Arrays.asList(children);
			}
		});
		define(new MultiResolver(
                "Crossjoin", "Crossjoin(<Set1>, <Set2>)", "Returns the cross product of two sets.",
                new String[]{"fxxx"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new CrossJoinFunDef(dummyFunDef);
            }
            public void testCrossjoinNested(FoodMartTestCase test) {
                test.assertAxisReturns(
                        "  CrossJoin(" + nl +
                        "    CrossJoin(" + nl +
                        "      [Gender].members," + nl +
                        "      [Marital Status].members)," + nl +
                        "   {[Store], [Store].children})",

                        "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores]}" + nl +
                        "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores].[Canada]}" + nl +
                        "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores].[Mexico]}" + nl +
                        "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores].[USA]}" + nl +
                        "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores]}" + nl +
                        "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Canada]}" + nl +
                        "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Mexico]}" + nl +
                        "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[USA]}" + nl +
                        "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores]}" + nl +
                        "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}" + nl +
                        "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}" + nl +
                        "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores].[Canada]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores].[Mexico]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores].[USA]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Canada]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Mexico]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[USA]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores].[Canada]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores].[Mexico]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores].[USA]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Canada]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Mexico]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[USA]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}");
            }
            public void testCrossjoinSingletonTuples(FoodMartTestCase test) {
                test.assertAxisReturns("CrossJoin({([Gender].[M])}, {([Marital Status].[S])})",
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}");
            }
            public void testCrossjoinSingletonTuplesNested(FoodMartTestCase test) {
                test.assertAxisReturns("CrossJoin({([Gender].[M])}, CrossJoin({([Marital Status].[S])}, [Store].children))",
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}");
            }
		});
        define(new MultiResolver(
                "*", "<Set1> * <Set2>", "Returns the cross product of two sets.",
                new String[]{"ixxx", "ixmx", "ixxm", "ixmm"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new CrossJoinFunDef(dummyFunDef);
            }
            public void testCrossjoinAsterisk(FoodMartTestCase test) {
                test.assertAxisReturns("{[Gender].[M]} * {[Marital Status].[S]}",
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}");
            }
            public void testCrossjoinAsteriskAssoc(FoodMartTestCase test) {
                test.assertAxisReturns("Order({[Gender].Children} * {[Marital Status].Children} * {[Time].[1997].[Q2].Children}," +
                        "[Measures].[Unit Sales])",
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[4]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[6]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[5]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[4]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[5]}" + nl +
                        "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[6]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[4]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[5]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[6]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[6]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[4]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[5]}");
            }
            public void testCrossjoinAsteriskInsideBraces(FoodMartTestCase test) {
                test.assertAxisReturns("{[Gender].[M] * [Marital Status].[S] * [Time].[1997].[Q2].Children}",
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[4]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[5]}" + nl +
                        "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[6]}");
            }
            public void testCrossJoinAsteriskQuery(FoodMartTestCase test) {
                test.runQueryCheckResult(
                        "SELECT {[Measures].members * [1997].children} ON COLUMNS," + nl +
                        " {[Store].[USA].children * [Position].[All Position].children} DIMENSION PROPERTIES [Store].[Store SQFT] ON ROWS" + nl +
                        "FROM [HR]",

                        "Axis #0:" + nl +
                        "{}" + nl +
                        "Axis #1:" + nl +
                        "{[Measures].[Org Salary], [Time].[1997].[Q1]}" + nl +
                        "{[Measures].[Org Salary], [Time].[1997].[Q2]}" + nl +
                        "{[Measures].[Org Salary], [Time].[1997].[Q3]}" + nl +
                        "{[Measures].[Org Salary], [Time].[1997].[Q4]}" + nl +
                        "{[Measures].[Count], [Time].[1997].[Q1]}" + nl +
                        "{[Measures].[Count], [Time].[1997].[Q2]}" + nl +
                        "{[Measures].[Count], [Time].[1997].[Q3]}" + nl +
                        "{[Measures].[Count], [Time].[1997].[Q4]}" + nl +
                        "{[Measures].[Number of Employees], [Time].[1997].[Q1]}" + nl +
                        "{[Measures].[Number of Employees], [Time].[1997].[Q2]}" + nl +
                        "{[Measures].[Number of Employees], [Time].[1997].[Q3]}" + nl +
                        "{[Measures].[Number of Employees], [Time].[1997].[Q4]}" + nl +
                        "Axis #2:" + nl +
                        "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Middle Management]}" + nl +
                        "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Senior Management]}" + nl +
                        "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Store Full Time Staf]}" + nl +
                        "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Store Management]}" + nl +
                        "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Store Temp Staff]}" + nl +
                        "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Middle Management]}" + nl +
                        "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Senior Management]}" + nl +
                        "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Store Full Time Staf]}" + nl +
                        "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Store Management]}" + nl +
                        "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Store Temp Staff]}" + nl +
                        "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Middle Management]}" + nl +
                        "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Senior Management]}" + nl +
                        "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Store Full Time Staf]}" + nl +
                        "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Store Management]}" + nl +
                        "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Store Temp Staff]}" + nl +
                        "Row #0: $275.40" + nl +
                        "Row #0: $275.40" + nl +
                        "Row #0: $275.40" + nl +
                        "Row #0: $275.40" + nl +
                        "Row #0: 27" + nl +
                        "Row #0: 27" + nl +
                        "Row #0: 27" + nl +
                        "Row #0: 27" + nl +
                        "Row #0: 9" + nl +
                        "Row #0: 9" + nl +
                        "Row #0: 9" + nl +
                        "Row #0: 9" + nl +
                        "Row #1: $837.00" + nl +
                        "Row #1: $837.00" + nl +
                        "Row #1: $837.00" + nl +
                        "Row #1: $837.00" + nl +
                        "Row #1: 24" + nl +
                        "Row #1: 24" + nl +
                        "Row #1: 24" + nl +
                        "Row #1: 24" + nl +
                        "Row #1: 8" + nl +
                        "Row #1: 8" + nl +
                        "Row #1: 8" + nl +
                        "Row #1: 8" + nl +
                        "Row #2: $1,728.45" + nl +
                        "Row #2: $1,727.02" + nl +
                        "Row #2: $1,727.72" + nl +
                        "Row #2: $1,726.55" + nl +
                        "Row #2: 357" + nl +
                        "Row #2: 357" + nl +
                        "Row #2: 357" + nl +
                        "Row #2: 357" + nl +
                        "Row #2: 119" + nl +
                        "Row #2: 119" + nl +
                        "Row #2: 119" + nl +
                        "Row #2: 119" + nl +
                        "Row #3: $473.04" + nl +
                        "Row #3: $473.04" + nl +
                        "Row #3: $473.04" + nl +
                        "Row #3: $473.04" + nl +
                        "Row #3: 51" + nl +
                        "Row #3: 51" + nl +
                        "Row #3: 51" + nl +
                        "Row #3: 51" + nl +
                        "Row #3: 17" + nl +
                        "Row #3: 17" + nl +
                        "Row #3: 17" + nl +
                        "Row #3: 17" + nl +
                        "Row #4: $401.35" + nl +
                        "Row #4: $405.73" + nl +
                        "Row #4: $400.61" + nl +
                        "Row #4: $402.31" + nl +
                        "Row #4: 120" + nl +
                        "Row #4: 120" + nl +
                        "Row #4: 120" + nl +
                        "Row #4: 120" + nl +
                        "Row #4: 40" + nl +
                        "Row #4: 40" + nl +
                        "Row #4: 40" + nl +
                        "Row #4: 40" + nl +
                        "Row #5: (null)" + nl +
                        "Row #5: (null)" + nl +
                        "Row #5: (null)" + nl +
                        "Row #5: (null)" + nl +
                        "Row #5: (null)" + nl +
                        "Row #5: (null)" + nl +
                        "Row #5: (null)" + nl +
                        "Row #5: (null)" + nl +
                        "Row #5: (null)" + nl +
                        "Row #5: (null)" + nl +
                        "Row #5: (null)" + nl +
                        "Row #5: (null)" + nl +
                        "Row #6: (null)" + nl +
                        "Row #6: (null)" + nl +
                        "Row #6: (null)" + nl +
                        "Row #6: (null)" + nl +
                        "Row #6: (null)" + nl +
                        "Row #6: (null)" + nl +
                        "Row #6: (null)" + nl +
                        "Row #6: (null)" + nl +
                        "Row #6: (null)" + nl +
                        "Row #6: (null)" + nl +
                        "Row #6: (null)" + nl +
                        "Row #6: (null)" + nl +
                        "Row #7: $1,343.62" + nl +
                        "Row #7: $1,342.61" + nl +
                        "Row #7: $1,342.57" + nl +
                        "Row #7: $1,343.65" + nl +
                        "Row #7: 279" + nl +
                        "Row #7: 279" + nl +
                        "Row #7: 279" + nl +
                        "Row #7: 279" + nl +
                        "Row #7: 93" + nl +
                        "Row #7: 93" + nl +
                        "Row #7: 93" + nl +
                        "Row #7: 93" + nl +
                        "Row #8: $286.74" + nl +
                        "Row #8: $286.74" + nl +
                        "Row #8: $286.74" + nl +
                        "Row #8: $286.74" + nl +
                        "Row #8: 30" + nl +
                        "Row #8: 30" + nl +
                        "Row #8: 30" + nl +
                        "Row #8: 30" + nl +
                        "Row #8: 10" + nl +
                        "Row #8: 10" + nl +
                        "Row #8: 10" + nl +
                        "Row #8: 10" + nl +
                        "Row #9: $333.20" + nl +
                        "Row #9: $332.65" + nl +
                        "Row #9: $331.28" + nl +
                        "Row #9: $332.43" + nl +
                        "Row #9: 99" + nl +
                        "Row #9: 99" + nl +
                        "Row #9: 99" + nl +
                        "Row #9: 99" + nl +
                        "Row #9: 33" + nl +
                        "Row #9: 33" + nl +
                        "Row #9: 33" + nl +
                        "Row #9: 33" + nl +
                        "Row #10: (null)" + nl +
                        "Row #10: (null)" + nl +
                        "Row #10: (null)" + nl +
                        "Row #10: (null)" + nl +
                        "Row #10: (null)" + nl +
                        "Row #10: (null)" + nl +
                        "Row #10: (null)" + nl +
                        "Row #10: (null)" + nl +
                        "Row #10: (null)" + nl +
                        "Row #10: (null)" + nl +
                        "Row #10: (null)" + nl +
                        "Row #10: (null)" + nl +
                        "Row #11: (null)" + nl +
                        "Row #11: (null)" + nl +
                        "Row #11: (null)" + nl +
                        "Row #11: (null)" + nl +
                        "Row #11: (null)" + nl +
                        "Row #11: (null)" + nl +
                        "Row #11: (null)" + nl +
                        "Row #11: (null)" + nl +
                        "Row #11: (null)" + nl +
                        "Row #11: (null)" + nl +
                        "Row #11: (null)" + nl +
                        "Row #11: (null)" + nl +
                        "Row #12: $2,768.60" + nl +
                        "Row #12: $2,769.18" + nl +
                        "Row #12: $2,766.78" + nl +
                        "Row #12: $2,769.50" + nl +
                        "Row #12: 579" + nl +
                        "Row #12: 579" + nl +
                        "Row #12: 579" + nl +
                        "Row #12: 579" + nl +
                        "Row #12: 193" + nl +
                        "Row #12: 193" + nl +
                        "Row #12: 193" + nl +
                        "Row #12: 193" + nl +
                        "Row #13: $736.29" + nl +
                        "Row #13: $736.29" + nl +
                        "Row #13: $736.29" + nl +
                        "Row #13: $736.29" + nl +
                        "Row #13: 81" + nl +
                        "Row #13: 81" + nl +
                        "Row #13: 81" + nl +
                        "Row #13: 81" + nl +
                        "Row #13: 27" + nl +
                        "Row #13: 27" + nl +
                        "Row #13: 27" + nl +
                        "Row #13: 27" + nl +
                        "Row #14: $674.70" + nl +
                        "Row #14: $674.54" + nl +
                        "Row #14: $676.25" + nl +
                        "Row #14: $676.48" + nl +
                        "Row #14: 201" + nl +
                        "Row #14: 201" + nl +
                        "Row #14: 201" + nl +
                        "Row #14: 201" + nl +
                        "Row #14: 67" + nl +
                        "Row #14: 67" + nl +
                        "Row #14: 67" + nl +
                        "Row #14: 67" + nl);
            }
        });

		defineReserved(DescendantsFlags.instance);
		define(new MultiResolver(
				"Descendants", "Descendants(<Member>[, <Level>[, <Desc_flag>]])", "Returns the set of descendants of a member at a specified level, optionally including or excluding descendants in other levels.",
				new String[]{"fxm", "fxml", "fxmly", "fxmn", "fxmny"}) {
			protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
				int depthLimit = -1; // unlimited
				boolean depthSpecified = false;
				int flag = DescendantsFlags.SELF;
				if (args.length == 1) {
					depthLimit = -1;
					flag = DescendantsFlags.SELF_BEFORE_AFTER;
				}
				if (args.length >= 2) {
					if (args[1] instanceof Literal) {
						Literal literal = (Literal) args[1];
						if (literal.getValue() instanceof Number) {
							Number number = (Number) literal.getValue();
							depthLimit = number.intValue();
							depthSpecified = true;
						}
					}
				}
				if (args.length >= 3) {
					flag = getLiteralArg(args, 2, DescendantsFlags.SELF, DescendantsFlags.instance, dummyFunDef);
				}
				final int depthLimitFinal = depthLimit < 0 ? Integer.MAX_VALUE : depthLimit;
				final int flagFinal = flag;
				final boolean depthSpecifiedFinal = depthSpecified;
				return new FunDefBase(dummyFunDef) {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Member member = getMemberArg(evaluator, args, 0, true);
						Level level;
						if (depthSpecifiedFinal) {
							level = null;
						} else if (args.length > 1) {
							level = getLevelArg(evaluator, args, 1, true);
						} else {
							level = member.getLevel();
						}
						// Expand member to its children, until we get to the right
						// level. We assume that all children are in the same
						// level.
						final SchemaReader schemaReader = evaluator.getSchemaReader();
						Member[] children = {member};
						int depth = 0;
						List result = new ArrayList();
						while (true) {
							final int currentDepth;
							final int targetDepth;
							if (level == null) {
								currentDepth = depth++;
								targetDepth = depthLimitFinal;
							} else {
								final Member firstChild = children[0];
								currentDepth = firstChild.getLevel().getDepth();
								targetDepth = level.getDepth();
							}
							if (currentDepth == targetDepth) {
								if ((flagFinal & DescendantsFlags.SELF) == DescendantsFlags.SELF) {
									Util.addAll(result, children);
								}
								if ((flagFinal & DescendantsFlags.AFTER) != DescendantsFlags.AFTER) {
									break; // no more results after this level
								}
							} else if (currentDepth < targetDepth) {
								if ((flagFinal & DescendantsFlags.BEFORE) == DescendantsFlags.BEFORE) {
									Util.addAll(result, children);
								}
							} else {
								if ((flagFinal & DescendantsFlags.AFTER) == DescendantsFlags.AFTER) {
									Util.addAll(result, children);
								} else {
									break; // no more results after this level
								}
							}

							children = schemaReader.getMemberChildren(children);
							if (children.length == 0) {
								break;
							}
						}
						return result;
					}
				};
			}
			public void testDescendantsM(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997].[Q1])",
						"[Time].[1997].[Q1]" + nl +
						"[Time].[1997].[Q1].[1]" + nl +
						"[Time].[1997].[Q1].[2]" + nl +
						"[Time].[1997].[Q1].[3]");
			}
			public void testDescendantsML(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997], [Time].[Month])",
						months);
			}
			public void testDescendantsMLSelf(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], SELF)",
						quarters);
			}
			public void testDescendantsMLSelfBefore(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], SELF_AND_BEFORE)",
						year1997 + nl + quarters);
			}
			public void testDescendantsMLSelfBeforeAfter(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], SELF_BEFORE_AFTER)",
						year1997 + nl + quarters + nl + months);
			}
			public void testDescendantsMLBefore(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], BEFORE)",
						year1997);
			}
			public void testDescendantsMLBeforeAfter(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], BEFORE_AND_AFTER)",
						year1997 + nl + months);
			}
			public void testDescendantsMLAfter(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], AFTER)",
						months);
			}
			public void testDescendantsMLAfterEnd(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997], [Time].[Month], AFTER)",
						"");
			}
			public void _testDescendantsMLLeaves(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997], [Time].[Month], LEAVES)", "foo");
			}
			public void testDescendantsM0(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997], 0)",
						year1997);
			}
			public void testDescendantsM2(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997], 2)",
						months);
			}
			public void testDescendantsMNY(FoodMartTestCase test) {
				test.assertAxisReturns("Descendants([Time].[1997], 1, BEFORE_AND_AFTER)",
						year1997 + nl + months);
			}
			public void testDescendantsParentChild(FoodMartTestCase test) {
				test.assertAxisReturns("HR", "Descendants([Employees], 2)",
						"[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply]" + nl +
						"[Employees].[All Employees].[Sheri Nowmer].[Michael Spence]" + nl +
						"[Employees].[All Employees].[Sheri Nowmer].[Maya Gutierrez]" + nl +
						"[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra]" + nl +
						"[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]" + nl +
						"[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]" + nl +
						"[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]");
			}
			public void testDescendantsParentChildBefore(FoodMartTestCase test) {
				test.assertAxisReturns("HR", "Descendants([Employees], 2, BEFORE)",
						"[Employees].[All Employees]" + nl +
						"[Employees].[All Employees].[Sheri Nowmer]");
			}
		});
		if (false) define(new FunDefBase("Distinct", "Distinct(<Set>)", "Eliminates duplicate tuples from a set.", "fxx"));

		define(new FunkResolver("DrilldownLevel", "DrilldownLevel(<Set>[, <Level>]) or DrilldownLevel(<Set>, , <Index>)", "Drills down the members of a set, at a specified level, to one level below. Alternatively, drills down on a specified dimension in the set.",
				new String[]{"fxx", "fxxl"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						//todo add fssl functionality
						List set0 = (List) getArg(evaluator, args, 0);
						int[] depthArray = new int[set0.size()];
						List drilledSet = new ArrayList();

						for (int i = 0, m = set0.size(); i < m; i++) {
							Member member = (Member) set0.get(i);
							depthArray[i] = member.getLevel().getDepth();
							// Object o0 = set0.get(i);
							//   depthList.addElement(new Object[] {o0});
						}
						Arrays.sort(depthArray);
						int maxDepth = depthArray[depthArray.length - 1];
						for (int i = 0, m = set0.size(); i < m; i++) {
							Member member = (Member) set0.get(i);
							drilledSet.add(member);
							if (member.getLevel().getDepth() == maxDepth) {
								Member[] childMembers = evaluator.getSchemaReader().getMemberChildren(member);
								for (int j = 0; j < childMembers.length; j++) {
									drilledSet.add(childMembers[j]);
								}
							}
						}
						return drilledSet;
					}
				}
		));

		if (false) define(new FunDefBase("DrilldownLevelBottom", "DrilldownLevelBottom(<Set>, <Count>[, [<Level>][, <Numeric Expression>]])", "Drills down the bottom N members of a set, at a specified level, to one level below.", "fx*"));
		if (false) define(new FunDefBase("DrilldownLevelTop", "DrilldownLevelTop(<Set>, <Count>[, [<Level>][, <Numeric Expression>]])", "Drills down the top N members of a set, at a specified level, to one level below.", "fx*"));
		if (false) define(new FunDefBase("DrilldownMember", "DrilldownMember(<Set1>, <Set2>[, RECURSIVE])", "Drills down the members in a set that are present in a second specified set.", "fx*"));
		if (false) define(new FunDefBase("DrilldownMemberBottom", "DrilldownMemberBottom(<Set1>, <Set2>, <Count>[, [<Numeric Expression>][, RECURSIVE]])", "Like DrilldownMember except that it includes only the bottom N children.", "fx*"));
		if (false) define(new FunDefBase("DrilldownMemberTop", "DrilldownMemberTop(<Set1>, <Set2>, <Count>[, [<Numeric Expression>][, RECURSIVE]])", "Like DrilldownMember except that it includes only the top N children.", "fx*"));
		if (false) define(new FunDefBase("DrillupLevel", "DrillupLevel(<Set>[, <Level>])", "Drills up the members of a set that are below a specified level.", "fx*"));
		if (false) define(new FunDefBase("DrillupMember", "DrillupMember(<Set1>, <Set2>)", "Drills up the members in a set that are present in a second specified set.", "fx*"));
		define(new FunkResolver(
				"Except", "Except(<Set1>, <Set2>[, ALL])", "Finds the difference between two sets, optionally retaining duplicates.",
				new String[]{"fxxx", "fxxxs"},
				new FunkBase() {
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
				}));
		if (false) define(new FunDefBase("Extract", "Extract(<Set>, <Dimension>[, <Dimension>...])", "Returns a set of tuples from extracted dimension elements. The opposite of Crossjoin.", "fx*"));

		define(new FunDefBase("Filter", "Filter(<Set>, <Search Condition>)", "Returns the set resulting from filtering a set based on a search condition.", "fxxb") {
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
					if (b.booleanValue()) {
						result.add(o);
					}
				}
				return result;
			}

			/**
			 * Make sure that slicer is in force when expression is applied
			 * on axis, E.g. select filter([Customers].members, [Unit Sales] > 100)
			 * from sales where ([Time].[1998])
			 **/
			public void testFilterWithSlicer(FoodMartTestCase test) {
				Result result = test.execute(
						"select {[Measures].[Unit Sales]} on columns," + nl +
						" filter([Customers].[USA].children," + nl +
						"        [Measures].[Unit Sales] > 20000) on rows" + nl +
						"from Sales" + nl +
						"where ([Time].[1997].[Q1])");
				Axis rows = result.getAxes()[1];
				// if slicer were ignored, there would be 3 rows
				Assert.assertEquals(1, rows.positions.length);
				Cell cell = result.getCell(new int[] {0,0});
				Assert.assertEquals("30,114", cell.getFormattedValue());
			}
			public void testFilterCompound(FoodMartTestCase test) {
				Result result = test.execute(
						"select {[Measures].[Unit Sales]} on columns," + nl +
						"  Filter(" + nl +
						"    CrossJoin(" + nl +
						"      [Gender].Children," + nl +
						"      [Customers].[USA].Children)," + nl +
						"    [Measures].[Unit Sales] > 9500) on rows" + nl +
						"from Sales" + nl +
						"where ([Time].[1997].[Q1])");
				Position[] rows = result.getAxes()[1].positions;
                Assert.assertTrue(rows.length == 3);
				Assert.assertEquals("F", rows[0].members[0].getName());
				Assert.assertEquals("WA", rows[0].members[1].getName());
				Assert.assertEquals("M", rows[1].members[0].getName());
				Assert.assertEquals("OR", rows[1].members[1].getName());
				Assert.assertEquals("M", rows[2].members[0].getName());
				Assert.assertEquals("WA", rows[2].members[1].getName());
			}
		});

		define(new MultiResolver(
                "Generate", "Generate(<Set1>, <Set2>[, ALL])", "Applies a set to each member of another set and joins the resulting sets by union.",
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
            public void testGenerate(FoodMartTestCase test) {
                test.assertAxisReturns(
                        "Generate({[Store].[USA], [Store].[USA].[CA]}, {[Store].CurrentMember.Children})",
                        "[Store].[All Stores].[USA].[CA]" + nl +
                        "[Store].[All Stores].[USA].[OR]" + nl +
                        "[Store].[All Stores].[USA].[WA]" + nl +
                        "[Store].[All Stores].[USA].[CA].[Alameda]" + nl +
                        "[Store].[All Stores].[USA].[CA].[Beverly Hills]" + nl +
                        "[Store].[All Stores].[USA].[CA].[Los Angeles]" + nl +
                        "[Store].[All Stores].[USA].[CA].[San Diego]" + nl +
                        "[Store].[All Stores].[USA].[CA].[San Francisco]");
            }
            public void testGenerateAll(FoodMartTestCase test) {
                test.assertAxisReturns(
                        "Generate({[Store].[USA].[CA], [Store].[USA].[OR].[Portland]}," +
                        " Ascendants([Store].CurrentMember)," +
                        " ALL)",
                        "[Store].[All Stores].[USA].[CA]" + nl +
                        "[Store].[All Stores].[USA]" + nl +
                        "[Store].[All Stores]" + nl +
                        "[Store].[All Stores].[USA].[OR].[Portland]" + nl +
                        "[Store].[All Stores].[USA].[OR]" + nl +
                        "[Store].[All Stores].[USA]" + nl +
                        "[Store].[All Stores]");
            }
            public void testGenerateUnique(FoodMartTestCase test) {
                test.assertAxisReturns(
                        "Generate({[Store].[USA].[CA], [Store].[USA].[OR].[Portland]}," +
                        " Ascendants([Store].CurrentMember))",
                        "[Store].[All Stores].[USA].[CA]" + nl +
                        "[Store].[All Stores].[USA]" + nl +
                        "[Store].[All Stores]" + nl +
                        "[Store].[All Stores].[USA].[OR].[Portland]" + nl +
                        "[Store].[All Stores].[USA].[OR]");
            }
            public void testGenerateCrossJoin(FoodMartTestCase test) {
                // Note that the different regions have different Top 2.
                test.assertAxisReturns(
                        "Generate({[Store].[USA].[CA], [Store].[USA].[CA].[San Francisco]}," + nl +
                        "  CrossJoin({[Store].CurrentMember}," + nl +
                        "    TopCount([Product].[Brand Name].members, " + nl +
                        "    2," + nl +
                        "    [Measures].[Unit Sales])))",
                        "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos]}" + nl +
                        "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale]}" + nl +
                        "{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony]}" + nl +
                        "{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top]}");
            }
        });

        define(new FunkResolver(
                "Head", "Head(<Set>[, < Numeric Expression >])", "Returns the first specified number of elements in a set.",
                new String[] {"fxx", "fxxn"},
                new FunkBase() {
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
                    public void testHead(FoodMartTestCase test) {
                        test.assertAxisReturns("Head([Store].Children, 2)",
                                "[Store].[All Stores].[Canada]" + nl +
                                "[Store].[All Stores].[Mexico]");
                    }
                    public void testHeadNegative(FoodMartTestCase test) {
                        test.assertAxisReturns("Head([Store].Children, 2 - 3)",
                                "");
                    }
                    public void testHeadDefault(FoodMartTestCase test) {
                        test.assertAxisReturns("Head([Store].Children)",
                                "[Store].[All Stores].[Canada]");
                    }
                    public void testHeadOvershoot(FoodMartTestCase test) {
                        test.assertAxisReturns("Head([Store].Children, 2 + 2)",
                                "[Store].[All Stores].[Canada]" + nl +
                                "[Store].[All Stores].[Mexico]" + nl +
                                "[Store].[All Stores].[USA]");
                    }
                    public void testHeadEmpty(FoodMartTestCase test) {
                        test.assertAxisReturns("Head([Gender].[F].Children, 2)",
                                "");
                    }
                }));

		defineReserved(new String[] {"PRE","POST"});
		define(new MultiResolver(
				"Hierarchize", "Hierarchize(<Set>[, POST])", "Orders the members of a set in a hierarchy.",
				new String[] {"fxx", "fxxy"}) {
			protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
				String order = getLiteralArg(args, 1, "PRE", new String[] {"PRE", "POST"}, dummyFunDef);
				final boolean post = order.equals("POST");
				return new FunDefBase(dummyFunDef) {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						List members = (List) getArg(evaluator, args, 0);
						hierarchize(members, post);
						return members;
					}
				};
			}
			public void testHierarchize(FoodMartTestCase test) {
				test.assertAxisReturns(
						"Hierarchize(" + nl +
						"    {[Product].[All Products], " +
						"     [Product].[Food]," + nl +
						"     [Product].[Drink]," + nl +
						"     [Product].[Non-Consumable]," + nl +
						"     [Product].[Food].[Eggs]," + nl +
						"     [Product].[Drink].[Dairy]})",

						"[Product].[All Products]" + nl +
						"[Product].[All Products].[Drink]" + nl +
						"[Product].[All Products].[Drink].[Dairy]" + nl +
						"[Product].[All Products].[Food]" + nl +
						"[Product].[All Products].[Food].[Eggs]" + nl +
						"[Product].[All Products].[Non-Consumable]");
			}
			public void testHierarchizePost(FoodMartTestCase test) {
				test.assertAxisReturns(
						"Hierarchize(" + nl +
						"    {[Product].[All Products], " +
						"     [Product].[Food]," + nl +
						"     [Product].[Food].[Eggs]," + nl +
						"     [Product].[Drink].[Dairy]}," + nl +
						"  POST)",

						"[Product].[All Products].[Drink].[Dairy]" + nl +
						"[Product].[All Products].[Food].[Eggs]" + nl +
						"[Product].[All Products].[Food]" + nl +
						"[Product].[All Products]");
			}
			public void testHierarchizeCrossJoinPre(FoodMartTestCase test) {
				test.assertAxisReturns(
						"Hierarchize(" + nl +
						"  CrossJoin(" + nl +
						"    {[Product].[All Products], " +
						"     [Product].[Food]," + nl +
						"     [Product].[Food].[Eggs]," + nl +
						"     [Product].[Drink].[Dairy]}," + nl +
						"    [Gender].MEMBERS)," + nl +
						"  PRE)",

						"{[Product].[All Products], [Gender].[All Gender]}" + nl +
						"{[Product].[All Products], [Gender].[All Gender].[F]}" + nl +
						"{[Product].[All Products], [Gender].[All Gender].[M]}" + nl +
						"{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender]}" + nl +
						"{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[F]}" + nl +
						"{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[M]}" + nl +
						"{[Product].[All Products].[Food], [Gender].[All Gender]}" + nl +
						"{[Product].[All Products].[Food], [Gender].[All Gender].[F]}" + nl +
						"{[Product].[All Products].[Food], [Gender].[All Gender].[M]}" + nl +
						"{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender]}" + nl +
						"{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[F]}" + nl +
						"{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[M]}");
			}
			public void testHierarchizeCrossJoinPost(FoodMartTestCase test) {
				test.assertAxisReturns(
						"Hierarchize(" + nl +
						"  CrossJoin(" + nl +
						"    {[Product].[All Products], " +
						"     [Product].[Food]," + nl +
						"     [Product].[Food].[Eggs]," + nl +
						"     [Product].[Drink].[Dairy]}," + nl +
						"    [Gender].MEMBERS)," + nl +
						"  POST)",

						"{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[F]}" + nl +
						"{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[M]}" + nl +
						"{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender]}" + nl +
						"{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[F]}" + nl +
						"{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[M]}" + nl +
						"{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender]}" + nl +
						"{[Product].[All Products].[Food], [Gender].[All Gender].[F]}" + nl +
						"{[Product].[All Products].[Food], [Gender].[All Gender].[M]}" + nl +
						"{[Product].[All Products].[Food], [Gender].[All Gender]}" + nl +
						"{[Product].[All Products], [Gender].[All Gender].[F]}" + nl +
						"{[Product].[All Products], [Gender].[All Gender].[M]}" + nl +
						"{[Product].[All Products], [Gender].[All Gender]}");
			}
		});

		define(new MultiResolver(
				"Intersect", "Intersect(<Set1>, <Set2>[, ALL])", "Returns the intersection of two input sets, optionally retaining duplicates.",
				new String[] {"fxxxy", "fxxx"}) {
			protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
				final boolean all = getLiteralArg(args, 2, "", new String[] {"ALL"}, dummyFunDef).equalsIgnoreCase("ALL");
				return new FunDefBase(dummyFunDef) {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						List left = (List) getArg(evaluator, args, 0);
						if (left == null) {
							left = Collections.EMPTY_LIST;
						}
						List right = (List) getArg(evaluator, args, 1);
						if (right == null) {
							right = Collections.EMPTY_LIST;
						}
						ArrayList result = new ArrayList();
						for (Iterator i = left.iterator(); i.hasNext();) {
							Object leftObject = i.next();
							if (right.contains(leftObject)) {
								if (all || !result.contains(leftObject)) {
									result.add(leftObject);
								}
							}
						}
						return result;
					}
				};
			}
			public void testIntersect(FoodMartTestCase test) {
				// Note: duplicates retained from left, not from right; and order is preserved.
				test.assertAxisReturns("Intersect({[Time].[1997].[Q2], [Time].[1997], [Time].[1997].[Q1], [Time].[1997].[Q2]}, " +
						"{[Time].[1998], [Time].[1997], [Time].[1997].[Q2], [Time].[1997]}, " +
						"ALL)",
						"[Time].[1997].[Q2]" + nl +
						"[Time].[1997]" + nl +
						"[Time].[1997].[Q2]");
			}
			public void testIntersectRightEmpty(FoodMartTestCase test) {
				test.assertAxisReturns("Intersect({[Time].[1997]}, {})",
						"");
			}
			public void testIntersectLeftEmpty(FoodMartTestCase test) {
				test.assertAxisReturns("Intersect({}, {[Store].[USA].[CA]})",
						"");
			}
		});
		if (false) define(new FunDefBase("LastPeriods", "LastPeriods(<Index>[, <Member>])", "Returns a set of members prior to and including a specified member.", "fx*"));
		define(new FunDefBase("Members", "<Dimension>.Members", "Returns the set of all members in a dimension.", "pxd") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Dimension dimension = (Dimension) getArg(evaluator, args, 0);
				Hierarchy hierarchy = dimension.getHierarchy();
				return addMembers(evaluator.getSchemaReader(), new ArrayList(), hierarchy);
			}
		});
		define(new FunDefBase("Members", "<Hierarchy>.Members", "Returns the set of all members in a hierarchy.", "pxh") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Hierarchy hierarchy =
						(Hierarchy) getArg(evaluator, args, 0);
				return addMembers(evaluator.getSchemaReader(), new ArrayList(), hierarchy);
			}
		});
		define(new FunDefBase("Members", "<Level>.Members", "Returns the set of all members in a level.", "pxl") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Level level = (Level) getArg(evaluator, args, 0);
				return Arrays.asList(evaluator.getSchemaReader().getLevelMembers(level));
			}
		});
		define(new FunkResolver(
				"Mtd", "Mtd([<Member>])", "A shortcut function for the PeriodsToDate function that specifies the level to be Month.",
				new String[]{"fx", "fxm"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						return periodsToDate(
								evaluator,
								evaluator.getCube().getMonthLevel(),
								getMemberArg(evaluator, args, 0, false));
					}
				}));

		defineReserved(OrderFlags.instance);
		define(new MultiResolver(
				"Order", "Order(<Set>, <Value Expression>[, ASC | DESC | BASC | BDESC])", "Arranges members of a set, optionally preserving or breaking the hierarchy.",
				new String[]{"fxxvy", "fxxv"}) {
			protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
				int order = getLiteralArg(args, 2, OrderFlags.ASC, OrderFlags.instance, dummyFunDef);
				final boolean desc = OrderFlags.isDescending(order);
				final boolean brk = OrderFlags.isBreak(order);
				return new FunDefBase(dummyFunDef) {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						List members = (List) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArgNoEval(args, 1);
						sort(evaluator, members, exp, desc, brk);
						return members;
					}
				};
			}
			public void testOrder(FoodMartTestCase test) {
				test.runQueryCheckResult(
						"select {[Measures].[Unit Sales]} on columns," + nl +
						" order({" + nl +
						"  [Product].[All Products].[Drink]," + nl +
						"  [Product].[All Products].[Drink].[Beverages]," + nl +
						"  [Product].[All Products].[Drink].[Dairy]," + nl +
						"  [Product].[All Products].[Food]," + nl +
						"  [Product].[All Products].[Food].[Baked Goods]," + nl +
						"  [Product].[All Products].[Food].[Eggs]," + nl +
						"  [Product].[All Products]}," + nl +
						" [Measures].[Unit Sales]) on rows" + nl +
						"from Sales",

						"Axis #0:" + nl +
						"{}" + nl +
						"Axis #1:" + nl +
						"{[Measures].[Unit Sales]}" + nl +
						"Axis #2:" + nl +
						"{[Product].[All Products]}" + nl +
						"{[Product].[All Products].[Drink]}" + nl +
						"{[Product].[All Products].[Drink].[Dairy]}" + nl +
						"{[Product].[All Products].[Drink].[Beverages]}" + nl +
						"{[Product].[All Products].[Food]}" + nl +
						"{[Product].[All Products].[Food].[Eggs]}" + nl +
						"{[Product].[All Products].[Food].[Baked Goods]}" + nl +
						"Row #0: 266,773" + nl +
						"Row #1: 24,597" + nl +
						"Row #2: 4,186" + nl +
						"Row #3: 13,573" + nl +
						"Row #4: 191,940" + nl +
						"Row #5: 4,132" + nl +
						"Row #6: 7,870" + nl);
			}
			public void testOrderParentsMissing(FoodMartTestCase test) {
				// Paradoxically, [Alcoholic Beverages] comes before
				// [Eggs] even though it has a larger value, because
				// its parent [Drink] has a smaller value than [Food].
				test.runQueryCheckResult(
						"select {[Measures].[Unit Sales]} on columns," +
						" order({" + nl +
						"  [Product].[All Products].[Drink].[Alcoholic Beverages]," + nl +
						"  [Product].[All Products].[Food].[Eggs]}," + nl +
						" [Measures].[Unit Sales], ASC) on rows" + nl +
						"from Sales",

						"Axis #0:" + nl +
						"{}" + nl +
						"Axis #1:" + nl +
						"{[Measures].[Unit Sales]}" + nl +
						"Axis #2:" + nl +
						"{[Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
						"{[Product].[All Products].[Food].[Eggs]}" + nl +
						"Row #0: 6,838" + nl +
						"Row #1: 4,132" + nl);
			}
			public void testOrderCrossJoinBreak(FoodMartTestCase test) {
				test.runQueryCheckResult(
						"select {[Measures].[Unit Sales]} on columns," + nl +
						"  Order(" + nl +
						"    CrossJoin(" + nl +
						"      [Gender].children," + nl +
						"      [Marital Status].children)," + nl +
						"    [Measures].[Unit Sales]," + nl +
						"    BDESC) on rows" + nl +
						"from Sales" + nl +
						"where [Time].[1997].[Q1]",

						"Axis #0:" + nl +
						"{[Time].[1997].[Q1]}" + nl +
						"Axis #1:" + nl +
						"{[Measures].[Unit Sales]}" + nl +
						"Axis #2:" + nl +
						"{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}" + nl +
						"{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M]}" + nl +
						"{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}" + nl +
						"{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}" + nl +
						"Row #0: 17,070" + nl +
						"Row #1: 16,790" + nl +
						"Row #2: 16,311" + nl +
						"Row #3: 16,120" + nl);
			}
			public void testOrderCrossJoin(FoodMartTestCase test) {
				// Note:
				// 1. [Alcoholic Beverages] collates before [Eggs] and
				//    [Seafood] because its parent, [Drink], is less
				//    than [Food]
				// 2. [Seattle] generally sorts after [CA] and [OR]
				//    because invisible parent [WA] is greater.
				test.runQueryCheckResult(
						"select CrossJoin(" + nl +
						"    {[Time].[1997]," + nl +
						"     [Time].[1997].[Q1]}," + nl +
						"    {[Measures].[Unit Sales]}) on columns," + nl +
						"  Order(" + nl +
						"    CrossJoin( " + nl +
						"      {[Product].[All Products].[Food].[Eggs]," + nl +
						"       [Product].[All Products].[Food].[Seafood]," + nl +
						"       [Product].[All Products].[Drink].[Alcoholic Beverages]}," + nl +
						"      {[Store].[USA].[WA].[Seattle]," + nl +
						"       [Store].[USA].[CA]," + nl +
						"       [Store].[USA].[OR]})," + nl +
						"    ([Time].[1997].[Q1], [Measures].[Unit Sales])," + nl +
						"    ASC) on rows" + nl +
						"from Sales",

						"Axis #0:" + nl +
						"{}" + nl +
						"Axis #1:" + nl +
						"{[Time].[1997], [Measures].[Unit Sales]}" + nl +
						"{[Time].[1997].[Q1], [Measures].[Unit Sales]}" + nl +
						"Axis #2:" + nl +
						"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Store].[All Stores].[USA].[OR]}" + nl +
						"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Store].[All Stores].[USA].[CA]}" + nl +
						"{[Product].[All Products].[Drink].[Alcoholic Beverages], [Store].[All Stores].[USA].[WA].[Seattle]}" + nl +
						"{[Product].[All Products].[Food].[Seafood], [Store].[All Stores].[USA].[CA]}" + nl +
						"{[Product].[All Products].[Food].[Seafood], [Store].[All Stores].[USA].[OR]}" + nl +
						"{[Product].[All Products].[Food].[Seafood], [Store].[All Stores].[USA].[WA].[Seattle]}" + nl +
						"{[Product].[All Products].[Food].[Eggs], [Store].[All Stores].[USA].[CA]}" + nl +
						"{[Product].[All Products].[Food].[Eggs], [Store].[All Stores].[USA].[OR]}" + nl +
						"{[Product].[All Products].[Food].[Eggs], [Store].[All Stores].[USA].[WA].[Seattle]}" + nl +
						"Row #0: 1,680" + nl +
						"Row #0: 393" + nl +
						"Row #1: 1,936" + nl +
						"Row #1: 431" + nl +
						"Row #2: 635" + nl +
						"Row #2: 142" + nl +
						"Row #3: 441" + nl +
						"Row #3: 91" + nl +
						"Row #4: 451" + nl +
						"Row #4: 107" + nl +
						"Row #5: 217" + nl +
						"Row #5: 44" + nl +
						"Row #6: 1,116" + nl +
						"Row #6: 240" + nl +
						"Row #7: 1,119" + nl +
						"Row #7: 251" + nl +
						"Row #8: 373" + nl +
						"Row #8: 57" + nl);
			}
			public void testOrderHierarchicalDesc(FoodMartTestCase test) {
				test.assertAxisReturns(
						"Order(" + nl +
						"    {[Product].[All Products], " +
						"     [Product].[Food]," + nl +
						"     [Product].[Drink]," + nl +
						"     [Product].[Non-Consumable]," + nl +
						"     [Product].[Food].[Eggs]," + nl +
						"     [Product].[Drink].[Dairy]}," + nl +
						"  [Measures].[Unit Sales]," + nl +
						"  DESC)",

						"[Product].[All Products]" + nl +
						"[Product].[All Products].[Food]" + nl +
						"[Product].[All Products].[Food].[Eggs]" + nl +
						"[Product].[All Products].[Non-Consumable]" + nl +
						"[Product].[All Products].[Drink]" + nl +
						"[Product].[All Products].[Drink].[Dairy]");
			}
			public void testOrderCrossJoinDesc(FoodMartTestCase test) {
				test.assertAxisReturns(
						"Order(" + nl +
						"  CrossJoin(" + nl +
						"    {[Gender].[M], [Gender].[F]}," + nl +
						"    {[Product].[All Products], " +
						"     [Product].[Food]," + nl +
						"     [Product].[Drink]," + nl +
						"     [Product].[Non-Consumable]," + nl +
						"     [Product].[Food].[Eggs]," + nl +
						"     [Product].[Drink].[Dairy]})," + nl +
						"  [Measures].[Unit Sales]," + nl +
						"  DESC)",

						"{[Gender].[All Gender].[M], [Product].[All Products]}" + nl +
						"{[Gender].[All Gender].[M], [Product].[All Products].[Food]}" + nl +
						"{[Gender].[All Gender].[M], [Product].[All Products].[Food].[Eggs]}" + nl +
						"{[Gender].[All Gender].[M], [Product].[All Products].[Non-Consumable]}" + nl +
						"{[Gender].[All Gender].[M], [Product].[All Products].[Drink]}" + nl +
						"{[Gender].[All Gender].[M], [Product].[All Products].[Drink].[Dairy]}" + nl +
						"{[Gender].[All Gender].[F], [Product].[All Products]}" + nl +
						"{[Gender].[All Gender].[F], [Product].[All Products].[Food]}" + nl +
						"{[Gender].[All Gender].[F], [Product].[All Products].[Food].[Eggs]}" + nl +
						"{[Gender].[All Gender].[F], [Product].[All Products].[Non-Consumable]}" + nl +
						"{[Gender].[All Gender].[F], [Product].[All Products].[Drink]}" + nl +
						"{[Gender].[All Gender].[F], [Product].[All Products].[Drink].[Dairy]}");
			}
			public void testOrderBug656802(FoodMartTestCase test) {
				// Note:
				// 1. [Alcoholic Beverages] collates before [Eggs] and
				//    [Seafood] because its parent, [Drink], is less
				//    than [Food]
				// 2. [Seattle] generally sorts after [CA] and [OR]
				//    because invisible parent [WA] is greater.
				test.runQueryCheckResult(
						"select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns, " + nl +
						"Order(" + nl +
						"  ToggleDrillState(" + nl +
						"    {([Promotion Media].[All Media], [Product].[All Products])}," + nl +
						"    {[Product].[All Products]} ), " + nl +
						"  [Measures].[Unit Sales], DESC) ON rows " + nl +
						"from [Sales] where ([Time].[1997])",

						"Axis #0:" + nl +
						"{[Time].[1997]}" + nl +
						"Axis #1:" + nl +
						"{[Measures].[Unit Sales]}" + nl +
						"{[Measures].[Store Cost]}" + nl +
						"{[Measures].[Store Sales]}" + nl +
						"Axis #2:" + nl +
						"{[Promotion Media].[All Media], [Product].[All Products]}" + nl +
						"{[Promotion Media].[All Media], [Product].[All Products].[Food]}" + nl +
						"{[Promotion Media].[All Media], [Product].[All Products].[Non-Consumable]}" + nl +
						"{[Promotion Media].[All Media], [Product].[All Products].[Drink]}" + nl +
						"Row #0: 266,773" + nl +
						"Row #0: 225,627.23" + nl +
						"Row #0: 565,238.13" + nl +
						"Row #1: 191,940" + nl +
						"Row #1: 163,270.72" + nl +
						"Row #1: 409,035.59" + nl +
						"Row #2: 50,236" + nl +
						"Row #2: 42,879.28" + nl +
						"Row #2: 107,366.33" + nl +
						"Row #3: 24,597" + nl +
						"Row #3: 19,477.23" + nl +
						"Row #3: 48,836.21" + nl);
			}
			public void testOrderBug712702_Simplified(FoodMartTestCase test) {
				test.runQueryCheckResult("SELECT Order({[Time].[Year].members}, [Measures].[Unit Sales]) on columns" + nl +
						"from [Sales]",
						"Axis #0:" + nl +
						"{}" + nl +
						"Axis #1:" + nl +
						"{[Time].[1998]}" + nl +
						"{[Time].[1997]}" + nl +
						"Row #0: (null)" + nl +
						"Row #0: 266,773" + nl);
			}
			public void testOrderBug712702_Original(FoodMartTestCase test) {
				test.runQueryCheckResult("with member [Measures].[Average Unit Sales] as 'Avg(Descendants([Time].CurrentMember, [Time].[Month]), " + nl +
						"[Measures].[Unit Sales])' " + nl +
						"member [Measures].[Max Unit Sales] as 'Max(Descendants([Time].CurrentMember, [Time].[Month]), [Measures].[Unit Sales])' " + nl +
						"select {[Measures].[Average Unit Sales], [Measures].[Max Unit Sales], [Measures].[Unit Sales]} ON columns, " + nl +
						"  NON EMPTY Order(" + nl +
						"    Crossjoin( " + nl +
						"      {[Store].[All Stores].[USA].[OR].[Portland]," + nl +
						"       [Store].[All Stores].[USA].[OR].[Salem]," + nl +
						"       [Store].[All Stores].[USA].[OR].[Salem].[Store 13]," + nl +
						"       [Store].[All Stores].[USA].[CA].[San Francisco]," + nl +
						"       [Store].[All Stores].[USA].[CA].[San Diego]," + nl +
						"       [Store].[All Stores].[USA].[CA].[Beverly Hills]," + nl +
						"       [Store].[All Stores].[USA].[CA].[Los Angeles]," + nl +
						"       [Store].[All Stores].[USA].[WA].[Walla Walla]," + nl +
						"       [Store].[All Stores].[USA].[WA].[Bellingham]," + nl +
						"       [Store].[All Stores].[USA].[WA].[Yakima]," + nl +
						"       [Store].[All Stores].[USA].[WA].[Spokane]," + nl +
						"       [Store].[All Stores].[USA].[WA].[Seattle], " + nl +
						"       [Store].[All Stores].[USA].[WA].[Bremerton]," + nl +
						"       [Store].[All Stores].[USA].[WA].[Tacoma]}," + nl +
						"     [Time].[Year].Members), " + nl +
						"  [Measures].[Average Unit Sales], ASC) ON rows" + nl +
						"from [Sales] ",
						"Axis #0:" + nl +
						"{}" + nl +
						"Axis #1:" + nl +
						"{[Measures].[Average Unit Sales]}" + nl +
						"{[Measures].[Max Unit Sales]}" + nl +
						"{[Measures].[Unit Sales]}" + nl +
						"Axis #2:" + nl +
						"{[Store].[All Stores].[USA].[OR].[Portland], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[OR].[Salem], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[OR].[Salem].[Store 13], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[CA].[San Francisco], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[CA].[Beverly Hills], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[CA].[San Diego], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[CA].[Los Angeles], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[WA].[Walla Walla], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[WA].[Bellingham], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[WA].[Yakima], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[WA].[Spokane], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[WA].[Bremerton], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[WA].[Seattle], [Time].[1997]}" + nl +
						"{[Store].[All Stores].[USA].[WA].[Tacoma], [Time].[1997]}" + nl +
						"Row #0: 2,173" + nl +
						"Row #0: 2,933" + nl +
						"Row #0: 26,079" + nl +
						"Row #1: 3,465" + nl +
						"Row #1: 5,891" + nl +
						"Row #1: 41,580" + nl +
						"Row #2: 3,465" + nl +
						"Row #2: 5,891" + nl +
						"Row #2: 41,580" + nl +
						"Row #3: 176" + nl +
						"Row #3: 222" + nl +
						"Row #3: 2,117" + nl +
						"Row #4: 1,778" + nl +
						"Row #4: 2,545" + nl +
						"Row #4: 21,333" + nl +
						"Row #5: 2,136" + nl +
						"Row #5: 2,686" + nl +
						"Row #5: 25,635" + nl +
						"Row #6: 2,139" + nl +
						"Row #6: 2,669" + nl +
						"Row #6: 25,663" + nl +
						"Row #7: 184" + nl +
						"Row #7: 301" + nl +
						"Row #7: 2,203" + nl +
						"Row #8: 186" + nl +
						"Row #8: 275" + nl +
						"Row #8: 2,237" + nl +
						"Row #9: 958" + nl +
						"Row #9: 1,163" + nl +
						"Row #9: 11,491" + nl +
						"Row #10: 1,966" + nl +
						"Row #10: 2,634" + nl +
						"Row #10: 23,591" + nl +
						"Row #11: 2,048" + nl +
						"Row #11: 2,623" + nl +
						"Row #11: 24,576" + nl +
						"Row #12: 2,084" + nl +
						"Row #12: 2,304" + nl +
						"Row #12: 25,011" + nl +
						"Row #13: 2,938" + nl +
						"Row #13: 3,818" + nl +
						"Row #13: 35,257" + nl);
			}
		});

		define(new FunkResolver(
				"PeriodsToDate", "PeriodsToDate([<Level>[, <Member>]])", "Returns a set of periods (members) from a specified level starting with the first period and ending with a specified member.",
				new String[]{"fx", "fxl", "fxlm"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Level level = getLevelArg(evaluator, args, 0, false);
						Member member = getMemberArg(evaluator, args, 1, false);
						return periodsToDate(evaluator, level, member);
					}
				}));
		define(new FunkResolver(
				"Qtd", "Qtd([<Member>])", "A shortcut function for the PeriodsToDate function that specifies the level to be Quarter.",
				new String[]{"fx", "fxm"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						return periodsToDate(
								evaluator,
								evaluator.getCube().getQuarterLevel(),
								getMemberArg(evaluator, args, 0, false));
					}
				}));
		if (false) define(new FunDefBase("StripCalculatedMembers", "StripCalculatedMembers(<Set>)", "Removes calculated members from a set.", "fx*"));
		// "Siblings" is not a standard MDX function.
		define(new FunDefBase("Siblings", "<Member>.Siblings", "Returns the set of siblings of the specified member.", "pxm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				Member parent = member.getParentMember();
				final SchemaReader schemaReader = evaluator.getSchemaReader();
				Member[] siblings;
				if (parent == null) {
					siblings = schemaReader.getHierarchyRootMembers(member.getHierarchy());
				} else {
					siblings = schemaReader.getMemberChildren(parent);
				}
				return Arrays.asList(siblings);
			}
			public void testSiblingsA(FoodMartTestCase test) {
				test.assertAxisReturns("{[Time].[1997].Siblings}",
						"[Time].[1997]" + nl +
						"[Time].[1998]");
			}
			public void testSiblingsB(FoodMartTestCase test) {
				test.assertAxisReturns("{[Store].Siblings}",
						"[Store].[All Stores]");
			}
			public void testSiblingsC(FoodMartTestCase test) {
				test.assertAxisReturns("{[Store].[USA].[CA].Siblings}",
						"[Store].[All Stores].[USA].[CA]" + nl +
						"[Store].[All Stores].[USA].[OR]" + nl +
						"[Store].[All Stores].[USA].[WA]");
			}
		});

		define(new FunDefBase("StrToSet", "StrToSet(<String Expression>)", "Constructs a set from a string expression.", "fxS") {
			public Hierarchy getHierarchy(Exp[] args) {
				// StrToSet(s, <Hie1>, ... <HieN>) is of type [Hie1] x ... x [HieN];
				// so, for example, So StrToTuple("[Time].[1997]", [Time]) is of type
				// [Time].  But if n > 1, we cannot represent the compound type, and we
				// return null.
				return (args.length == 2) ?
						(Hierarchy) args[1] :
						null;
			}
		});

        define(new FunkResolver(
                "Subset", "Subset(<Set>, <Start>[, <Count>])", "Returns a subset of elements from a set.",
                new String[] {"fxxn", "fxxnn"},
                new FunkBase() {
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
                    public void testSubset(FoodMartTestCase test) {
                        test.assertAxisReturns("Subset([Promotion Media].Children, 7, 2)",
                                "[Promotion Media].[All Media].[Product Attachment]" + nl +
                                "[Promotion Media].[All Media].[Radio]");
                    }
                    public void testSubsetNegativeCount(FoodMartTestCase test) {
                        test.assertAxisReturns("Subset([Promotion Media].Children, 3, -1)",
                                "");
                    }
                    public void testSubsetNegativeStart(FoodMartTestCase test) {
                        test.assertAxisReturns("Subset([Promotion Media].Children, -2, 4)",
                                "");
                    }
                    public void testSubsetDefault(FoodMartTestCase test) {
                        test.assertAxisReturns("Subset([Promotion Media].Children, 11)",
                                "[Promotion Media].[All Media].[Sunday Paper, Radio]" + nl +
                                "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]" + nl +
                                "[Promotion Media].[All Media].[TV]");
                    }
                    public void testSubsetOvershoot(FoodMartTestCase test) {
                        test.assertAxisReturns("Subset([Promotion Media].Children, 15)",
                                "");
                    }
                    public void testSubsetEmpty(FoodMartTestCase test) {
                        test.assertAxisReturns("Subset([Gender].[F].Children, 1)",
                                "");
                    }
                }));

        define(new FunkResolver(
                "Tail", "Tail(<Set>[, <Count>])", "Returns a subset from the end of a set.",
                new String[] {"fxx", "fxxn"},
                new FunkBase() {
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
                    public void testTail(FoodMartTestCase test) {
                        test.assertAxisReturns("Tail([Store].Children, 2)",
                                "[Store].[All Stores].[Mexico]" + nl +
                                "[Store].[All Stores].[USA]");
                    }
                    public void testTailNegative(FoodMartTestCase test) {
                        test.assertAxisReturns("Tail([Store].Children, 2 - 3)",
                                "");
                    }
                    public void testTailDefault(FoodMartTestCase test) {
                        test.assertAxisReturns("Tail([Store].Children)",
                                "[Store].[All Stores].[USA]");
                    }
                    public void testTailOvershoot(FoodMartTestCase test) {
                        test.assertAxisReturns("Tail([Store].Children, 2 + 2)",
                                "[Store].[All Stores].[Canada]" + nl +
                                "[Store].[All Stores].[Mexico]" + nl +
                                "[Store].[All Stores].[USA]");
                    }
                    public void testTailEmpty(FoodMartTestCase test) {
                        test.assertAxisReturns("Tail([Gender].[F].Children, 2)",
                                "");
                    }
                }));

		defineReserved("RECURSIVE");
		define(new FunkResolver(
				"ToggleDrillState", "ToggleDrillState(<Set1>, <Set2>[, RECURSIVE])", "Toggles the drill state of members. This function is a combination of DrillupMember and DrilldownMember.",
				new String[]{"fxxx", "fxxx#"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						List v0 = (List) getArg(evaluator, args, 0),
								v1 = (List) getArg(evaluator, args, 1);
						if (args.length > 2) {
							throw Util.newInternal(
									"ToggleDrillState(RECURSIVE) not supported");
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
					public void testToggleDrillState(FoodMartTestCase test) {
						test.assertAxisReturns("ToggleDrillState({[Customers].[USA],[Customers].[Canada]},{[Customers].[USA],[Customers].[USA].[CA]})",
								"[Customers].[All Customers].[USA]" + nl +
								"[Customers].[All Customers].[USA].[CA]" + nl +
								"[Customers].[All Customers].[USA].[OR]" + nl +
								"[Customers].[All Customers].[USA].[WA]" + nl +
								"[Customers].[All Customers].[Canada]");
					}
					public void testToggleDrillState2(FoodMartTestCase test) {
						test.assertAxisReturns("ToggleDrillState([Product].[Product Department].members, {[Product].[All Products].[Food].[Snack Foods]})",
								"[Product].[All Products].[Drink].[Alcoholic Beverages]" + nl +
								"[Product].[All Products].[Drink].[Beverages]" + nl +
								"[Product].[All Products].[Drink].[Dairy]" + nl +
								"[Product].[All Products].[Food].[Baked Goods]" + nl +
								"[Product].[All Products].[Food].[Baking Goods]" + nl +
								"[Product].[All Products].[Food].[Breakfast Foods]" + nl +
								"[Product].[All Products].[Food].[Canned Foods]" + nl +
								"[Product].[All Products].[Food].[Canned Products]" + nl +
								"[Product].[All Products].[Food].[Dairy]" + nl +
								"[Product].[All Products].[Food].[Deli]" + nl +
								"[Product].[All Products].[Food].[Eggs]" + nl +
								"[Product].[All Products].[Food].[Frozen Foods]" + nl +
								"[Product].[All Products].[Food].[Meat]" + nl +
								"[Product].[All Products].[Food].[Produce]" + nl +
								"[Product].[All Products].[Food].[Seafood]" + nl +
								"[Product].[All Products].[Food].[Snack Foods]" + nl +
								"[Product].[All Products].[Food].[Snack Foods].[Snack Foods]" + nl +
								"[Product].[All Products].[Food].[Snacks]" + nl +
								"[Product].[All Products].[Food].[Starchy Foods]" + nl +
								"[Product].[All Products].[Non-Consumable].[Carousel]" + nl +
								"[Product].[All Products].[Non-Consumable].[Checkout]" + nl +
								"[Product].[All Products].[Non-Consumable].[Health and Hygiene]" + nl +
								"[Product].[All Products].[Non-Consumable].[Household]" + nl +
								"[Product].[All Products].[Non-Consumable].[Periodicals]");
					}
					public void testToggleDrillState3(FoodMartTestCase test) {
						test.assertAxisReturns(
								"ToggleDrillState(" +
								"{[Time].[1997].[Q1]," +
								" [Time].[1997].[Q2]," +
								" [Time].[1997].[Q2].[4]," +
								" [Time].[1997].[Q2].[6]," +
								" [Time].[1997].[Q3]}," +
								"{[Time].[1997].[Q2]})",
								"[Time].[1997].[Q1]" + nl +
								"[Time].[1997].[Q2]" + nl +
								"[Time].[1997].[Q3]");
					}
					// bug 634860
					public void testToggleDrillStateTuple(FoodMartTestCase test) {
                        test.assertAxisReturns(
								"ToggleDrillState(" + nl +
								"{([Store].[All Stores].[USA].[CA]," +
								"  [Product].[All Products].[Drink].[Alcoholic Beverages])," + nl +
								" ([Store].[All Stores].[USA]," +
								"  [Product].[All Products].[Drink])}," + nl +
								"{[Store].[All stores].[USA].[CA]})",
								"{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
								"{[Store].[All Stores].[USA].[CA].[Alameda], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
								"{[Store].[All Stores].[USA].[CA].[Beverly Hills], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
								"{[Store].[All Stores].[USA].[CA].[Los Angeles], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
								"{[Store].[All Stores].[USA].[CA].[San Diego], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
								"{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
								"{[Store].[All Stores].[USA], [Product].[All Products].[Drink]}");
					}
				}));
		define(new FunkResolver(
				"TopCount",
				"TopCount(<Set>, <Count>[, <Numeric Expression>])",
				"Returns a specified number of items from the top of a set, optionally ordering the set first.",
				new String[]{"fxxnn", "fxxn"},
				new FunkBase() {
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
					public void testTopCount(FoodMartTestCase test) {
						test.assertAxisReturns("TopCount({[Promotion Media].[Media Type].members}, 2, [Measures].[Unit Sales])",
								"[Promotion Media].[All Media].[No Media]" + nl +
								"[Promotion Media].[All Media].[Daily Paper, Radio, TV]");
					}
					public void testTopCountTuple(FoodMartTestCase test) {
						test.assertAxisReturns("TopCount([Customers].[Name].members,2,(Time.[1997].[Q1],[Measures].[Store Sales]))",
							"[Customers].[All Customers].[USA].[WA].[Spokane].[Grace McLaughlin]" + nl +
							"[Customers].[All Customers].[USA].[WA].[Spokane].[Matt Bellah]");
					}
				}));

		define(new FunkResolver(
			"TopPercent", "TopPercent(<Set>, <Percentage>, <Numeric Expression>)", "Sorts a set and returns the top N elements whose cumulative total is at least a specified percentage.",
			new String[]{"fxxnn"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					List members = (List) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArgNoEval(args, 2);
					Double n = getDoubleArg(evaluator, args, 1);
					return topOrBottom(evaluator.push(), members, exp, true, true, n.doubleValue());

				}
				public void testTopPercent(FoodMartTestCase test) {
					test.assertAxisReturns("TopPercent({[Promotion Media].[Media Type].members}, 70, [Measures].[Unit Sales])",
							"[Promotion Media].[All Media].[No Media]");
				}
				//todo: test precision
			}));

		define(new FunkResolver(
			"TopSum", "TopSum(<Set>, <Value>, <Numeric Expression>)", "Sorts a set and returns the top N elements whose cumulative total is at least a specified value.",
			new String[]{"fxxnn"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					List members = (List) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArgNoEval(args, 2);
					Double n = getDoubleArg(evaluator, args, 1);
					return topOrBottom(evaluator.push(), members, exp, true, false, n.doubleValue());

				}
				public void testTopSum(FoodMartTestCase test) {
					test.assertAxisReturns("TopSum({[Promotion Media].[Media Type].members}, 200000, [Measures].[Unit Sales])",
							"[Promotion Media].[All Media].[No Media]" + nl +
							"[Promotion Media].[All Media].[Daily Paper, Radio, TV]");
				}
			}));

		defineReserved(new String[] {"ALL", "DISTINCT"});
		define(new MultiResolver(
				"Union", "Union(<Set1>, <Set2>[, ALL])", "Returns the union of two sets, optionally retaining duplicates.",
				new String[] {"fxxx", "fxxxy"}) {
			protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
				String allString = getLiteralArg(args, 2, "DISTINCT", new String[] {"ALL", "DISTINCT"}, dummyFunDef);
				final boolean all = allString.equalsIgnoreCase("ALL");
				checkCompatible(args[0], args[1], dummyFunDef);
				return new FunDefBase(dummyFunDef) {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						List left = (List) getArg(evaluator, args, 0),
								right = (List) getArg(evaluator, args, 1);
						if (all) {
							if (left == null || left.isEmpty()) {
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
			public void testUnionAll(FoodMartTestCase test) {
				test.assertAxisReturns("Union({[Gender].[M]}, {[Gender].[F]}, ALL)",
						"[Gender].[All Gender].[M]" + nl +
						"[Gender].[All Gender].[F]"); // order is preserved
			}
			public void testUnion(FoodMartTestCase test) {
				test.assertAxisReturns("Union({[Store].[USA], [Store].[USA], [Store].[USA].[OR]}, {[Store].[USA].[CA], [Store].[USA]})",
						"[Store].[All Stores].[USA]" + nl +
						"[Store].[All Stores].[USA].[OR]" + nl +
						"[Store].[All Stores].[USA].[CA]");
			}
			public void testUnionEmptyBoth(FoodMartTestCase test) {
				test.assertAxisReturns("Union({}, {})",
						"");
			}
			public void testUnionEmptyRight(FoodMartTestCase test) {
				test.assertAxisReturns("Union({[Gender].[M]}, {})",
						"[Gender].[All Gender].[M]");
			}
			public void testUnionTuple(FoodMartTestCase test) {
				test.assertAxisReturns(
						"Union({" +
						" ([Gender].[M], [Marital Status].[S])," +
						" ([Gender].[F], [Marital Status].[S])" +
						"}, {" +
						" ([Gender].[M], [Marital Status].[M])," +
						" ([Gender].[M], [Marital Status].[S])" +
						"})",

						"{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}" + nl +
						"{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}" + nl +
						"{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}");
			}
			public void testUnionQuery(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns," + nl +
						" Hierarchize( " + nl +
						"   Union(" + nl +
						"     Crossjoin(" + nl +
						"       Crossjoin([Gender].[All Gender].children," + nl +
						"                 [Marital Status].[All Marital Status].children )," + nl +
						"       Crossjoin([Customers].[All Customers].children," + nl +
						"                 [Product].[All Products].children ) ) ," + nl +
						"     Crossjoin( {([Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M] )}," + nl +
						"       Crossjoin(" + nl +
						"         [Customers].[All Customers].[USA].children," + nl +
						"         [Product].[All Products].children ) ) )) on rows" +nl +
						"from Sales where ([Time].[1997])");
				final Axis rowsAxis = result.getAxes()[1];
				Assert.assertEquals(45, rowsAxis.positions.length);
			}
		});

		if (false) define(new FunDefBase("VisualTotals", "VisualTotals(<Set>, <Pattern>)", "Dynamically totals child members specified in a set using a pattern for the total label in the result set.", "fx*"));
		define(new FunkResolver(
				"Wtd", "Wtd([<Member>])", "A shortcut function for the PeriodsToDate function that specifies the level to be Week.",
				new String[]{"fx", "fxm"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						return periodsToDate(
								evaluator,
								evaluator.getCube().getWeekLevel(),
								getMemberArg(evaluator, args, 0, false));
					}
				}));
		define(new FunkResolver(
				"Ytd", "Ytd([<Member>])", "A shortcut function for the PeriodsToDate function that specifies the level to be Year.",
				new String[]{"fx", "fxm"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						return periodsToDate(
								evaluator,
								evaluator.getCube().getYearLevel(),
								getMemberArg(evaluator, args, 0, false));
					}
				}));
		define(new FunDefBase(
				":", "<Member>:<Member>", "Infix colon operator returns the set of members between a given pair of members.", "ixmm") {
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

			public void testRange(FoodMartTestCase test) {
				test.assertAxisReturns("[Time].[1997].[Q1].[2] : [Time].[1997].[Q2].[5]",
						"[Time].[1997].[Q1].[2]" + nl +
						"[Time].[1997].[Q1].[3]" + nl +
						"[Time].[1997].[Q2].[4]" + nl +
						"[Time].[1997].[Q2].[5]"); // not parents
			}
			/**
			 * Large dimensions use a different member reader, therefore need to
			 * be tested separately.
			 */
			public void testRangeLarge(FoodMartTestCase test) {
				test.assertAxisReturns("[Customers].[USA].[CA].[San Francisco] : [Customers].[USA].[WA].[Bellingham]",
						"[Customers].[All Customers].[USA].[CA].[San Francisco]" + nl +
						"[Customers].[All Customers].[USA].[CA].[San Gabriel]" + nl +
						"[Customers].[All Customers].[USA].[CA].[San Jose]" + nl +
						"[Customers].[All Customers].[USA].[CA].[Santa Cruz]" + nl +
						"[Customers].[All Customers].[USA].[CA].[Santa Monica]" + nl +
						"[Customers].[All Customers].[USA].[CA].[Spring Valley]" + nl +
						"[Customers].[All Customers].[USA].[CA].[Torrance]" + nl +
						"[Customers].[All Customers].[USA].[CA].[West Covina]" + nl +
						"[Customers].[All Customers].[USA].[CA].[Woodland Hills]" + nl +
						"[Customers].[All Customers].[USA].[OR].[Albany]" + nl +
						"[Customers].[All Customers].[USA].[OR].[Beaverton]" + nl +
						"[Customers].[All Customers].[USA].[OR].[Corvallis]" + nl +
						"[Customers].[All Customers].[USA].[OR].[Lake Oswego]" + nl +
						"[Customers].[All Customers].[USA].[OR].[Lebanon]" + nl +
						"[Customers].[All Customers].[USA].[OR].[Milwaukie]" + nl +
						"[Customers].[All Customers].[USA].[OR].[Oregon City]" + nl +
						"[Customers].[All Customers].[USA].[OR].[Portland]" + nl +
						"[Customers].[All Customers].[USA].[OR].[Salem]" + nl +
						"[Customers].[All Customers].[USA].[OR].[W. Linn]" + nl +
						"[Customers].[All Customers].[USA].[OR].[Woodburn]" + nl +
						"[Customers].[All Customers].[USA].[WA].[Anacortes]" + nl +
						"[Customers].[All Customers].[USA].[WA].[Ballard]" + nl +
						"[Customers].[All Customers].[USA].[WA].[Bellingham]");
			}
			public void testRangeStartEqualsEnd(FoodMartTestCase test) {
				test.assertAxisReturns("[Time].[1997].[Q3].[7] : [Time].[1997].[Q3].[7]",
						"[Time].[1997].[Q3].[7]");
			}
			public void testRangeStartEqualsEndLarge(FoodMartTestCase test) {
				test.assertAxisReturns("[Customers].[USA].[CA] : [Customers].[USA].[CA]",
						"[Customers].[All Customers].[USA].[CA]");
			}
			public void testRangeEndBeforeStart(FoodMartTestCase test) {
				test.assertAxisReturns("[Time].[1997].[Q3].[7] : [Time].[1997].[Q2].[5]",
						"[Time].[1997].[Q2].[5]" + nl +
						"[Time].[1997].[Q2].[6]" + nl +
						"[Time].[1997].[Q3].[7]"); // same as if reversed
			}
			public void testRangeEndBeforeStartLarge(FoodMartTestCase test) {
				test.assertAxisReturns("[Customers].[USA].[WA] : [Customers].[USA].[CA]",
						"[Customers].[All Customers].[USA].[CA]" + nl +
						"[Customers].[All Customers].[USA].[OR]" + nl +
						"[Customers].[All Customers].[USA].[WA]");
			}
			public void testRangeBetweenDifferentLevelsIsError(FoodMartTestCase test) {
				test.assertAxisThrows("[Time].[1997].[Q2] : [Time].[1997].[Q2].[5]",
						"Members must belong to the same level");
			}
			public void testRangeBoundedByAll(FoodMartTestCase test) {
				test.assertAxisReturns("[Gender] : [Gender]",
						"[Gender].[All Gender]");
			}
			public void testRangeBoundedByAllLarge(FoodMartTestCase test) {
				test.assertAxisReturns("[Customers].DefaultMember : [Customers]",
						"[Customers].[All Customers]");
			}
			public void testRangeBoundedByNull(FoodMartTestCase test) {
				test.assertAxisReturns("[Gender].[F] : [Gender].[M].NextMember",
						"");
			}
			public void testRangeBoundedByNullLarge(FoodMartTestCase test) {
				test.assertAxisReturns("[Customers].PrevMember : [Customers].[USA].[OR]",
						"");
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

			public void testSetContainingLevelFails(FoodMartTestCase test) {
				test.assertAxisThrows(
						"[Store].[Store City]",
						"no function matches signature '{<Level>}'");
			}
			public void testBug715177(FoodMartTestCase test) {
				test.runQueryCheckResult(
						"WITH MEMBER [Product].[All Products].[Non-Consumable].[Other] AS" + nl +
						" 'Sum( Except( [Product].[Product Department].Members," + nl +
						"       TopCount( [Product].[Product Department].Members, 3 ))," + nl +
						"       Measures.[Unit Sales] )'" + nl +
						"SELECT" + nl +
						"  { [Measures].[Unit Sales] } ON COLUMNS ," + nl +
						"  { TopCount( [Product].[Product Department].Members,3 )," + nl +
						"              [Product].[All Products].[Non-Consumable].[Other] } ON ROWS" + nl +
						"FROM [Sales]",
						"Axis #0:" + nl +
						"{}" + nl +
						"Axis #1:" + nl +
						"{[Measures].[Unit Sales]}" + nl +
						"Axis #2:" + nl +
						"{[Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
						"{[Product].[All Products].[Drink].[Beverages]}" + nl +
						"{[Product].[All Products].[Drink].[Dairy]}" + nl +
						"{[Product].[All Products].[Non-Consumable].[Other]}" + nl +
						"Row #0: 6,838" + nl +
						"Row #1: 13,573" + nl +
						"Row #2: 4,186" + nl +
						"Row #3: 242,176" + nl);
			}
			public void testBug714707(FoodMartTestCase test) {
				// Same issue as bug 715177 -- "children" returns immutable
				// list, which set operator must make mutable.
				test.assertAxisReturns("{[Store].[USA].[CA].children, [Store].[USA]}",
						"[Store].[All Stores].[USA].[CA].[Alameda]" + nl +
						"[Store].[All Stores].[USA].[CA].[Beverly Hills]" + nl +
						"[Store].[All Stores].[USA].[CA].[Los Angeles]" + nl +
						"[Store].[All Stores].[USA].[CA].[San Diego]" + nl +
						"[Store].[All Stores].[USA].[CA].[San Francisco]" + nl +
						"[Store].[All Stores].[USA]");
			}
			public void todo_testBug715177c(FoodMartTestCase test) {
				test.assertAxisReturns("Order(TopCount({[Store].[USA].[CA].children}, [Measures].[Unit Sales], 2), [Measures].[Unit Sales])",
						"foo");
			}
		});

		//
		// STRING FUNCTIONS
        define(new MultiResolver("Format", "Format(<Numeric Expression>, <String Expression>)", "Formats a number to string.", new String[] { "fSmS", "fSnS" }) {
            protected FunDef createFunDef(final Exp[] args, final FunDef dummyFunDef) {
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
            public void testFormatFixed(FoodMartTestCase test) {
                String s = test.executeExpr("Format(12.2, \"#,##0.00\")");
                Assert.assertEquals("12.20", s);
            }
            public void testFormatVariable(FoodMartTestCase test) {
                String s = test.executeExpr("Format(1234.5, \"#,#\" || \"#0.00\")");
                Assert.assertEquals("1,234.50", s);
            }
        });

		define(new FunDefBase("IIf", "IIf(<Logical Expression>, <String Expression1>, <String Expression2>)", "Returns one of two string values determined by a logical test.", "fSbSS") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				boolean logical = getBooleanArg(evaluator, args, 0);
				return getStringArg(evaluator, args, logical ? 1 : 2, null);
			}

			public void testIIf(FoodMartTestCase test) {
				String s = test.executeExpr(
						"IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, \"Yes\",\"No\")");
				Assert.assertEquals("Yes", s);
			}
		});
		define(new FunDefBase("Name", "<Dimension>.Name", "Returns the name of a dimension.", "pSd") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Dimension dimension = getDimensionArg(evaluator, args, 0, true);
				return dimension.getName();
			}

			public void testDimensionName(FoodMartTestCase test) {
				String s = test.executeExpr("[Time].[1997].Dimension.Name");
				Assert.assertEquals("Time", s);
			}
		});
		define(new FunDefBase("Name", "<Hierarchy>.Name", "Returns the name of a hierarchy.", "pSh") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
				return hierarchy.getName();
			}

			public void testHierarchyName(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Time].[1997].Hierarchy.Name");
				Assert.assertEquals("Time", s);
			}
		});
		define(new FunDefBase("Name", "<Level>.Name", "Returns the name of a level.", "pSl") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Level level = getLevelArg(evaluator, args, 0, true);
				return level.getName();
			}

			public void testLevelName(FoodMartTestCase test) {
				String s = test.executeExpr("[Time].[1997].Level.Name");
				Assert.assertEquals("Year", s);
			}
		});
		define(new FunDefBase("Name", "<Member>.Name", "Returns the name of a member.", "pSm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return member.getName();
			}

			public void testMemberName(FoodMartTestCase test) {
				String s = test.executeExpr("[Time].[1997].Name");
				Assert.assertEquals("1997", s);
			}
		});
		define(new FunDefBase("SetToStr", "SetToStr(<Set>)", "Constructs a string from a set.", "fSx"));
		define(new FunDefBase("TupleToStr", "TupleToStr(<Tuple>)", "Constructs a string from a tuple.", "fSt"));
		define(new FunDefBase("UniqueName", "<Dimension>.UniqueName", "Returns the unique name of a dimension.", "pSd") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Dimension dimension = getDimensionArg(evaluator, args, 0, true);
				return dimension.getUniqueName();
			}

			public void testDimensionUniqueName(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Gender].DefaultMember.Dimension.UniqueName");
				Assert.assertEquals("[Gender]", s);
			}
		});
		define(new FunDefBase("UniqueName", "<Hierarchy>.UniqueName", "Returns the unique name of a hierarchy.", "pSh") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
				return hierarchy.getUniqueName();
			}

			public void testHierarchyUniqueName(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Gender].DefaultMember.Hierarchy.UniqueName");
				Assert.assertEquals("[Gender]", s);
			}
		});
		define(new FunDefBase("UniqueName", "<Level>.UniqueName", "Returns the unique name of a level.", "pSl") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Level level = getLevelArg(evaluator, args, 0, true);
				return level.getUniqueName();
			}

			public void testLevelUniqueName(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Gender].DefaultMember.Level.UniqueName");
				Assert.assertEquals("[Gender].[(All)]", s);
			}
		});
		define(new FunDefBase("UniqueName", "<Member>.UniqueName", "Returns the unique name of a member.", "pSm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return member.getUniqueName();
			}

			public void testMemberUniqueName(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Gender].DefaultMember.UniqueName");
				Assert.assertEquals("[Gender].[All Gender]", s);
			}

			public void testMemberUniqueNameOfNull(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Measures].[Unit Sales].FirstChild.UniqueName");
				Assert.assertEquals("[Measures].[#Null]", s); // MSOLAP gives "" here
			}
		});

		//
		// TUPLE FUNCTIONS
		define(new FunDefBase("Current", "<Set>.Current", "Returns the current tuple from a set during an iteration.", "ptx"));
		if (false) define(new FunDefBase("Item", "<Set>.Item(<String Expression>[, <String Expression>...] | <Index>)", "Returns a tuple from a set.", "mt*"));
		define(new FunDefBase("StrToTuple", "StrToTuple(<String Expression>)", "Constructs a tuple from a string.", "ftS") {
			public Hierarchy getHierarchy(Exp[] args) {
				// StrToTuple(s, <Hie1>, ... <HieN>) is of type [Hie1] x
				// ... x [HieN]; so, for example, So
				// StrToTuple("[Time].[1997]", [Time]) is of type [Time].
				// But if n > 1, we cannot represent the compound type, and
				// we return null.
				return (args.length == 2) ?
						(Hierarchy) args[1] :
						null;
			}
		});

		// special resolver for "()"
		define(new ResolverBase("()", null, null, Syntax.Parentheses) {
			public FunDef resolve(Exp[] args, int[] conversionCount) {
				// Compare with TupleFunDef.getReturnType().  For example,
				//   ([Gender].members) is a set,
				//   ([Gender].[M]) is a member,
				//   (1 + 2) is a numeric,
				// but
				//   ([Gender].[M], [Marital Status].[S]) is a tuple.
				if (args.length == 1) {
					return new ParenthesesFunDef(args[0].getType());
				} else {
					return new TupleFunDef(ExpBase.getTypes(args));
				}
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
						return new FunDefBase(this, type, ExpBase.getTypes(args));
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
				int j = 0,
						clauseCount = args.length / 2,
						mismatchingArgs = 0;
				int returnType = args[1].getType();
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
				int clauseCount = args.length / 2,
					j = 0;
				for (int i = 0; i < clauseCount; i++) {
					boolean logical = getBooleanArg(evaluator, args, j++);
					if (logical) {
						return getArg(evaluator, args, j);
					} else {
						j++;
					}
				}
				if (j < args.length) {
					return getArg(evaluator,  args, j); // ELSE
				} else {
					return null;
				}
			}
			public void testCaseTestMatch(FoodMartTestCase test) {
				String s = test.executeExpr(
						"CASE WHEN 1=0 THEN \"first\" WHEN 1=1 THEN \"second\" WHEN 1=2 THEN \"third\" ELSE \"fourth\" END");
				Assert.assertEquals("second", s);
			}
			public void testCaseTestMatchElse(FoodMartTestCase test) {
				String s = test.executeExpr(
						"CASE WHEN 1=0 THEN \"first\" ELSE \"fourth\" END");
				Assert.assertEquals("fourth", s);
			}
			public void testCaseTestMatchNoElse(FoodMartTestCase test) {
				String s = test.executeExpr(
						"CASE WHEN 1=0 THEN \"first\" END");
				Assert.assertEquals("(null)", s);
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
				int valueType = args[0].getType();
				int returnType = args[2].getType();
				int j = 0,
						clauseCount = (args.length - 1) / 2,
						mismatchingArgs = 0;
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
				int clauseCount = (args.length - 1)/ 2,
					j = 0;
				Object value = getArg(evaluator, args, j++);
				for (int i = 0; i < clauseCount; i++) {
					Object match = getArg(evaluator, args, j++);
					if (match.equals(value)) {
						return getArg(evaluator, args, j);
					} else {
						j++;
					}
				}
				if (j < args.length) {
					return getArg(evaluator,  args, j); // ELSE
				} else {
					return null;
				}
			}
			public void testCaseMatch(FoodMartTestCase test) {
				String s = test.executeExpr(
						"CASE 2 WHEN 1 THEN \"first\" WHEN 2 THEN \"second\" WHEN 3 THEN \"third\" ELSE \"fourth\" END");
				Assert.assertEquals("second", s);
			}
			public void testCaseMatchElse(FoodMartTestCase test) {
				String s = test.executeExpr(
						"CASE 7 WHEN 1 THEN \"first\" ELSE \"fourth\" END");
				Assert.assertEquals("fourth", s);
			}
			public void testCaseMatchNoElse(FoodMartTestCase test) {
				String s = test.executeExpr(
						"CASE 8 WHEN 0 THEN \"first\" END");
				Assert.assertEquals("(null)", s);
			}
		});

		define(new ResolverBase(
					   "Properties",
					   "<Member>.Properties(<String Expression>)",
					   "Returns the value of a member property.",
					   Syntax.Method) {
			public FunDef resolve(Exp[] args, int[] conversionCount) {
				final int[] argTypes = new int[]{Category.Member, Category.String};
				if (args.length != 2 ||
						args[0].getType() != Category.Member ||
						args[1].getType() != Category.String) {
					return null;
				}
				int returnType;
				if (args[1] instanceof Literal) {
					String propertyName = (String) ((Literal) args[1]).getValue();
					Hierarchy hierarchy = args[0].getHierarchy();
					Level[] levels = hierarchy.getLevels();
					Property property = lookupProperty(
							levels[levels.length - 1], propertyName);
					if (property == null) {
						// we'll likely get a runtime error
						returnType = Category.Value;
					} else {
						switch (property.getType()) {
						case Property.TYPE_BOOLEAN:
							returnType = Category.Logical;
							break;
						case Property.TYPE_NUMERIC:
							returnType = Category.Numeric;
							break;
						case Property.TYPE_STRING:
							returnType = Category.String;
							break;
						default:
							throw Util.newInternal("Unknown property type " + property.getType());
						}
					}
				} else {
					returnType = Category.Value;
				}
				return new PropertiesFunDef(name, signature, description, syntax, returnType, argTypes);
			}

            public boolean requiresExpression(int k) {
                return true;
            }

			public void testPropertiesExpr(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Store Type\")");
				Assert.assertEquals("Gourmet Supermarket", s);
			}

			/** Tests that non-existent property throws an error. **/
			public void testPropertiesNonExistent(FoodMartTestCase test) {
				test.assertExprThrows(
						"[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Foo\")",
						"Property 'Foo' is not valid for");
			}

			public void testPropertiesFilter(FoodMartTestCase test) {
				Result result = test.execute(
						"SELECT { [Store Sales] } ON COLUMNS," + nl +
						" TOPCOUNT( Filter( [Store].[Store Name].Members," + nl +
						"                   [Store].CurrentMember.Properties(\"Store Type\") = \"Supermarket\" )," + nl +
						"           10, [Store Sales]) ON ROWS" + nl +
						"FROM [Sales]");
				Assert.assertEquals(8, result.getAxes()[1].positions.length);
			}

			public void testPropertyInCalculatedMember(FoodMartTestCase test) {
				Result result = test.execute(
					"WITH MEMBER [Measures].[Store Sales per Sqft]" + nl +
					"AS '[Measures].[Store Sales] / " +
					"  [Store].CurrentMember.Properties(\"Store Sqft\")'" + nl +
					"SELECT " + nl +
					"  {[Measures].[Unit Sales], [Measures].[Store Sales per Sqft]} ON COLUMNS," + nl +
					"  {[Store].[Store Name].members} ON ROWS" + nl +
					"FROM Sales");
				Member member;
				Cell cell;
				member = result.getAxes()[1].positions[18].members[0];
				Assert.assertEquals("[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]", member.getUniqueName());
				cell = result.getCell(new int[] {0,18});
				Assert.assertEquals("2,237", cell.getFormattedValue());
				cell = result.getCell(new int[] {1,18});
				Assert.assertEquals(".17", cell.getFormattedValue());
				member = result.getAxes()[1].positions[3].members[0];
				Assert.assertEquals("[Store].[All Stores].[Mexico].[DF].[San Andres].[Store 21]", member.getUniqueName());
				cell = result.getCell(new int[] {0,3});
				Assert.assertEquals("(null)", cell.getFormattedValue());
				cell = result.getCell(new int[] {1,3});
				Assert.assertEquals("(null)", cell.getFormattedValue());
			}
		});

		//
		// PARAMETER FUNCTIONS
		defineReserved(new String[] {"NUMERIC","STRING"});
		define(new MultiResolver("Parameter", "Parameter(<Name>, <Type>, <DefaultValue>, <Description>)", "Returns default value of parameter.",
				new String[] {
					"fS#yS#", "fs#yS", // Parameter(string const, symbol, string[, string const]): string
					"fn#yn#", "fn#yn", // Parameter(string const, symbol, numeric[, string const]): numeric
				    "fm#hm#", "fm#hm",  // Parameter(string const, hierarchy constant, member[, string const]): member
				}) {
			protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
				String parameterName;
				if (args[0] instanceof Literal &&
						args[0].getType() == Category.String) {
					parameterName = (String) ((Literal) args[0]).getValue();
				} else {
					throw newEvalException(dummyFunDef, "Parameter name must be a string constant");
				}
				Exp typeArg = args[1];
				Hierarchy hierarchy;
				int type;
				switch (typeArg.getType()) {
				case Category.Hierarchy:
				case Category.Dimension:
					hierarchy = typeArg.getHierarchy();
					if (hierarchy == null || !isConstantHierarchy(typeArg)) {
						throw newEvalException(dummyFunDef, "Invalid hierarchy for parameter '" + parameterName + "'");
					}
					type = Category.Member;
					break;
				case Category.Symbol:
					hierarchy = null;
					String s = (String) ((Literal) typeArg).getValue();
					if (s.equalsIgnoreCase("NUMERIC")) {
						type = Category.Numeric;
						break;
					} else if (s.equalsIgnoreCase("STRING")) {
						type = Category.String;
						break;
					}
					// fall through and throw error
				default:
					// Error is internal because the function call has already been
					// type-checked.
					throw newEvalException(dummyFunDef,
							"Invalid type for parameter '" + parameterName + "'; expecting NUMERIC, STRING or a hierarchy");
				}
				Exp exp = args[2];
				if (exp.getType() != type) {
					String typeName = Category.instance.getName(type).toUpperCase();
					throw newEvalException(dummyFunDef, "Default value of parameter '" + parameterName + "' is inconsistent with its type, " + typeName);
				}
				if (type == Category.Member) {
					Hierarchy expHierarchy = exp.getHierarchy();
					if (expHierarchy != hierarchy) {
						throw newEvalException(dummyFunDef, "Default value of parameter '" + parameterName + "' must belong to the hierarchy " + hierarchy);
					}
				}
				String parameterDescription = null;
				if (args.length > 3) {
					if (args[3] instanceof Literal &&
							args[3].getType() == Category.String) {
						parameterDescription = (String) ((Literal) args[3]).getValue();
					} else {
						throw newEvalException(dummyFunDef, "Description of parameter '" + parameterName + "' must be a string constant");
					}
				}

				return new ParameterFunDef(dummyFunDef, parameterName, hierarchy, type, exp, parameterDescription);
			}
		});
		define(new MultiResolver("ParamRef", "ParamRef(<Name>)", "Returns current value of parameter. If it's null, returns default.",
				new String[] {"fv#"}) {
			protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
				String parameterName;
				if (args[0] instanceof Literal &&
						args[0].getType() == Category.String) {
					parameterName = (String) ((Literal) args[0]).getValue();
				} else {
					throw newEvalException(dummyFunDef, "Parameter name must be a string constant");
				}
				return new ParameterFunDef(dummyFunDef, parameterName, null, Category.Unknown, null, null);
			}
		});

		//
		// OPERATORS
		define(new FunDefBase("+", "<Numeric Expression> + <Numeric Expression>", "Adds two numbers.", "innn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0, null),
						o1 = getDoubleArg(evaluator, args, 1, null);
				if (o0 == null || o1 == null)
					return null;
				return new Double(o0.doubleValue() + o1.doubleValue());
			}
			public void testPlus(FoodMartTestCase test) {
				String s = test.executeExpr("1+2");
				Assert.assertEquals("3", s);
			}
		});
		define(new FunDefBase("-", "<Numeric Expression> - <Numeric Expression>", "Subtracts two numbers.", "innn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0, null),
						o1 = getDoubleArg(evaluator, args, 1, null);
				if (o0 == null || o1 == null)
					return null;
				return new Double(o0.doubleValue() - o1.doubleValue());
			}
			public void testMinus(FoodMartTestCase test) {
				String s = test.executeExpr("1-3");
				Assert.assertEquals("-2", s);
			}
			public void testMinusAssociativity(FoodMartTestCase test) {
				String s = test.executeExpr("11-7-5");
				// right-associative would give 11-(7-5) = 9, which is wrong
				Assert.assertEquals("-1", s);
			}
		});
		define(new FunDefBase("*", "<Numeric Expression> * <Numeric Expression>", "Multiplies two numbers.", "innn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0, null),
						o1 = getDoubleArg(evaluator, args, 1, null);
				if (o0 == null || o1 == null)
					return null;
				return new Double(o0.doubleValue() * o1.doubleValue());
			}
			public void testMultiply(FoodMartTestCase test) {
				String s = test.executeExpr("4*7");
				Assert.assertEquals("28", s);
			}
			public void testMultiplyPrecedence(FoodMartTestCase test) {
				String s = test.executeExpr("3 + 4 * 5 + 6");
				Assert.assertEquals("29", s);
			}
            /** Bug 774807 caused expressions to be mistaken for the crossjoin
             * operator. */
            public void testMultiplyBug774807(FoodMartTestCase test) {
                final String desiredResult = "Axis #0:" + nl +
                        "{}" + nl +
                        "Axis #1:" + nl +
                        "{[Store].[All Stores]}" + nl +
                        "Axis #2:" + nl +
                        "{[Measures].[Store Sales]}" + nl +
                        "{[Measures].[A]}" + nl +
                        "Row #0: 565,238.13" + nl +
                        "Row #1: 319,494,143,605.90" + nl;
                test.runQueryCheckResult(
                        "WITH MEMBER [Measures].[A] AS" + nl +
                        " '([Measures].[Store Sales] * [Measures].[Store Sales])'" + nl +
                        "SELECT {[Store]} ON COLUMNS," + nl +
                        " {[Measures].[Store Sales], [Measures].[A]} ON ROWS" + nl +
                        "FROM Sales",
                        desiredResult);
                // as above, no parentheses
                test.runQueryCheckResult(
                        "WITH MEMBER [Measures].[A] AS" + nl +
                        " '[Measures].[Store Sales] * [Measures].[Store Sales]'" + nl +
                        "SELECT {[Store]} ON COLUMNS," + nl +
                        " {[Measures].[Store Sales], [Measures].[A]} ON ROWS" + nl +
                        "FROM Sales",
                        desiredResult);
                // as above, plus 0
                test.runQueryCheckResult(
                        "WITH MEMBER [Measures].[A] AS" + nl +
                        " '[Measures].[Store Sales] * [Measures].[Store Sales] + 0'" + nl +
                        "SELECT {[Store]} ON COLUMNS," + nl +
                        " {[Measures].[Store Sales], [Measures].[A]} ON ROWS" + nl +
                        "FROM Sales",
                        desiredResult);
            }
		});
		define(new FunDefBase("/", "<Numeric Expression> / <Numeric Expression>", "Divides two numbers.", "innn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0, null),
						o1 = getDoubleArg(evaluator, args, 1, null);
				if (o0 == null || o1 == null)
					return null;
				return new Double(o0.doubleValue() / o1.doubleValue());
			}
			// todo: use this, via reflection
			public double evaluate(double d1, double d2) {
				return d1 / d2;
			}
			public void testDivide(FoodMartTestCase test) {
				String s = test.executeExpr("10 / 5");
				Assert.assertEquals("2", s);
			}
			public void testDivideByZero(FoodMartTestCase test) {
				String s = test.executeExpr("-3 / (2 - 2)");
				Assert.assertEquals("-Infinity", s);
			}
			public void testDividePrecedence(FoodMartTestCase test) {
				String s = test.executeExpr("24 / 4 / 2 * 10 - -1");
				Assert.assertEquals("31", s);
			}
		});
		define(new FunDefBase("-", "- <Numeric Expression>", "Returns the negative of a number.", "Pnn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0, null);
				if (o0 == null)
					return null;
				return new Double(- o0.doubleValue());
			}
			public void testUnaryMinus(FoodMartTestCase test) {
				String s = test.executeExpr("-3");
				Assert.assertEquals("-3", s);
			}
			public void testUnaryMinusMember(FoodMartTestCase test) {
				String s = test.executeExpr("- ([Measures].[Unit Sales],[Gender].[F])");
				Assert.assertEquals("-131,558", s);
			}
			public void testUnaryMinusPrecedence(FoodMartTestCase test) {
				String s = test.executeExpr("1 - -10.5 * 2 -3");
				Assert.assertEquals("19", s);
			}
		});
		define(new FunDefBase("||", "<String Expression> || <String Expression>", "Concatenates two strings.", "iSSS") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				String o0 = getStringArg(evaluator, args, 0, null),
						o1 = getStringArg(evaluator, args, 1, null);
				return o0 + o1;
			}
			public void testStringConcat(FoodMartTestCase test) {
				String s = test.executeExpr(" \"foo\" || \"bar\"  ");
				Assert.assertEquals("foobar", s);
			}
			public void testStringConcat2(FoodMartTestCase test) {
				String s = test.executeExpr(" \"foo\" || [Gender].[M].Name || \"\" ");
				Assert.assertEquals("fooM", s);
			}
		});
		define(new FunDefBase("AND", "<Logical Expression> AND <Logical Expression>", "Returns the conjunction of two conditions.", "ibbb") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				return toBoolean(
						getBooleanArg(evaluator, args, 0) &&
						getBooleanArg(evaluator, args, 1));
			}
			public void testAnd(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=1 AND 2=2 ");
				Assert.assertEquals("true", s);
			}
			public void testAnd2(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=1 AND 2=0 ");
				Assert.assertEquals("false", s);
			}
		});
		define(new FunDefBase("OR", "<Logical Expression> OR <Logical Expression>", "Returns the disjunction of two conditions.", "ibbb") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				// Only evaluate 2nd if first is false.
				return toBoolean(
						getBooleanArg(evaluator, args, 0) ||
						getBooleanArg(evaluator, args, 1));
			}
			public void testOr(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=0 OR 2=0 ");
				Assert.assertEquals("false", s);
			}
			public void testOr2(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=0 OR 0=0 ");
				Assert.assertEquals("true", s);
			}
			public void testOrAssociativity1(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=1 AND 1=0 OR 1=1 ");
				// Would give 'false' if OR were stronger than AND (wrong!)
				Assert.assertEquals("true", s);
			}
			public void testOrAssociativity2(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=1 OR 1=0 AND 1=1 ");
				// Would give 'false' if OR were stronger than AND (wrong!)
				Assert.assertEquals("true", s);
			}
			public void testOrAssociativity3(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" (1=0 OR 1=1) AND 1=1 ");
				Assert.assertEquals("true", s);
			}
		});
		define(new FunDefBase("XOR", "<Logical Expression> XOR <Logical Expression>", "Returns whether two conditions are mutually exclusive.", "ibbb") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				final boolean b0 = getBooleanArg(evaluator, args, 0);
				final boolean b1 = getBooleanArg(evaluator, args, 1);
				return toBoolean(b0 != b1);
			}
			public void testXor(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=1 XOR 2=2 ");
				Assert.assertEquals("false", s);
			}
			public void testXorAssociativity(FoodMartTestCase test) {
				// Would give 'false' if XOR were stronger than AND (wrong!)
				String s = test.executeBooleanExpr(" 1 = 1 AND 1 = 1 XOR 1 = 0 ");
				Assert.assertEquals("true", s);
			}
		});
		define(new FunDefBase("NOT", "NOT <Logical Expression>", "Returns the negation of a condition.", "Pbb") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				return toBoolean(!getBooleanArg(evaluator, args, 0));
			}
			public void testNot(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" NOT 1=1 ");
				Assert.assertEquals("false", s);
			}
			public void testNotNot(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" NOT NOT 1=1 ");
				Assert.assertEquals("true", s);
			}
			public void testNotAssociativity(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=1 AND NOT 1=1 OR NOT 1=1 AND 1=1 ");
				Assert.assertEquals("false", s);
			}
		});
		define(new FunDefBase("=", "<String Expression> = <String Expression>", "Returns whether two expressions are equal.", "ibSS") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				String o0 = getStringArg(evaluator, args, 0, null),
						o1 = getStringArg(evaluator, args, 1, null);
				return toBoolean(o0.equals(o1));
			}
			public void testStringEquals(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" \"foo\" = \"bar\" ");
				Assert.assertEquals("false", s);
			}
			public void testStringEqualsAssociativity(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" \"foo\" = \"fo\" || \"o\" ");
				Assert.assertEquals("true", s);
			}
			public void testStringEqualsEmpty(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" \"\" = \"\" ");
				Assert.assertEquals("true", s);
			}
		});
		define(new FunDefBase("=", "<Numeric Expression> = <Numeric Expression>", "Returns whether two expressions are equal.", "ibnn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				return toBoolean(o0.equals(o1));
			}
			public void testEq(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1.0 = 1 ");
				Assert.assertEquals("true", s);
			}
		});
		define(new FunDefBase("<>", "<String Expression> <> <String Expression>", "Returns whether two expressions are not equal.", "ibSS") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				String o0 = getStringArg(evaluator, args, 0, null),
						o1 = getStringArg(evaluator, args, 1, null);
				return toBoolean(!o0.equals(o1));
			}
			public void testStringNe(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" \"foo\" <> \"bar\" ");
				Assert.assertEquals("true", s);
			}
		});
		define(new FunDefBase("<>", "<Numeric Expression> <> <Numeric Expression>", "Returns whether two expressions are not equal.", "ibnn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				return toBoolean(!o0.equals(o1));
			}
			public void testNe(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 2 <> 1.0 + 1.0 ");
				Assert.assertEquals("false", s);
			}
			public void testNeInfinity(FoodMartTestCase test) {
				String s = test.executeBooleanExpr("(1 / 0) <> (1 / 0)");
				// Infinity does not equal itself
				Assert.assertEquals("false", s);
			}
		});
		define(new FunDefBase("<", "<Numeric Expression> < <Numeric Expression>", "Returns whether an expression is less than another.", "ibnn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				return toBoolean(o0.compareTo(o1) < 0);
			}
			public void testLt(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 2 < 1.0 + 1.0 ");
				Assert.assertEquals("false", s);
			}
		});
		define(new FunDefBase("<=", "<Numeric Expression> <= <Numeric Expression>", "Returns whether an expression is less than or equal to another.", "ibnn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				return toBoolean(o0.compareTo(o1) <= 0);
			}
			public void testLe(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 2 <= 1.0 + 1.0 ");
				Assert.assertEquals("true", s);
			}
		});
		define(new FunDefBase(">", "<Numeric Expression> > <Numeric Expression>", "Returns whether an expression is greater than another.", "ibnn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				return toBoolean(o0.compareTo(o1) > 0);
			}
			public void testGt(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 2 > 1.0 + 1.0 ");
				Assert.assertEquals("false", s);
			}
		});
		define(new FunDefBase(">=", "<Numeric Expression> >= <Numeric Expression>", "Returns whether an expression is greater than or equal to another.", "ibnn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				return toBoolean(o0.compareTo(o1) >= 0);
			}
			public void testGe(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 2 > 1.0 + 1.0 ");
				Assert.assertEquals("false", s);
			}
		});
	}

    private static boolean isConstantHierarchy(Exp typeArg) {
		if (typeArg instanceof Hierarchy) {
			// e.g. "[Time].[By Week]"
			return true;
		}
		if (typeArg instanceof Dimension) {
			// e.g. "[Time]"
			return true;
		}
		if (typeArg instanceof FunCall) {
			// e.g. "[Time].CurrentMember.Hierarchy". They probably wrote
			// "[Time]", and the automatic type conversion did the rest.
			FunCall hierarchyCall = (FunCall) typeArg;
			if (hierarchyCall.getFunName().equals("Hierarchy") &&
					hierarchyCall.args.length > 0 &&
					hierarchyCall.args[0] instanceof FunCall) {
				FunCall currentMemberCall = (FunCall) hierarchyCall.args[0];
				if (currentMemberCall.getFunName().equals("CurrentMember") &&
						currentMemberCall.args.length > 0 &&
						currentMemberCall.args[0] instanceof Dimension) {
					return true;
				}
			}
		}
		return false;
	}

	TestSuite createSuite() {
		TestSuite suite = new TestSuite("builtin functions");
		for (Iterator resolverses = mapNameToResolvers.values().iterator();
			 resolverses.hasNext();) {
			Resolver[] resolvers = (Resolver[]) resolverses.next();
			for (int i = 0; i < resolvers.length; i++) {
				Resolver resolver = resolvers[i];
				resolver.addTests(suite);
			}
		}
		return suite;
	}

	/** Standard method recognised by JUnit. **/
	public static Test suite() {
		return ((BuiltinFunTable) instance()).createSuite();
	}

	private boolean isValidProperty(
			Member member, String propertyName) {
		return lookupProperty(member.getLevel(), propertyName) != null;
	}

	/**
	 * Finds a member property called <code>propertyName</code> at, or above,
	 * <code>level</code>.
	 */
	private Property lookupProperty(
			Level level, String propertyName) {
		do {
			Property[] properties = level.getProperties();
			for (int i = 0; i < properties.length; i++) {
				Property property = properties[i];
				if (property.getName().equals(propertyName)) {
					return property;
				}
			}
			level = level.getParentLevel();
		} while (level != null);
		return null;
	}

	private class PropertiesFunDef extends FunDefBase {
		public PropertiesFunDef(
				String name, String signature, String description,
				Syntax syntax, int returnType, int[] parameterTypes) {
			super(name, signature, description, syntax, returnType, parameterTypes);
		}

		public Object evaluate(Evaluator evaluator, Exp[] args) {
			Member member = getMemberArg(evaluator, args, 0, true);
			String s = getStringArg(evaluator, args, 1, null);
			Object o = member.getPropertyValue(s);
			if (o == null) {
				if (isValidProperty(member, s)) {
					o = member.getHierarchy().getNullMember();
				} else {
					throw new MondrianEvaluationException(
							"Property '" + s +
							"' is not valid for member '" + member + "'");
				}
			}
			return o;
		}
	}

	private static class DescendantsFlags extends EnumeratedValues {
		static final DescendantsFlags instance = new DescendantsFlags();
		private DescendantsFlags() {
			super(
					new String[] {
						"SELF","AFTER","BEFORE","BEFORE_AND_AFTER","SELF_AND_AFTER",
						"SELF_AND_BEFORE","SELF_BEFORE_AFTER","LEAVES"},
					new int[] {
						SELF,AFTER,BEFORE,BEFORE_AND_AFTER,SELF_AND_AFTER,
						SELF_AND_BEFORE,SELF_BEFORE_AFTER,LEAVES});
		}
		public static final int SELF = 1;
		public static final int AFTER = 2;
		public static final int BEFORE = 4;
		public static final int BEFORE_AND_AFTER = BEFORE | AFTER;
		public static final int SELF_AND_AFTER = SELF | AFTER;
		public static final int SELF_AND_BEFORE = SELF | BEFORE;
		public static final int SELF_BEFORE_AFTER = SELF | BEFORE | AFTER;
		public static final int LEAVES = 8;
	}

	private static class OrderFlags extends EnumeratedValues {
		static final OrderFlags instance = new OrderFlags();
		private OrderFlags() {
			super(new String[] {"ASC","DESC","BASC","BDESC"});
		}
		public static final int ASC = 0;
		public static final int DESC = 1;
		public static final int BASC = 2;
		public static final int BDESC = 3;
		public static final boolean isDescending(int value) {
			return (value & DESC) == DESC;
		}
		public static final boolean isBreak(int value) {
			return (value & BASC) == BASC;
		}
	}

    private static class CrossJoinFunDef extends FunDefBase {
        public CrossJoinFunDef(FunDef dummyFunDef) {
            super(dummyFunDef);
        }

        public Hierarchy getHierarchy(Exp[] args) {
            // CROSSJOIN(<Set1>,<Set2>) has Hierarchy [Hie1] x [Hie2], which we
            // can't represent, so we return null.
            return null;
        }

        public Object evaluate(Evaluator evaluator, Exp[] args) {
            List set0 = getArgAsList(evaluator, args, 0);
            List set1 = getArgAsList(evaluator, args, 1);
            if (set0.isEmpty() || set1.isEmpty()) {
                return Collections.EMPTY_LIST;
            }
            boolean neitherSideIsTuple = true;
            int arity0 = 1,
                arity1 = 1;
            if (set0.get(0) instanceof Member[]) {
                arity0 = ((Member[]) set0.get(0)).length;
                neitherSideIsTuple = false;
            }
            if (set1.get(0) instanceof Member[]) {
                arity1 = ((Member[]) set1.get(0)).length;
                neitherSideIsTuple = false;
            }
            List result = new ArrayList();
            if (neitherSideIsTuple) {
                // Simpler routine if we know neither side contains tuples.
                for (int i = 0, m = set0.size(); i < m; i++) {
                    Member o0 = (Member) set0.get(i);
                    for (int j = 0, n = set1.size(); j < n; j++) {
                        Member o1 = (Member) set1.get(j);
                        result.add(new Member[]{o0, o1});
                    }
                }
            } else {
                // More complex routine if one or both sides are arrays
                // (probably the product of nested CrossJoins).
                Member[] row = new Member[arity0 + arity1];
                for (int i = 0, m = set0.size(); i < m; i++) {
                    int x = 0;
                    Object o0 = set0.get(i);
                    if (o0 instanceof Member) {
                        row[x++] = (Member) o0;
                    } else {
                        assertTrue(o0 instanceof Member[]);
                        final Member[] members = (Member[]) o0;
                        for (int k = 0; k < members.length; k++) {
                            row[x++] = members[k];
                        }
                    }
                    for (int j = 0, n = set1.size(); j < n; j++) {
                        Object o1 = set1.get(j);
                        if (o1 instanceof Member) {
                            row[x++] = (Member) o1;
                        } else {
                            assertTrue(o1 instanceof Member[]);
                            final Member[] members = (Member[]) o1;
                            for (int k = 0; k < members.length; k++) {
                                row[x++] = members[k];
                            }
                        }
                        result.add(row.clone());
                        x = arity0;
                    }
                }
            }
            return result;
        }

        private static List getArgAsList(Evaluator evaluator, Exp[] args, int index) {
            final Object arg = getArg(evaluator, args, index);
            if (arg instanceof List) {
                return (List) arg;
            } else {
                List list = new ArrayList();
                list.add(arg);
                return list;
            }
        }
    }
}

// End BuiltinFunTable.java
