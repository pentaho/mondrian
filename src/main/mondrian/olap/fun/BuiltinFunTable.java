/*
// $Id$
// (C) Copyright 2002 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 February, 2002
*/
package mondrian.olap.fun;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;
import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

import java.io.PrintWriter;
import java.util.*;

/**
 * <code>BuiltinFunTable</code> contains a list of all functions.
 *
 * @author jhyde
 * @since 26 February, 2002
 * @version $Id$
 **/
public class BuiltinFunTable extends FunTable {
	/** Maps the upper-case name of a function to an array of {@link Resolver}s
	 * for that name. **/
	private HashMap upperName2Resolvers;

	private static final Resolver[] emptyResolvers = new Resolver[0];

	/**
	 * Creates a <code>BuiltinFunTable</code>. This method should only be
	 * called from {@link FunTable#instance}.
	 **/
	public BuiltinFunTable() {
		init();
	}

	/** Calls {@link #defineFunctions} to load function definitions into a
	 * vector, then indexes that collection. **/
	private void init() {
		v = new Vector();
		defineFunctions();
		// Map upper-case function names to resolvers.
		upperName2Resolvers = new HashMap();
		for (int i = 0, n = v.size(); i < n; i++) {
			Resolver resolver = (Resolver) v.elementAt(i);
			String key = resolver.getName().toUpperCase();
			Vector v2 = (Vector) upperName2Resolvers.get(key);
			if (v2 == null) {
				v2 = new Vector();
				upperName2Resolvers.put(key, v2);
			}
			v2.addElement(resolver);
		}
		// Convert the vectors into arrays.
		for (Iterator keys = upperName2Resolvers.keySet().iterator(); keys.hasNext();) {
			String key = (String) keys.next();
			Vector v2 = (Vector) upperName2Resolvers.get(key);
			Resolver[] resolvers = new Resolver[v2.size()];
			v2.copyInto(resolvers);
			upperName2Resolvers.put(key, resolvers);
		}
	}

	protected void define(FunDef funDef) {
		define(new SimpleResolver(funDef));
	}

	protected void define(Resolver resolver) {
		v.addElement(resolver);
	}

	static int decodeSyntacticType(String flags) {
		char c = flags.charAt(0);
		switch (c) {
		case 'p':
			return FunDef.TypeProperty;
		case 'f':
			return FunDef.TypeFunction;
		case 'm':
			return FunDef.TypeMethod;
		case 'i':
			return FunDef.TypeInfix;
		case 'P':
			return FunDef.TypePrefix;
		default:
			throw Util.newInternal(
					"unknown syntax code '" + c + "' in string '" + flags + "'");
		}
	}

	static int decodeReturnType(String flags) {
		return decodeType(flags, 1);
	}

	static int decodeType(String flags, int offset) {
		char c = flags.charAt(offset);
		switch (c) {
		case 'a':
			return Exp.CatArray;
		case 'd':
			return Exp.CatDimension;
		case 'h':
			return Exp.CatHierarchy;
		case 'l':
			return Exp.CatLevel;
		case 'b':
			return Exp.CatLogical;
		case 'm':
			return Exp.CatMember;
		case 'n':
			return Exp.CatNumeric;
		case 'N':
			return Exp.CatNumeric | Exp.CatExpression;
		case 'x':
			return Exp.CatSet;
		case 's':
			throw new Error("aaagh");
		case '#':
			return Exp.CatString;
		case 'S':
			return Exp.CatString | Exp.CatExpression;
		case 't':
			return Exp.CatTuple;
		case 'v':
			return Exp.CatValue;
		case 'y':
			return Exp.CatSymbol;
		default:
			throw Util.newInternal(
					"unknown type code '" + c + "' in string '" + flags + "'");
		}
	}

	/**
	 * Converts an argument to a parameter type.
	 */
	public Exp convert(Exp fromExp, int to) {
		Exp exp = convert_(fromExp, to);
		if (exp == null) {
			throw Util.newInternal("cannot convert " + fromExp + " to " + to);
		}
		return exp;
	}

	private static Exp convert_(Exp fromExp, int to) {
		int from = fromExp.getType();
		if (from == to) {
			return fromExp;
		}
		switch (from) {
		case Exp.CatArray:
			return null;
		case Exp.CatDimension:
			// Seems funny that you can 'downcast' from a dimension, doesn't
			// it? But we add an implicit 'CurrentMember', for example,
			// '[Time].PrevMember' actually means
			// '[Time].CurrentMember.PrevMember'.
			switch (to) {
			case Exp.CatHierarchy:
				// "<Dimension>.CurrentMember.Hierarchy"
				return new FunCall(
						"Hierarchy", new Exp[]{
						new FunCall(
								"CurrentMember",
								new Exp[]{fromExp},
								FunDef.TypeProperty)},
						FunDef.TypeProperty);
			case Exp.CatLevel:
				// "<Dimension>.CurrentMember.Level"
				return new FunCall(
						"Level", new Exp[]{
						new FunCall(
								"CurrentMember",
								new Exp[]{fromExp},
								FunDef.TypeProperty)},
						FunDef.TypeProperty);
			case Exp.CatMember:
				// "<Dimension>.CurrentMember"
				return new FunCall("CurrentMember", new Exp[]{fromExp}, FunDef.TypeProperty);
			default:
				return null;
			}
		case Exp.CatHierarchy:
			switch (to) {
			case Exp.CatDimension:
				// "<Hierarchy>.Dimension"
				return new FunCall("Dimension", new Exp[]{fromExp}, FunDef.TypeProperty);
			default:
				return null;
			}
		case Exp.CatLevel:
			switch (to) {
			case Exp.CatDimension:
				// "<Level>.Dimension"
				return new FunCall("Dimension", new Exp[]{fromExp}, FunDef.TypeProperty);
			case Exp.CatHierarchy:
				// "<Level>.Hierarchy"
				return new FunCall("Hierarchy", new Exp[]{fromExp}, FunDef.TypeProperty);
			default:
				return null;
			}
		case Exp.CatLogical:
			return null;
		case Exp.CatMember:
			switch (to) {
			case Exp.CatDimension:
				// "<Member>.Dimension"
				return new FunCall("Dimension", new Exp[]{fromExp}, FunDef.TypeProperty);
			case Exp.CatHierarchy:
				// "<Member>.Hierarchy"
				return new FunCall("Hierarchy", new Exp[]{fromExp}, FunDef.TypeProperty);
			case Exp.CatLevel:
				// "<Member>.Level"
				return new FunCall("Level", new Exp[]{fromExp}, FunDef.TypeProperty);
			case Exp.CatNumeric:
			case Exp.CatString: //todo: assert is a string member
				// "<Member>.Value"
				return new FunCall("Value", new Exp[]{fromExp}, FunDef.TypeProperty);
			case Exp.CatValue:
			case Exp.CatNumeric | Exp.CatExpression:
			case Exp.CatString | Exp.CatExpression:
				return fromExp;
			default:
				return null;
			}
		case Exp.CatNumeric:
			switch (to) {
			case Exp.CatValue:
			case Exp.CatNumeric | Exp.CatExpression:
				return fromExp;
			default:
				return null;
			}
		case Exp.CatNumeric | Exp.CatExpression:
			switch (to) {
			case Exp.CatValue:
				return fromExp;
			case Exp.CatNumeric:
				return new FunCall("_Value", new Exp[] {fromExp}, FunDef.TypeFunction);
			default:
				return null;
			}
		case Exp.CatSet:
			return null;
		case Exp.CatString:
			switch (to) {
			case Exp.CatValue:
			case Exp.CatString | Exp.CatExpression:
				return fromExp;
			default:
				return null;
			}
		case Exp.CatString | Exp.CatExpression:
			switch (to) {
			case Exp.CatValue:
				return fromExp;
			case Exp.CatString:
				return new FunCall("_Value", new Exp[] {fromExp}, FunDef.TypeFunction);
			default:
				return null;
			}
		case Exp.CatTuple:
			switch (to) {
			case Exp.CatValue:
				return fromExp;
			case Exp.CatNumeric:
				return new FunCall("_Value", new Exp[] {fromExp}, FunDef.TypeFunction);
			default:
				return null;
			}
		case Exp.CatValue:
			return null;
		case Exp.CatSymbol:
			return null;
		default:
			throw Util.newInternal("unknown category " + from);
		}
	}

	/**
	 * Returns whether we can convert an argument to a parameter tyoe.
	 * @param from argument type
	 * @param to   parameter type
	 * @param conversionCount in/out count of number of conversions performed;
	 *             is incremented if the conversion is non-trivial (for
	 *             example, converting a member to a level).
	 *
	 * @see #convert
	 */
	static boolean canConvert(Exp fromExp, int to, int[] conversionCount) {
		int from = fromExp.getType();
		if (from == to) {
			return true;
		}
		switch (from) {
		case Exp.CatArray:
			return false;
		case Exp.CatDimension:
			// Seems funny that you can 'downcast' from a dimension, doesn't
			// it? But we add an implicit 'CurrentMember', for example,
			// '[Time].PrevMember' actually means
			// '[Time].CurrentMember.PrevMember'.
			if (to == Exp.CatHierarchy ||
					to == Exp.CatLevel ||
					to == Exp.CatMember) {
				conversionCount[0]++;
				return true;
			} else {
				return false;
			}
		case Exp.CatHierarchy:
			if (to == Exp.CatDimension) {
				conversionCount[0]++;
				return true;
			} else {
				return false;
			}
		case Exp.CatLevel:
			if (to == Exp.CatDimension ||
					to == Exp.CatHierarchy) {
				conversionCount[0]++;
				return true;
			} else {
				return false;
			}
		case Exp.CatLogical:
			return false;
		case Exp.CatMember:
			if (to == Exp.CatDimension ||
					to == Exp.CatHierarchy ||
					to == Exp.CatLevel ||
					to == Exp.CatNumeric) {
				conversionCount[0]++;
				return true;
			} else if (to == Exp.CatValue ||
					to == (Exp.CatNumeric | Exp.CatExpression) ||
					to == (Exp.CatString | Exp.CatExpression)) {
				return true;
			} else {
				return false;
			}
		case Exp.CatNumeric:
			return to == Exp.CatValue ||
				to == (Exp.CatNumeric | Exp.CatExpression);
		case Exp.CatNumeric | Exp.CatExpression:
			return to == Exp.CatValue ||
				to == Exp.CatNumeric;
		case Exp.CatSet:
			return false;
		case Exp.CatString:
			return to == Exp.CatValue ||
				to == (Exp.CatString | Exp.CatExpression);
		case Exp.CatString | Exp.CatExpression:
			return to == Exp.CatValue ||
				to == Exp.CatExpression;
		case Exp.CatTuple:
			return to == Exp.CatValue ||
				to == Exp.CatNumeric;
		case Exp.CatValue:
			return false;
		case Exp.CatSymbol:
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

	/**
	 * Resolves a function call to a particular function. If the function is
	 * overloaded, returns as precise a match to the argument types as
	 * possible.
	 **/
	public FunDef getDef(FunCall call) {
		boolean bIgnoreCase = true;	// this may become a parameter
		String name = call.getFunName();
		String upperName = name.toUpperCase();

		// Resolve function by its upper-case name first.  If there is only one
		// function with that name, stop immediately.  If there is more than
		// function, use some custom method, which generally involves looking
		// at the type of one of its arguments.
		String signature = call.getSignature();
		Resolver[] resolvers = (Resolver[]) upperName2Resolvers.get(upperName);
		if (resolvers == null) {
			resolvers = emptyResolvers;
		}

		int[] conversionCount = new int[1];
		int minConversions = Integer.MAX_VALUE;
		int matchCount = 0;
		FunDef matchDef = null;
		for (int i = 0; i < resolvers.length; i++) {
			conversionCount[0] = 0;
			FunDef def = resolvers[i].resolve(
					call.getSyntacticType(), call.args, conversionCount);
			if (def != null) {
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
			throw Util.newInternal(
					"no function matches signature '" + signature + "'");
		case 1:
			return matchDef;
		default:
			throw Util.newInternal(
					"more than one function matches signature '" + signature +
					"'");
		}
	}

	/**
	 * Derived class can override this method to add more functions.
	 **/
	protected void defineFunctions() {
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
				test.assertEquals("Time", s);
			}
		});

		define(new FunDefBase("Dimension", "<Level>.Dimension", "Returns the dimension that contains a specified level.", "pdl") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Level level = getLevelArg(evaluator, args, 0, true);
				return level.getDimension();
			}

			public void testLevelDimension(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Time].[Year].Dimension");
				test.assertEquals("[Time]", s);
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
				test.assertEquals("[Time]", s);
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
				String s = test.executeExpr(
						"Dimensions(2).Name");
				test.assertEquals("Store", s);
			}
		});
		define(new FunDefBase("Dimensions", "Dimensions(<String Expression>)", "Returns the dimension whose name is specified by a string.", "fdS") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				String defValue = "Default Value";
				String s = getStringArg(evaluator, args, 0, defValue);
				if (s.indexOf("[") == -1) {
					s = Util.quoteMdxIdentifier(s);
				}
				Cube cube = evaluator.getCube();
				boolean fail = false;
				OlapElement o = Util.lookupCompound(cube, s, cube, fail);
				if (o == null) {
					throw newEvalException(
							this, "Dimensions '" + s + "' not found");
				} else if (o instanceof Dimension) {
					return (Dimension) o;
				} else {
					throw newEvalException(
							this, "Dimensions(" + s + ") found " + o);
				}
			}

			public void testDimensionsString(FoodMartTestCase test) {
				String s = test.executeExpr(
						"Dimensions(\"Store\").UniqueName");
				test.assertEquals("[Store]", s);
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
				test.assertEquals("[Time]", s);
			}

			public void testBasic9(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Gender].[All Gender].[F].Hierarchy");
				test.assertEquals("[Gender]", s);
			}

			public void testFirstInLevel9(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Education Level].[All Education Levels].[Bachelors Degree].Hierarchy");
				test.assertEquals("[Education Level]", s);
			}

			public void testHierarchyAll(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Gender].[All Gender].Hierarchy");
				test.assertEquals("[Gender]", s);
			}

			public void testHierarchyNull(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Gender].[All Gender].Parent.Hierarchy");
				test.assertEquals("[Gender]", s); // MSOLAP gives "#ERR"
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
				test.assertEquals("[Time].[Month]", s);
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
				test.assertEquals("Quarter", s);
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
				boolean fail = false;
				OlapElement o = null;
				if (s.startsWith("[")) {
					o = Util.lookupCompound(cube, s, cube, fail);
				} else {
					// lookupCompound barfs if "s" doesn't have matching
					// brackets, so don't even try
					o = null;
				}
				if (o == null) {
					throw newEvalException(
							this, "could not find level '" + s + "'");
				} else if (o instanceof Level) {
					return (Level) o;
				} else {
					throw newEvalException(
							this,
							"found '" + o.getDescription() + "', not a level");
				}
			}

			public void testLevelsString(FoodMartTestCase test) {
				String s = test.executeExpr(
						"Levels(\"[Time].[Year]\").UniqueName");
				test.assertEquals("[Time].[Year]", s);
			}

			public void testLevelsStringFail(FoodMartTestCase test) {
				test.assertExprThrows(
						"Levels(\"nonexistent\").UniqueName",
						"could not find level 'nonexistent'");
			}
		});

		//
		// LOGICAL FUNCTIONS
		define(new FunDefBase("IsEmpty", "IsEmpty(<Value Expression>)", "Determines if an expression evaluates to the empty cell value.", "fbS"));

		define(new FunDefBase("IsEmpty", "IsEmpty(<Value Expression>)", "Determines if an expression evaluates to the empty cell value.", "fbn"));
		//
		// MEMBER FUNCTIONS
		//	if (false) define(new FunDefBase("Ancestor", "Ancestor(<Member>, <Level>)", "Returns the ancestor of a member at a specified level.", "fm*");


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
				test.assertEquals("USA", member.getName());
			}

			public void testAncestorHigher(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Ancestor([Store].[USA],[Store].[Store City])");
				test.assertNull(member); // MSOLAP returns null
			}

			public void testAncestorSameLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Ancestor([Store].[Canada],[Store].[Store Country])");
				test.assertEquals("Canada", member.getName());
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
				test.assertTrue(member.isAll());
			}
		});

		define(new FunDefBase("ClosingPeriod", "ClosingPeriod([<Level>[, <Member>]])", "Returns the last sibling among the descendants of a member at a level.", "fm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Cube cube = evaluator.getCube();
				Dimension timeDimension = cube.getYearLevel().getDimension();
				Member member = evaluator.getContext(timeDimension);
				Level level = member.getLevel().getChildLevel();
				return openClosingPeriod(this, member, level);
			}
			public void testClosingPeriodNoArgs(FoodMartTestCase test) {
				// MSOLAP returns [1997].[Q4], because [Time].CurrentMember =
				// [1997].
				Member member = test.executeAxis("ClosingPeriod()");
				test.assertEquals("[Time].[1997].[Q4]", member.getUniqueName());
			}
		});
		define(new FunDefBase("ClosingPeriod", "ClosingPeriod([<Level>[, <Member>]])", "Returns the last sibling among the descendants of a member at a level.", "fml") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Cube cube = evaluator.getCube();
				Dimension timeDimension = cube.getYearLevel().getDimension();
				Member member = evaluator.getContext(timeDimension);
				Level level = getLevelArg(evaluator, args, 0, true);
				return openClosingPeriod(this, member, level);
			}
			public void testClosingPeriodLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"ClosingPeriod([Month])");
				test.assertEquals("[Time].[1997].[Q4].[12]", member.getUniqueName());
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
				return openClosingPeriod(this, member, level);
			}
			public void testClosingPeriodMember(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"ClosingPeriod([USA])");
				test.assertEquals("WA", member.getName());
			}
		});
		define(new FunDefBase("ClosingPeriod", "ClosingPeriod([<Level>[, <Member>]])", "Returns the last sibling among the descendants of a member at a level.", "fmlm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Level level = getLevelArg(evaluator, args, 0, true);
				Member member = getMemberArg(evaluator, args, 1, true);
				return openClosingPeriod(this, member, level);
			}
			public void testClosingPeriod(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"ClosingPeriod([Month],[1997])");
				test.assertEquals("[Time].[1997].[Q4].[12]", member.getUniqueName());
			}
			public void testClosingPeriodBelow(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"ClosingPeriod([Quarter],[1997].[Q3].[8])");
				test.assertNull(member);
			}
		});

		define(new FunDefBase("Cousin", "Cousin(<Member1>, <Member2>)", "Returns the member with the same relative position under a member as the member specified.", "fmmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member1 = getMemberArg(evaluator, args, 0, true);
				Member member2 = getMemberArg(evaluator, args, 1, true);
				Member cousin = cousin(member1, member2);
				if (cousin == null) {
					cousin = member1.getHierarchy().getNullMember();
				}
				return cousin;
			}
			private Member cousin(Member member1, Member member2) {
				if (member1.getHierarchy() != member2.getHierarchy()) {
					throw newEvalException(
							this,
							"Members '" + member1 + "' and '" + member2 +
							"' are not compatible as cousins");
				}
				if (member1.getLevel().getDepth() < member2.getLevel().getDepth()) {
					return null;
				}
				return cousin2(member1, member2);
			}
			private Member cousin2(Member member1, Member member2) {
				if (member1.getLevel() == member2.getLevel()) {
					return member2;
				}
				Member uncle = cousin2(member1.getParentMember(), member2);
				if (uncle == null) {
					return null;
				}
				int ordinal = getOrdinalInParent(member1);
				Member[] cousins = uncle.getMemberChildren();
				if (cousins.length < ordinal) {
					return null;
				}
				return cousins[ordinal];
			}
			private int getOrdinalInParent(Member member) {
				Member parent = member.getParentMember();
				Member[] siblings;
				if (parent == null) {
					siblings = member.getHierarchy().getRootMembers();
				} else {
					siblings = parent.getMemberChildren();
				}
				for (int i = 0; i < siblings.length; i++) {
					if (siblings[i] == member) {
						return i;
					}
				}
				throw Util.newInternal(
						"could not find member " + member + " amongst its siblings");
			}

			public void testCousin1(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Cousin([1997].[Q4],[1998])");
				test.assertEquals("[Time].[1998].[Q4]", member.getUniqueName());
			}

			public void testCousin2(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Cousin([1997].[Q4].[12],[1998].[Q1])");
				test.assertEquals("[Time].[1998].[Q1].[3]", member.getUniqueName());
			}

			public void testCousinOverrun(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Cousin([Customers].[USA].[CA].[San Jose], [Customers].[USA].[OR])");
				// CA has more cities than OR
				test.assertNull(member);
			}

			public void testCousinThreeDown(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Cousin([Customers].[USA].[CA].[Berkeley].[Alma Shelton], [Customers].[Mexico])");
				// Alma Shelton is the 3rd child
				// of the 4th child (Berkeley)
				// of the 1st child (CA)
				// of USA
				test.assertEquals("[Customers].[All Customers].[Mexico].[DF].[Tixapan].[Albert Clouse]", member.getUniqueName());
			}

			public void testCousinSameLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Cousin([Gender].[M], [Gender].[F])");
				test.assertEquals("F", member.getName());
			}

			public void testCousinHigherLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"Cousin([Time].[1997], [Time].[1998].[Q1])");
				test.assertNull(member);
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
				test.assertEquals("F", result.getCell(new int[] {0}).getValue());
			}
			public void testCurrentMemberFromDefaultMember(FoodMartTestCase test) {
				Result result = test.runQuery(
						"with member [Measures].[Foo] as '[Time].CurrentMember.Name'" + nl +
						"select {[Measures].[Foo]} on columns" + nl +
						"from Sales");
				test.assertEquals("1997", result.getCell(new int[] {0}).getValue());
			}
			public void testCurrentMemberFromAxis(FoodMartTestCase test) {
				Result result = test.runQuery(
						"with member [Measures].[Foo] as '[Gender].CurrentMember.Name || [Marital Status].CurrentMember.Name'" + nl +
						"select {[Measures].[Foo]} on columns," + nl +
						" CrossJoin({[Gender].children}, {[Marital Status].children}) on rows" + nl +
						"from Sales");
				test.assertEquals("FM", result.getCell(new int[] {0,0}).getValue());
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
				test.assertEquals("Unit Sales", result.getCell(new int[] {0}).getValue());
			}
		});
		define(new FunDefBase("DefaultMember", "<Dimension>.DefaultMember", "Returns the default member of a dimension.", "pmd") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Dimension dimension = getDimensionArg(evaluator, args, 0, true);
				return dimension.getHierarchy().getDefaultMember();
			}

			public void testDimensionDefaultMember(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Measures].DefaultMember");
				test.assertEquals("Unit Sales", member.getName());
			}
		});

		define(new FunDefBase("FirstChild", "<Member>.FirstChild", "Returns the first child of a member.", "pmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				Member[] children = member.getMemberChildren();
				if (children.length == 0) {
					return member.getHierarchy().getNullMember();
				} else {
					return children[0];
				}
			}

			public void testFirstChildFirstInLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Time].[1997].[Q4].FirstChild");
				test.assertEquals("10", member.getName());
			}

			public void testFirstChildAll(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[All Gender].FirstChild");
				test.assertEquals("F", member.getName());
			}

			public void testFirstChildOfChildless(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[All Gender].[F].FirstChild");
				test.assertNull(member);
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
					children = member.getHierarchy().getRootMembers();
				} else {
					children = parent.getMemberChildren();
				}
				return children[0];
			}

			public void testFirstSiblingFirstInLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[F].FirstSibling");
				test.assertEquals("F", member.getName());
			}

			public void testFirstSiblingLastInLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Time].[1997].[Q4].FirstSibling");
				test.assertEquals("Q1", member.getName());
			}

			public void testFirstSiblingAll(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[All Gender].FirstSibling");
				test.assertTrue(member.isAll());
			}

			public void testFirstSiblingRoot(FoodMartTestCase test) {
				// The [Measures] hierarchy does not have an 'all' member, so
				// [Unit Sales] does not have a parent.
				Member member = test.executeAxis(
						"[Measures].[Store Sales].FirstSibling");
				test.assertEquals("Unit Sales", member.getName());
			}

			public void testFirstSiblingNull(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[F].FirstChild.FirstSibling");
				test.assertNull(member);
			}
		});

		if (false) define(new FunDefBase("Item", "<Tuple>.Item(<Numeric Expression>)", "Returns a member from a tuple.", "mm*"));

		define(new MultiResolver(
				"Lag", "<Member>.Lag(<Numeric Expression>)", "Returns a member further along the specified member's dimension.",
				new String[]{"mmmn"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Member member = getMemberArg(evaluator, args, 0, true);
						int n = getIntArg(evaluator, args, 1);
						return member.getLeadMember(-n);
					}

					public void testLag(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Time].[1997].[Q4].[12].Lag(4)");
						test.assertEquals("8", member.getName());
					}

					public void testLagFirstInLevel(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Gender].[F].Lag(1)");
						test.assertNull(member);
					}

					public void testLagAll(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Gender].DefaultMember.Lag(2)");
						test.assertNull(member);
					}

					public void testLagRoot(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Time].[1998].Lag(1)");
						test.assertEquals("1997", member.getName());
					}

					public void testLagRootTooFar(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Time].[1998].Lag(2)");
						test.assertNull(null);
					}
				}));

		define(new FunDefBase("LastChild", "<Member>.LastChild", "Returns the last child of a member.", "pmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				Member[] children = member.getMemberChildren();
				if (children.length == 0) {
					return member.getHierarchy().getNullMember();
				} else {
					return children[children.length - 1];
				}
			}

			public void testLastChild(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].LastChild");
				test.assertEquals("M", member.getName());
			}

			public void testLastChildLastInLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Time].[1997].[Q4].LastChild");
				test.assertEquals("12", member.getName());
			}

			public void testLastChildAll(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[All Gender].LastChild");
				test.assertEquals("M", member.getName());
			}

			public void testLastChildOfChildless(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[M].LastChild");
				test.assertNull(member);
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
					children = member.getHierarchy().getRootMembers();
				} else {
					children = parent.getMemberChildren();
				}
				return children[children.length - 1];
			}

			public void testLastSibling(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[F].LastSibling");
				test.assertEquals("M", member.getName());
			}

			public void testLastSiblingFirstInLevel(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Time].[1997].[Q1].LastSibling");
				test.assertEquals("Q4", member.getName());
			}

			public void testLastSiblingAll(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[All Gender].LastSibling");
				test.assertTrue(member.isAll());
			}

			public void testLastSiblingRoot(FoodMartTestCase test) {
				// The [Time] hierarchy does not have an 'all' member, so
				// [1997], [1998] do not have parents.
				Member member = test.executeAxis(
						"[Time].[1998].LastSibling");
				test.assertEquals("1998", member.getName());
			}

			public void testLastSiblingNull(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[F].FirstChild.LastSibling");
				test.assertNull(member);
			}
		});

		define(new MultiResolver(
				"Lead", "<Member>.Lead(<Numeric Expression>)", "Returns a member further along the specified member's dimension.",
				new String[]{"mmmn"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Member member = getMemberArg(evaluator, args, 0, true);
						int n = getIntArg(evaluator, args, 1);
						return member.getLeadMember(n);
					}

					public void testLead(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Time].[1997].[Q2].[4].Lead(4)");
						test.assertEquals("8", member.getName());
					}

					public void testLeadNegative(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Gender].[M].Lead(-1)");
						test.assertEquals("F", member.getName());
					}

					public void testLeadLastInLevel(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Gender].[M].Lead(3)");
						test.assertNull(member);
					}
				}));

		define(new FunDefBase("Members", "Members(<String Expression>)", "Returns the member whose name is specified by a string expression.", "fmS"));

		define(new FunDefBase(
				"NextMember", "<Member>.NextMember", "Returns the next member in the level that contains a specified member.", "pmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return member.getLeadMember(+1);
			}

			public void testBasic2(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Gender].[F].NextMember} ON COLUMNS from Sales");
				test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("M"));
			}

			public void testFirstInLevel2(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Gender].[M].NextMember} ON COLUMNS from Sales");
				test.assertTrue(result.getAxes()[0].positions.length == 0);
			}

			public void testAll2(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Gender].PrevMember} ON COLUMNS from Sales");
				// previous to [Gender].[All] is null, so no members are returned
				test.assertTrue(result.getAxes()[0].positions.length == 0);
			}
		});

		if (false) define(new FunDefBase("OpeningPeriod", "OpeningPeriod([<Level>[, <Member>]])", "Returns the first sibling among the descendants of a member at a level.", "fm*"));
		if (false) define(new FunDefBase("ParallelPeriod", "ParallelPeriod([<Level>[, <Numeric Expression>[, <Member>]]])", "Returns a member from a prior period in the same relative position as a specified member.", "fm*"));

		define(new FunDefBase("Parent", "<Member>.Parent", "Returns the parent of a member.", "pmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				Member parent = member.getParentMember();
				if (parent == null) {
					parent = member.getHierarchy().getNullMember();
				}
				return parent;
			}

			public void testBasic5(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select{ [Product].[All Products].[Drink].Parent} on columns from Sales");
				test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("All Products"));
			}

			public void testFirstInLevel5(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Time].[1997].[Q2].[4].Parent} on columns,{[Gender].[M]} on rows from Sales");
				test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Q2"));
			}

			public void testAll5(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Time].[1997].[Q2].Parent} on columns,{[Gender].[M]} on rows from Sales");
				// previous to [Gender].[All] is null, so no members are returned
				test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("1997"));
			}
		});

		define(new FunDefBase("PrevMember", "<Member>.PrevMember", "Returns the previous member in the level that contains a specified member.", "pmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return member.getLeadMember(-1);
			}

			public void testBasic(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Gender].[M].PrevMember} ON COLUMNS from Sales");
				test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("F"));
			}

			public void testFirstInLevel(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Gender].[F].PrevMember} ON COLUMNS from Sales");
				test.assertTrue(result.getAxes()[0].positions.length == 0);
			}

			public void testAll(FoodMartTestCase test) {
				Result result = test.runQuery(
						"select {[Gender].PrevMember} ON COLUMNS from Sales");
				// previous to [Gender].[All] is null, so no members are returned
				test.assertTrue(result.getAxes()[0].positions.length == 0);
			}
		});
		if (false) define(new FunDefBase("ValidMeasure", "ValidMeasure(<Tuple>)", "Returns a valid measure in a virtual cube by forcing inapplicable dimensions to their top level.", "fm*"));
		//
		// NUMERIC FUNCTIONS
		if (false) define(new FunDefBase("Aggregate", "Aggregate(<Set>[, <Numeric Expression>])", "Returns a calculated value using the appropriate aggregate function, based on the context of the query.", "fn*"));
		define(new MultiResolver(
			"Avg", "Avg(<Set>[, <Numeric Expression>])", "Returns the average value of a numeric expression evaluated over a set.",
			new String[]{"fnx", "fnxN"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector members = (Vector) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
					return avg(evaluator.push(), members, exp);
				}
				public void testAvg(FoodMartTestCase test) {
					String result = test.executeExpr(
							"AVG({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
					test.assertEquals("188412.71", result);
				}
				//todo: testAvgWithNulls
			}));
		define(new MultiResolver(
			"Correlation", "Correlation(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Returns the correlation of two series evaluated over a set.",			
			new String[]{"fnxN","fnxNN"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector members = (Vector) getArg(evaluator, args, 0);
					ExpBase exp1 = (ExpBase) getArg(evaluator, args, 1);
					ExpBase exp2 = (ExpBase) getArg(evaluator, args, 2);
					return correlation(evaluator.push(), members, exp1, exp2);
				}
				public void testCorrelation(FoodMartTestCase test) {
					String result = test.executeExpr("Correlation({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])");
					test.assertEquals("0.9999063938016924", result);
				}
			}));
		define(new MultiResolver(
			"Count", "Count(<Set>[, EXCLUDEEMPTY | INCLUDEEMPTY])", "Returns the number of tuples in a set, empty cells included unless the optional EXCLUDEEMPTY flag is used.",
			new String[]{"fnx", "fnxy"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector members = (Vector) getArg(evaluator, args, 0);
					String  empties = (String) getArg(evaluator, args, 1, "INCLUDEEMPTY");
					if (empties.equals("INCLUDEEMPTY")) {
						return new Double(members.size());
					}
					else {
						int retval = 0;
						for (int i = 0; i < members.size(); i++) {
							if ((members.elementAt(i) != Util.nullValue) && (members.elementAt(i) != null)) {
								retval++;
							}
						}
						return new Double(retval);
					}
				}
				public void testCount(FoodMartTestCase test) {
					String result = test.executeExpr(
							"count({[Promotion Media].[Media Type].members})");
					test.assertEquals("14.0", result);
				}
				//todo: testCountNull, testCountNoExp
			}));
		define(new MultiResolver(
			"Covariance", "Covariance(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Returns the covariance of two series evaluated over a set (biased).",
			new String[]{"fnxN","fnxNN"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector members = (Vector) getArg(evaluator, args, 0);
					ExpBase exp1 = (ExpBase) getArg(evaluator, args, 1);
					ExpBase exp2 = (ExpBase) getArg(evaluator, args, 2);
					return covariance(evaluator.push(), members, exp1, exp2, true);
				}
				public void testCovariance(FoodMartTestCase test) {
					String result = test.executeExpr("Covariance({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])");
					test.assertEquals("1.3557618990466664E9", result);
				}
			}));
		define(new MultiResolver(
			"CovarianceN", "CovarianceN(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Returns the covariance of two series evaluated over a set (unbiased).",
			new String[]{"fnxN","fnxNN"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector members = (Vector) getArg(evaluator, args, 0);
					ExpBase exp1 = (ExpBase) getArg(evaluator, args, 1);
					ExpBase exp2 = (ExpBase) getArg(evaluator, args, 2);
					return covariance(evaluator.push(), members, exp1, exp2, false);
				}
				public void testCovarianceN(FoodMartTestCase test) {
					String result = test.executeExpr("CovarianceN({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])");
					test.assertEquals("2.0336428485699995E9", result);
				}
			}));
		define(new FunDefBase("IIf", "IIf(<Logical Expression>, <Numeric Expression1>, <Numeric Expression2>)", "Returns one of two numeric values determined by a logical test.", "fnbnn"));
		if (false) define(new FunDefBase("LinRegIntercept", "LinRegIntercept(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns the value of b in the regression line y = ax + b.", "fn*"));
		if (false) define(new FunDefBase("LinRegPoint", "LinRegPoint(<Numeric Expression>, <Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns the value of y in the regression line y = ax + b.", "fn*"));
		if (false) define(new FunDefBase("LinRegR2", "LinRegR2(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns R2 (the coefficient of determination).", "fn*"));
		if (false) define(new FunDefBase("LinRegSlope", "LinRegSlope(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns the value of a in the regression line y = ax + b.", "fn*"));
		if (false) define(new FunDefBase("LinRegVariance", "LinRegVariance(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns the variance associated with the regression line y = ax + b.", "fn*"));
		define(new MultiResolver(
			"Max", "Max(<Set>[, <Numeric Expression>])", "Returns the maximum value of a numeric expression evaluated over a set.",
			new String[]{"fnx", "fnxN"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector members = (Vector) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
					return max(evaluator.push(), members, exp);
				}
				public void testMax(FoodMartTestCase test) {
					String result = test.executeExpr(
							"MAX({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
					test.assertEquals("263793.22", result);
				}
			}));
		define(new MultiResolver(
			"Median", "Median(<Set>[, <Numeric Expression>])", "Returns the median value of a numeric expression evaluated over a set.",
			new String[]{"fnx", "fnxN"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector members = (Vector) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
					//todo: ignore nulls, do we need to ignore the vector?
					return median(evaluator.push(), members, exp);

				}
				public void testMedian(FoodMartTestCase test) {
					String result = test.executeExpr(
							"MEDIAN({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
					test.assertEquals("159167.84", result);
				}
			}));

		define(new MultiResolver(
			"Min", "Min(<Set>[, <Numeric Expression>])", "Returns the minimum value of a numeric expression evaluated over a set.",
			new String[]{"fnx", "fnxN"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector members = (Vector) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
					return min(evaluator.push(), members, exp);
				}
				public void testMin(FoodMartTestCase test) {
					String result = test.executeExpr(
							"MIN({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
					test.assertEquals("142277.07", result);
				}
			}));
		define(new FunDefBase("Ordinal", "<Level>.Ordinal", "Returns the zero-based ordinal value associated with a level.", "pnl"));
		if (false) define(new FunDefBase("Rank", "Rank(<Tuple>, <Set>)", "Returns the one-based rank of a tuple in a set.", "fn*"));
		define(new MultiResolver(
				"Stddev", "Stddev(<Set>[, <Numeric Expression>])", "Alias for Stdev.",
				new String[]{"fnx", "fnxN"},
				new FunkBase() {
						public Object evaluate(Evaluator evaluator, Exp[] args) {
							Vector members = (Vector) getArg(evaluator, args, 0);
							ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
							return stdev(evaluator.push(), members, exp, false);
						}
				}));
		define(new MultiResolver(
				"Stdev", "Stdev(<Set>[, <Numeric Expression>])", "Returns the standard deviation of a numeric expression evaluated over a set (unbiased).",
				new String[]{"fnx", "fnxN"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Vector members = (Vector) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
						return stdev(evaluator.push(), members, exp, false);
					}
					public void testStdev(FoodMartTestCase test) {
						String result = test.executeExpr(
								"STDEV({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
						test.assertEquals("65825.4547549297", result);
					}
				}));
		define(new MultiResolver(
				"StddevP", "StddevP(<Set>[, <Numeric Expression>])", "Alias for StdevP.",
				new String[]{"fnx", "fnxN"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Vector members = (Vector) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
						return stdev(evaluator.push(), members, exp, true);
					}
				}));
		define(new MultiResolver(
				"StdevP", "StdevP(<Set>[, <Numeric Expression>])", "Returns the standard deviation of a numeric expression evaluated over a set (biased).",
				new String[]{"fnx", "fnxN"},
				new FunkBase() {
						public Object evaluate(Evaluator evaluator, Exp[] args) {
							Vector members = (Vector) getArg(evaluator, args, 0);
							ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
							return stdev(evaluator.push(), members, exp, true);
						}
					public void testStdevP(FoodMartTestCase test) {
						String result = test.executeExpr(
								"STDEVP({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
						test.assertEquals("53746.25874541283", result);
					}
				}));		
		define(new MultiResolver(
				"Sum", "Sum(<Set>[, <Numeric Expression>])", "Returns the sum of a numeric expression evaluated over a set.",
				new String[]{"fnx", "fnxN"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Vector members = (Vector) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
						return sum(evaluator.push(), members, exp);
					}
					public void testSumNoExp(FoodMartTestCase test) {
						String result = test.executeExpr(
								"SUM({[Promotion Media].[Media Type].members})");
						test.assertEquals("266773.0", result);
					}
				}));
		define(new FunDefBase("Value", "<Measure>.Value", "Returns the value of a measure.", "pnm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return member.evaluateScalar(evaluator);
			}
		});
		define(new FunDefBase("_Value", "_Value(<Tuple>)", "Returns the value of the current measure within the context of a tuple.", "fvt") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member[] members = getTupleArg(evaluator, args, 0);
				Evaluator evaluator2 = evaluator.push(members);
				return evaluator2.evaluateCurrent();
			}
		});
		// _Value is a pseudo-function which evaluates a tuple to a number.
		// It needs a custom resolver.
		if (false)
		define(new ResolverBase("_Value", null, null, FunDef.TypeParentheses) {
			public FunDef resolve(Exp[] args, int[] conversionCount) {
				if (args.length == 1 &&
						args[0].getType() == FunCall.CatTuple) {
					return new ValueFunDef(new int[] {FunCall.CatTuple});
				}
				for (int i = 0; i < args.length; i++) {
					Exp arg = args[i];
					if (!canConvert(arg, FunCall.CatMember,  conversionCount)) {
						return null;
					}
				}
				int[] argTypes = new int[args.length];
				for (int i = 0; i < argTypes.length; i++) {
					argTypes[i] = FunCall.CatMember;
				}
				return new ValueFunDef(argTypes);
			}
		});
		define(new MultiResolver(
				"Var", "Var(<Set>[, <Numeric Expression>])", "Returns the variance of a numeric expression evaluated over a set (unbiased).",
				new String[]{"fnx", "fnxN"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Vector members = (Vector) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
						return var(evaluator.push(), members, exp, false);
					}
					public void testVar(FoodMartTestCase test) {
						String result = test.executeExpr(
								"VAR({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
						test.assertEquals("4.332990493693297E9", result);
					}
				}));
		define(new MultiResolver(
				"Variance", "Variance(<Set>[, <Numeric Expression>])", "Alias for Var.",
				new String[]{"fnx", "fnxN"},
				new FunkBase() {
						public Object evaluate(Evaluator evaluator, Exp[] args) {
							Vector members = (Vector) getArg(evaluator, args, 0);
							ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
							return var(evaluator.push(), members, exp, false);
						}
				}));
		define(new MultiResolver(
				"VarianceP", "VarianceP(<Set>[, <Numeric Expression>])", "Alias for VarP.",
				new String[]{"fnx", "fnxN"},
				new FunkBase() {
						public Object evaluate(Evaluator evaluator, Exp[] args) {
							Vector members = (Vector) getArg(evaluator, args, 0);
							ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
							return var(evaluator.push(), members, exp, true);
						}
				}));
		define(new MultiResolver(
				"VarP", "VarP(<Set>[, <Numeric Expression>])", "Returns the variance of a numeric expression evaluated over a set (biased).",
				new String[]{"fnx", "fnxN"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Vector members = (Vector) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
						return var(evaluator.push(), members, exp, true);
					}
					public void testVarP(FoodMartTestCase test) {
						String result = test.executeExpr(
								"VARP({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
						test.assertEquals("2.888660329128865E9", result);
					}
				}));

		//
		// SET FUNCTIONS
		if (false) define(new FunDefBase("AddCalculatedMembers", "AddCalculatedMembers(<Set>)", "Adds calculated members to a set.", "fx*"));
		define(new MultiResolver(
			"BottomCount",
			"BottomCount(<Set>, <Count>[, <Numeric Expression>])",
			"Returns a specified number of items from the bottom of a set, optionally ordering the set first.",
			new String[]{"fxxnN", "fxxn"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector set = (Vector) getArg(evaluator, args, 0);
					int n = getIntArg(evaluator, args, 1);
					ExpBase exp = (ExpBase) getArg(evaluator, args, 2, null);
					if (exp != null) {
						boolean desc = false, brk = true;
						sort(evaluator, set, exp, desc, brk);
					}
					if (n < set.size()) {
						set.setSize(n);
					}
					return set;
				}
				public void testBottomCount(FoodMartTestCase test) {
					Axis axis = test.executeAxis2(
							"BottomCount({[Promotion Media].[Media Type].members}, 2, [Measures].[Unit Sales])");
					String expected = "[Promotion Media].[All Media].[Radio]" + nl +
							"[Promotion Media].[All Media].[Sunday Paper, Radio, TV]";
					test.assertEquals(expected, test.toString(axis.positions));
				}
				//todo: test unordered

			}));
		define(new MultiResolver(
			"BottomPercent", "BottomPercent(<Set>, <Percentage>, <Numeric Expression>)", "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified percentage.",
			new String[]{"fxxnN"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector members = (Vector) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArg(evaluator, args, 2);
					Double n = getDoubleArg(evaluator, args, 1);
					return topOrBottom(evaluator.push(), members, exp, false, true, n.doubleValue());

				}
				public void testBottomPercent(FoodMartTestCase test) {
					Axis axis = test.executeAxis2(
							"BottomPercent({[Promotion Media].[Media Type].members}, 1, [Measures].[Unit Sales])");
					String expected = "[Promotion Media].[All Media].[Radio]" + nl +
							"[Promotion Media].[All Media].[Sunday Paper, Radio, TV]";
					test.assertEquals(expected, test.toString(axis.positions));
				}
				//todo: test precision
			}));

		define(new MultiResolver(
			"BottomSum", "BottomSum(<Set>, <Value>, <Numeric Expression>)", "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified value.",
			new String[]{"fxxnN"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector members = (Vector) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArg(evaluator, args, 2);
					Double n = getDoubleArg(evaluator, args, 1);
					return topOrBottom(evaluator.push(), members, exp, false, false, n.doubleValue());

				}
				public void testBottomSum(FoodMartTestCase test) {
					Axis axis = test.executeAxis2(
							"BottomSum({[Promotion Media].[Media Type].members}, 5000, [Measures].[Unit Sales])");
					String expected = "[Promotion Media].[All Media].[Radio]" + nl +
							"[Promotion Media].[All Media].[Sunday Paper, Radio, TV]";
					test.assertEquals(expected, test.toString(axis.positions));
				}
			}));
		define(new FunDefBase("Children", "<Member>.Children", "Returns the children of a member.", "pxm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				Member[] children = member.getMemberChildren();
				return toVector(children);
			}
		});
		define(new FunDefBase("Crossjoin", "Crossjoin(<Set1>, <Set2>)", "Returns the cross product of two sets.", "fxxx") {
			public Hierarchy getHierarchy(Exp[] args) {
				// CROSSJOIN(<Set1>,<Set2>) has Hierarchy [Hie1] x [Hie2], which we
				// can't represent, so we return null.
				return null;
			}

			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Vector set0 = (Vector) getArg(evaluator, args, 0),
						set1 = (Vector) getArg(evaluator, args, 1),
						result = new Vector();
				for (int i = 0, m = set0.size(); i < m; i++) {
					Member o0 = (Member) set0.elementAt(i);
					for (int j = 0, n = set1.size(); j < n; j++) {
						Member o1 = (Member) set1.elementAt(j);
						result.addElement(new Member[]{o0, o1});
					}
				}
				return result;
			}
		});
		define(new MultiResolver(
				"Descendants", "Descendants(<Member>, <Level>[, <Desc_flag>])", "Returns the set of descendants of a member at a specified level, optionally including or excluding descendants in other levels.",
				new String[]{"fxml", "fxmls"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Member member = getMemberArg(evaluator, args, 0, true);
						Level level = getLevelArg(evaluator, args, 1, true);
//					String descFlag = getStringArg(evaluator, args, 2, ??);
						if (member.getLevel().getDepth() > level.getDepth()) {
							return new Member[0];
						}
						// Expand member to its children, until we get to the right
						// level. We assume that all children are in the same
						// level.
						final Hierarchy hierarchy = member.getHierarchy();
						Member[] children = {member};
						while (children.length > 0 &&
								children[0].getLevel().getDepth() <
								level.getDepth()) {
							children = hierarchy.getChildMembers(children);
						}
						return toVector(children);
					}
				}));
		if (false) define(new FunDefBase("Distinct", "Distinct(<Set>)", "Eliminates duplicate tuples from a set.", "fxx"));

		define(new MultiResolver("DrilldownLevel", "DrilldownLevel(<Set>[, <Level>]) or DrilldownLevel(<Set>, , <Index>)", "Drills down the members of a set, at a specified level, to one level below. Alternatively, drills down on a specified dimension in the set.",
				new String[]{"fxx", "fxxl"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						//todo add fssl functionality
						Vector set0 = (Vector) getArg(evaluator, args, 0);
						int[] depthArray = new int[set0.size()];
						Vector drilledSet = new Vector();

						for (int i = 0, m = set0.size(); i < m; i++) {
							Member member = (Member) set0.elementAt(i);
							depthArray[i] = member.getDepth();
							// Object o0 = set0.elementAt(i);
							//   depthVector.addElement(new Object[] {o0});
						}
						Arrays.sort(depthArray);
						int maxDepth = depthArray[depthArray.length - 1];
						for (int i = 0, m = set0.size(); i < m; i++) {
							Member member = (Member) set0.elementAt(i);
							drilledSet.addElement(member);
							if (member.getDepth() == maxDepth) {
								Member[] childMembers = member.getMemberChildren();
								for (int j = 0; j < childMembers.length; j++) {
									drilledSet.addElement(childMembers[j]);
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
		define(new MultiResolver(
				"Except", "Except(<Set1>, <Set2>[, ALL])", "Finds the difference between two sets, optionally retaining duplicates.",
				new String[]{"fxxx", "fxxxs"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						// todo: implement ALL
						HashSet set2 = toHashSet((Vector) getArg(evaluator, args, 1));
						Vector set1 = (Vector) getArg(evaluator, args, 0);
						Vector result = new Vector();
						for (int i = 0, count = set1.size(); i < count; i++) {
							Object o = set1.elementAt(i);
							if (!set2.contains(o)) {
								result.addElement(o);
							}
						}
						return result;
					}
				}));
		if (false) define(new FunDefBase("Extract", "Extract(<Set>, <Dimension>[, <Dimension>...])", "Returns a set of tuples from extracted dimension elements. The opposite of Crossjoin.", "fx*"));

		define(new FunDefBase("Filter", "Filter(<Set>, <Search Condition>)", "Returns the set resulting from filtering a set based on a search condition.", "fxxb") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Vector members = (Vector) getArg(evaluator, args, 0);
				Exp exp = args[1];
				Vector result = new Vector();
				Evaluator evaluator2 = evaluator.push();
				for (int i = 0, count = members.size(); i < count; i++) {
					Object o = members.elementAt(i);
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
				test.assertEquals(1, rows.positions.length);
				Cell cell = result.getCell(new int[] {0,0});
				test.assertEquals("30,114", cell.getFormattedValue());
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
				test.assertTrue(rows.length == 3);
				test.assertEquals("F", rows[0].members[0].getName());
				test.assertEquals("WA", rows[0].members[1].getName());
				test.assertEquals("M", rows[1].members[0].getName());
				test.assertEquals("OR", rows[1].members[1].getName());
				test.assertEquals("M", rows[2].members[0].getName());
				test.assertEquals("WA", rows[2].members[1].getName());
			}
		});

		if (false) define(new FunDefBase("Generate", "Generate(<Set1>, <Set2>[, ALL])", "Applies a set to each member of another set and joins the resulting sets by union.", "fx*"));
		if (false) define(new FunDefBase("Head", "Head(<Set>[, < Numeric Expression >])", "Returns the first specified number of elements in a set.", "fx*"));
		if (false) define(new FunDefBase("Hierarchize", "Hierarchize(<Set>)", "Orders the members of a set in a hierarchy.", "fx*"));
		if (false) define(new FunDefBase("Intersect", "Intersect(<Set1>, <Set2>[, ALL])", "Returns the intersection of two input sets, optionally retaining duplicates.", "fx*"));
		if (false) define(new FunDefBase("LastPeriods", "LastPeriods(<Index>[, <Member>])", "Returns a set of members prior to and including a specified member.", "fx*"));
		define(new FunDefBase("Members", "<Dimension>.Members", "Returns the set of all members in a dimension.", "pxd") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Dimension dimension = (Dimension) getArg(evaluator, args, 0);
				Hierarchy hierarchy = dimension.getHierarchy();
				return addMembers(new Vector(), hierarchy);
			}
		});
		define(new FunDefBase("Members", "<Hierarchy>.Members", "Returns the set of all members in a hierarchy.", "pxh") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Hierarchy hierarchy =
						(Hierarchy) getArg(evaluator, args, 0);
				return addMembers(new Vector(), hierarchy);
			}
		});
		define(new FunDefBase("Members", "<Level>.Members", "Returns the set of all members in a level.", "pxl") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Level level = (Level) getArg(evaluator, args, 0);
				return toVector(level.getMembers());
			}
		});
		define(new MultiResolver(
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
		define(new MultiResolver(
				"Order", "Order(<Set>, <Value Expression>[, ASC | DESC | BASC | BDESC])", "Arranges members of a set, optionally preserving or breaking the hierarchy.",
				new String[]{"fxxvy", "fxxv"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Vector members = (Vector) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArgNoEval(args, 1);
						String order = (String) getArg(evaluator, args, 2, "ASC");
						boolean desc = order.equals("DESC") || order.equals("BDESC");
						boolean brk = order.equals("BASC") || order.equals("BDESC");
						sort(evaluator, members, exp, desc, brk);
						return members;
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
								"{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M]}" + nl +
								"{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}" + nl +
								"{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}" + nl +
								"{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}" + nl +
								"Row #0: 17,097" + nl +
								"Row #1: 16,845" + nl +
								"Row #2: 16,536" + nl +
								"Row #3: 15,813" + nl);
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
				}));
		define(new MultiResolver(
				"PeriodsToDate", "PeriodsToDate([<Level>[, <Member>]])", "Returns a set of periods (members) from a specified level starting with the first period and ending with a specified member.",
				new String[]{"fx", "fxl", "fxlm"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Level level = getLevelArg(evaluator, args, 0, false);
						Member member = getMemberArg(evaluator, args, 1, false);
						return periodsToDate(evaluator, level, member);
					}
				}));
		define(new MultiResolver(
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
		define(new FunDefBase("StrToSet", "StrToSet(<String Expression>)", "Constructs a set from a string expression.", "fxS") {
			public void unparse(Exp[] args, PrintWriter pw, ElementCallback callback) {
				if (callback.isPlatoMdx()) {
					// omit extra args (they're for us, not Plato)
					super.unparse(new Exp[]{args[0]}, pw, callback);
				} else {
					super.unparse(args, pw, callback);
				}
			}

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
		if (false) define(new FunDefBase("Subset", "Subset(<Set>, <Start>[, <Count>])", "Returns a subset of elements from a set.", "fx*"));
		if (false) define(new FunDefBase("Tail", "Tail(<Set>[, <Count>])", "Returns a subset from the end of a set.", "fx*"));
		define(new MultiResolver(
				"ToggleDrillState", "ToggleDrillState(<Set1>, <Set2>[, RECURSIVE])", "Toggles the drill state of members. This function is a combination of DrillupMember and DrilldownMember.",
				new String[]{"fxxx", "fxxx#"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Vector v0 = (Vector) getArg(evaluator, args, 0),
								v1 = (Vector) getArg(evaluator, args, 1);
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
						HashSet set1 = toHashSet(v1);
						Vector result = new Vector();
						int i = 0, n = v0.size();
						while (i < n) {
							Object o = v0.elementAt(i++);
							result.addElement(o);
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
								Object next = v0.elementAt(i);
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
									Object next = (Member) v0.elementAt(i);
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
								Member[] children = m.getMemberChildren();
								for (int j = 0; j < children.length; j++) {
									if (k < 0) {
										result.addElement(children[j]);
									} else {
										Member[] members = (Member[]) ((Member[]) o).clone();
										members[k] = children[j];
										result.addElement(members);
									}
								}
							}
						}
						return result;
					}
					public void testToggleDrillState(FoodMartTestCase test) {
						Axis axis = test.executeAxis2(
								"ToggleDrillState({[Customers].[USA],[Customers].[Canada]},{[Customers].[USA],[Customers].[USA].[CA]})");
						String expected = "[Customers].[All Customers].[USA]" + nl +
								"[Customers].[All Customers].[USA].[CA]" + nl +
								"[Customers].[All Customers].[USA].[OR]" + nl +
								"[Customers].[All Customers].[USA].[WA]" + nl +
								"[Customers].[All Customers].[Canada]";
						test.assertEquals(expected, test.toString(axis.positions));
					}
					public void testToggleDrillState2(FoodMartTestCase test) {
						Axis axis = test.executeAxis2(
								"ToggleDrillState([Product].[Product Department].members, {[Product].[All Products].[Food].[Snack Foods]})");
						String expected = "[Product].[All Products].[Drink].[Alcoholic Beverages]" + nl +
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
								"[Product].[All Products].[Non-Consumable].[Periodicals]";
						test.assertEquals(expected, test.toString(axis.positions));
					}
					public void testToggleDrillState3(FoodMartTestCase test) {
						Axis axis = test.executeAxis2(
								"ToggleDrillState(" +
								"{[Time].[1997].[Q1]," +
								" [Time].[1997].[Q2]," +
								" [Time].[1997].[Q2].[4]," +
								" [Time].[1997].[Q2].[6]," +
								" [Time].[1997].[Q3]}," +
								"{[Time].[1997].[Q2]})");
						String expected = "[Time].[1997].[Q1]" + nl +
								"[Time].[1997].[Q2]" + nl +
								"[Time].[1997].[Q3]";
						test.assertEquals(expected, test.toString(axis.positions));
					}
					// bug 634860
					public void testToggleDrillStateTuple(FoodMartTestCase test) {
                        Axis axis = test.executeAxis2(
								"ToggleDrillState(" + nl +
								"{([Store].[All Stores].[USA].[CA]," +
								"  [Product].[All Products].[Drink].[Alcoholic Beverages])," + nl +
								" ([Store].[All Stores].[USA]," +
								"  [Product].[All Products].[Drink])}," + nl +
								"{[Store].[All stores].[USA].[CA]})");
						String expected = "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
								"{[Store].[All Stores].[USA].[CA].[Beverly Hills], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
								"{[Store].[All Stores].[USA].[CA].[Los Angeles], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
								"{[Store].[All Stores].[USA].[CA].[San Diego], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
								"{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
								"{[Store].[All Stores].[USA], [Product].[All Products].[Drink]}";
						test.assertEquals(expected, test.toString(axis.positions));
					}
				}));
		define(new MultiResolver(
				"TopCount",
				"TopCount(<Set>, <Count>[, <Numeric Expression>])",
				"Returns a specified number of items from the top of a set, optionally ordering the set first.",
				new String[]{"fxxnN", "fxxn"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Vector set = (Vector) getArg(evaluator, args, 0);
						int n = getIntArg(evaluator, args, 1);
						ExpBase exp = (ExpBase) getArg(evaluator, args, 2, null);
						if (exp != null) {
							boolean desc = true, brk = true;
							sort(evaluator, set, exp, desc, brk);
						}
						if (n < set.size()) {
							set.setSize(n);
						}
						return set;
					}
					public void testTopCount(FoodMartTestCase test) {
						Axis axis = test.executeAxis2(
								"TopCount({[Promotion Media].[Media Type].members}, 2, [Measures].[Unit Sales])");
						String expected = "[Promotion Media].[All Media].[No Media]" + nl +
								"[Promotion Media].[All Media].[Daily Paper, Radio, TV]";
						test.assertEquals(expected, test.toString(axis.positions));
					}
				}));

		define(new MultiResolver(
			"TopPercent", "TopPercent(<Set>, <Percentage>, <Numeric Expression>)", "Sorts a set and returns the top N elements whose cumulative total is at least a specified percentage.",
			new String[]{"fxxnN"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector members = (Vector) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArg(evaluator, args, 2);
					Double n = getDoubleArg(evaluator, args, 1);
					return topOrBottom(evaluator.push(), members, exp, true, true, n.doubleValue());

				}
				public void testTopPercent(FoodMartTestCase test) {
					Axis axis = test.executeAxis2(
							"TopPercent({[Promotion Media].[Media Type].members}, 70, [Measures].[Unit Sales])");
					String expected = "[Promotion Media].[All Media].[No Media]";
					test.assertEquals(expected, test.toString(axis.positions));
				}
				//todo: test precision
			}));

		define(new MultiResolver(
			"TopSum", "TopSum(<Set>, <Value>, <Numeric Expression>)", "Sorts a set and returns the top N elements whose cumulative total is at least a specified value.",
			new String[]{"fxxnN"},
			new FunkBase() {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Vector members = (Vector) getArg(evaluator, args, 0);
					ExpBase exp = (ExpBase) getArg(evaluator, args, 2);
					Double n = getDoubleArg(evaluator, args, 1);
					return topOrBottom(evaluator.push(), members, exp, true, false, n.doubleValue());

				}
				public void testTopSum(FoodMartTestCase test) {
					Axis axis = test.executeAxis2(
							"TopSum({[Promotion Media].[Media Type].members}, 200000, [Measures].[Unit Sales])");
					String expected = "[Promotion Media].[All Media].[No Media]" + nl +
							"[Promotion Media].[All Media].[Daily Paper, Radio, TV]";
					test.assertEquals(expected, test.toString(axis.positions));
				}
			}));

		if (false) define(new FunDefBase("Union", "Union(<Set1>, <Set2>[, ALL])", "Returns the union of two sets, optionally retaining duplicates.", "fx*"));
		if (false) define(new FunDefBase("VisualTotals", "VisualTotals(<Set>, <Pattern>)", "Dynamically totals child members specified in a set using a pattern for the total label in the result set.", "fx*"));
		define(new MultiResolver(
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
		define(new MultiResolver(
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
		define(new FunDefBase(":", "<Member>:<Member>", "Infix colon operator returns the set of members between a given pair of members.", "ixmm"));

		// special resolver for the "{...}" operator
		define(new ResolverBase(
				"{}",
				"{<Member> [, <Member>]...}",
				"Brace operator constructs a set.",
				FunDef.TypeBraces) {
			protected FunDef resolve(Exp[] args, int[] conversionCount) {
				int[] parameterTypes = new int[args.length];
				for (int i = 0; i < args.length; i++) {
					if (canConvert(
							args[i], Exp.CatMember, conversionCount)) {
						parameterTypes[i] = Exp.CatMember;
						continue;
					}
					if (canConvert(
							args[i], Exp.CatSet, conversionCount)) {
						parameterTypes[i] = Exp.CatSet;
						continue;
					}
					if (canConvert(
							args[i], Exp.CatTuple, conversionCount)) {
						parameterTypes[i] = Exp.CatTuple;
						continue;
					}
					return null;
				}
				return new SetFunDef(this, syntacticType, parameterTypes);
			}

			public void testSetContainingLevelFails(FoodMartTestCase test) {
				test.assertAxisThrows(
						"[Store].[Store City]",
						"no function matches signature '{<Level>}'");
			}
		});

		//
		// STRING FUNCTIONS
		define(new FunDefBase("IIf", "IIf(<Logical Expression>, <String Expression1>, <String Expression2>)", "Returns one of two string values determined by a logical test.", "f#b##") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				boolean logical = getBooleanArg(evaluator, args, 0);
				return getStringArg(evaluator, args, logical ? 1 : 2, null);
			}

			public void testIIf(FoodMartTestCase test) {
				String s = test.executeExpr(
						"IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, \"Yes\",\"No\")");
				test.assertEquals("Yes", s);
			}
		});
		define(new FunDefBase("Name", "<Dimension>.Name", "Returns the name of a dimension.", "pSd") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Dimension dimension = getDimensionArg(evaluator, args, 0, true);
				return dimension.getName();
			}

			public void testDimensionName(FoodMartTestCase test) {
				String s = test.executeExpr("[Time].[1997].Dimension.Name");
				test.assertEquals("Time", s);
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
				test.assertEquals("Time", s);
			}
		});
		define(new FunDefBase("Name", "<Level>.Name", "Returns the name of a level.", "pSl") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Level level = getLevelArg(evaluator, args, 0, true);
				return level.getName();
			}

			public void testLevelName(FoodMartTestCase test) {
				String s = test.executeExpr("[Time].[1997].Level.Name");
				test.assertEquals("Year", s);
			}
		});
		define(new FunDefBase("Name", "<Member>.Name", "Returns the name of a member.", "pSm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return member.getName();
			}

			public void testMemberName(FoodMartTestCase test) {
				String s = test.executeExpr("[Time].[1997].Name");
				test.assertEquals("1997", s);
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
				test.assertEquals("[Gender]", s);
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
				test.assertEquals("[Gender]", s);
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
				test.assertEquals("[Gender].[(All)]", s);
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
				test.assertEquals("[Gender].[All Gender]", s);
			}

			public void testMemberUniqueNameOfNull(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Measures].[Unit Sales].FirstChild.UniqueName");
				test.assertEquals("[Measures].[#Null]", s); // MSOLAP gives "" here
			}
		});

		//
		// TUPLE FUNCTIONS
		define(new FunDefBase("Current", "<Set>.Current", "Returns the current tuple from a set during an iteration.", "ptx"));
		if (false) define(new FunDefBase("Item", "<Set>.Item(<String Expression>[, <String Expression>...] | <Index>)", "Returns a tuple from a set.", "mt*"));
		define(new FunDefBase("StrToTuple", "StrToTuple(<String Expression>)", "Constructs a tuple from a string.", "ftS") {
			public void unparse(Exp[] args, PrintWriter pw, ElementCallback callback) {
				if (callback.isPlatoMdx()) {
					// omit extra args (they're for us, not Plato)
					super.unparse(new Exp[]{args[0]}, pw, callback);
				} else {
					super.unparse(args, pw, callback);
				}
			}

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
		define(new ResolverBase("()", null, null, FunDef.TypeParentheses) {
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
				FunDef.TypeFunction) {
			protected FunDef resolve(Exp[] args, int[] conversionCount) {
				if (args.length < 1) {
					return null;
				}
				final int[] types = {Exp.CatNumeric, Exp.CatString};
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
						return new FunDefBase(
								this, FunDef.TypeFunction, type,
								ExpBase.getTypes(args));
					}
				}
				return null;
			}
		});

		define(new ResolverBase(
				"_CaseTest",
				"Case When <Logical Expression> Then <Expression> [...] [Else <Expression>] End",
				"Evaluates various conditions, and returns the corresponding expression for the first which evaluates to true.",
				FunDef.TypeCase) {
			protected FunDef resolve(Exp[] args, int[] conversionCount) {
				if (args.length < 1) {
					return null;
				}
				int j = 0,
						clauseCount = args.length / 2,
						mismatchingArgs = 0;
				int returnType = args[1].getType();
				for (int i = 0; i < clauseCount; i++) {
					if (!canConvert(args[j++], Exp.CatLogical, conversionCount)) {
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
					return new FunDefBase(
							this, FunDef.TypeFunction, returnType,
							ExpBase.getTypes(args)) {
						// implement FunDef
						public Object evaluate(Evaluator evaluator, Exp[] args) {
							return evaluateCaseTest(evaluator, args);
						}
					};
				} else {
					return null;
				}
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
				test.assertEquals("second", s);
			}
			public void testCaseTestMatchElse(FoodMartTestCase test) {
				String s = test.executeExpr(
						"CASE WHEN 1=0 THEN \"first\" ELSE \"fourth\" END");
				test.assertEquals("fourth", s);
			}
			public void testCaseTestMatchNoElse(FoodMartTestCase test) {
				String s = test.executeExpr(
						"CASE WHEN 1=0 THEN \"first\" END");
				test.assertEquals("(null)", s);
			}
		});

		define(new ResolverBase(
				"_CaseMatch",
				"Case <Expression> When <Expression> Then <Expression> [...] [Else <Expression>] End",
				"Evaluates various expressions, and returns the corresponding expression for the first which matches a particular value.",
				FunDef.TypeCase) {
			protected FunDef resolve(Exp[] args, int[] conversionCount) {
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
					return new FunDefBase(
							this, FunDef.TypeFunction, returnType,
							ExpBase.getTypes(args)) {
						// implement FunDef
						public Object evaluate(Evaluator evaluator, Exp[] args) {
							return evaluateCaseMatch(evaluator, args);
						}
					};
				} else {
					return null;
				}
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
				test.assertEquals("second", s);
			}
			public void testCaseMatchElse(FoodMartTestCase test) {
				String s = test.executeExpr(
						"CASE 7 WHEN 1 THEN \"first\" ELSE \"fourth\" END");
				test.assertEquals("fourth", s);
			}
			public void testCaseMatchNoElse(FoodMartTestCase test) {
				String s = test.executeExpr(
						"CASE 8 WHEN 0 THEN \"first\" END");
				test.assertEquals("(null)", s);
			}
		});

		define(new ResolverBase(
					   "Properties",
					   "<Member>.Properties(<String Expression>)",
					   "Returns the value of a member property.",
					   FunDef.TypeMethod) {
			public FunDef resolve(Exp[] args, int[] conversionCount) {
				final int[] argTypes = new int[]{Exp.CatMember, Exp.CatString | Exp.CatExpression};
				if (args.length != 2 ||
						args[0].getType() != Exp.CatMember ||
						args[1].getType() != Exp.CatString) {
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
						returnType = Exp.CatValue;
					} else {
						switch (property.getType()) {
						case Property.TYPE_BOOLEAN:
							returnType = Exp.CatLogical;
							break;
						case Property.TYPE_NUMERIC:
							returnType = Exp.CatNumeric;
							break;
						case Property.TYPE_STRING:
							returnType = Exp.CatString;
							break;
						default:
							throw Util.newInternal("Unknown property type " + property.getType());
						}
					}
				} else {
					returnType = Exp.CatValue;
				}
				return new PropertiesFunDef(name, signature, description, syntacticType, returnType, argTypes);
			}
			public void testPropertiesExpr(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Store Type\")");
				test.assertEquals("Gourmet Supermarket", s);
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
				test.assertEquals(8, result.getAxes()[1].positions.length);
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
				member = result.getAxes()[1].positions[17].members[0];
				test.assertEquals("[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]", member.getUniqueName());
				cell = result.getCell(new int[] {0,17});
				test.assertEquals("2,237", cell.getFormattedValue());
				cell = result.getCell(new int[] {1,17});
				test.assertEquals("0.16802205204566403", cell.getFormattedValue());
				member = result.getAxes()[1].positions[3].members[0];
				test.assertEquals("[Store].[All Stores].[Mexico].[DF].[San Andres].[Store 21]", member.getUniqueName());
				cell = result.getCell(new int[] {0,3});
				test.assertEquals("(null)", cell.getFormattedValue());
				cell = result.getCell(new int[] {1,3});
				test.assertEquals("NaN", cell.getFormattedValue());
			}
		});

		//
		// PARAMETER FUNCTIONS
		if (false) define(new FunDefBase("Parameter", "Parameter(<Name>, <Type>, <DefaultValue>, <Description>)", "Returns default value of parameter.", "f*"));
		if (false) define(new FunDefBase("ParamRef", "ParamRef(<Name>)", "Returns current value of parameter. If it's null, returns default.", "f*"));

		//
		// OPERATORS
		define(new FunDefBase("+", "<Numeric Expression> + <Numeric Expression>", "Adds two numbers.", "innn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				return new Double(o0.doubleValue() + o1.doubleValue());
			}
			public void testPlus(FoodMartTestCase test) {
				String s = test.executeExpr("1+2");
				test.assertEquals("3.0", s);
			}
		});
		define(new FunDefBase("-", "<Numeric Expression> - <Numeric Expression>", "Subtracts two numbers.", "innn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				return new Double(o0.doubleValue() - o1.doubleValue());
			}
			public void testMinus(FoodMartTestCase test) {
				String s = test.executeExpr("1-3");
				test.assertEquals("-2.0", s);
			}
			public void testMinusAssociativity(FoodMartTestCase test) {
				String s = test.executeExpr("11-7-5");
				// right-associative would give 11-(7-5) = 9, which is wrong
				test.assertEquals("-1.0", s);
			}
		});
		define(new FunDefBase("*", "<Numeric Expression> * <Numeric Expression>", "Multiplies two numbers.", "innn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				return new Double(o0.doubleValue() * o1.doubleValue());
			}
			public void testMultiply(FoodMartTestCase test) {
				String s = test.executeExpr("4*7");
				test.assertEquals("28.0", s);
			}
			public void testMultiplyPrecedence(FoodMartTestCase test) {
				String s = test.executeExpr("3 + 4 * 5 + 6");
				test.assertEquals("29.0", s);
			}
		});
		define(new FunDefBase("/", "<Numeric Expression> / <Numeric Expression>", "Divides two numbers.", "innn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				Double result = new Double(o0.doubleValue() / o1.doubleValue());
				return result;
			}
			// todo: use this, via reflection
			public double evaluate(double d1, double d2) {
				return d1 / d2;
			}
			public void testDivide(FoodMartTestCase test) {
				String s = test.executeExpr("10 / 5");
				test.assertEquals("2.0", s);
			}
			public void testDivideByZero(FoodMartTestCase test) {
				String s = test.executeExpr("-3 / (2 - 2)");
				test.assertEquals("-Infinity", s);
			}
			public void testDividePrecedence(FoodMartTestCase test) {
				String s = test.executeExpr("24 / 4 / 2 * 10 - -1");
				test.assertEquals("31.0", s);
			}
		});
		define(new FunDefBase("-", "- <Numeric Expression>", "Returns the negative of a number.", "Pnn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0);
				return new Double(- o0.doubleValue());
			}
			public void testUnaryMinus(FoodMartTestCase test) {
				String s = test.executeExpr("-3");
				test.assertEquals("-3.0", s);
			}
			public void testUnaryMinusMember(FoodMartTestCase test) {
				String s = test.executeExpr("- ([Measures].[Unit Sales],[Gender].[F])");
				test.assertEquals("-131558.0", s);
			}
			public void testUnaryMinusPrecedence(FoodMartTestCase test) {
				String s = test.executeExpr("1 - -10.5 * 2 -3");
				test.assertEquals("19.0", s);
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
				test.assertEquals("foobar", s);
			}
			public void testStringConcat2(FoodMartTestCase test) {
				String s = test.executeExpr(" \"foo\" || [Gender].[M].Name || \"\" ");
				test.assertEquals("fooM", s);
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
				test.assertEquals("true", s);
			}
			public void testAnd2(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=1 AND 2=0 ");
				test.assertEquals("false", s);
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
				test.assertEquals("false", s);
			}
			public void testOr2(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=0 OR 0=0 ");
				test.assertEquals("true", s);
			}
			public void testOrAssociativity1(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=1 AND 1=0 OR 1=1 ");
				// Would give 'false' if OR were stronger than AND (wrong!)
				test.assertEquals("true", s);
			}
			public void testOrAssociativity2(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=1 OR 1=0 AND 1=1 ");
				// Would give 'false' if OR were stronger than AND (wrong!)
				test.assertEquals("true", s);
			}
			public void testOrAssociativity3(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" (1=0 OR 1=1) AND 1=1 ");
				test.assertEquals("true", s);
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
				test.assertEquals("false", s);
			}
			public void testXorAssociativity(FoodMartTestCase test) {
				// Would give 'false' if XOR were stronger than AND (wrong!)
				String s = test.executeBooleanExpr(" 1 = 1 AND 1 = 1 XOR 1 = 0 ");
				test.assertEquals("true", s);
			}
		});
		define(new FunDefBase("NOT", "NOT <Logical Expression>", "Returns the negation of a condition.", "Pbb") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				return toBoolean(!getBooleanArg(evaluator, args, 0));
			}
			public void testNot(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" NOT 1=1 ");
				test.assertEquals("false", s);
			}
			public void testNotNot(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" NOT NOT 1=1 ");
				test.assertEquals("true", s);
			}
			public void testNotAssociativity(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" 1=1 AND NOT 1=1 OR NOT 1=1 AND 1=1 ");
				test.assertEquals("false", s);
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
				test.assertEquals("false", s);
			}
			public void testStringEqualsAssociativity(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" \"foo\" = \"fo\" || \"o\" ");
				test.assertEquals("true", s);
			}
			public void testStringEqualsEmpty(FoodMartTestCase test) {
				String s = test.executeBooleanExpr(" \"\" = \"\" ");
				test.assertEquals("true", s);
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
				test.assertEquals("true", s);
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
				test.assertEquals("true", s);
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
				test.assertEquals("false", s);
			}
			public void testNeInfinity(FoodMartTestCase test) {
				String s = test.executeBooleanExpr("(1 / 0) <> (1 / 0)");
				// Infinity does not equal itself
				test.assertEquals("false", s);
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
				test.assertEquals("false", s);
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
				test.assertEquals("true", s);
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
				test.assertEquals("false", s);
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
				test.assertEquals("false", s);
			}
		});
	}

	private Boolean toBoolean(boolean b) {
		return b ? Boolean.TRUE : Boolean.FALSE;
	}

	TestSuite createSuite() {
		TestSuite suite = new TestSuite("builtin functions");
		for (Iterator resolverses = upperName2Resolvers.values().iterator();
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

	void test() {
		String[] stmts = new String[]{
			"select CoalesceEmpty(1,2) from Sales",
			"select CoalesceEmpty(\"a\",\"b\") from Sales",
			"select <a dimension>.Dimension from Sales",
			"select <a hierarchy>.Dimension from Sales",
			"select <a level>.Dimension from Sales",
			"select <a member>.Dimension from Sales",
			"select Dimensions(1) from Sales",
			"select Dimensions(\"a\") from Sales",
			"select <a level>.Hierarchy from Sales",
			"select <a member>.Hierarchy from Sales",
			"select IIf(<b>, <n>, <n>) from Sales",
			"select IIf(<b>, <s>, <s>) from Sales",
			"select <tuple>.Item(<n>) from Sales",
			"select <set>.Item(\"a\", \"b\") from Sales",
			"select <set>.Item(<n>) from Sales",
			"select <a dimension>.Levels(1) from Sales",
			"select Levels(\"a\") from Sales",
			"select <a dimension>.Members from Sales",
			"select <a hierarchy>.Members from Sales",
			"select <a level>.Members from Sales",
			"select <a dimension>.Name from Sales",
			"select <a hierarchy>.Name from Sales",
			"select <a level>.Name from Sales",
			"select <a member>.Name from Sales",
			"select <a dimension>.UniqueName from Sales",
			"select <a hierarchy>.UniqueName from Sales",
			"select <a level>.UniqueName from Sales",
			"select <a member>.UniqueName from Sales",
		};
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
				int syntacticType, int returnType, int[] parameterTypes) {
			super(name, signature, description, syntacticType, returnType, parameterTypes);
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

}

// End BuiltinFunTable.java
