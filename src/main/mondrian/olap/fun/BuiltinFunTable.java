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
	 * Converts an argument to a parameter type
	 *
	 * @see #canConvert
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
				// "<Dimension>.DefaultMember.Hierarchy"
				return new FunCall(
						"Hierarchy", new Exp[]{
						new FunCall(
								"DefaultMember",
								new Exp[]{fromExp},
								FunDef.TypeProperty)},
						FunDef.TypeProperty);
			case Exp.CatLevel:
				// "<Dimension>.DefaultMember.Level"
				return new FunCall(
						"Level", new Exp[]{
						new FunCall(
								"DefaultMember",
								new Exp[]{fromExp},
								FunDef.TypeProperty)},
						FunDef.TypeProperty);
			case Exp.CatMember:
				// "<Dimension>.DefaultMember"
				return new FunCall("DefaultMember", new Exp[]{fromExp}, FunDef.TypeProperty);
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
			break;
		case 1:
			return matchDef;
		default:
			throw Util.newInternal(
					"more than one function matches signature '" + signature +
					"'");
		}

		throw Util.newInternal(
				"no function matches signature '" + signature + "'");
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
						"[Gender].[All Genders].[F].Hierarchy");
				test.assertEquals("[Gender]", s);
			}

			public void testFirstInLevel9(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Education Level].[All Education Levels].[Bachelors Degree].Hierarchy");
				test.assertEquals("[Education Level]", s);
			}

			public void testHierarchyAll(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Gender].[All Genders].Hierarchy");
				test.assertEquals("[Gender]", s);
			}

			public void testHierarchyNull(FoodMartTestCase test) {
				String s = test.executeExpr(
						"[Gender].[All Genders].Parent.Hierarchy");
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
						"member '[Gender].[All Genders].[M]' is not in the same hierarchy as level '[Store].[Store Country]'");
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
				Member[] cousins = uncle.getCube().getMemberChildren(
						new Member[] {uncle});
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
					siblings = member.getCube().getMemberChildren(
							new Member[]{parent});
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
						"Members '[Time].[1997]' and '[Gender].[All Genders].[M]' are not compatible as cousins");
			}
		});
		define(new FunDefBase("CurrentMember", "<Dimension>.CurrentMember", "Returns the current member along a dimension during an iteration.", "pmd"));
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
				Member[] children = evaluator.getCube().getMemberChildren(
						new Member[]{member});
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
						"[Gender].[All Genders].FirstChild");
				test.assertEquals("F", member.getName());
			}

			public void testFirstChildOfChildless(FoodMartTestCase test) {
				Member member = test.executeAxis(
						"[Gender].[All Genders].[F].FirstChild");
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
					children = evaluator.getCube().getMemberChildren(
							new Member[]{parent});
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
						"[Gender].[All Genders].FirstSibling");
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
						test.assertNull(member);
					}
				}));

		define(new FunDefBase("LastChild", "<Member>.LastChild", "Returns the last child of a member.", "pmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				Member[] children = evaluator.getCube().getMemberChildren(
						new Member[]{member});
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
						"[Gender].[All Genders].LastChild");
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
					children = evaluator.getCube().getMemberChildren(
							new Member[]{parent});
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
						"[Gender].[All Genders].LastSibling");
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

					public void testBasic3(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Time].[1997].[Q2].[4].Lead(4)");
						test.assertEquals("8", member.getName());
					}

					public void testFirstInLevel3(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Gender].[F].Lead(1)");
						test.assertEquals("M", member.getName());
					}

					public void testAll3(FoodMartTestCase test) {
						Member member = test.executeAxis(
								"[Time].[1997].[Q2].Lead(2)");
						test.assertEquals("Q4", member.getName());
					}
				}));

		define(new FunDefBase("Members", "Members( )", "Returns the member whose name is specified by a string expression.", "fmS"));

		define(new FunDefBase(
				"NextMember", "<Member>.NextMember", "Returns the next member in the level that contains a specified member.", "pmm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				return member.getLeadMember(+1);
			}

			public void testBasic2(TestCase test) {
				Result result = TestContext.instance().executeFoodMart(
						"select {[Gender].[F].NextMember} ON COLUMNS from Sales");
				test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("M"));
			}

			public void testFirstInLevel2(TestCase test) {
				Result result = TestContext.instance().executeFoodMart(
						"select {[Gender].[M].NextMember} ON COLUMNS from Sales");
				test.assertTrue(result.getAxes()[0].positions.length == 0);
			}

			public void testAll2(TestCase test) {
				Result result = TestContext.instance().executeFoodMart(
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

			public void testBasic5(TestCase test) {
				Result result = TestContext.instance().executeFoodMart(
						"select{ [Product].[All Products].[Drink].Parent} on columns from Sales");
				test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("All Products"));
			}

			public void testFirstInLevel5(TestCase test) {
				Result result = TestContext.instance().executeFoodMart(
						"select {[Time].[1997].[Q2].[4].Parent} on columns,{[Gender].[M]} on rows from Sales");
				test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Q2"));
			}

			public void testAll5(TestCase test) {
				Result result = TestContext.instance().executeFoodMart(
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

			public void testBasic(TestCase test) {
				Result result = TestContext.instance().executeFoodMart(
						"select {[Gender].[M].PrevMember} ON COLUMNS from Sales");
				test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("F"));
			}

			public void testFirstInLevel(TestCase test) {
				Result result = TestContext.instance().executeFoodMart(
						"select {[Gender].[F].PrevMember} ON COLUMNS from Sales");
				test.assertTrue(result.getAxes()[0].positions.length == 0);
			}

			public void testAll(TestCase test) {
				Result result = TestContext.instance().executeFoodMart(
						"select {[Gender].PrevMember} ON COLUMNS from Sales");
				// previous to [Gender].[All] is null, so no members are returned
				test.assertTrue(result.getAxes()[0].positions.length == 0);
			}
		});
		if (false) define(new FunDefBase("ValidMeasure", "ValidMeasure(<Tuple>)", "Returns a valid measure in a virtual cube by forcing inapplicable dimensions to their top level.", "fm*"));
		//
		// NUMERIC FUNCTIONS
		if (false) define(new FunDefBase("Aggregate", "Aggregate(<Set>[, <Numeric Expression>])", "Returns a calculated value using the appropriate aggregate function, based on the context of the query.", "fn*"));
		if (false) define(new FunDefBase("Avg", "Avg(<Set>[, <Numeric Expression>])", "Returns the average value of a numeric expression evaluated over a set.", "fn*"));
		if (false) define(new FunDefBase("Correlation", "Correlation(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Returns the correlation of two series evaluated over a set.", "fn*"));
		if (false) define(new FunDefBase("Count", "Count(<Set>[, EXCLUDEEMPTY | INCLUDEEMPTY])", "Returns the number of tuples in a set, empty cells included unless the optional EXCLUDEEMPTY flag is used.", "fn*"));
		if (false) define(new FunDefBase("Covariance", "Covariance(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Returns the covariance of two series evaluated over a set (biased).", "fn*"));
		if (false) define(new FunDefBase("CovarianceN", "CovarianceN(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Returns the covariance of two series evaluated over a set (unbiased).", "fn*"));
		define(new FunDefBase("IIf", "IIf(<Logical Expression>, <Numeric Expression1>, <Numeric Expression2>)", "Returns one of two numeric values determined by a logical test.", "fnbnn"));
		if (false) define(new FunDefBase("LinRegIntercept", "LinRegIntercept(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns the value of b in the regression line y = ax + b.", "fn*"));
		if (false) define(new FunDefBase("LinRegPoint", "LinRegPoint(<Numeric Expression>, <Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns the value of y in the regression line y = ax + b.", "fn*"));
		if (false) define(new FunDefBase("LinRegR2", "LinRegR2(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns R2 (the coefficient of determination).", "fn*"));
		if (false) define(new FunDefBase("LinRegSlope", "LinRegSlope(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns the value of a in the regression line y = ax + b.", "fn*"));
		if (false) define(new FunDefBase("LinRegVariance", "LinRegVariance(<Set>, <Numeric Expression>[, <Numeric Expression>])", "Calculates the linear regression of a set and returns the variance associated with the regression line y = ax + b.", "fn*"));
		if (false) define(new FunDefBase("Max", "Max(<Set>[, <Numeric Expression>])", "Returns the maximum value of a numeric expression evaluated over a set.", "fn*"));
		if (false) define(new FunDefBase("Median", "Median(<Set>[, <Numeric Expression>])", "Returns the median value of a numeric expression evaluated over a set.", "fn*"));
		if (false) define(new FunDefBase("Min", "Min(<Set>[, <Numeric Expression>])", "Returns the minimum value of a numeric expression evaluated over a set.", "fn*"));
		define(new FunDefBase("Ordinal", "<Level>.Ordinal", "Returns the zero-based ordinal value associated with a level.", "pnl"));
		if (false) define(new FunDefBase("Rank", "Rank(<Tuple>, <Set>)", "Returns the one-based rank of a tuple in a set.", "fn*"));
		if (false) define(new FunDefBase("Stddev", "Stddev(<Set>[, <Numeric Expression>])", "Alias for Stdev.", "fn*"));
		if (false) define(new FunDefBase("StddevP", "StddevP(<Set>[, <Numeric Expression>])", "Alias for StdevP.", "fn*"));
		if (false) define(new FunDefBase("Stdev", "Stdev(<Set>[, <Numeric Expression>])", "Returns the standard deviation of a numeric expression evaluated over a set (unbiased).", "fn*"));
		if (false) define(new FunDefBase("StdevP", "StdevP(<Set>[, <Numeric Expression>])", "Returns the standard deviation of a numeric expression evaluated over a set (biased).", "fn*"));
		define(new MultiResolver(
				"Sum", "Sum(<Set>[, <Numeric Expression>])", "Returns the sum of a numeric expression evaluated over a set.",
				new String[]{"fnx", "fnxN"},
				new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Vector members = (Vector) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
						return sum(evaluator.push(new Member[0]), members, exp);
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
		if (false) define(new FunDefBase("Var", "Var(<Set>[, <Numeric Expression>])", "Returns the variance of a numeric expression evaluated over a set (unbiased).", "fn*"));
		if (false) define(new FunDefBase("Variance", "Variance(<Set>[, <Numeric Expression>])", "Alias for Var.", "fn*"));
		if (false) define(new FunDefBase("VarianceP", "VarianceP(<Set>[, <Numeric Expression>])", "Alias for VarP.", "fn*"));
		if (false) define(new FunDefBase("VarP", "VarP(<Set>[, <Numeric Expression>])", "Returns the variance of a numeric expression evaluated over a set (biased).", "fn*"));
		//
		// SET FUNCTIONS
		if (false) define(new FunDefBase("AddCalculatedMembers", "AddCalculatedMembers(<Set>)", "Adds calculated members to a set.", "fx*"));
		if (false) define(new FunDefBase("BottomCount", "BottomCount(<Set>, <Count>[, <Numeric Expression>])", "Returns a specified number of items from the bottom of a set, optionally ordering the set first.", "fx*"));
		if (false) define(new FunDefBase("BottomPercent", "BottomPercent(<Set>, <Percentage>, <Numeric Expression>)", "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified percentage.", "fx*"));
		if (false) define(new FunDefBase("BottomSum", "BottomSum(<Set>, <Value>, <Numeric Expression>)", "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified value.", "fx*"));
		define(new FunDefBase("Children", "<Member>.Children", "Returns the children of a member.", "pxm") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Member member = getMemberArg(evaluator, args, 0, true);
				Member[] children =
						evaluator.getCube().getMemberChildren(
								new Member[]{member});
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
						Member[] children = {member};
						while (children.length > 0 &&
								children[0].getLevel().getDepth() <
								level.getDepth()) {
							children = evaluator.getCube().getMemberChildren(
									children);
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
								Member[] childMembers = {member};// = member.getLevel().getChildLevel().getMembers();
								childMembers = evaluator.getCube().getMemberChildren(childMembers);
								if (childMembers.length != 0) {
									for (int j = 0,p = childMembers.length; j < p; j++)
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
						Hashtable set2 = toHashtable(
								(Vector) getArg(evaluator, args, 1));
						Vector set1 = (Vector) getArg(evaluator, args, 0);
						Vector result = new Vector();
						for (int i = 0, count = set1.size(); i < count; i++) {
							Object o = set1.elementAt(i);
							if (set2.get(o) == null) {
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
				Evaluator evaluator2 = evaluator.push(new Member[0]);
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
			public void testFilterWithSilcer(FoodMartTestCase test) {
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
				test.assertEquals("30,114.00", cell.getFormattedValue());
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
						ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
						String order = (String) getArg(evaluator, args, 2, "ASC");
						sort(
								evaluator, members, exp,
								order.equals("DESC") || order.equals("BDESC"),
								order.equals("BASC") || order.equals("BDESC"));
						return members;
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
						HashSet set1 = toHashSet(v1);
						Vector result = new Vector();
						int i = 0, n = v0.size();
						while (i < n) {
							Member m = (Member) v0.elementAt(i++);
							result.addElement(m);
							if (!set1.contains(m)) {
								continue;
							}
							boolean isDrilledDown = false;
							if (i < n) {
								Member next = (Member) v0.elementAt(i);
								boolean strict = true;
								if (isAncestorOf(m, next, strict)) {
									isDrilledDown = true;
								}
							}
							if (isDrilledDown) {
								// skip descendants of this member
								while (i < n) {
									Member next = (Member) v0.elementAt(i++);
									boolean strict = true;
									if (!isAncestorOf(m, next, strict)) {
										break;
									}
								}
							} else {
								Member[] children =
										evaluator.getCube().getMemberChildren(
												new Member[]{m});
								for (int j = 0; j < children.length; j++) {
									result.addElement(children[j]);
								}
							}
						}
						return result;
					}
					public void testToggleDrillState(FoodMartTestCase test) {
						Axis axis = test.executeAxis2(
								"ToggleDrillState({[Customers].[USA],[Customers].[Canada]},{[Customers].[USA],[Customers].[USA].[CA]})");
						test.assertEquals("[Customers].[All Customers].[USA], [Customers].[All Customers].[USA].[CA], [Customers].[All Customers].[USA].[OR], [Customers].[All Customers].[USA].[WA], [Customers].[All Customers].[Canada]", toString(axis.positions));
					}
					public void testToggleDrillState2(FoodMartTestCase test) {
						Axis axis = test.executeAxis2(
								"ToggleDrillState([Product].[Product Department].members, {[Product].[All Products].[Food].[Snack Foods]})");
						test.assertEquals("foo",toString(axis.positions));
					}
					private String toString(Position[] positions) {
						StringBuffer sb = new StringBuffer();
						for (int i = 0; i < positions.length; i++) {
							Position position = positions[i];
							if (i > 0) {
								sb.append(", ");
							}
							if (position.members.length != 1) {
								sb.append("{");
							}
							for (int j = 0; j < position.members.length; j++) {
								Member member = position.members[j];
								if (j > 0) {
									sb.append(", ");
								}
								sb.append(member.getUniqueName());
							}
							if (position.members.length != 1) {
								sb.append("}");
							}
						}
						return sb.toString();
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
				}));
		if (false) define(new FunDefBase("TopPercent", "TopPercent(<Set>, <Percentage>, <Numeric Expression>)", "Sorts a set and returns the top N elements whose cumulative total is at least a specified percentage.", "fx*"));
		if (false) define(new FunDefBase("TopSum", "TopSum(<Set>, <Value>, <Numeric Expression>)", "Sorts a set and returns the top N elements whose cumulative total is at least a specified value.", "fx*"));
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
		define(new FunDefBase("Name", "<Member>.Name", "Returns the name of a member.", "pSm"));
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
				test.assertEquals("[Gender].[All Genders]", s);
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
		});
		define(new FunDefBase("-", "<Numeric Expression> - <Numeric Expression>", "Subtracts two numbers.", "innn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				return new Double(o0.doubleValue() - o1.doubleValue());
			}
		});
		define(new FunDefBase("*", "<Numeric Expression> * <Numeric Expression>", "Multiplies two numbers.", "innn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				return new Double(o0.doubleValue() * o1.doubleValue());
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
		});
		define(new FunDefBase("-", "- <Numeric Expression>", "Returns the negative of a number.", "Pnn"));
		define(new FunDefBase("||", "<String Expression> || <String Expression>", "Concatenates two strings.", "iSSS"));
		define(new FunDefBase("AND", "<Logical Expression> AND <Logical Expression>", "Returns the conjunction of two conditions.", "ibbb"));
		define(new FunDefBase("OR", "<Logical Expression> OR <Logical Expression>", "Returns the disjunction of two conditions.", "ibbb"));
		define(new FunDefBase("XOR", "<Logical Expression> XOR <Logical Expression>", "Returns whether two conditions are mutually exclusive.", "ibbb"));
		define(new FunDefBase("NOT", "NOT <Logical Expression>", "Returns the negation of a condition.", "Pbb"));
		define(new FunDefBase("=", "<Numeric Expression> = <Numeric Expression>", "Returns whether two expressions are equal.", "ibnn"));
		define(new FunDefBase("<>", "<Numeric Expression> <> <Numeric Expression>", "Returns whether two expressions are not equal.", "ibnn"));
		define(new FunDefBase("<", "<Numeric Expression> < <Numeric Expression>", "Returns whether an expression is less than another.", "ibnn"));
		define(new FunDefBase("<=", "<Numeric Expression> <= <Numeric Expression>", "Returns whether an expression is less than or equal to another.", "ibnn"));
		define(new FunDefBase(">", "<Numeric Expression> > <Numeric Expression>", "Returns whether an expression is greater than another.", "ibnn") {
			public Object evaluate(Evaluator evaluator, Exp[] args) {
				Double o0 = getDoubleArg(evaluator, args, 0),
						o1 = getDoubleArg(evaluator, args, 1);
				return o0.doubleValue() > o1.doubleValue() ?
						Boolean.TRUE :
						Boolean.FALSE;
			}
		});
		define(new FunDefBase(">=", "<Numeric Expression> >= <Numeric Expression>", "Returns whether an expression is greater than or equal to another.", "ibnn"));
	}

	public TestSuite suite() {
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
}

// End BuiltinFunTable.java
