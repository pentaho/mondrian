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

import junit.framework.TestSuite;
import junit.framework.TestCase;
import mondrian.olap.*;
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
	private void init()
	{
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
		case 'p': return FunDef.TypeProperty;
		case 'f': return FunDef.TypeFunction;
		case 'm': return FunDef.TypeMethod;
		case 'i': return FunDef.TypeInfix;
		case 'P': return FunDef.TypePrefix;
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
		case 'a': return Exp.CatArray;
		case 'd': return Exp.CatDimension;
		case 'h': return Exp.CatHierarchy;
		case 'l': return Exp.CatLevel;
		case 'b': return Exp.CatLogical;
		case 'm': return Exp.CatMember;
		case 'n': return Exp.CatNumeric;
		case 's': return Exp.CatSet;
		case 'S': return Exp.CatString;
		case 't': return Exp.CatTuple;
		case 'v': return Exp.CatValue;
		case 'y': return Exp.CatSymbol;
		default:
			throw Util.newInternal(
				"unknown type code '" + c + "' in string '" + flags + "'");
		}
	}

	/**
	 * Returns whether we can convert an argument to a parameter tyoe.
	 * @param from argument type
	 * @param to   parameter type
	 * @param conversionCount in/out count of number of conversions performed;
	 *             is incremented if the conversion is non-trivial (for
	 *             example, converting a member to a level).
	 */
	static boolean canConvert(Exp fromExp, int to, int[] conversionCount)
	{
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
			} else if (to == Exp.CatValue) {
				return true;
			} else {
				return false;
			}
		case Exp.CatNumeric:
			return to == Exp.CatValue;
		case Exp.CatSet:
			return false;
		case Exp.CatString:
			return to == Exp.CatValue;
		case Exp.CatTuple:
			return to == Exp.CatValue;
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
	public FunDef getDef(FunCall call)
	{
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
	protected void defineFunctions()
	{
		// first char: p=Property, m=Method, i=Infix, P=Prefix
		// 2nd:

		// ARRAY FUNCTIONS
		if (false) define(new FunDefBase("SetToArray", "SetToArray(<Set>[, <Set>]...[, <Numeric Expression>])", "Converts one or more sets to an array for use in a user-if (false) defined function.", "fa*"));
		//
		// DIMENSION FUNCTIONS
        define(new FunDefBase("Dimension", "<Hierarchy>.Dimension", "Returns the dimension that contains a specified hierarchy.","pdh"){
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
                return   hierarchy.getDimension();
            }
        }
        );
        //??Had to add this to get <Hierarchy>.Dimension to work?
        define(new FunDefBase("Dimension", "<Dimension>.Dimension", "Returns the dimension that contains a specified hierarchy.", "pdd") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
                return   hierarchy.getDimension();
            }
        });

		define(new FunDefBase("Dimension", "<Level>.Dimension", "Returns the dimension that contains a specified level.", "pdl") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
               	Level level = getLevelArg(args, 0, true);
                return   level.getDimension();
            }
        });

		define(new FunDefBase("Dimension", "<Member>.Dimension", "Returns the dimension that contains a specified member.", "pdm"){
            public Object evaluate(Evaluator evaluator, Exp[] args) {
               	Member member = getMemberArg(evaluator,args, 0, true);
                return   member.getDimension();
            }
        });

		define(new FunDefBase("Dimensions", "Dimensions(<Numeric Expression>)", "Returns the dimension whose zero-based position within the cube is specified by a numeric expression.", "fdn") {
                public Object evaluate(Evaluator evaluator, Exp[] args) {
                    Cube cube = evaluator.getCube();
                    Dimension [] dimensions = cube.getDimensions();
                    int n = getIntArg(evaluator, args, 0);
                    if ((n > dimensions.length) || (n < 1))
                    {
                    	throw Util.newEvalException(
				        "Numerical argument must be between 1 and '" + dimensions.length +
				        "'");
                    }

                    return dimensions[n - 1];
                }
        });
		define(new FunDefBase("Dimensions", "Dimensions(<String Expression>)", "Returns the dimension whose name is specified by a string.", "fdS"){
            public Object evaluate(Evaluator evaluator, Exp[] args)  {
                String defValue = "Default Value";
                String s = getStringArg(evaluator, args, 0,defValue);
             if(s.indexOf("[") == -1)
                    s = Util.quoteMdxIdentifier(s);
                Cube cube = evaluator.getCube();
                boolean fail = false;
                OlapElement o = Util.lookupCompound(cube, s, cube, fail);
                if (o == null) {
                    throw Util.newEvalException("Dimensions '" + s + "' not found");
                } else if (o instanceof Dimension) {
                    return (Dimension) o;
                } else {
                    throw Util.newEvalException("Dimensions(" + s + ") found " + o);
             }
         }

        });

		//
		// HIERARCHY FUNCTIONS
		define(new FunDefBase("Hierarchy", "<Level>.Hierarchy", "Returns a level's hierarchy.", "phl") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Level level = getLevelArg(args, 0, true);
                return   level.getHierarchy();
            }
        });
		define(new FunDefBase("Hierarchy", "<Member>.Hierarchy", "Returns a member's hierarchy.", "phm") {
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                return   member.getHierarchy();
            }

            public void testTime(TestCase test) {
                Result result = TestContext.instance().executeFoodMart(
                        "with member [Measures].[Foo] as" +
                        " '[Time].[1997].[Q1].[1].Hierarchy.UniqueName'\r\n"+
                        "select {[Measures].[Foo]} on columns,\r\n" +
                        " {[Gender].[M]} on rows \r\n" +
                        "from Sales");
                Cell cell = result.getCell(new int[]{0,0});
                test.assertEquals("[Time]", cell.getFormattedValue());
            }
            public void testBasic9(TestCase test) {
                Result result = TestContext.instance().executeFoodMart(
                        "select {[Gender].[All Genders].[F].Hierarchy} ON COLUMNS from Sales");
                test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Gender"));

            }
            public void testFirstInLevel9(TestCase test) {
                Result result = TestContext.instance().executeFoodMart(
                        "select {[Education Level].[All Education Levels].[Bachelors Degree].Hierarchy} on columns,{[Gender].[M]} on rows from Sales");
                test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Education Level"));
            }
            public void testAll9(TestCase test) {
                Result result = TestContext.instance().executeFoodMart(
                        "select {[Time].[1997].[Q1].Hierarchy} on columns,{[Gender].[M]} on rows from Sales");
                // previous to [Gender].[All] is null, so no members are returned
                test.assertTrue(result.getAxes()[0].positions[1].members[0].getName().equals("Bachelors Degree"));
            }
        });

		//
		// LEVEL FUNCTIONS
		define(new FunDefBase("Level", "<Member>.Level", "Returns a member's level.", "plm"){
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator, args, 0, true);
                return   member.getLevel();
            }
        });

		define(new FunDefBase("Levels", "<Hierarchy>.Levels(<Numeric Expression>)", "Returns the level whose position in a hierarchy is specified by a numeric expression.", "mlhn") {
                public Object evaluate(Evaluator evaluator, Exp[] args) {
                    Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
                    Level[] levels = hierarchy.getLevels();

                    int n = getIntArg(evaluator, args, 1);
                    if ((n > levels.length) || (n < 1))
                    {
                    	throw Util.newEvalException(
				        "Numerical argument must be between 1 and '" + levels.length +
				        "'");
                    }

                    return levels[n - 1];
                }
				public void test(TestCase test) {
                    Result result = TestContext.instance().executeFoodMart(
						"with member [Measures].[Foo] as" +
						" '[Time].Levels(2)' \r\n" +
                        "select {[Measures].[Foo]} on columns,\r\n" +
                        " {[Gender].[M]} on rows \r\n" +
                        "from Sales");
                   Cell cell = result.getCell(new int[]{0,0});
                   test.assertEquals(
					   "[Time].[Quarter]", cell.getFormattedValue());
                }
			});

        define(new FunDefBase("Levels", "Levels(<String Expression>)", "Returns the level whose name is specified by a string expression.", "flS"){
            public Object evaluate(Evaluator evaluator, Exp[] args)  {
                String defValue = "Default Value";
                String s = getStringArg(evaluator, args, 0,defValue);
             if(s.indexOf("[") == -1)
                    s = Util.quoteForMdx(s);
                Cube cube = evaluator.getCube();
                boolean fail = false;
                OlapElement o = Util.lookupCompound(cube, s, cube, fail);
                if (o == null) {
                    throw Util.newEvalException("level '" + s + "' not found");
                } else if (o instanceof Level) {
                    return (Level) o;
                } else {
                    throw Util.newEvalException("Levels(" + s + ") found " + o);
             }
         }

        });

		//
		// LOGICAL FUNCTIONS
		define(new FunDefBase("IsEmpty", "IsEmpty(<Value Expression>)", "Determines if an expression evaluates to the empty cell value.", "fbS"));

		define(new FunDefBase("IsEmpty", "IsEmpty(<Value Expression>)", "Determines if an expression evaluates to the empty cell value.", "fbn"));
		//
		// MEMBER FUNCTIONS
        //	if (false) define(new FunDefBase("Ancestor", "Ancestor(<Member>, <Level>)", "Returns the ancestor of a member at a specified level.", "fm*");


        define(new FunDefBase("Ancestor", "Ancestor(<Member>, <Level>)", "Returns the ancestor of a member at a specified level.", "fmml"){
            public Object evaluate(Evaluator evaluator, Exp[] args) {
                Member member = getMemberArg(evaluator,args, 0,false);
                Level level = getLevelArg(args,1,false);
                Member[] members = member.getAncestorMembers();
                if (member.getLevel().equals(level))
                     return member;
                for(int i = 0; i < members.length; i++)
                {
                      if(members[i].getLevel().equals(level))
                        return members[i];

                }
                return members[0]; //what to return if not found?
            }
        });

        define(new MultiResolver(
                "ClosingPeriod", "ClosingPeriod([<Level>[, <Member>]])", "Returns the last sibling among the descendants of a member at a level.",
                new String []  {"fm","fmm","fml","fmlm"},
                new FunkBase(){
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        Level level = null;
                        Member member = null;
                        //level = (Level)getArg(evaluator, args,0) ;
                        if(ExpBase.getTypes(args)[0] == Exp.CatLevel )
                            level = getLevelArg(args, 0, false);
                        else if(ExpBase.getTypes(args)[0] == Exp.CatMember )
                            member = getMemberArg(evaluator, args, 0, false);
                        if(ExpBase.getTypes(args).length == 2 )
                            member = getMemberArg(evaluator, args, 1, false);

                        if (level == null)   //member is only arg
                            level = member.getLevel().getChildLevel();
                        if (member == null)    //level is only arg
                            member = level.getMembers()[level.getMembers().length - 1];
                       // if (member == null && level == null)
                         //   member = evaluator.getCube().
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
                       return toVector(children).lastElement();

                    }
        }));

		define( new MultiResolver ("Cousin", "Cousin(<Member1>, <Member2>)", "Returns the member with the same relative position under a member as the member specified.",
               new String[] {"fmmm"},
                new FunkBase()
                {
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        Member member1 = getMemberArg(evaluator, args, 0, false);
                        Member member2 = getMemberArg(evaluator, args, 1, false);

                        int depthLevel = member1.getLevel().getDepth();
                        Member parentMember1 = member1.getAncestorMembers()[0];

                        if(parentMember1.getLevel().equals(member2.getLevel()))
                        {
                            Member[] cousins =   member1.getLevel().getMembers();
                            int memberDepth = 0;
                            for (int i = 0; i < cousins.length; i++)
                            {
                                memberDepth = i;
                                if (cousins[i].equals(member1))
                                    break;
                            }
                            Member[] cousinMembers = member2.getHierarchy().getLevels()[depthLevel].getMembers();
                            if(memberDepth < cousinMembers.length)
                                return cousinMembers[memberDepth] ;
                            else
                                throw Util.newEvalException(
                                        "A cousin member does not exist in '" + member2.toString() +
                                        "'");
                        }
                        else

                            throw Util.newEvalException(
                                    "These two members are not compatible as cousins '" + member1 +" " + member2 +
                                    "'");
                    }
                }
        ));
		define(new FunDefBase("CurrentMember", "<Dimension>.CurrentMember", "Returns the current member along a dimension during an iteration.", "pmd"));
		define(new FunDefBase("DefaultMember", "<Dimension>.DefaultMember", "Returns the default member of a dimension.", "pm") {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Hierarchy hierarchy = getHierarchyArg(evaluator, args, 0, true);
					return hierarchy.getDefaultMember();
				}
			});

		define(new FunDefBase("FirstChild", "<Member>.FirstChild", "Returns the first child of a member.", "pmm") {

				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Member member = getMemberArg(evaluator, args, 0, true);
					Member[] children =
						evaluator.getCube().getMemberChildren(
							new Member[]{member});
					return toVector(children).firstElement();

				}
				public void testBasic7(TestCase test) {
					Result result = TestContext.instance().executeFoodMart(
						"select {[Gender].FirstChild} ON COLUMNS from Sales");
					test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("F"));
				}
				public void testFirstInLevel7(TestCase test) {
					Result result = TestContext.instance().executeFoodMart(
						"select {[Time].[1997].[Q4].FirstChild} on columns,{[Gender].[M]} on rows from Sales");
					test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("10"));
				}
				public void testAll7(TestCase test) {
					Result result = TestContext.instance().executeFoodMart(
						"select {[Time].[1997].FirstChild} on columns,{[Gender].[M]} on rows from Sales");
					// previous to [Gender].[All] is null, so no members are returned
					test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Q1"));
				}
			});

		define(new FunDefBase("FirstSibling", "<Member>.FirstSibling", "Returns the first child of the parent of a member.", "pmm") {

				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Member member = getMemberArg(evaluator, args, 0, true);
					Member[] children =
						evaluator.getCube().getMemberChildren(
							new Member[]{member.getParentMember()});
					return toVector(children).firstElement();

				}
				public void testBasic8(TestCase test) {
					Result result = TestContext.instance().executeFoodMart(
						"select {[Gender].[M].FirstSibling} ON COLUMNS from Sales");
					test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("F"));
				}
				public void testFirstInLevel8(TestCase test) {
					Result result = TestContext.instance().executeFoodMart(
						"select {[Time].[1997].[Q4].FirstSibling} on columns,{[Gender].[M]} on rows from Sales");
					test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Q1"));
				}
				public void testAll8(TestCase test) {
					Result result = TestContext.instance().executeFoodMart(
						"select {[Time].[1997].[Q4].[12].FirstSibling} on columns,{[Gender].[M]} on rows from Sales");
					// previous to [Gender].[All] is null, so no members are returned
					test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("10"));
				}
			});

		if (false) define(new FunDefBase("Item", "<Tuple>.Item(<Numeric Expression>)", "Returns a member from a tuple.", "mm*"));

		define(new MultiResolver(
			"Lag", "<Member>.Lag(<Numeric Expression>)", "Returns a member further along the specified member's dimension.",
			new String[]{"mmmn"},
			new FunkBase(){
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Member member = getMemberArg(evaluator, args, 0, true);
						int n = getIntArg(evaluator, args, 1);
						return member.getLeadMember(-n);
					}


					public void testBasic3(TestCase test) {
						Result result = TestContext.instance().executeFoodMart(
							"select {[Time].[1997].[Q4].[12].Lag(4)} on columns,{[Gender].[M]} on rows from Sales");
						test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("8"));
					}
					public void testFirstInLevel3(TestCase test) {
						Result result = TestContext.instance().executeFoodMart(
							"select {[Gender].[M].Lag(1)} ON COLUMNS from Sales");
						test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("F"));
					}
					public void testAll3(TestCase test) {
						Result result = TestContext.instance().executeFoodMart(
							"select {[Time].[1997].[Q4].Lag(2)} on columns,{[Gender].[M]} on rows from Sales");
						// previous to [Gender].[All] is null, so no members are returned
						test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Q2"));
					}
				}));

		define(new FunDefBase("LastChild", "<Member>.LastChild", "Returns the last child of a member.", "pmm"){

				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Member member = getMemberArg(evaluator, args, 0, true);
					Member[] children =
						evaluator.getCube().getMemberChildren(
							new Member[]{member});
					return toVector(children).lastElement();

				}
				public void testBasic6(TestCase test) {
					Result result = TestContext.instance().executeFoodMart(
						"select {[Gender].LastChild} ON COLUMNS from Sales");
					test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("M"));
				}
				public void testFirstInLevel6(TestCase test) {
					Result result = TestContext.instance().executeFoodMart(
						"select {[Time].[1997].[Q4].LastChild} on columns,{[Gender].[M]} on rows from Sales");
					test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("12"));
				}
				public void testAll6(TestCase test) {
					Result result = TestContext.instance().executeFoodMart(
						"select {[Time].[1997].LastChild} on columns,{[Gender].[M]} on rows from Sales");
					// previous to [Gender].[All] is null, so no members are returned
					test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Q4"));
				}
			});

		define(new FunDefBase("LastSibling", "<Member>.LastSibling", "Returns the last child of the parent of a member.", "pmm") {

				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Member member = getMemberArg(evaluator, args, 0, true);
					Member[] children =
						evaluator.getCube().getMemberChildren(
							new Member[]{member.getParentMember()});
					return toVector(children).lastElement();

				}
				public void testBasic8(TestCase test) {
					Result result = TestContext.instance().executeFoodMart(
						"select {[Gender].[F].LastSibling} ON COLUMNS from Sales");
					test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("M"));
				}
				public void testFirstInLevel8(TestCase test) {
					Result result = TestContext.instance().executeFoodMart(
						"select {[Time].[1997].[Q1].LastSibling} on columns,{[Gender].[M]} on rows from Sales");
					test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Q4"));
				}
				public void testAll8(TestCase test) {
					Result result = TestContext.instance().executeFoodMart(
						"select {[Time].[1997].[Q4].[10].LastSibling} on columns,{[Gender].[M]} on rows from Sales");
					// previous to [Gender].[All] is null, so no members are returned
					test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("12"));
				}
			});

        define(new MultiResolver(
			"Lead", "<Member>.Lead(<Numeric Expression>)", "Returns a member further along the specified member's dimension.",
			new String[]{"mmmn"},
			new FunkBase(){
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Member member = getMemberArg(evaluator, args, 0, true);
						int n = getIntArg(evaluator, args, 1);
						return member.getLeadMember(n);
					}


					public void testBasic3(TestCase test) {
						Result result = TestContext.instance().executeFoodMart(
							"select {[Time].[1997].[Q2].[4].Lead(4)} on columns,{[Gender].[M]} on rows from Sales");
						test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("8"));
					}
					public void testFirstInLevel3(TestCase test) {
						Result result = TestContext.instance().executeFoodMart(
							"select {[Gender].[F].Lead(1)} ON COLUMNS from Sales");
						test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("M"));
					}
					public void testAll3(TestCase test) {
						Result result = TestContext.instance().executeFoodMart(
							"select {[Time].[1997].[Q2].Lead(2)} on columns,{[Gender].[M]} on rows from Sales");
						// previous to [Gender].[All] is null, so no members are returned
						test.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Q4"));
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

		define(new FunDefBase("Parent", "<Member>.Parent", "Returns the parent of a member.", "pmm"){
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Member member = getMemberArg(evaluator, args, 0, true);

					return member.getParentMember();
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
			new String[] {"fns", "fnsn"},
			new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Vector members = (Vector) getArg(evaluator, args, 0);
						ExpBase exp = (ExpBase) getArg(evaluator, args, 1);
						return sum(evaluator.push(new Member[0]), members, exp);
					}
				}));
		define(new FunDefBase("Value", "<Measure>.Value", "Returns the value of a measure.", "pnm"));
		if (false) define(new FunDefBase("Var", "Var(<Set>[, <Numeric Expression>])", "Returns the variance of a numeric expression evaluated over a set (unbiased).", "fn*"));
		if (false) define(new FunDefBase("Variance", "Variance(<Set>[, <Numeric Expression>])", "Alias for Var.", "fn*"));
		if (false) define(new FunDefBase("VarianceP", "VarianceP(<Set>[, <Numeric Expression>])", "Alias for VarP.", "fn*"));
		if (false) define(new FunDefBase("VarP", "VarP(<Set>[, <Numeric Expression>])", "Returns the variance of a numeric expression evaluated over a set (biased).", "fn*"));
		//
		// SET FUNCTIONS
		if (false) define(new FunDefBase("AddCalculatedMembers", "AddCalculatedMembers(<Set>)", "Adds calculated members to a set.", "fs*"));
		if (false) define(new FunDefBase("BottomCount", "BottomCount(<Set>, <Count>[, <Numeric Expression>])", "Returns a specified number of items from the bottom of a set, optionally ordering the set first.", "fs*"));
		if (false) define(new FunDefBase("BottomPercent", "BottomPercent(<Set>, <Percentage>, <Numeric Expression>)", "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified percentage.", "fs*"));
		if (false) define(new FunDefBase("BottomSum", "BottomSum(<Set>, <Value>, <Numeric Expression>)", "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified value.", "fs*"));
		define(new FunDefBase("Children", "<Member>.Children", "Returns the children of a member.", "psm") {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Member member = getMemberArg(evaluator, args, 0, true);
					Member[] children =
						evaluator.getCube().getMemberChildren(
							new Member[]{member});
					return toVector(children);
				}
			});
		define(new FunDefBase("Crossjoin", "Crossjoin(<Set1>, <Set2>)", "Returns the cross product of two sets.", "fsss") {
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
						Object o0 = set0.elementAt(i);
						for (int j = 0, n = set1.size(); j < n; j++) {
							Object o1 = set1.elementAt(j);
							result.addElement(new Object[] {o0, o1});
						}
					}
					return result;
				}
			});
		define(new MultiResolver(
			"Descendants", "Descendants(<Member>, <Level>[, <Desc_flag>])", "Returns the set of descendants of a member at a specified level, optionally including or excluding descendants in other levels.",
			new String[] {"fsml", "fsmls"},
			new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Member member = getMemberArg(evaluator, args, 0, true);
						Level level = getLevelArg(args, 1, true);
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
		if (false) define(new FunDefBase("Distinct", "Distinct(<Set>)", "Eliminates duplicate tuples from a set.", "fs*"));

        define(new MultiResolver("DrilldownLevel", "DrilldownLevel(<Set>[, <Level>]) or DrilldownLevel(<Set>, , <Index>)", "Drills down the members of a set, at a specified level, to one level below. Alternatively, drills down on a specified dimension in the set.",
                new String [] {"fss","fssl"},
                new FunkBase(){
                    public Object evaluate(Evaluator evaluator, Exp[] args) {
                        //todo add fssl functionality
                        Vector set0 = (Vector) getArg(evaluator, args, 0);
                        int[] depthArray = new int[set0.size()];
                        Vector drilledSet = new Vector();

                        for (int i = 0, m = set0.size(); i < m; i++) {
                            Member member = (Member)set0.elementAt(i);
                            depthArray[i] = member.getDepth();
                            // Object o0 = set0.elementAt(i);
                            //   depthVector.addElement(new Object[] {o0});
                        }
                        Arrays.sort(depthArray);
                        int maxDepth = depthArray[depthArray.length - 1];

                        for (int i = 0, m = set0.size(); i < m; i++) {
                            Member member = (Member)set0.elementAt(i);
                            drilledSet.addElement( member);
                            if(member.getDepth() == maxDepth)
                            {
                                Member [] childMembers = {member};// = member.getLevel().getChildLevel().getMembers();
                                childMembers = evaluator.getCube().getMemberChildren(childMembers);
                                if(childMembers.length != 0)
                                {
                                    for(int j = 0,p = childMembers.length; j < p; j++)
                                        drilledSet.addElement(childMembers[j]);
                                }
                            }
                        }


                        return drilledSet;

                    }
                }
        ));

		if (false) define(new FunDefBase("DrilldownLevelBottom", "DrilldownLevelBottom(<Set>, <Count>[, [<Level>][, <Numeric Expression>]])", "Drills down the bottom N members of a set, at a specified level, to one level below.", "fs*"));
		if (false) define(new FunDefBase("DrilldownLevelTop", "DrilldownLevelTop(<Set>, <Count>[, [<Level>][, <Numeric Expression>]])", "Drills down the top N members of a set, at a specified level, to one level below.", "fs*"));
		if (false) define(new FunDefBase("DrilldownMember", "DrilldownMember(<Set1>, <Set2>[, RECURSIVE])", "Drills down the members in a set that are present in a second specified set.", "fs*"));
		if (false) define(new FunDefBase("DrilldownMemberBottom", "DrilldownMemberBottom(<Set1>, <Set2>, <Count>[, [<Numeric Expression>][, RECURSIVE]])", "Like DrilldownMember except that it includes only the bottom N children.", "fs*"));
		if (false) define(new FunDefBase("DrilldownMemberTop", "DrilldownMemberTop(<Set1>, <Set2>, <Count>[, [<Numeric Expression>][, RECURSIVE]])", "Like DrilldownMember except that it includes only the top N children.", "fs*"));
		if (false) define(new FunDefBase("DrillupLevel", "DrillupLevel(<Set>[, <Level>])", "Drills up the members of a set that are below a specified level.", "fs*"));
		if (false) define(new FunDefBase("DrillupMember", "DrillupMember(<Set1>, <Set2>)", "Drills up the members in a set that are present in a second specified set.", "fs*"));
		define(new MultiResolver(
			"Except", "Except(<Set1>, <Set2>[, ALL])", "Finds the difference between two sets, optionally retaining duplicates.",
			new String[] {"fsss", "fsssS"},
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
		if (false) define(new FunDefBase("Extract", "Extract(<Set>, <Dimension>[, <Dimension>...])", "Returns a set of tuples from extracted dimension elements. The opposite of Crossjoin.", "fs*"));
		if (false) define(new FunDefBase("Filter", "Filter(<Set>, <Search Condition>)", "Returns the set resulting from filtering a set based on a search condition.", "fs*"));
		if (false) define(new FunDefBase("Generate", "Generate(<Set1>, <Set2>[, ALL])", "Applies a set to each member of another set and joins the resulting sets by union.", "fs*"));
		if (false) define(new FunDefBase("Head", "Head(<Set>[, < Numeric Expression >])", "Returns the first specified number of elements in a set.", "fs*"));
		if (false) define(new FunDefBase("Hierarchize", "Hierarchize(<Set>)", "Orders the members of a set in a hierarchy.", "fs*"));
		if (false) define(new FunDefBase("Intersect", "Intersect(<Set1>, <Set2>[, ALL])", "Returns the intersection of two input sets, optionally retaining duplicates.", "fs*"));
		if (false) define(new FunDefBase("LastPeriods", "LastPeriods(<Index>[, <Member>])", "Returns a set of members prior to and including a specified member.", "fs*"));
		define(new FunDefBase("Members", "<Dimension>.Members", "Returns the set of all members in a dimension.", "psd") {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Dimension dimension = (Dimension) getArg(evaluator, args, 0);
					Hierarchy hierarchy = dimension.getHierarchy();
					return addMembers(new Vector(), hierarchy);
				}
			});
		define(new FunDefBase("Members", "<Hierarchy>.Members", "Returns the set of all members in a hierarchy.", "psh") {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Hierarchy hierarchy =
						(Hierarchy) getArg(evaluator, args, 0);
					return addMembers(new Vector(), hierarchy);
				}
			});
		define(new FunDefBase("Members", "<Level>.Members", "Returns the set of all members in a level.", "psl") {
				public Object evaluate(Evaluator evaluator, Exp[] args) {
					Level level = (Level) getArg(evaluator, args, 0);
					return toVector(level.getMembers());
				}
			});
		define(new MultiResolver(
			"Mtd", "Mtd([<Member>])", "A shortcut function for the PeriodsToDate function that specifies the level to be Month.",
			new String[] {"fs", "fsm"},
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
			new String[] {"fssvy", "fssv"},
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
			new String[] {"fs", "fsl", "fslm"},
			new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						Level level = getLevelArg(args, 0, false);
						Member member = getMemberArg(evaluator, args, 1, false);
						return periodsToDate(evaluator, level, member);
					}
				}));
		define(new MultiResolver(
			"Qtd", "Qtd([<Member>])", "A shortcut function for the PeriodsToDate function that specifies the level to be Quarter.",
			new String[] {"fs", "fsm"},
			new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						return periodsToDate(
							evaluator,
							evaluator.getCube().getQuarterLevel(),
							getMemberArg(evaluator, args, 0, false));
					}
				}));
		if (false) define(new FunDefBase("StripCalculatedMembers", "StripCalculatedMembers(<Set>)", "Removes calculated members from a set.", "fs*"));
		define(new FunDefBase("StrToSet", "StrToSet(<String Expression>)", "Constructs a set from a string expression.", "fsS") {
				public void unparse(Exp[] args, PrintWriter pw, ElementCallback callback) {
					if (callback.isPlatoMdx()) {
						// omit extra args (they're for us, not Plato)
						super.unparse(new Exp[] {args[0]}, pw, callback);
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
		if (false) define(new FunDefBase("Subset", "Subset(<Set>, <Start>[, <Count>])", "Returns a subset of elements from a set.", "fs*"));
		if (false) define(new FunDefBase("Tail", "Tail(<Set>[, <Count>])", "Returns a subset from the end of a set.", "fs*"));
		define(new MultiResolver(
			"ToggleDrillState", "ToggleDrillState(<Set1>, <Set2>[, RECURSIVE])", "Toggles the drill state of members. This function is a combination of DrillupMember and DrilldownMember.",
			new String[] {"fss", "fssS"},
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
										new Member[] {m});
								for (int j = 0; j < children.length; j++) {
									result.addElement(children[j]);
								}
							}
						}
						return result;
					}
				}));
		define(new MultiResolver(
			"TopCount",
			"TopCount(<Set>, <Count>[, <Numeric Expression>])",
			"Returns a specified number of items from the top of a set, optionally ordering the set first.",
			new String[] {"fssnn", "fssn"},
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
		if (false) define(new FunDefBase("TopPercent", "TopPercent(<Set>, <Percentage>, <Numeric Expression>)", "Sorts a set and returns the top N elements whose cumulative total is at least a specified percentage.", "fs*"));
		if (false) define(new FunDefBase("TopSum", "TopSum(<Set>, <Value>, <Numeric Expression>)", "Sorts a set and returns the top N elements whose cumulative total is at least a specified value.", "fs*"));
		if (false) define(new FunDefBase("Union", "Union(<Set1>, <Set2>[, ALL])", "Returns the union of two sets, optionally retaining duplicates.", "fs*"));
		if (false) define(new FunDefBase("VisualTotals", "VisualTotals(<Set>, <Pattern>)", "Dynamically totals child members specified in a set using a pattern for the total label in the result set.", "fs*"));
		define(new MultiResolver(
			"Wtd", "Wtd([<Member>])", "A shortcut function for the PeriodsToDate function that specifies the level to be Week.",
			new String[] {"fs", "fsm"},
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
			new String[] {"fs", "fsm"},
			new FunkBase() {
					public Object evaluate(Evaluator evaluator, Exp[] args) {
						return periodsToDate(
							evaluator,
							evaluator.getCube().getYearLevel(),
							getMemberArg(evaluator, args, 0, false));
					}
				}));
		define(new FunDefBase(":", "<Member>:<Member>", "Infix colon operator returns the set of members between a given pair of members.", "ismm"));

		// special resolver for the "{...}" operator
		define(new ResolverBase(
			"{}",
			"{<Member> [, <Member>]...}",
			"Brace operator constructs a set.",
			FunDef.TypeBraces) {
				protected FunDef resolve(Exp[] args, int[] conversionCount) {
					for (int i = 0; i < args.length; i++) {
						if (canConvert(
							args[i], Exp.CatMember, conversionCount)) {
							continue;
						}
						if (canConvert(
							args[i], Exp.CatSet, conversionCount)) {
							continue;
						}
						if (canConvert(
							args[i], Exp.CatTuple, conversionCount)) {
							continue;
						}
						return null;
					}
					return new SetFunDef(
						this, syntacticType, ExpBase.getTypes(args));
				}
				public void testDontAllowLevel(TestCase test) {
					Throwable throwable =
						TestContext.instance().executeFoodMartCatch(
							"select {[Store].[Store City]} on columns," +
							"{[Gender].[M]} on rows from Sales");
					test.assertTrue(throwable != null);
					test.assertTrue(
						throwable.toString().indexOf(
							"no function matches signature '{<Level>}'")
						>= 0);
				}
			});

		//
		// STRING FUNCTIONS
		define(new FunDefBase("IIf", "IIf(<Logical Expression>, <String Expression1>, <String Expression2>)", "Returns one of two string values determined by a logical test.", "fSbSS"));
		define(new FunDefBase("Name", "<Dimension>.Name", "Returns the name of a dimension.", "pSd"));
		define(new FunDefBase("Name", "<Hierarchy>.Name", "Returns the name of a hierarchy.", "pSh"));
		define(new FunDefBase("Name", "<Level>.Name", "Returns the name of a level.", "pSl"));
		define(new FunDefBase("Name", "<Member>.Name", "Returns the name of a member.", "pSm"));
		define(new FunDefBase("SetToStr", "SetToStr(<Set>)", "Constructs a string from a set.", "fSs"));
		define(new FunDefBase("TupleToStr", "TupleToStr(<Tuple>)", "Constructs a string from a tuple.", "fSt"));
		define(new FunDefBase("UniqueName", "<Dimension>.UniqueName", "Returns the unique name of a dimension.", "pSd"));
		define(new FunDefBase("UniqueName", "<Level>.UniqueName", "Returns the unique name of a level.", "pSl"));
		define(new FunDefBase("UniqueName", "<Member>.UniqueName", "Returns the unique name of a member.", "pSh"));
		//
		// TUPLE FUNCTIONS
		define(new FunDefBase("Current", "<Set>.Current", "Returns the current tuple from a set during an iteration.", "pts"));
		if (false) define(new FunDefBase("Item", "<Set>.Item(<String Expression>[, <String Expression>...] | <Index>)", "Returns a tuple from a set.", "mt*"));
		define(new FunDefBase("StrToTuple", "StrToTuple(<String Expression>)", "Constructs a tuple from a string.", "ftS") {
				public void unparse(Exp[] args, PrintWriter pw, ElementCallback callback) {
					if (callback.isPlatoMdx()) {
						// omit extra args (they're for us, not Plato)
						super.unparse(new Exp[] {args[0]}, pw, callback);
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
		define(new FunDefBase("+", "<Numeric Expression> + <Numeric Expression>", "Adds two numbers.", "innn"){
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
		define(new FunDefBase("*", "<Numeric Expression> * <Numeric Expression>", "Multiplies two numbers.", "innn"){
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
		define(new FunDefBase("<=", "<Numeric Expression> = <Numeric Expression>", "Returns whether an expression is less than or equal to another.", "ibnn"));
		define(new FunDefBase(">", "<Numeric Expression> = <Numeric Expression>", "Returns whether an expression is greater than another.", "ibnn"));
		define(new FunDefBase(">=", "<Numeric Expression> = <Numeric Expression>", "Returns whether an expression is greater than or equal to another.", "ibnn"));
	}

	public TestSuite suite()
	{
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

	void test()
	{
		String[] stmts = new String[] {
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
