/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.DoubleCalc;
import mondrian.calc.ResultStyle;
import mondrian.calc.TupleCursor;
import mondrian.calc.TupleIterable;
import mondrian.calc.TupleList;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.DimensionExpr;
import mondrian.mdx.HierarchyExpr;
import mondrian.mdx.LevelExpr;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Access;
import mondrian.olap.Annotation;
import mondrian.olap.Category;
import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.ExpBase;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Id;
import mondrian.olap.Level;
import mondrian.olap.Literal;
import mondrian.olap.MatchType;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.OlapElement;
import mondrian.olap.Property;
import mondrian.olap.Query;
import mondrian.olap.ResultStyleException;
import mondrian.olap.SchemaReader;
import mondrian.olap.Syntax;
import mondrian.olap.Util;
import mondrian.olap.Validator;
import mondrian.olap.fun.sort.OrderKey;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.ScalarType;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.Type;
import mondrian.olap.type.TypeUtil;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.RolapUtil;
import mondrian.server.Execution;
import mondrian.util.CancellationChecker;
import mondrian.util.ConcatenableList;
import mondrian.util.IdentifierParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static mondrian.olap.fun.sort.Sorter.hierarchizeTupleList;

/**
 * {@code FunUtil} contains a set of methods useful within the {@code mondrian.olap.fun} package.
 *
 * @author jhyde
 * @since 1.0
 */
public class FunUtil extends Util {

  static final String[] emptyStringArray = new String[ 0 ];
  public static final NullMember NullMember = new NullMember();

  /**
   * Special value which indicates that a {@code double} computation has returned the MDX null value. See {@link
   * DoubleCalc}.
   */
  public static final double DoubleNull = 0.000000012345;

  /**
   * Special value which indicates that a {@code double} computation has returned the MDX EMPTY value. See {@link
   * DoubleCalc}.
   */
  public static final double DoubleEmpty = -0.000000012345;

  /**
   * Special value which indicates that an {@code int} computation has returned the MDX null value. See {@link
   * mondrian.calc.IntegerCalc}.
   */
  public static final int IntegerNull = Integer.MIN_VALUE + 1;

  /**
   * Null value in three-valued boolean logic. Actually, a placeholder until we actually implement 3VL.
   */
  public static final boolean BooleanNull = false;

  /**
   * Creates an exception which indicates that an error has occurred while executing a given function.
   *
   * @param funDef  Function being executed
   * @param message Explanatory message
   * @return Exception that can be used as a cell result
   */
  public static RuntimeException newEvalException(
    FunDef funDef,
    String message ) {
    Util.discard( funDef ); // TODO: use this
    return new MondrianEvaluationException( message );
  }

  /**
   * Creates an exception which indicates that an error has occurred while executing a given function.
   *
   * @param throwable Exception
   * @return Exception that can be used as a cell result
   */
  public static RuntimeException newEvalException( Throwable throwable ) {
    return new MondrianEvaluationException(
      throwable.getClass().getName() + ": " + throwable.getMessage() );
  }

  /**
   * Creates an exception which indicates that an error has occurred while executing a given function.
   *
   * @param message   Explanatory message
   * @param throwable Exception
   * @return Exception that can be used as a cell result
   */
  public static RuntimeException newEvalException(
    String message,
    Throwable throwable ) {
    return new MondrianEvaluationException(
      message
        + ": " + Util.getErrorMessage( throwable ) );
  }

  public static void checkIterListResultStyles( Calc calc ) {
    switch ( calc.getResultStyle() ) {
      case ITERABLE:
      case LIST:
      case MUTABLE_LIST:
        break;
      default:
        throw ResultStyleException.generateBadType(
          ResultStyle.ITERABLE_LIST_MUTABLELIST,
          calc.getResultStyle() );
    }
  }

  public static void checkListResultStyles( Calc calc ) {
    switch ( calc.getResultStyle() ) {
      case LIST:
      case MUTABLE_LIST:
        break;
      default:
        throw ResultStyleException.generateBadType(
          ResultStyle.LIST_MUTABLELIST,
          calc.getResultStyle() );
    }
  }

  /**
   * Returns an argument whose value is a literal.
   */
  static String getLiteralArg(
    ResolvedFunCall call,
    int i,
    String defaultValue,
    String[] allowedValues ) {
    if ( i >= call.getArgCount() ) {
      if ( defaultValue == null ) {
        throw newEvalException(
          call.getFunDef(),
          "Required argument is missing" );
      } else {
        return defaultValue;
      }
    }
    Exp arg = call.getArg( i );
    if ( !( arg instanceof Literal )
      || arg.getCategory() != Category.Symbol ) {
      throw newEvalException(
        call.getFunDef(),
        "Expected a symbol, found '" + arg + "'" );
    }
    String s = (String) ( (Literal) arg ).getValue();
    StringBuilder sb = new StringBuilder( 64 );
    for ( int j = 0; j < allowedValues.length; j++ ) {
      String allowedValue = allowedValues[ j ];
      if ( allowedValue.equalsIgnoreCase( s ) ) {
        return allowedValue;
      }
      if ( j > 0 ) {
        sb.append( ", " );
      }
      sb.append( allowedValue );
    }
    throw newEvalException(
      call.getFunDef(),
      "Allowed values are: {" + sb + "}" );
  }

  /**
   * Returns the ordinal of a literal argument. If the argument does not belong to the supplied enumeration, returns
   * -1.
   */
  static <E extends Enum<E>> E getLiteralArg(
    ResolvedFunCall call,
    int i,
    E defaultValue,
    Class<E> allowedValues ) {
    if ( i >= call.getArgCount() ) {
      if ( defaultValue == null ) {
        throw newEvalException(
          call.getFunDef(),
          "Required argument is missing" );
      } else {
        return defaultValue;
      }
    }
    Exp arg = call.getArg( i );
    if ( !( arg instanceof Literal )
      || arg.getCategory() != Category.Symbol ) {
      throw newEvalException(
        call.getFunDef(),
        "Expected a symbol, found '" + arg + "'" );
    }
    String s = (String) ( (Literal) arg ).getValue();
    for ( E e : allowedValues.getEnumConstants() ) {
      if ( e.name().equalsIgnoreCase( s ) ) {
        return e;
      }
    }
    StringBuilder buf = new StringBuilder( 64 );
    int k = 0;
    for ( E e : allowedValues.getEnumConstants() ) {
      if ( k++ > 0 ) {
        buf.append( ", " );
      }
      buf.append( e.name() );
    }
    throw newEvalException(
      call.getFunDef(),
      "Allowed values are: {" + buf + "}" );
  }

  /**
   * Throws an error if the expressions don't have the same hierarchy.
   *
   * @throws MondrianEvaluationException if expressions don't have the same hierarchy
   */
  static void checkCompatible( Exp left, Exp right, FunDef funDef ) {
    final Type leftType = TypeUtil.stripSetType( left.getType() );
    final Type rightType = TypeUtil.stripSetType( right.getType() );
    if ( !TypeUtil.isUnionCompatible( leftType, rightType ) ) {
      throw newEvalException(
        funDef, "Expressions must have the same hierarchy" );
    }
  }

  /**
   * Adds every element of {@code right} which is not in {@code set} to both {@code set} and {@code left}.
   */
  static void addUnique(
    TupleList left,
    TupleList right,
    Set<List<Member>> set ) {
    assert left != null;
    assert right != null;
    if ( right.isEmpty() ) {
      return;
    }
    for ( int i = 0, n = right.size(); i < n; i++ ) {
      List<Member> o = right.get( i );
      if ( set.add( o ) ) {
        left.add( o );
      }
    }
  }

  /**
   * Returns the default hierarchy of a dimension, or null if there is no default.
   *
   * @param dimension Dimension
   * @return Default hierarchy, or null
   * @see MondrianResource#CannotImplicitlyConvertDimensionToHierarchy
   */
  public static Hierarchy getDimensionDefaultHierarchy( Dimension dimension ) {
    final Hierarchy[] hierarchies = dimension.getHierarchies();
    if ( hierarchies.length == 1 ) {
      return hierarchies[ 0 ];
    }
    if ( MondrianProperties.instance().SsasCompatibleNaming.get() ) {
      // In SSAS 2005, dimensions with more than one hierarchy do not have
      // a default hierarchy.
      return null;
    }
    for ( Hierarchy hierarchy : hierarchies ) {
      if ( hierarchy.getName() == null
        || hierarchy.getUniqueName().equals( dimension.getUniqueName() ) ) {
        return hierarchy;
      }
    }
    return null;
  }

  static List<Member> addMembers(
    final SchemaReader schemaReader,
    final List<Member> members,
    final Hierarchy hierarchy ) {
    // only add accessible levels
    for ( Level level : schemaReader.getHierarchyLevels( hierarchy ) ) {
      addMembers( schemaReader, members, level );
    }
    return members;
  }

  static List<Member> addMembers(
    SchemaReader schemaReader,
    List<Member> members,
    Level level ) {
    List<Member> levelMembers = schemaReader.getLevelMembers( level, true );
    members.addAll( levelMembers );
    return members;
  }

  /**
   * Removes every member from a list which is calculated. The list must not be null, and must consist only of members.
   *
   * @param memberList Member list
   * @return List of non-calculated members
   */
  static List<Member> removeCalculatedMembers( List<Member> memberList ) {
    List<Member> clone = new ArrayList<Member>();
    for ( Member member : memberList ) {
      if ( member.isCalculated()
        && !member.isParentChildPhysicalMember() ) {
        continue;
      }
      clone.add( member );
    }
    return clone;
  }

  /**
   * Removes every tuple from a list which is calculated. The list must not be null, and must consist only of members.
   *
   * @param memberList Member list
   * @return List of non-calculated members
   */
  static TupleList removeCalculatedMembers( TupleList memberList ) {
    if ( memberList.getArity() == 1 ) {
      return new UnaryTupleList(
        removeCalculatedMembers(
          memberList.slice( 0 ) ) );
    } else {
      final TupleList clone = memberList.cloneList( memberList.size() );
      outer:
      for ( List<Member> members : memberList ) {
        for ( Member member : members ) {
          if ( member.isCalculated()
            && !member.isParentChildPhysicalMember() ) {
            continue outer;
          }
        }
        clone.add( members );
      }
      return clone;
    }
  }

  /**
   * Returns whether {@code m0} is an ancestor of {@code m1}.
   *
   * @param strict if true, a member is not an ancestor of itself
   */
  public static boolean isAncestorOf( Member m0, Member m1, boolean strict ) {
    if ( strict ) {
      if ( m1 == null ) {
        return false;
      }
      m1 = m1.getParentMember();
    }
    while ( m1 != null ) {
      if ( m1.equals( m0 ) ) {
        return true;
      }
      m1 = m1.getParentMember();
    }
    return false;
  }

  /**
   * Compares double-precision values according to MDX semantics.
   *
   * <p>MDX requires a total order:
   * <blockquote>
   * -inf &lt; NULL &lt; ... &lt; -1 &lt; ... &lt; 0 &lt; ... &lt; NaN &lt; +inf
   * </blockquote>
   * but this is different than Java semantics, specifically with regard to {@link Double#NaN}.
   */
  public static int compareValues( double d1, double d2 ) {
    if ( Double.isNaN( d1 ) ) {
      if ( d2 == Double.POSITIVE_INFINITY ) {
        return -1;
      } else if ( Double.isNaN( d2 ) ) {
        return 0;
      } else {
        return 1;
      }
    } else if ( Double.isNaN( d2 ) ) {
      if ( d1 == Double.POSITIVE_INFINITY ) {
        return 1;
      } else {
        return -1;
      }
    } else if ( d1 == d2 ) {
      return 0;
    } else if ( d1 == FunUtil.DoubleNull ) {
      if ( d2 == Double.NEGATIVE_INFINITY ) {
        return 1;
      } else {
        return -1;
      }
    } else if ( d2 == FunUtil.DoubleNull ) {
      if ( d1 == Double.NEGATIVE_INFINITY ) {
        return -1;
      } else {
        return 1;
      }
    } else if ( d1 < d2 ) {
      return -1;
    } else {
      return 1;
    }
  }

  /**
   * Compares two cell values.
   *
   * <p>Nulls compare last, exceptions (including the
   * object which indicates the the cell is not in the cache yet) next, then numbers and strings are compared by value.
   *
   * @param value0 First cell value
   * @param value1 Second cell value
   * @return -1, 0, or 1, depending upon whether first cell value is less than, equal to, or greater than the second
   */
  public static int compareValues( Object value0, Object value1 ) {
    if ( value0 == value1 ) {
      return 0;
    }
    // null is less than anything else
    if ( value0 == null ) {
      return -1;
    }
    if ( value1 == null ) {
      return 1;
    }

    if ( value0 == RolapUtil.valueNotReadyException ) {
      // the left value is not in cache; continue as best as we can
      return -1;
    } else if ( value1 == RolapUtil.valueNotReadyException ) {
      // the right value is not in cache; continue as best as we can
      return 1;
    } else if ( value0 == Util.nullValue ) {
      return -1; // null == -infinity
    } else if ( value1 == Util.nullValue ) {
      return 1; // null == -infinity
    } else if ( value0 instanceof String ) {
      return ( (String) value0 ).compareToIgnoreCase( (String) value1 );
    } else if ( value0 instanceof Number ) {
      return FunUtil.compareValues(
        ( (Number) value0 ).doubleValue(),
        ( (Number) value1 ).doubleValue() );
    } else if ( value0 instanceof Date ) {
      return ( (Date) value0 ).compareTo( (Date) value1 );
    } else if ( value0 instanceof OrderKey ) {
      return ( (OrderKey) value0 ).compareTo( value1 );
    } else {
      throw Util.newInternal( "cannot compare " + value0 );
    }
  }

  /**
   * Turns the mapped values into relative values (percentages) for easy use by the general topOrBottom function. This
   * might also be a useful function in itself.
   */
  static void toPercent(
    TupleList members,
    Map<List<Member>, Object> mapMemberToValue ) {
    double total = 0;
    int memberCount = members.size();
    for ( int i = 0; i < memberCount; i++ ) {
      final List<Member> key = members.get( i );
      final Object o = mapMemberToValue.get( key );
      if ( o instanceof Number ) {
        total += ( (Number) o ).doubleValue();
      }
    }
    for ( int i = 0; i < memberCount; i++ ) {
      final List<Member> key = members.get( i );
      final Object o = mapMemberToValue.get( key );
      if ( o instanceof Number ) {
        double d = ( (Number) o ).doubleValue();
        mapMemberToValue.put(
          key,
          d / total * (double) 100 );
      }
    }
  }


  /**
   * Decodes the syntactic type of an operator.
   *
   * @param flags A encoded string which represents an operator signature, as used by the {@code flags} parameter used
   *              to construct a {@link FunDefBase}.
   * @return A {@link Syntax}
   */
  public static Syntax decodeSyntacticType( String flags ) {
    char c = flags.charAt( 0 );
    switch ( c ) {
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
      case 'Q':
        return Syntax.Postfix;
      case 'I':
        return Syntax.Internal;
      default:
        throw newInternal(
          "unknown syntax code '" + c + "' in string '" + flags + "'" );
    }
  }

  /**
   * Decodes the signature of a function into a category code which describes the return type of the operator.
   *
   * <p>For example, <code>decodeReturnType("fnx")</code> returns
   * <code>{@link Category#Numeric}</code>, indicating this function has a
   * numeric return value.
   *
   * @param flags The signature of an operator, as used by the {@code flags} parameter used to construct a {@link
   *              FunDefBase}.
   * @return An array {@link Category} codes.
   */
  public static int decodeReturnCategory( String flags ) {
    final int returnCategory = decodeCategory( flags, 1 );
    if ( ( returnCategory & Category.Mask ) != returnCategory ) {
      throw newInternal( "bad return code flag in flags '" + flags + "'" );
    }
    return returnCategory;
  }

  /**
   * Decodes the {@code offset}th character of an encoded method signature into a type category.
   *
   * <p>The codes are:
   * <table border="1">
   *
   * <tr><td>a</td><td>{@link Category#Array}</td></tr>
   *
   * <tr><td>d</td><td>{@link Category#Dimension}</td></tr>
   *
   * <tr><td>h</td><td>{@link Category#Hierarchy}</td></tr>
   *
   * <tr><td>l</td><td>{@link Category#Level}</td></tr>
   *
   * <tr><td>b</td><td>{@link Category#Logical}</td></tr>
   *
   * <tr><td>m</td><td>{@link Category#Member}</td></tr>
   *
   * <tr><td>N</td><td>Constant {@link Category#Numeric}</td></tr>
   *
   * <tr><td>n</td><td>{@link Category#Numeric}</td></tr>
   *
   * <tr><td>x</td><td>{@link Category#Set}</td></tr>
   *
   * <tr><td>#</td><td>Constant {@link Category#String}</td></tr>
   *
   * <tr><td>S</td><td>{@link Category#String}</td></tr>
   *
   * <tr><td>t</td><td>{@link Category#Tuple}</td></tr>
   *
   * <tr><td>v</td><td>{@link Category#Value}</td></tr>
   *
   * <tr><td>y</td><td>{@link Category#Symbol}</td></tr>
   *
   * </table>
   *
   * @param flags  Encoded signature string
   * @param offset 0-based offset of character within string
   * @return A {@link Category}
   */
  public static int decodeCategory( String flags, int offset ) {
    char c = flags.charAt( offset );
    switch ( c ) {
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
      case 'I':
        return Category.Numeric | Category.Integer | Category.Constant;
      case 'i':
        return Category.Numeric | Category.Integer;
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
      case 'U':
        return Category.Null;
      case 'e':
        return Category.Empty;
      case 'D':
        return Category.DateTime;
      default:
        throw newInternal(
          "unknown type code '" + c + "' in string '" + flags + "'" );
    }
  }

  /**
   * Decodes a string of parameter types into an array of type codes.
   *
   * <p>Each character is decoded using {@link #decodeCategory(String, int)}.
   * For example, <code>decodeParameterTypes("nx")</code> returns
   * <code>{{@link Category#Numeric}, {@link Category#Set}}</code>.
   *
   * @param flags The signature of an operator, as used by the {@code flags} parameter used to construct a {@link
   *              FunDefBase}.
   * @return An array {@link Category} codes.
   */
  public static int[] decodeParameterCategories( String flags ) {
    int[] parameterCategories = new int[ flags.length() - 2 ];
    for ( int i = 0; i < parameterCategories.length; i++ ) {
      parameterCategories[ i ] = decodeCategory( flags, i + 2 );
    }
    return parameterCategories;
  }

  /**
   * Converts a double (primitive) value to a Double. {@link #DoubleNull} becomes null.
   */
  public static Double box( double d ) {
    return d == DoubleNull
      ? null
      : d;
  }

  /**
   * Converts an int (primitive) value to an Integer. {@link #IntegerNull} becomes null.
   */
  public static Integer box( int n ) {
    return n == IntegerNull
      ? null
      : n;
  }

  static double percentile(
    Evaluator evaluator,
    TupleList members,
    Calc exp,
    double p ) {
    SetWrapper sw = evaluateSet( evaluator, members, exp );
    if ( sw.errorCount > 0 ) {
      return Double.NaN;
    } else if ( sw.v.size() == 0 ) {
      return FunUtil.DoubleNull;
    }
    double[] asArray = new double[ sw.v.size() ];
    for ( int i = 0; i < asArray.length; i++ ) {
      asArray[ i ] = (Double) sw.v.get( i );
    }
    Arrays.sort( asArray );

    // The median is defined as the value that has exactly the same
    // number of entries before it in the sorted list as after.
    // So, if the number of entries in the list is odd, the
    // median is the entry at (length-1)/2 (using zero-based indexes).
    // If the number of entries is even, the median is defined as the
    // arithmetic mean of the two numbers in the middle of the list, or
    // (entries[length/2 - 1] + entries[length/2]) / 2.
    int length = asArray.length;
    if ( length == 1 ) {
      // if array contains a single element return it
      return asArray[ 0 ];
    }
    if ( p <= 0.0 ) {
      return asArray[ 0 ];
    } else if ( p >= 1.0 ) {
      return asArray[ length - 1 ];
    } else if ( length == 1 ) {
      return asArray[ 0 ];
    } else if ( p == 0.5 ) {
      // Special case for median.
      if ( ( length & 1 ) == 1 ) {
        // The length is odd. Note that length/2 is an integer
        // expression, and it's positive so we save ourselves a divide.
        return asArray[ length >> 1 ];
      } else {
        return ( asArray[ ( length >> 1 ) - 1 ] + asArray[ length >> 1 ] )
          / 2.0;
      }
    } else {
      final double rank = ( ( length - 1 ) * p ) + 1;
      final int integerPart = (int) Math.floor( rank );
      assert integerPart >= 1;
      final double decimalPart = rank - integerPart;
      assert decimalPart >= 0;
      assert decimalPart <= 1;
      int indexForFormula = integerPart - 1;
      double percentile = asArray[ indexForFormula ]
        + ( ( asArray[ indexForFormula + 1 ] - asArray[ indexForFormula ] )
        * decimalPart );
      return percentile;
    }
  }

  /**
   * Returns the member which lies upon a particular quartile according to a given expression.
   *
   * @param evaluator Evaluator
   * @param members   List of members
   * @param exp       Expression to rank members
   * @param range     Quartile (1, 2 or 3)
   * @pre range >= 1 && range <= 3
   */
  protected static double quartile(
    Evaluator evaluator,
    TupleList members,
    Calc exp,
    int range ) {
    assert range >= 1 && range <= 3;

    SetWrapper sw = evaluateSet( evaluator, members, exp );
    if ( sw.errorCount > 0 ) {
      return Double.NaN;
    } else if ( sw.v.size() == 0 ) {
      return DoubleNull;
    }

    double[] asArray = new double[ sw.v.size() ];
    for ( int i = 0; i < asArray.length; i++ ) {
      asArray[ i ] = ( (Double) sw.v.get( i ) ).doubleValue();
    }

    Arrays.sort( asArray );
    // get a quartile, median is a second q
    double dm = 0.25 * asArray.length * range;
    int median = (int) Math.floor( dm );
    return dm == median && median < asArray.length - 1
      ? ( asArray[ median ] + asArray[ median + 1 ] ) / 2
      : asArray[ median ];
  }

  public static Object min(
    Evaluator evaluator,
    TupleList members,
    Calc calc ) {
    SetWrapper sw = evaluateSet( evaluator, members, calc );
    if ( sw.errorCount > 0 ) {
      return Double.NaN;
    } else {
      final int size = sw.v.size();
      if ( size == 0 ) {
        return Util.nullValue;
      } else {
        Double min = ( (Number) sw.v.get( 0 ) ).doubleValue();
        for ( int i = 1; i < size; i++ ) {
          Double iValue = ( (Number) sw.v.get( i ) ).doubleValue();
          if ( iValue < min ) {
            min = iValue;
          }
        }
        return min;
      }
    }
  }

  public static Object max(
    Evaluator evaluator,
    TupleList members,
    Calc exp ) {
    SetWrapper sw = evaluateSet( evaluator, members, exp );
    if ( sw.errorCount > 0 ) {
      return Double.NaN;
    } else {
      final int size = sw.v.size();
      if ( size == 0 ) {
        return Util.nullValue;
      } else {
        Double max = ( (Number) sw.v.get( 0 ) ).doubleValue();
        for ( int i = 1; i < size; i++ ) {
          Double iValue = ( (Number) sw.v.get( i ) ).doubleValue();
          if ( iValue > max ) {
            max = iValue;
          }
        }
        return max;
      }
    }
  }

  static Object var(
    Evaluator evaluator,
    TupleList members,
    Calc exp,
    boolean biased ) {
    SetWrapper sw = evaluateSet( evaluator, members, exp );
    return _var( sw, biased );
  }

  private static Object _var( SetWrapper sw, boolean biased ) {
    if ( sw.errorCount > 0 ) {
      return new Double( Double.NaN );
    } else if ( sw.v.size() == 0 ) {
      return Util.nullValue;
    } else {
      double stdev = 0.0;
      double avg = _avg( sw );
      for ( int i = 0; i < sw.v.size(); i++ ) {
        stdev +=
          Math.pow( ( ( (Number) sw.v.get( i ) ).doubleValue() - avg ), 2 );
      }
      int n = sw.v.size();
      if ( !biased ) {
        n--;
      }
      return new Double( stdev / (double) n );
    }
  }

  static double correlation(
    Evaluator evaluator,
    TupleList memberList,
    Calc exp1,
    Calc exp2 ) {
    SetWrapper sw1 = evaluateSet( evaluator, memberList, exp1 );
    SetWrapper sw2 = evaluateSet( evaluator, memberList, exp2 );
    Object covar = _covariance( sw1, sw2, false );
    Object var1 = _var( sw1, false ); // this should be false, yes?
    Object var2 = _var( sw2, false );

    return ( (Number) covar ).doubleValue()
      / Math.sqrt(
      ( (Number) var1 ).doubleValue()
        * ( (Number) var2 ).doubleValue() );
  }

  static Object covariance(
    Evaluator evaluator,
    TupleList members,
    Calc exp1,
    Calc exp2,
    boolean biased ) {
    final int savepoint = evaluator.savepoint();
    SetWrapper sw1;
    try {
      sw1 = evaluateSet( evaluator, members, exp1 );
    } finally {
      evaluator.restore( savepoint );
    }
    SetWrapper sw2;
    try {
      sw2 = evaluateSet( evaluator, members, exp2 );
    } finally {
      evaluator.restore( savepoint );
    }
    // todo: because evaluateSet does not add nulls to the SetWrapper, this
    // solution may lead to mismatched lists and is therefore not robust
    return _covariance( sw1, sw2, biased );
  }


  private static Object _covariance(
    SetWrapper sw1,
    SetWrapper sw2,
    boolean biased ) {
    if ( sw1.v.size() != sw2.v.size() ) {
      return Util.nullValue;
    }
    double avg1 = _avg( sw1 );
    double avg2 = _avg( sw2 );
    double covar = 0.0;
    for ( int i = 0; i < sw1.v.size(); i++ ) {
      // all of this casting seems inefficient - can we make SetWrapper
      // contain an array of double instead?
      double diff1 = ( ( (Number) sw1.v.get( i ) ).doubleValue() - avg1 );
      double diff2 = ( ( (Number) sw2.v.get( i ) ).doubleValue() - avg2 );
      covar += ( diff1 * diff2 );
    }
    int n = sw1.v.size();
    if ( !biased ) {
      n--;
    }
    return new Double( covar / (double) n );
  }

  static Object stdev(
    Evaluator evaluator,
    TupleList members,
    Calc exp,
    boolean biased ) {
    Object o = var( evaluator, members, exp, biased );
    return ( o instanceof Double )
      ? new Double( Math.sqrt( ( (Number) o ).doubleValue() ) )
      : o;
  }

  public static Object avg(
    Evaluator evaluator,
    TupleList members,
    Calc calc ) {
    SetWrapper sw = evaluateSet( evaluator, members, calc );
    return ( sw.errorCount > 0 )
      ? new Double( Double.NaN )
      : ( sw.v.size() == 0 )
      ? Util.nullValue
      : new Double( _avg( sw ) );
  }

  // TODO: parameterize inclusion of nulls; also, maybe make _avg a method of
  // setwrapper, so we can cache the result (i.e. for correl)
  private static double _avg( SetWrapper sw ) {
    double sum = 0.0;
    for ( int i = 0; i < sw.v.size(); i++ ) {
      sum += ( (Number) sw.v.get( i ) ).doubleValue();
    }
    // TODO: should look at context and optionally include nulls
    return sum / (double) sw.v.size();
  }

  public static Object sum(
    Evaluator evaluator,
    TupleList members,
    Calc exp ) {
    double d = sumDouble( evaluator, members, exp );
    return d == DoubleNull ? Util.nullValue : new Double( d );
  }

  public static double sumDouble(
    Evaluator evaluator,
    TupleList members,
    Calc exp ) {
    SetWrapper sw = evaluateSet( evaluator, members, exp );
    if ( sw.errorCount > 0 ) {
      return Double.NaN;
    } else if ( sw.v.size() == 0 ) {
      return DoubleNull;
    } else {
      double sum = 0.0;
      for ( int i = 0; i < sw.v.size(); i++ ) {
        sum += ( (Number) sw.v.get( i ) ).doubleValue();
      }
      return sum;
    }
  }

  public static double sumDouble(
    Evaluator evaluator,
    TupleIterable iterable,
    Calc exp ) {
    SetWrapper sw = evaluateSet( evaluator, iterable, exp );
    if ( sw.errorCount > 0 ) {
      return Double.NaN;
    } else if ( sw.v.size() == 0 ) {
      return DoubleNull;
    } else {
      double sum = 0.0;
      for ( int i = 0; i < sw.v.size(); i++ ) {
        sum += ( (Number) sw.v.get( i ) ).doubleValue();
      }
      return sum;
    }
  }

  public static int count(
    Evaluator evaluator,
    TupleIterable iterable,
    boolean includeEmpty ) {
    if ( iterable == null ) {
      return 0;
    }
    if ( includeEmpty ) {
      if ( iterable instanceof TupleList ) {
        return ( (TupleList) iterable ).size();
      } else {
        int retval = 0;
        TupleCursor cursor = iterable.tupleCursor();
        while ( cursor.forward() ) {
          retval++;
        }
        return retval;
      }
    } else {
      int retval = 0;
      TupleCursor cursor = iterable.tupleCursor();
      while ( cursor.forward() ) {
        cursor.setContext( evaluator );
        if ( !evaluator.currentIsEmpty() ) {
          retval++;
        }
      }
      return retval;
    }
  }

  /**
   * Evaluates {@code exp} (if defined) over {@code members} to generate a {@link List} of {@link SetWrapper} objects,
   * which contains a {@link Double} value and meta information, unlike {@link #evaluateMembers}, which only produces
   * values.
   *
   * @pre exp != null
   */
  static SetWrapper evaluateSet(
    Evaluator evaluator,
    TupleIterable members,
    Calc calc ) {
    assert members != null;
    assert calc != null;
    assert calc.getType() instanceof ScalarType;

    // todo: treat constant exps as evaluateMembers() does
    SetWrapper retval = new SetWrapper();
    final TupleCursor cursor = members.tupleCursor();
    int currentIteration = 0;
    Execution execution =
      evaluator.getQuery().getStatement().getCurrentExecution();
    while ( cursor.forward() ) {
      CancellationChecker.checkCancelOrTimeout(
        currentIteration++, execution );
      cursor.setContext( evaluator );
      Object o = calc.evaluate( evaluator );
      if ( o == null || o == Util.nullValue ) {
        retval.nullCount++;
      } else if ( o == RolapUtil.valueNotReadyException ) {
        // Carry on summing, so that if we are running in a
        // BatchingCellReader, we find out all the dependent cells we
        // need
        retval.errorCount++;
      } else if ( o instanceof Number ) {
        retval.v.add( ( (Number) o ).doubleValue() );
      } else {
        retval.v.add( o );
      }
    }
    return retval;
  }

  /**
   * Evaluates one or more expressions against the member list returning a SetWrapper array. Where this differs very
   * significantly from the above evaluateSet methods is how it count null values and Throwables; this method adds nulls
   * to the SetWrapper Vector rather than not adding anything - as the above method does. The impact of this is that if,
   * for example, one was creating a list of x,y values then each list will have the same number of values (though some
   * might be null) - this allows higher level code to determine how to handle the lack of data rather than having a
   * non-equal number (if one is plotting x,y values it helps to have the same number and know where a potential gap is
   * the data is.
   */
  static SetWrapper[] evaluateSet(
    Evaluator evaluator,
    TupleList list,
    DoubleCalc[] calcs ) {
    Util.assertPrecondition( calcs != null, "calcs != null" );

    // todo: treat constant exps as evaluateMembers() does
    SetWrapper[] retvals = new SetWrapper[ calcs.length ];
    for ( int i = 0; i < calcs.length; i++ ) {
      retvals[ i ] = new SetWrapper();
    }
    final TupleCursor cursor = list.tupleCursor();
    int currentIteration = 0;
    Execution execution =
      evaluator.getQuery().getStatement().getCurrentExecution();
    while ( cursor.forward() ) {
      CancellationChecker.checkCancelOrTimeout(
        currentIteration++, execution );
      cursor.setContext( evaluator );
      for ( int i = 0; i < calcs.length; i++ ) {
        DoubleCalc calc = calcs[ i ];
        SetWrapper retval = retvals[ i ];
        double o = calc.evaluateDouble( evaluator );
        if ( o == FunUtil.DoubleNull ) {
          retval.nullCount++;
          retval.v.add( null );
        } else {
          retval.v.add( o );
        }
        // TODO: If the expression yielded an error, carry on
        // summing, so that if we are running in a
        // BatchingCellReader, we find out all the dependent cells
        // we need
      }
    }
    return retvals;
  }

  static List<Member> periodsToDate(
    Evaluator evaluator,
    Level level,
    Member member ) {
    if ( member == null ) {
      member = evaluator.getContext( level.getHierarchy() );
    }
    Member m = member;
    while ( m != null ) {
      if ( m.getLevel() == level ) {
        break;
      }
      m = m.getParentMember();
    }
    // If m == null, then "level" was lower than member's level.
    // periodsToDate([Time].[Quarter], [Time].[1997] is valid,
    //  but will return an empty List
    List<Member> members = new ArrayList<Member>();
    if ( m != null ) {
      // e.g. m is [Time].[1997] and member is [Time].[1997].[Q1].[3]
      // we now have to make m to be the first member of the range,
      // so m becomes [Time].[1997].[Q1].[1]
      SchemaReader reader = evaluator.getSchemaReader();
      m = Util.getFirstDescendantOnLevel( reader, m, member.getLevel() );
      reader.getMemberRange( level, m, member, members );
    }
    return members;
  }

  static List<Member> memberRange(
    Evaluator evaluator,
    Member startMember,
    Member endMember ) {
    final Level level = startMember.getLevel();
    assertTrue( level == endMember.getLevel() );
    List<Member> members = new ArrayList<Member>();
    evaluator.getSchemaReader().getMemberRange(
      level, startMember, endMember, members );

    if ( members.isEmpty() ) {
      // The result is empty, so maybe the members are reversed. This is
      // cheaper than comparing the members before we call getMemberRange.
      evaluator.getSchemaReader().getMemberRange(
        level, endMember, startMember, members );
    }
    return members;
  }

  /**
   * Returns the member under ancestorMember having the same relative position under member's parent.
   * <p>For exmaple, cousin([Feb 2001], [Q3 2001]) is [August 2001].
   *
   * @param schemaReader   The reader to use
   * @param member         The member for which we'll find the cousin.
   * @param ancestorMember The cousin's ancestor.
   * @return The child of {@code ancestorMember} in the same position under {@code ancestorMember} as {@code member} is
   * under its parent.
   */
  static Member cousin(
    SchemaReader schemaReader,
    Member member,
    Member ancestorMember ) {
    if ( ancestorMember.isNull() ) {
      return ancestorMember;
    }
    if ( member.getHierarchy() != ancestorMember.getHierarchy() ) {
      throw MondrianResource.instance().CousinHierarchyMismatch.ex(
        member.getUniqueName(), ancestorMember.getUniqueName() );
    }
    if ( member.getLevel().getDepth()
      < ancestorMember.getLevel().getDepth() ) {
      return member.getHierarchy().getNullMember();
    }

    Member cousin = cousin2( schemaReader, member, ancestorMember );
    if ( cousin == null ) {
      cousin = member.getHierarchy().getNullMember();
    }

    return cousin;
  }

  private static Member cousin2(
    SchemaReader schemaReader,
    Member member1,
    Member member2 ) {
    if ( member1.getLevel() == member2.getLevel() ) {
      return member2;
    }
    Member uncle =
      cousin2( schemaReader, member1.getParentMember(), member2 );
    if ( uncle == null ) {
      return null;
    }
    int ordinal = Util.getMemberOrdinalInParent( schemaReader, member1 );
    List<Member> cousins = schemaReader.getMemberChildren( uncle );
    if ( cousins.size() <= ordinal ) {
      return null;
    }
    return cousins.get( ordinal );
  }

  /**
   * Returns the ancestor of {@code member} at the given level or distance. It is assumed that any error checking
   * required has been done prior to calling this function.
   *
   * <p>This method takes into consideration the fact that there
   * may be intervening hidden members between {@code member} and the ancestor. If {@code targetLevel} is not null, then
   * the method will only return a member if the level at {@code distance} from the member is actually the {@code
   * targetLevel} specified.
   *
   * @param evaluator   The evaluation context
   * @param member      The member for which the ancestor is to be found
   * @param distance    The distance up the chain the ancestor is to be found.
   * @param targetLevel The desired targetLevel of the ancestor. If {@code null}, then the distance completely
   *                    determines the desired ancestor.
   * @return The ancestor member, or {@code null} if no such ancestor exists.
   */
  static Member ancestor(
    Evaluator evaluator,
    Member member,
    int distance,
    Level targetLevel ) {
    if ( ( targetLevel != null )
      && ( member.getHierarchy() != targetLevel.getHierarchy() ) ) {
      throw MondrianResource.instance().MemberNotInLevelHierarchy.ex(
        member.getUniqueName(), targetLevel.getUniqueName() );
    }

    if ( distance == 0 ) {
      // Shortcut if there's nowhere to go.
      return member;
    } else if ( distance < 0 ) {
      // Can't go backwards.
      return member.getHierarchy().getNullMember();
    }

    final List<Member> ancestors = new ArrayList<Member>();
    final SchemaReader schemaReader = evaluator.getSchemaReader();
    schemaReader.getMemberAncestors( member, ancestors );

    Member result = member.getHierarchy().getNullMember();

    searchLoop:
    for ( int i = 0; i < ancestors.size(); i++ ) {
      final Member ancestorMember = ancestors.get( i );

      if ( targetLevel != null ) {
        if ( ancestorMember.getLevel() == targetLevel ) {
          if ( schemaReader.isVisible( ancestorMember ) ) {
            result = ancestorMember;
            break;
          } else {
            result = member.getHierarchy().getNullMember();
            break;
          }
        }
      } else {
        if ( schemaReader.isVisible( ancestorMember ) ) {
          distance--;

          // Make sure that this ancestor is really on the right
          // targetLevel. If a targetLevel was specified and at least
          // one of the ancestors was hidden, this this algorithm goes
          // too far up the ancestor list. It's not a problem, except
          // that we need to check if it's happened and return the
          // hierarchy's null member instead.
          //
          // For example, consider what happens with
          // Ancestor([Store].[Israel].[Haifa], [Store].[Store
          // State]).  The distance from [Haifa] to [Store State] is
          // 1, but that lands us at the country targetLevel, which is
          // clearly wrong.
          if ( distance == 0 ) {
            result = ancestorMember;
            break;
          }
        }
      }
    }

    return result;
  }

  /**
   * Compares a pair of members according to their positions in a prefix-order (or postfix-order, if {@code post} is
   * true) walk over a hierarchy.
   *
   * @param m1   First member
   * @param m2   Second member
   * @param post Whether to sortMembers in postfix order. If true, a parent will sortMembers immediately after its last
   *             child. If false, a parent will sortMembers immediately before its first child.
   * @return -1 if m1 collates before m2, 0 if m1 equals m2, 1 if m1 collates after m2
   */
  public static int compareHierarchically(
    Member m1,
    Member m2,
    boolean post ) {
    // Strip away the LimitedRollupMember wrapper, if it exists. The
    // wrapper does not implement equals and comparisons correctly. This
    // is safe this method has no side-effects: it just returns an int.
    m1 = unwrapLimitedRollupMember( m1 );
    m2 = unwrapLimitedRollupMember( m2 );

    if ( equals( m1, m2 ) ) {
      return 0;
    }

    while ( true ) {
      int depth1 = m1.getDepth();
      int depth2 = m2.getDepth();
      if ( depth1 < depth2 ) {
        m2 = m2.getParentMember();
        if ( equals( m1, m2 ) ) {
          return post ? 1 : -1;
        }
      } else if ( depth1 > depth2 ) {
        m1 = m1.getParentMember();
        if ( equals( m1, m2 ) ) {
          return post ? -1 : 1;
        }
      } else {
        Member prev1 = m1;
        Member prev2 = m2;
        m1 = unwrapLimitedRollupMember( m1.getParentMember() );
        m2 = unwrapLimitedRollupMember( m2.getParentMember() );
        if ( equals( m1, m2 ) ) {
          final int c = compareSiblingMembers( prev1, prev2 );
          // compareHierarchically needs to impose a total order;
          // cannot return 0 for non-equal members
          assert c != 0
            : "Members " + prev1 + ", " + prev2
            + " are not equal, but compare returned 0.";
          return c;
        }
      }
    }
  }

  private static Member unwrapLimitedRollupMember( Member m ) {
    if ( m instanceof RolapHierarchy.LimitedRollupMember ) {
      return ( (RolapHierarchy.LimitedRollupMember) m ).member;
    }
    return m;
  }

  /**
   * Compares two members which are known to have the same parent.
   * <p>
   * First, compare by ordinal. This is only valid now we know they're siblings, because ordinals are only unique within
   * a parent. If the dimension does not use ordinals, both ordinals will be -1.
   *
   * <p>If the ordinals do not differ, compare using regular member
   * comparison.
   *
   * @param m1 First member
   * @param m2 Second member
   * @return -1 if m1 collates less than m2, 1 if m1 collates after m2, 0 if m1 == m2.
   */
  public static int compareSiblingMembers( Member m1, Member m2 ) {
    // calculated members collate after non-calculated
    final boolean calculated1 = m1.isCalculatedInQuery();
    final boolean calculated2 = m2.isCalculatedInQuery();
    if ( calculated1 ) {
      if ( !calculated2 ) {
        return 1;
      }
    } else {
      if ( calculated2 ) {
        return -1;
      }
    }
    final Comparable k1 = m1.getOrderKey();
    final Comparable k2 = m2.getOrderKey();
    if ( ( k1 != null ) && ( k2 != null ) ) {
      return k1.compareTo( k2 );
    } else {
      final int ordinal1 = m1.getOrdinal();
      final int ordinal2 = m2.getOrdinal();
      return ( ordinal1 == ordinal2 )
        ? m1.compareTo( m2 )
        : ( ordinal1 < ordinal2 )
        ? -1
        : 1;
    }
  }

  /**
   * Returns whether one of the members in a tuple is null.
   */
  public static boolean tupleContainsNullMember( Member[] tuple ) {
    for ( Member member : tuple ) {
      if ( member.isNull() ) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns whether one of the members in a tuple is null.
   */
  public static boolean tupleContainsNullMember( List<Member> tuple ) {
    for ( Member member : tuple ) {
      if ( member.isNull() ) {
        return true;
      }
    }
    return false;
  }

  public static Member[] makeNullTuple( final TupleType tupleType ) {
    final Type[] elementTypes = tupleType.elementTypes;
    Member[] members = new Member[ elementTypes.length ];
    for ( int i = 0; i < elementTypes.length; i++ ) {
      MemberType type = (MemberType) elementTypes[ i ];
      members[ i ] = makeNullMember( type );
    }
    return members;
  }

  static Member makeNullMember( MemberType memberType ) {
    Hierarchy hierarchy = memberType.getHierarchy();
    if ( hierarchy == null ) {
      return NullMember;
    }
    return hierarchy.getNullMember();
  }

  /**
   * Validates the arguments to a function and resolves the function.
   *
   * @param validator Validator used to validate function arguments and resolve the function
   * @param funDef    Function definition, or null to deduce definition from name, syntax and argument types
   * @param args      Arguments to the function
   * @param newArgs   Output parameter for the resolved arguments
   * @param name      Function name
   * @param syntax    Syntax style used to invoke function
   * @return resolved function definition
   */
  public static FunDef resolveFunArgs(
    Validator validator,
    FunDef funDef,
    Exp[] args,
    Exp[] newArgs,
    String name,
    Syntax syntax ) {
    for ( int i = 0; i < args.length; i++ ) {
      newArgs[ i ] = validator.validate( args[ i ], false );
    }
    if ( funDef == null || validator.alwaysResolveFunDef() ) {
      funDef = validator.getDef( newArgs, name, syntax );
    }
    checkNativeCompatible( validator, funDef, newArgs );
    return funDef;
  }

  /**
   * Functions that dynamically return one or more members of the measures dimension prevent us from using native
   * evaluation.
   *
   * @param validator Validator used to validate function arguments and resolve the function
   * @param funDef    Function definition, or null to deduce definition from name, syntax and argument types
   * @param args      Arguments to the function
   */
  private static void checkNativeCompatible(
    Validator validator,
    FunDef funDef,
    Exp[] args ) {
    // If the first argument to a function is either:
    // 1) the measures dimension or
    // 2) a measures member where the function returns another member or
    //    a set,
    // then these are functions that dynamically return one or more
    // members of the measures dimension.  In that case, we cannot use
    // native cross joins because the functions need to be executed to
    // determine the resultant measures.
    //
    // As a result, we disallow functions like AllMembers applied on the
    // Measures dimension as well as functions like the range operator,
    // siblings, and lag, when the argument is a measure member.
    // However, we do allow functions like isEmpty, rank, and topPercent.
    //
    // Also, the Set and Parentheses functions are ok since they're
    // essentially just containers.
    Query query = validator.getQuery();
    if ( !( funDef instanceof SetFunDef )
      && !( funDef instanceof ParenthesesFunDef )
      && query != null
      && query.nativeCrossJoinVirtualCube() ) {
      int[] paramCategories = funDef.getParameterCategories();
      if ( paramCategories.length > 0 ) {
        final int cat0 = paramCategories[ 0 ];
        final Exp arg0 = args[ 0 ];
        switch ( cat0 ) {
          case Category.Dimension:
          case Category.Hierarchy:
            if ( arg0 instanceof DimensionExpr
              && ( (DimensionExpr) arg0 ).getDimension().isMeasures()
              && !( funDef instanceof HierarchyCurrentMemberFunDef ) ) {
              query.setVirtualCubeNonNativeCrossJoin();
            }
            break;
          case Category.Member:
            if ( arg0 instanceof MemberExpr
              && ( (MemberExpr) arg0 ).getMember().isMeasure()
              && isMemberOrSet( funDef.getReturnCategory() ) ) {
              query.setVirtualCubeNonNativeCrossJoin();
            }
            break;
        }
      }
    }
  }

  private static boolean isMemberOrSet( int category ) {
    return category == Category.Member || category == Category.Set;
  }

  static void appendTuple( StringBuilder buf, Member[] members ) {
    buf.append( "(" );
    for ( int j = 0; j < members.length; j++ ) {
      if ( j > 0 ) {
        buf.append( ", " );
      }
      Member member = members[ j ];
      buf.append( member.getUniqueName() );
    }
    buf.append( ")" );
  }

  /**
   * Returns whether two tuples are equal.
   *
   * <p>The members are allowed to be in different positions. For example,
   * <code>([Gender].[M], [Store].[USA]) IS ([Store].[USA],
   * [Gender].[M])</code> returns {@code true}.
   */
  static boolean equalTuple( Member[] members0, Member[] members1 ) {
    final int count = members0.length;
    if ( count != members1.length ) {
      return false;
    }
    outer:
    for ( int i = 0; i < count; i++ ) {
      // First check the member at the corresponding ordinal. It is more
      // likely to be there.
      final Member member0 = members0[ i ];
      if ( member0.equals( members1[ i ] ) ) {
        continue;
      }
      // Look for this member in other positions.
      // We can assume that the members in members0 are distinct (because
      // they belong to different dimensions), so this test is valid.
      for ( int j = 0; j < count; j++ ) {
        if ( i != j && member0.equals( members1[ j ] ) ) {
          continue outer;
        }
      }
      // This member of members0 does not occur in any position of
      // members1. The tuples are not equal.
      return false;
    }
    return true;
  }

  static FunDef createDummyFunDef(
    Resolver resolver,
    int returnCategory,
    Exp[] args ) {
    final int[] argCategories = ExpBase.getTypes( args );
    return new FunDefBase( resolver, returnCategory, argCategories ) {
    };
  }

  public static List<Member> getNonEmptyMemberChildren(
    Evaluator evaluator,
    Member member ) {
    SchemaReader sr = evaluator.getSchemaReader();
    if ( evaluator.isNonEmpty() ) {
      return sr.getMemberChildren( member, evaluator );
    } else {
      return sr.getMemberChildren( member );
    }
  }

  public static Map<Member, Access> getNonEmptyMemberChildrenWithDetails(
    Evaluator evaluator, Member member ) {
    SchemaReader sr = evaluator.getSchemaReader();
    if ( evaluator.isNonEmpty() ) {
      return (Map<Member, Access>)
        sr.getMemberChildrenWithDetails( member, evaluator );
    } else {
      return (Map<Member, Access>)
        sr.getMemberChildrenWithDetails( member, null );
    }
  }

  /**
   * Returns members of a level which are not empty (according to the criteria expressed by the evaluator).
   *
   * @param evaluator          Evaluator, determines non-empty criteria
   * @param level              Level
   * @param includeCalcMembers Whether to include calculated members
   */
  static List<Member> getNonEmptyLevelMembers(
    final Evaluator evaluator,
    final Level level,
    final boolean includeCalcMembers ) {
    SchemaReader sr = evaluator.getSchemaReader();
    if ( evaluator.isNonEmpty() ) {
      List<Member> members = sr.getLevelMembers( level, evaluator );
      if ( includeCalcMembers ) {
        return addLevelCalculatedMembers( sr, level, members );
      }
      return members;
    }
    return sr.getLevelMembers( level, includeCalcMembers );
  }

  static TupleList levelMembers(
    final Level level,
    final Evaluator evaluator,
    final boolean includeCalcMembers ) {
    List<Member> memberList =
      getNonEmptyLevelMembers( evaluator, level, includeCalcMembers );
    TupleList tupleList;
    if ( !includeCalcMembers ) {
      memberList = removeCalculatedMembers( memberList );
    }
    final List<Member> memberListClone = new ArrayList<Member>( memberList );
    tupleList = new UnaryTupleList( memberListClone );
    return hierarchizeTupleList( tupleList, false );
  }

  static TupleList hierarchyMembers(
    Hierarchy hierarchy,
    Evaluator evaluator,
    final boolean includeCalcMembers ) {
    TupleList tupleList = new UnaryTupleList();
    final List<Member> memberList = tupleList.slice( 0 );
    if ( evaluator.isNonEmpty() ) {
      // Allow the SQL generator to generate optimized SQL since we know
      // we're only interested in non-empty members of this level.
      for ( Level level : hierarchy.getLevels() ) {
        List<Member> members =
          getNonEmptyLevelMembers(
            evaluator, level, includeCalcMembers );
        memberList.addAll( members );
      }
    } else {
      final List<Member> memberList1 = addMembers(
        evaluator.getSchemaReader(),
        new ConcatenableList<>(),
        hierarchy );
      if ( includeCalcMembers ) {
        memberList.addAll( memberList1 );
      } else {
        // Same effect as calling removeCalculatedMembers(tupleList)
        // but one fewer copy of the list.
        for ( Member member1 : memberList1 ) {
          if ( !member1.isCalculated()
            || member1.isParentChildPhysicalMember() ) {
            memberList.add( member1 );
          }
        }
      }
    }
    return hierarchizeTupleList( tupleList, false );
  }


  static TupleList parseTupleList(
    Evaluator evaluator,
    String string,
    List<Hierarchy> hierarchies ) {
    final IdentifierParser.TupleListBuilder builder =
      new IdentifierParser.TupleListBuilder(
        evaluator.getSchemaReader(),
        evaluator.getCube(),
        hierarchies );
    IdentifierParser.parseTupleList( builder, string );
    return builder.tupleList;
  }

  /**
   * Parses a tuple, of the form '(member, member, ...)'. There must be precisely one member for each hierarchy.
   *
   * @param evaluator   Evaluator, provides a {@link mondrian.olap.SchemaReader} and {@link mondrian.olap.Cube}
   * @param string      String to parse
   * @param i           Position to start parsing in string
   * @param members     Output array of members
   * @param hierarchies Hierarchies of the members
   * @return Position where parsing ended in string
   */
  private static int parseTuple(
    final Evaluator evaluator,
    String string,
    int i,
    final Member[] members,
    List<Hierarchy> hierarchies ) {
    final IdentifierParser.Builder builder =
      new IdentifierParser.TupleBuilder(
        evaluator.getSchemaReader(),
        evaluator.getCube(),
        hierarchies ) {
        public void tupleComplete() {
          super.tupleComplete();
          memberList.toArray( members );
        }
      };
    return IdentifierParser.parseTuple( builder, string, i );
  }

  /**
   * Parses a tuple, such as "([Gender].[M], [Marital Status].[S])".
   *
   * @param evaluator   Evaluator, provides a {@link mondrian.olap.SchemaReader} and {@link mondrian.olap.Cube}
   * @param string      String to parse
   * @param hierarchies Hierarchies of the members
   * @return Tuple represented as array of members
   */
  static Member[] parseTuple(
    Evaluator evaluator,
    String string,
    List<Hierarchy> hierarchies ) {
    final Member[] members = new Member[ hierarchies.size() ];
    int i = parseTuple( evaluator, string, 0, members, hierarchies );
    // todo: check for garbage at end of string
    if ( FunUtil.tupleContainsNullMember( members ) ) {
      return null;
    }
    return members;
  }

  static List<Member> parseMemberList(
    Evaluator evaluator,
    String string,
    Hierarchy hierarchy ) {
    IdentifierParser.MemberListBuilder builder =
      new IdentifierParser.MemberListBuilder(
        evaluator.getSchemaReader(),
        evaluator.getCube(),
        hierarchy );
    IdentifierParser.parseMemberList( builder, string );
    return builder.memberList;
  }

  private static int parseMember(
    Evaluator evaluator,
    String string,
    int i,
    final Member[] members,
    Hierarchy hierarchy ) {
    IdentifierParser.MemberListBuilder builder =
      new IdentifierParser.MemberListBuilder(
        evaluator.getSchemaReader(), evaluator.getCube(), hierarchy ) {
        @Override
        public void memberComplete() {
          members[ 0 ] = resolveMember( hierarchyList.get( 0 ) );
          segmentList.clear();
        }
      };
    return IdentifierParser.parseMember( builder, string, i );
  }

  static Member parseMember(
    Evaluator evaluator, String string, Hierarchy hierarchy ) {
    Member[] members = { null };
    int i = parseMember( evaluator, string, 0, members, hierarchy );
    // todo: check for garbage at end of string
    final Member member = members[ 0 ];
    if ( member == null ) {
      throw MondrianResource.instance().MdxChildObjectNotFound.ex(
        string, evaluator.getCube().getQualifiedName() );
    }
    return member;
  }

  /**
   * Returns whether an expression is worth wrapping in "Cache( ... )".
   *
   * @param exp Expression
   * @return Whether worth caching
   */
  public static boolean worthCaching( Exp exp ) {
    // Literal is not worth caching.
    if ( exp instanceof Literal ) {
      return false;
    }
    // Member, hierarchy, level, or dimension expression is not worth
    // caching.
    if ( exp instanceof MemberExpr
      || exp instanceof LevelExpr
      || exp instanceof HierarchyExpr
      || exp instanceof DimensionExpr ) {
      return false;
    }
    if ( exp instanceof ResolvedFunCall ) {
      ResolvedFunCall call = (ResolvedFunCall) exp;

      // A set of literals is not worth caching.
      if ( call.getFunDef() instanceof SetFunDef ) {
        for ( Exp setArg : call.getArgs() ) {
          if ( worthCaching( setArg ) ) {
            return true;
          }
        }
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true if leftTuple Exists w/in rightTuple
   *
   * @param leftTuple        tuple from arg one of EXISTS()
   * @param rightTuple       tuple from arg two of EXISTS()
   * @param leftHierarchies  list of hierarchies from leftTuple, in the same order
   * @param rightHierarchies list of the hiearchies from rightTuple, in the same order
   * @return true if each member from leftTuple is somewhere in the hierarchy chain of the corresponding member from
   * rightTuple, false otherwise. If there is no explicit corresponding member from either right or left, then the
   * default member is used.
   */
  static boolean existsInTuple(
    final List<Member> leftTuple, final List<Member> rightTuple,
    final List<Hierarchy> leftHierarchies,
    final List<Hierarchy> rightHierarchies,
    final Evaluator eval ) {
    List<Member> checkedMembers = new ArrayList<Member>();

    for ( Member leftMember : leftTuple ) {
      Member rightMember = getCorrespondingMember(
        leftMember, rightTuple, rightHierarchies, eval );
      checkedMembers.add( rightMember );
      if ( !leftMember.isOnSameHierarchyChain( rightMember ) ) {
        return false;
      }
    }
    // this loop handles members in the right tuple not present in left
    // Such a member could only impact the resulting tuple list if the
    // default member of the hierarchy is not the all member.
    for ( Member rightMember : rightTuple ) {
      if ( checkedMembers.contains( rightMember ) ) {
        // already checked in the previous loop
        continue;
      }
      Member leftMember = getCorrespondingMember(
        rightMember, leftTuple, leftHierarchies, eval );
      if ( !leftMember.isOnSameHierarchyChain( rightMember ) ) {
        return false;
      }
    }
    return true;
  }

  private static boolean isOnSameHierarchyChain( Member mA, Member mB ) {
    return ( FunUtil.isAncestorOf( mA, mB, false ) )
      || ( FunUtil.isAncestorOf( mB, mA, false ) );
  }

  /**
   * Returns the corresponding member from tuple, or the default member for the hierarchy if member is not explicitly
   * contained in the tuple.
   *
   * @param member           source member
   * @param tuple            tuple containing the target member
   * @param tupleHierarchies list of the hierarchies explicitly contained in the tuple, in the same order.
   * @return target member
   */
  private static Member getCorrespondingMember(
    final Member member, final List<Member> tuple,
    final List<Hierarchy> tupleHierarchies,
    final Evaluator eval ) {
    assert tuple.size() == tupleHierarchies.size();
    int dimPos = tupleHierarchies.indexOf( member.getHierarchy() );
    if ( dimPos >= 0 ) {
      return tuple.get( dimPos );
    } else if ( eval != null ) {
      return eval.getContext( member.getHierarchy() );
    } else {
      return member.getHierarchy().getDefaultMember();
    }
  }

  // ~ Inner classes ---------------------------------------------------------


  static class SetWrapper {
    List v = new ArrayList();
    public int errorCount = 0, nullCount = 0;

    // private double avg = Double.NaN;
    // TODO: parameterize inclusion of nulls
    // by making this a method of the SetWrapper, we can cache the result
    // this allows its reuse in Correlation
    // public double getAverage() {
    //     if (avg == Double.NaN) {
    //         double sum = 0.0;
    //         for (int i = 0; i < resolvers.size(); i++) {
    //             sum += ((Number) resolvers.elementAt(i)).doubleValue();
    //         }
    //         // TODO: should look at context and optionally include nulls
    //         avg = sum / (double) resolvers.size();
    //     }
    //     return avg;
    // }
  }

  /**
   * Compares cell values, so that larger values compare first.
   *
   * <p>Nulls compare last, exceptions (including the
   * object which indicates the the cell is not in the cache yet) next, then numbers and strings are compared by value.
   */
  public static class DescendingValueComparator implements Comparator {
    /**
     * The singleton.
     */
    static final DescendingValueComparator instance =
      new DescendingValueComparator();

    public int compare( Object o1, Object o2 ) {
      return -compareValues( o1, o2 );
    }
  }

  /**
   * Null member of unknown hierarchy.
   */
  private static class NullMember implements Member {
    public Member getParentMember() {
      throw new UnsupportedOperationException();
    }

    public Level getLevel() {
      throw new UnsupportedOperationException();
    }

    public Hierarchy getHierarchy() {
      throw new UnsupportedOperationException();
    }

    public String getParentUniqueName() {
      throw new UnsupportedOperationException();
    }

    public MemberType getMemberType() {
      throw new UnsupportedOperationException();
    }

    public boolean isParentChildLeaf() {
      return false;
    }

    public boolean isParentChildPhysicalMember() {
      return false;
    }

    public void setName( String name ) {
      throw new UnsupportedOperationException();
    }

    public boolean isAll() {
      return false;
    }

    public boolean isMeasure() {
      throw new UnsupportedOperationException();
    }

    public boolean isNull() {
      return true;
    }

    public boolean isChildOrEqualTo( Member member ) {
      throw new UnsupportedOperationException();
    }

    public boolean isCalculated() {
      throw new UnsupportedOperationException();
    }

    public boolean isEvaluated() {
      throw new UnsupportedOperationException();
    }

    public int getSolveOrder() {
      throw new UnsupportedOperationException();
    }

    public Exp getExpression() {
      throw new UnsupportedOperationException();
    }

    public List<Member> getAncestorMembers() {
      throw new UnsupportedOperationException();
    }

    public boolean isCalculatedInQuery() {
      throw new UnsupportedOperationException();
    }

    public Object getPropertyValue( String propertyName ) {
      throw new UnsupportedOperationException();
    }

    public Object getPropertyValue( String propertyName, boolean matchCase ) {
      throw new UnsupportedOperationException();
    }

    public String getPropertyFormattedValue( String propertyName ) {
      throw new UnsupportedOperationException();
    }

    public void setProperty( String name, Object value ) {
      throw new UnsupportedOperationException();
    }

    public Property[] getProperties() {
      throw new UnsupportedOperationException();
    }

    public int getOrdinal() {
      throw new UnsupportedOperationException();
    }

    public Comparable getOrderKey() {
      throw new UnsupportedOperationException();
    }

    public boolean isHidden() {
      throw new UnsupportedOperationException();
    }

    public int getDepth() {
      throw new UnsupportedOperationException();
    }

    public Member getDataMember() {
      throw new UnsupportedOperationException();
    }

    public String getUniqueName() {
      throw new UnsupportedOperationException();
    }

    @Override public boolean isOnSameHierarchyChain( Member otherMember ) {
      throw new UnsupportedOperationException();
    }

    public String getName() {
      throw new UnsupportedOperationException();
    }

    public String getDescription() {
      throw new UnsupportedOperationException();
    }

    public OlapElement lookupChild(
      SchemaReader schemaReader, Id.Segment s, MatchType matchType ) {
      throw new UnsupportedOperationException();
    }

    public String getQualifiedName() {
      throw new UnsupportedOperationException();
    }

    public String getCaption() {
      throw new UnsupportedOperationException();
    }

    public String getLocalized( LocalizedProperty prop, Locale locale ) {
      throw new UnsupportedOperationException();
    }

    public boolean isVisible() {
      throw new UnsupportedOperationException();
    }

    public Dimension getDimension() {
      throw new UnsupportedOperationException();
    }

    public Map<String, Annotation> getAnnotationMap() {
      throw new UnsupportedOperationException();
    }

    public int compareTo( Object o ) {
      throw new UnsupportedOperationException();
    }

    public boolean equals( Object obj ) {
      throw new UnsupportedOperationException();
    }

    public int hashCode() {
      throw new UnsupportedOperationException();
    }


  }


}

// End FunUtil.java
