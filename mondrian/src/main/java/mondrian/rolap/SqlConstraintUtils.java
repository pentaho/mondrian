/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2020 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.TupleCollections;
import mondrian.calc.TupleIterable;
import mondrian.calc.TupleList;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Access;
import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Role;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.olap.fun.AggregateFunDef;
import mondrian.olap.fun.MemberExtractingVisitor;
import mondrian.olap.fun.ParenthesesFunDef;
import mondrian.olap.fun.ValidMeasureFunDef;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RestrictedMemberReader.MultiCardinalityDefaultMember;
import mondrian.rolap.RolapHierarchy.LimitedRollupMember;
import mondrian.rolap.RolapStar.Column;
import mondrian.rolap.RolapStar.Table;
import mondrian.rolap.agg.AndPredicate;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.agg.ListColumnPredicate;
import mondrian.rolap.agg.LiteralStarPredicate;
import mondrian.rolap.agg.MemberColumnPredicate;
import mondrian.rolap.agg.OrPredicate;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.Dialect;
import mondrian.util.FilteredIterableList;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class used by implementations of {@link mondrian.rolap.sql.SqlConstraint}, used to generate constraints into
 * {@link mondrian.rolap.sql.SqlQuery}.
 *
 * @author av
 * @since Nov 21, 2005
 */
public class SqlConstraintUtils {

  private static final Logger LOG = LogManager.getLogger( SqlConstraintUtils.class );
  private static final MondrianResource mres = MondrianResource.instance();

  /** Utility class */
  private SqlConstraintUtils() {
  }

  /**
   * For every restricting member in the current context, generates a WHERE condition and a join to the fact table.
   *
   * @param sqlQuery
   *          the query to modify
   * @param aggStar
   *          Aggregate table, or null if query is against fact table
   * @param restrictMemberTypes
   *          defines the behavior if the current context contains calculated members. If true, thows an exception.
   * @param evaluator
   *          Evaluator
   */
  public static void addContextConstraint( SqlQuery sqlQuery, AggStar aggStar, Evaluator evaluator, RolapCube baseCube,
      boolean restrictMemberTypes ) {
    if ( baseCube == null && evaluator instanceof RolapEvaluator ) {
      baseCube = ( (RolapEvaluator) evaluator ).getCube();
    }

    RolapEvaluator rEvaluator = (RolapEvaluator) evaluator;
    // decide if we should use the tuple-based version instead
    TupleList slicerTuples = rEvaluator.getOptimizedSlicerTuples( baseCube );
    boolean disjointSlicerTuples = false;
    if ( slicerTuples != null && !slicerTuples.isEmpty() && ( SqlConstraintUtils.isDisjointTuple( slicerTuples )
        || rEvaluator.isMultiLevelSlicerTuple() ) ) {
      disjointSlicerTuples = true;
    }

    TupleConstraintStruct expandedSet =
        makeContextConstraintSet( rEvaluator, restrictMemberTypes, disjointSlicerTuples );

    final CellRequest request = RolapAggregationManager.makeRequest( expandedSet.getMembersArray() );

    if ( request == null ) {
      if ( restrictMemberTypes ) {
        throw Util.newInternal( "CellRequest is null - why?" );
      }
      // One or more of the members was null or calculated, so the
      // request is impossible to satisfy.
      return;
    }

    List<TupleList> slicerTupleList = expandedSet.getDisjoinedTupleLists();

    if ( disjointSlicerTuples ) {
      slicerTupleList.add( slicerTuples );
    }

    // add slicer tuples from the expanded members
    if ( !slicerTupleList.isEmpty() ) {
      LOG.warn( "Using tuple-based native slicer." );
      for ( TupleList tuple : slicerTupleList ) {
        addContextConstraintTuples( sqlQuery, aggStar, rEvaluator, baseCube, restrictMemberTypes, request, tuple );
      }
      return;
    }

    RolapStar.Column[] columns = request.getConstrainedColumns();
    Object[] values = request.getSingleValues();

    Map<MondrianDef.Expression, Set<RolapMember>> mapOfSlicerMembers = null;
    HashMap<MondrianDef.Expression, Boolean> done = new HashMap<MondrianDef.Expression, Boolean>();

    for ( int i = 0; i < columns.length; i++ ) {
      final RolapStar.Column column = columns[i];
      final String value = String.valueOf( values[i] );

      // choose from agg or regular star
      String expr = getColumnExpr( sqlQuery, aggStar, column );

      if ( mapOfSlicerMembers == null ) {
        mapOfSlicerMembers = getSlicerMemberMap( evaluator );
      }

      final MondrianDef.Expression keyForSlicerMap = column.getExpression();

      if ( mapOfSlicerMembers.containsKey( keyForSlicerMap ) ) {
        if ( !done.containsKey( keyForSlicerMap ) ) {
          Set<RolapMember> slicerMembersSet = mapOfSlicerMembers.get( keyForSlicerMap );

          // get only constraining members
          // TODO: can we do this right at getSlicerMemberMap?
          List<RolapMember> slicerMembers = getNonAllMembers( slicerMembersSet );

          if ( !slicerMembers.isEmpty() ) {
            // get level
            final int levelIndex = slicerMembers.get( 0 ).getHierarchy().getLevels().length - 1;
            RolapLevel levelForWhere = (RolapLevel) slicerMembers.get( 0 ).getHierarchy().getLevels()[levelIndex];
            // build where constraint
            final String where =
                generateSingleValueInExpr( sqlQuery, baseCube, aggStar, slicerMembers, levelForWhere,
                    restrictMemberTypes, false, false );
            if ( !where.equals( "" ) ) {
              // The where clause might be null because if the
              // list of members is greater than the limit
              // permitted, we won't constraint.
              sqlQuery.addWhere( where );
            }
          } else {
            addSimpleColumnConstraint( sqlQuery, column, expr, value );
          }
          done.put( keyForSlicerMap, Boolean.TRUE );
        }
        // if done, no op
      } else {
        // column not constrained by slicer
        addSimpleColumnConstraint( sqlQuery, column, expr, value );
      }
    }

    // force Role based Access filtering
    addRoleAccessConstraints( sqlQuery, aggStar, restrictMemberTypes, baseCube, evaluator );
  }

  private static TupleConstraintStruct makeContextConstraintSet( Evaluator evaluator, boolean restrictMemberTypes,
      boolean isTuple ) {
    // Add constraint using the current evaluator context
    List<Member> members = Arrays.asList( evaluator.getNonAllMembers() );

    // Expand the ones that can be expanded. For this particular code line,
    // since this will be used to build a cell request, we need to stay with
    // only one member per ordinal in cube.
    // This follows the same line of thought as the setContext in
    // RolapEvaluator.
    TupleConstraintStruct expandedSet = expandSupportedCalculatedMembers( members, evaluator, isTuple );
    members = expandedSet.getMembers();

    members = getUniqueOrdinalMembers( members );

    if ( restrictMemberTypes ) {
      if ( containsCalculatedMember( members, true ) ) {
        throw Util.newInternal( "can not restrict SQL to calculated Members" );
      }
    } else {
      members = removeCalculatedAndDefaultMembers( members );
    }

    expandedSet.setMembers( members );

    return expandedSet;
  }

  /**
   * Same as {@link addConstraint} but uses tuples
   */
  private static void addContextConstraintTuples( SqlQuery sqlQuery, AggStar aggStar, RolapEvaluator evaluator,
      RolapCube baseCube, boolean restrictMemberTypes, final CellRequest request, TupleList slicerTuples ) {
    assert slicerTuples != null;
    assert !slicerTuples.isEmpty();

    StarPredicate tupleListPredicate =
        getSlicerTuplesPredicate( slicerTuples, baseCube, aggStar, sqlQuery, evaluator );

    // get columns constrained by slicer
    BitKey slicerBitKey = tupleListPredicate.getConstrainedColumnBitKey();
    // constrain context members not in slicer tuples
    RolapStar.Column[] columns = request.getConstrainedColumns();
    Object[] values = request.getSingleValues();
    for ( int i = 0; i < columns.length; i++ ) {
      final RolapStar.Column column = columns[i];
      final String value = String.valueOf( values[i] );
      if ( !slicerBitKey.get( column.getBitPosition() ) ) {
        // column not constrained by tupleSlicer
        String expr = getColumnExpr( sqlQuery, aggStar, column );
        addSimpleColumnConstraint( sqlQuery, column, expr, value );
      }
      // ok to ignore otherwise, using optimizedSlicerTuples
      // that shouldn't have overridden tuples
    }

    // add our slicer tuples
    StringBuilder buffer = new StringBuilder();
    tupleListPredicate.toSql( sqlQuery, buffer );
    sqlQuery.addWhere( buffer.toString() );

    // force Role based Access filtering
    addRoleAccessConstraints( sqlQuery, aggStar, restrictMemberTypes, baseCube, evaluator );
  }

  public static boolean useTupleSlicer( RolapEvaluator evaluator ) {
    return evaluator.isDisjointSlicerTuple() || evaluator.isMultiLevelSlicerTuple();
  }

  public static Map<RolapLevel, List<RolapMember>> getRolesConstraints( Evaluator evaluator ) {
    Member[] mm = evaluator.getMembers();
    SchemaReader schemaReader = evaluator.getSchemaReader();
    Map<RolapLevel, List<RolapMember>> roleConstraints = new LinkedHashMap<RolapLevel, List<RolapMember>>( mm.length );
    for ( Member member : mm ) {
      boolean isRolesMember =
          ( member instanceof LimitedRollupMember ) || ( member instanceof MultiCardinalityDefaultMember );
      if ( isRolesMember ) {
        List<Level> hierarchyLevels = schemaReader.getHierarchyLevels( member.getHierarchy() );
        for ( Level affectedLevel : hierarchyLevels ) {
          List<Member> availableMembers = schemaReader.getLevelMembers( affectedLevel, false );
          List<RolapMember> slicerMembers = new ArrayList<RolapMember>( availableMembers.size() );
          for ( Member available : availableMembers ) {
            if ( !available.isAll() ) {
              slicerMembers.add( (RolapMember) available );
            }
          }
          if ( !slicerMembers.isEmpty() ) {
            roleConstraints.put( (RolapLevel) affectedLevel, slicerMembers );
          }
        }
      }
    }

    return roleConstraints.isEmpty() ? Collections.<RolapLevel, List<RolapMember>> emptyMap() : roleConstraints;
  }

  /**
   * Creates a predicate for the slicer tuple list
   */
  static StarPredicate getSlicerTuplesPredicate( TupleList tupleList, RolapCube baseCube, AggStar aggStar,
      SqlQuery sqlQuery, RolapEvaluator evaluator ) {
    List<StarPredicate> tupleListPredicate = new ArrayList<StarPredicate>();
    for ( List<Member> tuple : tupleList ) {
      tupleListPredicate.add( getTupleConstraint( tuple, baseCube, aggStar, sqlQuery, evaluator ) );
    }
    return new OrPredicate( tupleListPredicate );
  }

  public static boolean isDisjointTuple( TupleList tupleList ) {
    // This assumes the same level for each hierarchy;
    // won't work if the level restriction is eliminated
    List<Set<Member>> counters = new ArrayList<Set<Member>>( tupleList.getArity() );
    for ( int i = 0; i < tupleList.size(); i++ ) {
      final List<Member> tuple = tupleList.get( i );
      for ( int j = 0; j < tupleList.getArity(); j++ ) {
        final Member member = tuple.get( j );
        if ( i == 0 ) {
          counters.add( new HashSet<Member>() );
        }
        counters.get( j ).add( member );
      }
    }
    int piatory = 1;
    for ( Set<Member> counter : counters ) {
      piatory *= counter.size();
    }
    return tupleList.size() < piatory;
  }

  public static boolean hasMultipleLevelSlicer( Evaluator evaluator ) {
    Map<Dimension, Level> levels = new HashMap<Dimension, Level>();
    List<Member> slicerMembers =
        expandSupportedCalculatedMembers( ( (RolapEvaluator) evaluator ).getSlicerMembers(), evaluator ).getMembers();
    for ( Member member : slicerMembers ) {
      if ( member.isAll() ) {
        continue;
      }
      Level before = levels.put( member.getDimension(), member.getLevel() );
      if ( before != null && !before.equals( member.getLevel() ) ) {
        return true;
      }
    }
    return false;
  }

  /**
   * Creates constraint on a single tuple
   */
  private static AndPredicate getTupleConstraint( List<Member> tuple, RolapCube baseCube, AggStar aggStar,
      SqlQuery sqlQuery, RolapEvaluator evaluator ) {
    List<StarPredicate> predicateList = new ArrayList<StarPredicate>();
    // Member[] members = expandSupportedCalculatedMembers(tuple, evaluator);
    for ( Member member : tuple ) {
      addMember( (RolapMember) member, predicateList, baseCube, aggStar, sqlQuery );
    }
    return new AndPredicate( predicateList );
  }

  /**
   * add single member constraint to predicate list
   */
  private static void addMember( RolapMember member, List<StarPredicate> predicateList, RolapCube baseCube,
      AggStar aggStar, SqlQuery sqlQuery ) {
    ArrayList<MemberColumnPredicate> memberList = new ArrayList<MemberColumnPredicate>();
    // add parents until a unique level is reached
    for ( RolapMember currMember = member; currMember != null; currMember = currMember.getParentMember() ) {
      if ( currMember.isAll() ) {
        continue;
      }
      RolapLevel level = currMember.getLevel();
      RolapStar.Column column = getLevelColumn( level, baseCube, aggStar, sqlQuery );
      ( (RolapCubeLevel) level ).getBaseStarKeyColumn( baseCube );
      memberList.add( new MemberColumnPredicate( column, currMember ) );
      if ( level.isUnique() ) {
        break;
      }
    }
    for ( int i = memberList.size() - 1; i >= 0; i-- ) {
      predicateList.add( memberList.get( i ) );
    }
  }

  /**
   * Gets the column, using AggStar if available, and ensures the table is in the query.
   */
  private static RolapStar.Column getLevelColumn( RolapLevel level, RolapCube baseCube, AggStar aggStar,
      SqlQuery sqlQuery ) {
    final RolapStar.Column column = ( (RolapCubeLevel) level ).getBaseStarKeyColumn( baseCube );
    if ( aggStar != null ) {
      int bitPos = column.getBitPosition();
      final AggStar.Table.Column aggColumn = aggStar.lookupColumn( bitPos );
      AggStar.Table table = aggColumn.getTable();
      table.addToFrom( sqlQuery, false, true );
      // create a delegate to use the aggregated column's expression
      return new Column( aggColumn.getDatatype() ) {
        public String generateExprString( SqlQuery query ) {
          // used by predicates for sql generation
          return aggColumn.generateExprString( query );
        }

        public int getBitPosition() {
          // this is the same as the one in RolapStar.Column
          return aggColumn.getBitPosition();
        }

        public Table getTable() {
          return column.getTable();
        }

        public RolapStar getStar() {
          return column.getStar();
        }
      };
    } else {
      column.getTable().addToFrom( sqlQuery, false, true );
      return column;
    }
  }

  /**
   * Get the column expression from the AggStar if provided or the regular table if not, and ensure table is in From
   */
  public static String getColumnExpr( SqlQuery sqlQuery, AggStar aggStar, RolapStar.Column column ) {
    final String expr;
    if ( aggStar != null ) {
      int bitPos = column.getBitPosition();
      AggStar.Table.Column aggColumn = aggStar.lookupColumn( bitPos );
      AggStar.Table table = aggColumn.getTable();
      table.addToFrom( sqlQuery, false, true );
      expr = aggColumn.generateExprString( sqlQuery );
    } else {
      RolapStar.Table table = column.getTable();
      table.addToFrom( sqlQuery, false, true );
      expr = column.generateExprString( sqlQuery );
    }
    return expr;
  }

  /**
   * @return only non-all members
   */
  private static List<RolapMember> getNonAllMembers( Collection<RolapMember> slicerMembersSet ) {
    List<RolapMember> slicerMembers = new ArrayList<RolapMember>( slicerMembersSet );

    // search and destroy [all](s)
    List<RolapMember> allMembers = new ArrayList<RolapMember>();
    for ( RolapMember slicerMember : slicerMembers ) {
      if ( slicerMember.isAll() ) {
        allMembers.add( slicerMember );
      }
    }
    if ( !allMembers.isEmpty() ) {
      slicerMembers.removeAll( allMembers );
    }
    return slicerMembers;
  }

  /**
   * add 'expr = value' to where
   */
  private static void addSimpleColumnConstraint( SqlQuery sqlQuery, RolapStar.Column column, String expr,
      final String value ) {
    if ( ( RolapUtil.mdxNullLiteral().equalsIgnoreCase( value ) ) || ( value.equalsIgnoreCase( RolapUtil.sqlNullValue
        .toString() ) ) ) {
      sqlQuery.addWhere( expr, " is ", RolapUtil.sqlNullLiteral );
    } else {
      if ( column.getDatatype().isNumeric() ) {
        // make sure it can be parsed
        Double.valueOf( value );
      }

      // No extra slicers.... just use the = method
      final StringBuilder buf = new StringBuilder();
      sqlQuery.getDialect().quote( buf, value, column.getDatatype() );
      sqlQuery.addWhere( expr, " = ", buf.toString() );
    }
  }

  public static Map<Level, List<RolapMember>> getRoleConstraintMembers( SchemaReader schemaReader, Member[] members ) {
    // LinkedHashMap keeps insert-order
    Map<Level, List<RolapMember>> roleMembers = new LinkedHashMap<Level, List<RolapMember>>();
    Role role = schemaReader.getRole();
    for ( Member member : members ) {
      if ( member instanceof LimitedRollupMember || member instanceof MultiCardinalityDefaultMember ) {
        // iterate relevant levels to get accessible members
        List<Level> hierarchyLevels = schemaReader.getHierarchyLevels( member.getHierarchy() );
        for ( Level affectedLevel : hierarchyLevels ) {
          List<RolapMember> slicerMembers = new ArrayList<RolapMember>();
          boolean hasCustom = false;
          List<Member> availableMembers = schemaReader.getLevelMembers( affectedLevel, false );
          for ( Member availableMember : availableMembers ) {
            if ( !availableMember.isAll() ) {
              slicerMembers.add( (RolapMember) availableMember );
            }
            hasCustom |= role.getAccess( availableMember ) == Access.CUSTOM;
          }
          if ( !slicerMembers.isEmpty() ) {
            roleMembers.put( affectedLevel, slicerMembers );
          }
          if ( !hasCustom ) {
            // we don't have partial access, no need to go deeper
            break;
          }
        }
      }
    }
    return roleMembers;
  }

  private static void addRoleAccessConstraints( SqlQuery sqlQuery, AggStar aggStar, boolean restrictMemberTypes,
      RolapCube baseCube, Evaluator evaluator ) {
    Map<Level, List<RolapMember>> roleMembers =
        getRoleConstraintMembers( evaluator.getSchemaReader(), evaluator.getMembers() );
    for ( Map.Entry<Level, List<RolapMember>> entry : roleMembers.entrySet() ) {
      final String where =
          generateSingleValueInExpr( sqlQuery, baseCube, aggStar, entry.getValue(), (RolapCubeLevel) entry.getKey(),
              restrictMemberTypes, false, true );
      if ( where.length() > 1 ) {
        // The where clause might be null because if the
        // list of members is greater than the limit
        // permitted, we won't constrain.

        // When dealing with where clauses generated by Roles,it is
        // safer to join the fact table because in all likelihood, this
        // will be required.
        // TODO: check this against MONDRIAN-1133,1201
        joinLevelTableToFactTable( sqlQuery, baseCube, aggStar, evaluator, (RolapCubeLevel) entry.getKey() );
        // add constraints
        sqlQuery.addWhere( where );
      }
    }
  }

  /**
   * Gets a map of MondrianDef.Expression to the set of sliced members associated with each expression.
   *
   * This map is used by addContextConstraint() to get the set of slicer members associated with each column in the cell
   * request's constrained columns array, {@link CellRequest#getConstrainedColumns}
   */
  private static Map<MondrianDef.Expression, Set<RolapMember>> getSlicerMemberMap( Evaluator evaluator ) {
    Map<MondrianDef.Expression, Set<RolapMember>> mapOfSlicerMembers =
        new HashMap<MondrianDef.Expression, Set<RolapMember>>();
    List<Member> slicerMembers = ( (RolapEvaluator) evaluator ).getSlicerMembers();
    List<Member> expandedSlicers =
        evaluator.isEvalAxes() ? expandSupportedCalculatedMembers( slicerMembers, evaluator.push() ).getMembers()
            : slicerMembers;

    if ( hasMultiPositionSlicer( expandedSlicers ) ) {
      for ( Member slicerMember : expandedSlicers ) {
        if ( slicerMember.isMeasure() ) {
          continue;
        }
        addSlicedMemberToMap( mapOfSlicerMembers, slicerMember );
      }
    }
    return mapOfSlicerMembers;
  }

  /**
   * Adds the slicer member and all parent members to mapOfSlicerMembers capturing the sliced members associated with an
   * Expression.
   *
   */
  private static void addSlicedMemberToMap( Map<MondrianDef.Expression, Set<RolapMember>> mapOfSlicerMembers,
      Member slicerMember ) {
    if ( slicerMember == null || slicerMember.isAll() || slicerMember.isNull() ) {
      return;
    }
    assert slicerMember instanceof RolapMember;
    MondrianDef.Expression expression = ( (RolapLevel) slicerMember.getLevel() ).getKeyExp();
    if ( !mapOfSlicerMembers.containsKey( expression ) ) {
      mapOfSlicerMembers.put( expression, new LinkedHashSet<RolapMember>() );
    }
    mapOfSlicerMembers.get( expression ).add( (RolapMember) slicerMember );
    addSlicedMemberToMap( mapOfSlicerMembers, slicerMember.getParentMember() );
  }

  public static boolean hasMultiPositionSlicer( List<Member> slicerMembers ) {
    Map<Hierarchy, Member> mapOfSlicerMembers = new HashMap<Hierarchy, Member>();
    for ( Member slicerMember : slicerMembers ) {
      Hierarchy hierarchy = slicerMember.getHierarchy();
      if ( mapOfSlicerMembers.containsKey( hierarchy ) ) {
        // We have found a second member in this hierarchy
        return true;
      }
      mapOfSlicerMembers.put( hierarchy, slicerMember );
    }
    return false;
  }

  public static TupleConstraintStruct expandSupportedCalculatedMembers( List<Member> members, Evaluator evaluator ) {
    return expandSupportedCalculatedMembers( members, evaluator, false );
  }

  public static TupleConstraintStruct expandSupportedCalculatedMembers( List<Member> members, Evaluator evaluator,
      boolean disjointSlicerTuples ) {
    TupleConstraintStruct expandedSet = new TupleConstraintStruct();
    for ( Member member : members ) {
      expandSupportedCalculatedMember( member, evaluator, disjointSlicerTuples, expandedSet );
    }
    return expandedSet;
  }

  public static void expandSupportedCalculatedMember( Member member, Evaluator evaluator,
      TupleConstraintStruct expandedSet ) {
    expandSupportedCalculatedMember( member, evaluator, false, expandedSet );
  }

  public static void expandSupportedCalculatedMember( Member member, Evaluator evaluator, boolean disjointSlicerTuples,
      TupleConstraintStruct expandedSet ) {
    if ( member.isCalculated() && isSupportedCalculatedMember( member ) ) {
      expandExpressions( member, null, evaluator, expandedSet );
    } else if ( member instanceof RolapResult.CompoundSlicerRolapMember ) {
      if ( !disjointSlicerTuples ) {
        expandedSet.addMember( replaceCompoundSlicerPlaceholder( member, (RolapEvaluator) evaluator ) );
      }
    } else {
      // just the member
      expandedSet.addMember( member );
    }
  }

  static Member replaceCompoundSlicerPlaceholder( Member member, RolapEvaluator evaluator ) {
    Set<Member> slicerMembers = evaluator.getSlicerMembersByHierarchy().get( member.getHierarchy() );
    if ( slicerMembers != null && !slicerMembers.isEmpty() ) {
      return slicerMembers.iterator().next();
    }
    return member;
  }

  public static void expandExpressions( Member member, Exp expression, Evaluator evaluator,
      TupleConstraintStruct expandedSet ) {
    if ( expression == null ) {
      expression = member.getExpression();
    }
    if ( expression instanceof ResolvedFunCall ) {
      ResolvedFunCall fun = (ResolvedFunCall) expression;

      if ( fun.getFunDef() instanceof ParenthesesFunDef ) {
        assert ( fun.getArgCount() == 1 );
        expandExpressions( member, fun.getArg( 0 ), evaluator, expandedSet );
      } else if ( fun.getFunName().equals( "+" ) ) {
        Exp[] expressions = fun.getArgs();
        for ( Exp innerExp : expressions ) {
          expandExpressions( member, innerExp, evaluator, expandedSet );
        }
      } else {
        // Extract the list of members
        expandSetFromCalculatedMember( evaluator, member, expandedSet );
      }
    } else if ( expression instanceof MemberExpr ) {
      expandedSet.addMember( ( (MemberExpr) expression ).getMember() );
    } else {
      expandedSet.addMember( member );
    }
  }

  /**
   * Check to see if this is in a list of supported calculated members. Currently, only the Aggregate and the + function
   * is supported.
   *
   * @return <i>true</i> if the calculated member is supported for native evaluation
   */
  public static boolean isSupportedCalculatedMember( final Member member ) {
    // Is it a supported function?
    return isSupportedExpressionForCalculatedMember( member.getExpression() );
  }

  public static boolean isSupportedExpressionForCalculatedMember( final Exp expression ) {
    if ( expression instanceof ResolvedFunCall ) {
      ResolvedFunCall fun = (ResolvedFunCall) expression;

      if ( fun.getFunDef() instanceof AggregateFunDef ) {
        return true;
      }

      if ( fun.getFunDef() instanceof ParenthesesFunDef ) {
        if ( fun.getArgs().length == 1 ) {
          for ( Exp argsExp : fun.getArgs() ) {
            if ( !isSupportedExpressionForCalculatedMember( argsExp ) ) {
              return false;
            }
          }
        }
        return true;
      }

      if ( fun.getFunDef().getName().equals( "+" ) ) {
        for ( Exp argsExp : fun.getArgs() ) {
          if ( !isSupportedExpressionForCalculatedMember( argsExp ) ) {
            return false;
          }
        }
        return true;
      }
    }

    if ( expression instanceof MemberExpr ) {
      return true;
    }

    return false;
  }

  public static void expandSetFromCalculatedMember( Evaluator evaluator, Member member,
      TupleConstraintStruct expandedSet ) {
    assert member.getExpression() instanceof ResolvedFunCall;

    ResolvedFunCall fun = (ResolvedFunCall) member.getExpression();

    // Calling the main set evaluator to extend this.
    Exp exp = fun.getArg( 0 );
    TupleIterable tupleIterable = evaluator.getSetEvaluator( exp, true ).evaluateTupleIterable();

    Iterator<List<Member>> tupleIterator = tupleIterable.iterator();

    TupleList tupleList = TupleCollections.materialize( tupleIterable, false );

    boolean disjointSlicerTuple = false;
    if ( tupleList != null ) {
      disjointSlicerTuple = SqlConstraintUtils.isDisjointTuple( tupleList );
    }

    if ( disjointSlicerTuple ) {
      if ( !tupleList.isEmpty() ) {
        expandedSet.addTupleList( tupleList );
      }
    } else {
      while ( tupleIterator.hasNext() ) {
        expandedSet.addMembers( tupleIterator.next() );
      }
    }
  }

  /**
   * Gets a list of unique ordinal cube members to make sure our cell request isn't unsatisfiable, following the same
   * logic as RolapEvaluator
   * 
   * @return Unique ordinal cube members
   */
  protected static List<Member> getUniqueOrdinalMembers( List<Member> members ) {
    ArrayList<Integer> currentOrdinals = new ArrayList<Integer>();
    ArrayList<Member> uniqueMembers = new ArrayList<Member>();

    for ( Member member : members ) {
      final RolapMemberBase m = (RolapMemberBase) member;
      int ordinal = m.getHierarchyOrdinal();
      if ( !currentOrdinals.contains( ordinal ) ) {
        uniqueMembers.add( member );
        currentOrdinals.add( ordinal );
      }
    }

    return uniqueMembers;
  }

  protected static Member[] expandMultiPositionSlicerMembers( Member[] members, Evaluator evaluator ) {
    Map<Hierarchy, Set<Member>> mapOfSlicerMembers = null;
    if ( evaluator instanceof RolapEvaluator ) {
      // get the slicer members from the evaluator
      mapOfSlicerMembers = ( (RolapEvaluator) evaluator ).getSlicerMembersByHierarchy();
    }
    if ( mapOfSlicerMembers != null ) {
      List<Member> listOfMembers = new ArrayList<Member>();
      // Iterate the given list of members, removing any whose hierarchy
      // has multiple members on the slicer axis
      for ( Member member : members ) {
        Hierarchy hierarchy = member.getHierarchy();
        if ( !mapOfSlicerMembers.containsKey( hierarchy ) || mapOfSlicerMembers.get( hierarchy ).size() < 2
            || member instanceof RolapResult.CompoundSlicerRolapMember ) {
          listOfMembers.add( member );
        } else {
          listOfMembers.addAll( mapOfSlicerMembers.get( hierarchy ) );
        }
      }
      members = listOfMembers.toArray( new Member[listOfMembers.size()] );
    }
    return members;
  }

  /**
   * Removes calculated and default members from an array.
   *
   * <p>
   * This is required only if the default member is not the ALL member. The time dimension for example, has 1997 as
   * default member. When we evaluate the query
   * 
   * <pre>
   *   select NON EMPTY crossjoin(
   *     {[Time].[1998]}, [Customer].[All].children
   *  ) on columns
   *   from [sales]
   * </pre>
   * 
   * the <code>[Customer].[All].children</code> is evaluated with the default member <code>[Time].[1997]</code> in the
   * evaluator context. This is wrong because the NON EMPTY must filter out Customers with no rows in the fact table for
   * 1998 not 1997. So we do not restrict the time dimension and fetch all children.
   *
   * Package visibility level used for testing
   *
   * <p>
   * For calculated members, effect is the same as {@link #removeCalculatedMembers(java.util.List)}.
   *
   * @param members
   *          Array of members
   * @return Members with calculated members removed (except those that are leaves in a parent-child hierarchy) and with
   *         members that are the default member of their hierarchy
   */
  static List<Member> removeCalculatedAndDefaultMembers( List<Member> members ) {
    List<Member> memberList = new ArrayList<Member>( members.size() );
    Iterator<Member> iterator = members.iterator();
    if ( iterator.hasNext() ) {
      Member firstMember = iterator.next();
      if ( !isMemberCalculated( firstMember ) ) {
        memberList.add( firstMember );
      }

      Member curMember;
      while ( iterator.hasNext() ) {
        curMember = iterator.next();
        if ( !isMemberCalculated( curMember ) && !isMemberDefault( curMember ) ) {
          memberList.add( curMember );
        }
      }
    }

    return memberList;
  }

  private static boolean isMemberCalculated( Member member ) {
    // Skip calculated members (except if leaf of parent-child hier)
    return member.isCalculated() && !member.isParentChildLeaf();
  }

  private static boolean isMemberDefault( Member member ) {
    // Remove members that are the default for their hierarchy,
    // except for the measures hierarchy.
    return member.getHierarchy().getDefaultMember().equals( member );
  }

  static List<Member> removeCalculatedMembers( List<Member> members ) {
    return new FilteredIterableList<Member>( members, new FilteredIterableList.Filter<Member>() {
      public boolean accept( final Member m ) {
        return !m.isCalculated() || m.isParentChildPhysicalMember();
      }
    } );
  }

  public static boolean containsCalculatedMember( List<Member> members ) {
    return containsCalculatedMember( members, false );
  }

  public static boolean containsCalculatedMember( List<Member> members, boolean allowExpandableMembers ) {
    for ( Member member : members ) {
      if ( member.isCalculated() ) {
        if ( allowExpandableMembers ) {
          if ( !isSupportedCalculatedMember( member ) ) {
            return true;
          }
        } else {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Ensures that the table of <code>level</code> is joined to the fact table
   *
   * @param sqlQuery
   *          sql query under construction
   * @param aggStar
   *          The aggStar to use, if any.
   * @param e
   *          evaluator corresponding to query
   * @param level
   *          level to be added to query
   */
  public static void joinLevelTableToFactTable( SqlQuery sqlQuery, RolapCube baseCube, AggStar aggStar, Evaluator e,
      RolapCubeLevel level ) {
    RolapStar.Column starColumn = level.getBaseStarKeyColumn( baseCube );
    if ( aggStar != null ) {
      int bitPos = starColumn.getBitPosition();
      AggStar.Table.Column aggColumn = aggStar.lookupColumn( bitPos );
      AggStar.Table table = aggColumn.getTable();
      table.addToFrom( sqlQuery, false, true );
    } else {
      RolapStar.Table table = starColumn.getTable();
      assert table != null;
      table.addToFrom( sqlQuery, false, true );
    }
  }

  /**
   * Creates a "WHERE parent = value" constraint.
   *
   * @param sqlQuery
   *          the query to modify
   * @param baseCube
   *          base cube if virtual
   * @param aggStar
   *          Definition of the aggregate table, or null
   * @param parent
   *          the list of parent members
   * @param restrictMemberTypes
   *          defines the behavior if <code>parent</code> is a calculated member. If true, an exception is thrown
   */
  public static void addMemberConstraint( SqlQuery sqlQuery, RolapCube baseCube, AggStar aggStar, RolapMember parent,
      boolean restrictMemberTypes ) {
    List<RolapMember> list = Collections.singletonList( parent );
    boolean exclude = false;
    addMemberConstraint( sqlQuery, baseCube, aggStar, list, restrictMemberTypes, false, exclude );
  }

  /**
   * Creates a "WHERE exp IN (...)" condition containing the values of all parents. All parents must belong to the same
   * level.
   *
   * <p>
   * If this constraint is part of a native cross join, there are multiple constraining members, and the members
   * comprise the cross product of all unique member keys referenced at each level, then generating IN expressions would
   * result in incorrect results. In that case, "WHERE ((level1 = val1a AND level2 = val2a AND ...) OR (level1 = val1b
   * AND level2 = val2b AND ...) OR ..." is generated instead.
   *
   * @param sqlQuery
   *          the query to modify
   * @param baseCube
   *          base cube if virtual
   * @param aggStar
   *          (not used)
   * @param members
   *          the list of members for this constraint
   * @param restrictMemberTypes
   *          defines the behavior if <code>parents</code> contains calculated members. If true, and one of the members
   *          is calculated, an exception is thrown.
   * @param crossJoin
   *          true if constraint is being generated as part of a native crossjoin
   * @param exclude
   *          whether to exclude the members in the SQL predicate. e.g. not in { member list}.
   */
  public static void addMemberConstraint( SqlQuery sqlQuery, RolapCube baseCube, AggStar aggStar,
      List<RolapMember> members, boolean restrictMemberTypes, boolean crossJoin, boolean exclude ) {
    if ( members.isEmpty() ) {
      // Generate a predicate which is always false in order to produce
      // the empty set. It would be smarter to avoid executing SQL at
      // all in this case, but doing it this way avoid special-case
      // evaluation code.
      String predicate = "(1 = 0)";
      if ( exclude ) {
        predicate = "(1 = 1)";
      }
      sqlQuery.addWhere( predicate );
      return;
    }

    // Find out the first(lowest) unique parent level.
    // Only need to compare members up to that level.
    RolapMember member = members.get( 0 );
    RolapLevel memberLevel = member.getLevel();
    RolapMember firstUniqueParent = member;
    RolapLevel firstUniqueParentLevel = null;
    for ( ; firstUniqueParent != null && !firstUniqueParent.getLevel().isUnique(); firstUniqueParent =
        firstUniqueParent.getParentMember() ) {
    }

    if ( firstUniqueParent != null ) {
      // There's a unique parent along the hierarchy
      firstUniqueParentLevel = firstUniqueParent.getLevel();
    }

    String condition = "(";

    // If this constraint is part of a native cross join and there
    // are multiple values for the parent members, then we can't
    // use single value IN clauses
    if ( crossJoin && !memberLevel.isUnique() && !membersAreCrossProduct( members ) ) {
      assert ( member.getParentMember() != null );
      condition +=
          constrainMultiLevelMembers( sqlQuery, baseCube, aggStar, members, firstUniqueParentLevel,
              restrictMemberTypes, exclude );
    } else {
      condition +=
          generateSingleValueInExpr( sqlQuery, baseCube, aggStar, members, firstUniqueParentLevel, restrictMemberTypes,
              exclude, true );
    }

    if ( condition.length() > 1 ) {
      // condition is not empty
      condition += ")";
      sqlQuery.addWhere( condition );
    }
  }

  private static StarColumnPredicate getColumnPredicates( RolapStar.Column column, Collection<RolapMember> members ) {
    switch ( members.size() ) {
      case 0:
        return new LiteralStarPredicate( column, false );
      case 1:
        return new MemberColumnPredicate( column, members.iterator().next() );
      default:
        List<StarColumnPredicate> predicateList = new ArrayList<StarColumnPredicate>();
        for ( RolapMember member : members ) {
          predicateList.add( new MemberColumnPredicate( column, member ) );
        }
        return new ListColumnPredicate( column, predicateList );
    }
  }

  private static LinkedHashSet<RolapMember> getUniqueParentMembers( Collection<RolapMember> members ) {
    LinkedHashSet<RolapMember> set = new LinkedHashSet<RolapMember>();
    for ( RolapMember m : members ) {
      m = m.getParentMember();
      if ( m != null ) {
        set.add( m );
      }
    }
    return set;
  }

  /**
   * Adds to the where clause of a query expression matching a specified list of members
   *
   * @param sqlQuery
   *          query containing the where clause
   * @param baseCube
   *          base cube if virtual
   * @param aggStar
   *          aggregate star if available
   * @param members
   *          list of constraining members
   * @param fromLevel
   *          lowest parent level that is unique
   * @param restrictMemberTypes
   *          defines the behavior when calculated members are present
   * @param exclude
   *          whether to exclude the members. Default is false.
   *
   * @return a non-empty String if SQL is generated for the multi-level member list.
   */
  private static String constrainMultiLevelMembers( SqlQuery sqlQuery, RolapCube baseCube, AggStar aggStar,
      List<RolapMember> members, RolapLevel fromLevel, boolean restrictMemberTypes, boolean exclude ) {
    // Use LinkedHashMap so that keySet() is deterministic.
    Map<RolapMember, List<RolapMember>> parentChildrenMap = new LinkedHashMap<RolapMember, List<RolapMember>>();
    StringBuilder condition = new StringBuilder();
    StringBuilder condition1 = new StringBuilder();
    if ( exclude ) {
      condition.append( "not (" );
    }

    // First try to generate IN list for all members
    if ( sqlQuery.getDialect().supportsMultiValueInExpr() ) {
      condition1.append( generateMultiValueInExpr( sqlQuery, baseCube, aggStar, members, fromLevel,
          restrictMemberTypes, parentChildrenMap ) );

      // The members list might contain NULL values in the member levels.
      //
      // e.g.
      // [USA].[CA].[San Jose]
      // [null].[null].[San Francisco]
      // [null].[null].[Los Angeles]
      // [null].[CA].[San Diego]
      // [null].[CA].[Sacramento]
      //
      // Pick out such members to generate SQL later.
      // These members are organized in a map that maps the parant levels
      // containing NULL to all its children members in the list. e.g.
      // the member list above becomes the following map, after SQL is
      // generated for [USA].[CA].[San Jose] in the call above.
      //
      // [null].[null]->([San Francisco], [Los Angeles])
      // [null]->([CA].[San Diego], [CA].[Sacramento])
      //
      if ( parentChildrenMap.isEmpty() ) {
        condition.append( condition1.toString() );
        if ( exclude ) {
          // If there are no NULL values in the member levels, then
          // we're done except we need to also explicitly include
          // members containing nulls across all levels.
          condition.append( ")" );
          condition.append( " or " );
          condition.append( generateMultiValueIsNullExprs( sqlQuery, baseCube, members.get( 0 ), fromLevel,
              aggStar ) );
        }
        return condition.toString();
      }
    } else {
      // Multi-value IN list not supported
      // Classify members into List that share the same parent.
      //
      // Using the same example as above, the resulting map will be
      // [USA].[CA]->[San Jose]
      // [null].[null]->([San Francisco], [Los Angesles])
      // [null].[CA]->([San Diego],[Sacramento])
      //
      // The idea is to be able to "compress" the original member list
      // into groups that can use single value IN list for part of the
      // comparison that does not involve NULLs
      //
      for ( RolapMember m : members ) {
        if ( m.isCalculated() ) {
          if ( restrictMemberTypes ) {
            throw Util.newInternal( "addMemberConstraint: cannot " + "restrict SQL to calculated member :" + m );
          }
          continue;
        }
        RolapMember p = m.getParentMember();
        List<RolapMember> childrenList = parentChildrenMap.get( p );
        if ( childrenList == null ) {
          childrenList = new ArrayList<RolapMember>();
          parentChildrenMap.put( p, childrenList );
        }
        childrenList.add( m );
      }
    }

    // Now we try to generate predicates for the remaining
    // parent-children group.

    // Note that NULLs are not used to enforce uniqueness
    // so we ignore the fromLevel here.
    boolean firstParent = true;
    StringBuilder condition2 = new StringBuilder();

    if ( condition1.length() > 0 ) {
      // Some members have already been translated into IN list.
      firstParent = false;
      condition.append( condition1.toString() );
      condition.append( " or " );
    }

    RolapLevel memberLevel = members.get( 0 ).getLevel();

    // The children for each parent are turned into IN list so they
    // should not contain null.
    for ( RolapMember p : parentChildrenMap.keySet() ) {
      assert p != null;
      if ( condition2.toString().length() > 0 ) {
        condition2.append( " or " );
      }

      condition2.append( "(" );

      // First generate ANDs for all members in the parent lineage of
      // this parent-children group
      int levelCount = 0;
      for ( RolapMember gp = p; gp != null; gp = gp.getParentMember() ) {
        if ( gp.isAll() ) {
          // Ignore All member
          // Get the next parent
          continue;
        }

        RolapLevel level = gp.getLevel();

        // add the level to the FROM clause if this is the
        // first parent-children group we're generating sql for
        if ( firstParent ) {
          RolapHierarchy hierarchy = level.getHierarchy();

          // this method can be called within the context of shared
          // members, outside of the normal rolap star, therefore
          // we need to check the level to see if it is a shared or
          // cube level.

          RolapStar.Column column = null;
          if ( level instanceof RolapCubeLevel ) {
            column = ( (RolapCubeLevel) level ).getBaseStarKeyColumn( baseCube );
          }
          if ( column != null ) {
            if ( aggStar != null ) {
              int bitPos = column.getBitPosition();
              AggStar.Table.Column aggColumn = aggStar.lookupColumn( bitPos );
              AggStar.Table table = aggColumn.getTable();
              table.addToFrom( sqlQuery, false, true );
            } else {
              RolapStar.Table targetTable = column.getTable();
              hierarchy.addToFrom( sqlQuery, targetTable );
            }
          } else {
            assert ( aggStar == null );
            hierarchy.addToFrom( sqlQuery, level.getKeyExp() );
          }
        }

        if ( levelCount > 0 ) {
          condition2.append( " and " );
        }
        ++levelCount;

        condition2.append( constrainLevel( level, sqlQuery, baseCube, aggStar, getColumnValue( level.nameExp != null
            ? gp.getName() : gp.getKey(), sqlQuery.getDialect(), level.getDatatype() ), false ) );
        if ( gp.getLevel() == fromLevel ) {
          // SQL is completely generated for this parent
          break;
        }
      }
      firstParent = false;

      // Next, generate children for this parent-children group
      List<RolapMember> children = parentChildrenMap.get( p );

      // If no children to be generated for this parent then we are done
      if ( !children.isEmpty() ) {
        Map<RolapMember, List<RolapMember>> tmpParentChildrenMap = new HashMap<RolapMember, List<RolapMember>>();

        if ( levelCount > 0 ) {
          condition2.append( " and " );
        }
        RolapLevel childrenLevel = (RolapLevel) ( p.getLevel().getChildLevel() );

        if ( sqlQuery.getDialect().supportsMultiValueInExpr() && childrenLevel != memberLevel ) {
          // Multi-level children and multi-value IN list supported
          condition2.append( generateMultiValueInExpr( sqlQuery, baseCube, aggStar, children, childrenLevel,
              restrictMemberTypes, tmpParentChildrenMap ) );
          assert tmpParentChildrenMap.isEmpty();
        } else {
          // Can only be single level children
          // If multi-value IN list not supported, children will be on
          // the same level as members list. Only single value IN list
          // needs to be generated for this case.
          assert childrenLevel == memberLevel;
          condition2.append( generateSingleValueInExpr( sqlQuery, baseCube, aggStar, children, childrenLevel,
              restrictMemberTypes, false, true ) );
        }
      }
      // SQL is complete for this parent-children group.
      condition2.append( ")" );
    }

    // In the case where multi-value IN expressions are not generated,
    // condition2 contains the entire filter condition. In the
    // case of excludes, we also need to explicitly include null values,
    // minus the ones that are referenced in condition2. Therefore,
    // we OR on a condition that corresponds to an OR'ing of IS NULL
    // filters on each level PLUS an exclusion of condition2.
    //
    // Note that the expression generated is non-optimal in the case where
    // multi-value IN's cannot be used because we end up excluding
    // non-null values as well as the null ones. Ideally, we only need to
    // exclude the expressions corresponding to nulls, which is possible
    // in the multi-value IN case, since we have a map of the null values.
    condition.append( condition2.toString() );
    if ( exclude ) {
      condition.append( ") or (" );
      condition.append( generateMultiValueIsNullExprs( sqlQuery, baseCube, members.get( 0 ), fromLevel, aggStar ) );
      condition.append( " and not(" );
      condition.append( condition2.toString() );
      condition.append( "))" );
    }

    return condition.toString();
  }

  /**
   * @param members
   *          list of members
   *
   * @return true if the members comprise the cross product of all unique member keys referenced at each level
   */
  private static boolean membersAreCrossProduct( List<RolapMember> members ) {
    int crossProdSize = getNumUniqueMemberKeys( members );
    for ( Collection<RolapMember> parents = getUniqueParentMembers( members ); !parents.isEmpty(); parents =
        getUniqueParentMembers( parents ) ) {
      crossProdSize *= parents.size();
    }
    return ( crossProdSize == members.size() );
  }

  /**
   * @param members
   *          list of members
   *
   * @return number of unique member keys in a list of members
   */
  private static int getNumUniqueMemberKeys( List<RolapMember> members ) {
    final HashSet<Object> set = new HashSet<Object>();
    for ( RolapMember m : members ) {
      set.add( m.getKey() );
    }
    return set.size();
  }

  /**
   * @param key
   *          key corresponding to a member
   * @param dialect
   *          sql dialect being used
   * @param datatype
   *          data type of the member
   *
   * @return string value corresponding to the member
   */
  private static String getColumnValue( Object key, Dialect dialect, Dialect.Datatype datatype ) {
    if ( key != RolapUtil.sqlNullValue ) {
      return key.toString();
    } else {
      return RolapUtil.mdxNullLiteral();
    }
  }

  public static String constrainLevel( RolapLevel level, SqlQuery query, RolapCube baseCube, AggStar aggStar,
      String columnValue, boolean caseSensitive ) {
    return constrainLevel( level, query, baseCube, aggStar, new String[] { columnValue }, caseSensitive );
  }

  /**
   * Generates a sql expression constraining a level by some value
   *
   * @param level
   *          the level
   * @param query
   *          the query that the sql expression will be added to
   * @param baseCube
   *          base cube for virtual levels
   * @param aggStar
   *          aggregate star if available
   * @param columnValue
   *          value constraining the level
   * @param caseSensitive
   *          if true, need to handle case sensitivity of the member value
   *
   * @return generated string corresponding to the expression
   */
  public static String constrainLevel( RolapLevel level, SqlQuery query, RolapCube baseCube, AggStar aggStar,
      String[] columnValue, boolean caseSensitive ) {
    // this method can be called within the context of shared members,
    // outside of the normal rolap star, therefore we need to
    // check the level to see if it is a shared or cube level.

    RolapStar.Column column = null;
    if ( level instanceof RolapCubeLevel ) {
      column = ( (RolapCubeLevel) level ).getBaseStarKeyColumn( baseCube );
    }

    String columnString;
    Dialect.Datatype datatype;
    if ( column != null ) {
      if ( column.getNameColumn() == null ) {
        datatype = level.getDatatype();
      } else {
        column = column.getNameColumn();
        // The schema doesn't specify the datatype of the name column,
        // but we presume that it is a string.
        datatype = Dialect.Datatype.String;
      }
      if ( aggStar != null ) {
        // this makes the assumption that the name column is the same
        // as the key column
        int bitPos = column.getBitPosition();
        AggStar.Table.Column aggColumn = aggStar.lookupColumn( bitPos );

        if ( aggColumn == null ) {
          LOG.warn( mres.AggTableNoConstraintGenerated.str( aggStar.getFactTable().getName() ) );
          return "";
        }

        columnString = aggColumn.generateExprString( query );
      } else {
        columnString = column.generateExprString( query );
      }
    } else {
      assert ( aggStar == null );
      MondrianDef.Expression exp = level.getNameExp();
      if ( exp == null ) {
        exp = level.getKeyExp();
        datatype = level.getDatatype();
      } else {
        // The schema doesn't specify the datatype of the name column,
        // but we presume that it is a string.
        datatype = Dialect.Datatype.String;
      }
      columnString = exp.getExpression( query );
    }

    String constraint;

    constraint = getColumnValueConstraint( query, columnValue, caseSensitive, columnString, datatype );

    return constraint;
  }

  private static String getColumnValueConstraint( SqlQuery query, String[] columnValues, boolean caseSensitive,
      String columnString, Dialect.Datatype datatype ) {
    String constraint;
    List<String> values = new ArrayList<String>();
    boolean containsNull = false;

    for ( String columnValue : columnValues ) {
      if ( RolapUtil.mdxNullLiteral().equalsIgnoreCase( columnValue ) ) {
        containsNull = true;
        // constraint = columnString + " is " + RolapUtil.sqlNullLiteral;
      } else {
        if ( datatype.isNumeric() ) {
          // make sure it can be parsed
          Double.valueOf( columnValue );
        }
        final StringBuilder buf = new StringBuilder();
        query.getDialect().quote( buf, columnValue, datatype );
        String value = buf.toString();
        if ( caseSensitive && datatype == Dialect.Datatype.String ) {
          // Some databases (like DB2) compare case-sensitive.
          // We convert
          // the value to upper-case in the DBMS (e.g. UPPER('Foo'))
          // rather than in Java (e.g. 'FOO') in case the DBMS is
          // running a different locale.
          if ( !MondrianProperties.instance().CaseSensitive.get() ) {
            value = query.getDialect().toUpper( value );
          }
        }
        values.add( value );
      }
    }

    if ( caseSensitive && datatype == Dialect.Datatype.String && !MondrianProperties.instance().CaseSensitive.get() ) {
      columnString = query.getDialect().toUpper( columnString );
    }

    if ( values.size() == 1 ) {
      if ( containsNull ) {
        constraint = columnString + " IS " + RolapUtil.sqlNullLiteral;
      } else {
        constraint = columnString + " = " + values.get( 0 );
      }
    } else {
      StringBuilder builder = new StringBuilder();
      builder.append( "( " );
      if ( !values.isEmpty() ) {
        builder.append( columnString ).append( " IN (" );
        for ( int i = 0; i < values.size(); i++ ) {
          String value = values.get( i );
          builder.append( value );
          if ( i < values.size() - 1 ) {
            builder.append( "," );
          }
        }
        builder.append( ")" );
      }
      if ( containsNull ) {
        if ( !values.isEmpty() ) {
          builder.append( " OR " );
        }
        builder.append( columnString ).append( " IS NULL " );
      }
      builder.append( ")" );
      constraint = builder.toString();
    }
    return constraint;
  }

  /**
   * Generates a sql expression constraining a level by some value
   *
   * @param exp
   *          Key expression
   * @param datatype
   *          Key datatype
   * @param query
   *          the query that the sql expression will be added to
   * @param columnValue
   *          value constraining the level
   *
   * @return generated string corresponding to the expression
   */
  public static String constrainLevel2( SqlQuery query, MondrianDef.Expression exp, Dialect.Datatype datatype,
      Comparable columnValue ) {
    String columnString = exp.getExpression( query );
    if ( columnValue == RolapUtil.sqlNullValue ) {
      return columnString + " is " + RolapUtil.sqlNullLiteral;
    } else {
      final StringBuilder buf = new StringBuilder();
      buf.append( columnString );
      buf.append( " = " );
      query.getDialect().quote( buf, columnValue, datatype );
      return buf.toString();
    }
  }

  /**
   * Generates a multi-value IN expression corresponding to a list of member expressions, and adds the expression to the
   * WHERE clause of a query, provided the member values are all non-null
   *
   * @param sqlQuery
   *          query containing the where clause
   * @param baseCube
   *          base cube if virtual
   * @param aggStar
   *          aggregate star if available
   * @param members
   *          list of constraining members
   * @param fromLevel
   *          lowest parent level that is unique
   * @param restrictMemberTypes
   *          defines the behavior when calculated members are present
   * @param parentWithNullToChildrenMap
   *          upon return this map contains members that have Null values in its (parent) levels
   * @return a non-empty String if multi-value IN list was generated for some members
   */
  private static String generateMultiValueInExpr( SqlQuery sqlQuery, RolapCube baseCube, AggStar aggStar,
      List<RolapMember> members, RolapLevel fromLevel, boolean restrictMemberTypes,
      Map<RolapMember, List<RolapMember>> parentWithNullToChildrenMap ) {
    final StringBuilder columnBuf = new StringBuilder();
    final StringBuilder valueBuf = new StringBuilder();
    final StringBuilder memberBuf = new StringBuilder();

    columnBuf.append( "(" );

    // generate the left-hand side of the IN expression
    int ordinalInMultiple = 0;
    for ( RolapMember m = members.get( 0 ); m != null; m = m.getParentMember() ) {
      if ( m.isAll() ) {
        continue;
      }
      String columnString = getColumnString( sqlQuery, aggStar, m.getLevel(), baseCube );

      if ( ordinalInMultiple++ > 0 ) {
        columnBuf.append( ", " );
      }

      columnBuf.append( columnString );

      // Only needs to compare up to the first(lowest) unique level.
      if ( m.getLevel() == fromLevel ) {
        break;
      }
    }

    columnBuf.append( ")" );

    // generate the RHS of the IN predicate
    valueBuf.append( "(" );
    int memberOrdinal = 0;
    for ( RolapMember m : members ) {
      if ( m.isCalculated() ) {
        if ( restrictMemberTypes ) {
          throw Util.newInternal( "addMemberConstraint: cannot " + "restrict SQL to calculated member :" + m );
        }
        continue;
      }

      ordinalInMultiple = 0;
      memberBuf.setLength( 0 );
      memberBuf.append( "(" );

      boolean containsNull = false;
      for ( RolapMember p = m; p != null; p = p.getParentMember() ) {
        if ( p.isAll() ) {
          // Ignore the ALL level.
          // Generate SQL condition for the next level
          continue;
        }
        RolapLevel level = p.getLevel();

        String value = getColumnValue( p.getKey(), sqlQuery.getDialect(), level.getDatatype() );

        // If parent at a level is NULL, record this parent and all
        // its children(if there's any)
        if ( RolapUtil.mdxNullLiteral().equalsIgnoreCase( value ) ) {
          // Add to the nullParent map
          List<RolapMember> childrenList = parentWithNullToChildrenMap.get( p );
          if ( childrenList == null ) {
            childrenList = new ArrayList<RolapMember>();
            parentWithNullToChildrenMap.put( p, childrenList );
          }

          // If p has children
          if ( m != p ) {
            childrenList.add( m );
          }

          // Skip generating condition for this parent
          containsNull = true;
          break;
        }

        if ( ordinalInMultiple++ > 0 ) {
          memberBuf.append( ", " );
        }

        sqlQuery.getDialect().quote( memberBuf, value, level.getDatatype() );

        // Only needs to compare up to the first(lowest) unique level.
        if ( p.getLevel() == fromLevel ) {
          break;
        }
      }

      // Now check if sql string is sucessfully generated for this member.
      // If parent levels do not contain NULL then SQL must have been
      // generated successfully.
      if ( !containsNull ) {
        memberBuf.append( ")" );
        if ( memberOrdinal++ > 0 ) {
          valueBuf.append( ", " );
        }
        valueBuf.append( memberBuf );
      }
    }

    StringBuilder condition = new StringBuilder();
    if ( memberOrdinal > 0 ) {
      // SQLs are generated for some members.
      condition.append( columnBuf );
      condition.append( " in " );
      condition.append( valueBuf );
      condition.append( ")" );
    }

    return condition.toString();
  }

  /**
   * Returns the column expression for the level, assuring appropriate tables are added to the from clause of sqlQuery
   * if required. Determines the correct table and field based on the cube and whether an AggStar is present.
   */
  private static String getColumnString( SqlQuery sqlQuery, AggStar aggStar, RolapLevel level, RolapCube baseCube ) {
    String columnString;
    RolapStar.Column column = null;
    if ( level instanceof RolapCubeLevel ) {
      // this method can be called within the context of shared members,
      // outside of the normal rolap star, therefore we need to
      // check the level to see if it is a shared or cube level.
      column = ( (RolapCubeLevel) level ).getBaseStarKeyColumn( baseCube );
    }

    // REVIEW: The following code mostly uses the name column (or name
    // expression) of the level. Shouldn't it use the key column (or key
    // expression)?
    RolapHierarchy hierarchy = level.getHierarchy();
    if ( column != null ) {
      if ( aggStar != null ) {
        // this assumes that the name column is identical to the
        // id column
        int bitPos = column.getBitPosition();
        AggStar.Table.Column aggColumn = aggStar.lookupColumn( bitPos );
        AggStar.Table table = aggColumn.getTable();
        table.addToFrom( sqlQuery, false, true );
        columnString = aggColumn.generateExprString( sqlQuery );
      } else {
        RolapStar.Table targetTable = column.getTable();
        hierarchy.addToFrom( sqlQuery, targetTable );
        columnString = column.generateExprString( sqlQuery );
      }
    } else {
      assert ( aggStar == null );
      hierarchy.addToFrom( sqlQuery, level.getKeyExp() );

      MondrianDef.Expression nameExp = level.getNameExp();
      if ( nameExp == null ) {
        nameExp = level.getKeyExp();
      }
      columnString = nameExp.getExpression( sqlQuery );
    }
    return columnString;
  }

  /**
   * Generates an expression that is an OR of IS NULL expressions, one per level in a RolapMember.
   *
   * @param sqlQuery
   *          query corresponding to the expression
   * @param baseCube
   *          base cube if virtual
   * @param member
   *          the RolapMember
   * @param fromLevel
   *          lowest parent level that is unique
   * @param aggStar
   *          aggregate star if available
   * @return the text of the expression
   */
  private static String generateMultiValueIsNullExprs( SqlQuery sqlQuery, RolapCube baseCube, RolapMember member,
      RolapLevel fromLevel, AggStar aggStar ) {
    final StringBuilder conditionBuf = new StringBuilder();

    conditionBuf.append( "(" );

    // generate the left-hand side of the IN expression
    boolean isFirstLevelInMultiple = true;
    for ( RolapMember m = member; m != null; m = m.getParentMember() ) {
      if ( m.isAll() ) {
        continue;
      }

      String columnString = getColumnString( sqlQuery, aggStar, m.getLevel(), baseCube );

      if ( !isFirstLevelInMultiple ) {
        conditionBuf.append( " or " );
      } else {
        isFirstLevelInMultiple = false;
      }

      conditionBuf.append( columnString );
      conditionBuf.append( " is null" );

      // Only needs to compare up to the first(lowest) unique level.
      if ( m.getLevel() == fromLevel ) {
        break;
      }
    }

    conditionBuf.append( ")" );
    return conditionBuf.toString();
  }

  /**
   * Generates a multi-value IN expression corresponding to a list of member expressions, and adds the expression to the
   * WHERE clause of a query, provided the member values are all non-null
   *
   *
   * @param sqlQuery
   *          query containing the where clause
   * @param baseCube
   *          base cube if virtual
   * @param aggStar
   *          aggregate star if available
   * @param members
   *          list of constraining members
   * @param fromLevel
   *          lowest parent level that is unique
   * @param restrictMemberTypes
   *          defines the behavior when calculated members are present
   * @param exclude
   *          whether to exclude the members. Default is false.
   * @param includeParentLevels
   *          whether to include IN list constraint for parent levels.
   * @return a non-empty String if IN list was generated for the members.
   */
  private static String generateSingleValueInExpr( SqlQuery sqlQuery, RolapCube baseCube, AggStar aggStar,
      List<RolapMember> members, RolapLevel fromLevel, boolean restrictMemberTypes, boolean exclude,
      boolean includeParentLevels ) {
    int maxConstraints = MondrianProperties.instance().MaxConstraints.get();
    Dialect dialect = sqlQuery.getDialect();

    String condition = "";
    boolean firstLevel = true;
    for ( Collection<RolapMember> c = members; !c.isEmpty(); c = getUniqueParentMembers( c ) ) {
      RolapMember m = c.iterator().next();
      if ( m.isAll() ) {
        continue;
      }
      if ( m.isNull() ) {
        return "1 = 0";
      }
      if ( m.isCalculated() && !m.isParentChildLeaf() ) {
        if ( restrictMemberTypes ) {
          throw Util.newInternal( "addMemberConstraint: cannot " + "restrict SQL to calculated member :" + m );
        }
        continue;
      }

      boolean containsNullKey = false;
      Iterator<RolapMember> it = c.iterator();
      while ( it.hasNext() ) {
        m = it.next();
        if ( m.getKey() == RolapUtil.sqlNullValue ) {
          containsNullKey = true;
        }
      }

      RolapLevel level = m.getLevel();
      RolapHierarchy hierarchy = level.getHierarchy();

      // this method can be called within the context of shared members,
      // outside of the normal rolap star, therefore we need to
      // check the level to see if it is a shared or cube level.

      RolapStar.Column column = null;
      if ( level instanceof RolapCubeLevel ) {
        column = ( (RolapCubeLevel) level ).getBaseStarKeyColumn( baseCube );
      }

      String q;
      if ( column != null ) {
        if ( aggStar != null ) {
          int bitPos = column.getBitPosition();
          AggStar.Table.Column aggColumn = aggStar.lookupColumn( bitPos );
          if ( aggColumn == null ) {
            throw Util.newInternal( "AggStar " + aggStar + " has no column for " + column + " (bitPos " + bitPos
                + ")" );
          }
          AggStar.Table table = aggColumn.getTable();
          table.addToFrom( sqlQuery, false, true );
          q = aggColumn.generateExprString( sqlQuery );
        } else {
          RolapStar.Table targetTable = column.getTable();
          hierarchy.addToFrom( sqlQuery, targetTable );
          q = column.generateExprString( sqlQuery );
        }
      } else {
        assert ( aggStar == null );
        hierarchy.addToFrom( sqlQuery, level.getKeyExp() );
        q = level.getKeyExp().getExpression( sqlQuery );
      }

      StarColumnPredicate cc = getColumnPredicates( column, c );

      if ( !dialect.supportsUnlimitedValueList() && cc instanceof ListColumnPredicate && ( (ListColumnPredicate) cc )
          .getPredicates().size() > maxConstraints ) {
        // Simply get them all, do not create where-clause.
        // Below are two alternative approaches (and code). They
        // both have problems.
        LOG.debug( MondrianResource.instance().NativeSqlInClauseTooLarge.str( level.getUniqueName(), maxConstraints
            + "" ) );
        sqlQuery.setSupported( false );
      } else {
        String where = RolapStar.Column.createInExpr( q, cc, level.getDatatype(), sqlQuery );
        if ( !where.equals( "true" ) ) {
          if ( !firstLevel ) {
            if ( exclude ) {
              condition += " or ";
            } else {
              condition += " and ";
            }
          } else {
            firstLevel = false;
          }
          if ( exclude ) {
            where = "not (" + where + ")";
            if ( !containsNullKey ) {
              // Null key fails all filters so should add it here
              // if not already excluded. E.g., if the original
              // exclusion filter is :
              //
              // not(year = '1997' and quarter in ('Q1','Q3'))
              //
              // then with IS NULL checks added, the filter
              // becomes:
              //
              // (not(year = '1997') or year is null) or
              // (not(quarter in ('Q1','Q3')) or quarter is null)
              where = "(" + where + " or " + "(" + q + " is null))";
            }
          }
          condition += where;
        }
      }

      if ( m.getLevel().isUnique() || m.getLevel() == fromLevel || !includeParentLevels ) {
        break; // no further qualification needed
      }
    }

    return condition;
  }

  /**
   * Returns true if any measure calculations in the first arg references a dimension member with corresponding members
   * in the second arg which conflict with that member. A member "conflicts" if the member referenced by the measure is
   * not equal to or children of the corresponding dimension member. For example, given ( [unit sales], [Time].[1997].Q1
   * ) in the measures set, if the member [Time].[1997].[Q2] is in the members set this would conflict, since Q1 is not
   * equal to or a child of Q2.
   *
   * This method is used in native evaluation to determine whether any measures in the query could conflict with the SQL
   * constraint being constructed.
   */
  public static boolean measuresConflictWithMembers( Set<Member> measures, Member[] members ) {
    Set<Member> membersNestedInMeasures = getMembersNestedInMeasures( measures );
    for ( Member memberInMeasure : membersNestedInMeasures ) {
      if ( !anyMemberOverlaps( members, memberInMeasure ) ) {
        return true;
      }
    }
    return false;
  }

  public static Set<Member> getMembersNestedInMeasures( Set<Member> measures ) {
    Set<Member> membersNestedInMeasures = new HashSet<>();
    for ( Member m : measures ) {
      if ( m.isCalculated() ) {
        Exp exp = m.getExpression();
        exp.accept( new MemberExtractingVisitor( membersNestedInMeasures, null, false ) );
      }
    }
    return membersNestedInMeasures;
  }

  public static boolean measuresConflictWithMembers( Set<Member> measuresMembers, CrossJoinArg[] cjArgs ) {
    return measuresConflictWithMembers( measuresMembers, getCJArgMembers( cjArgs ) );
  }

  public static boolean containsValidMeasure( Exp... expressions ) {
    for ( Exp expression : expressions ) {
      if ( expression instanceof ResolvedFunCall ) {
        ResolvedFunCall fun = ( (ResolvedFunCall) expression );
        return fun.getFunDef() instanceof ValidMeasureFunDef || containsValidMeasure( fun.getArgs() );
      }
    }
    return false;
  }

  /**
   * Compares the array of members against memberInMeasure, returning true if any of the members are of the same
   * hierarchy and is either [All] or a equal to or a child of memberInMeasure.
   *
   * This is used to identify whether whether the memberInMeasure is "overlapped" by any of the members. For native
   * evaluation we need to make sure that a member included as a result of a calculated measure does not fall outside of
   * the set of members that will be used to constrain the native query, in which case we may exclude members
   * incorrectly.
   */
  private static boolean anyMemberOverlaps( Member[] members, Member memberInMeasure ) {
    boolean memberIsCovered = false;
    boolean encounteredHierarchy = false;
    for ( Member memberCheckedForConflict : members ) {
      final boolean sameHierarchy = memberInMeasure.getHierarchy().equals( memberCheckedForConflict.getHierarchy() );
      boolean childOrEqual = false;
      if ( sameHierarchy ) {
        encounteredHierarchy = true;
        childOrEqual =
            memberCheckedForConflict.isAll() || memberInMeasure.isChildOrEqualTo( memberCheckedForConflict );
      }
      if ( sameHierarchy && childOrEqual ) {
        memberIsCovered = true;
        break;
      }
    }
    return !encounteredHierarchy || memberIsCovered;
  }

  private static Member[] getCJArgMembers( CrossJoinArg[] cjArgs ) {
    Set<Member> members = new HashSet<Member>();
    for ( CrossJoinArg arg : cjArgs ) {
      if ( arg.getMembers() != null ) {
        members.addAll( arg.getMembers() );
      }
    }
    return members.toArray( new Member[members.size()] );
  }
}

// End SqlConstraintUtils.java
