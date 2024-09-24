/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapHierarchy;

/**
 * Definition of the <code>PeriodsToDate</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class PeriodsToDateFunDef extends FunDefBase {
  static final ReflectiveMultiResolver Resolver =
      new ReflectiveMultiResolver( "PeriodsToDate", "PeriodsToDate([<Level>[, <Member>]])",
          "Returns a set of periods (members) from a specified level starting with the first period and ending with a specified member.",
          new String[] { "fx", "fxl", "fxlm" }, PeriodsToDateFunDef.class );

  private static final String TIMING_NAME = PeriodsToDateFunDef.class.getSimpleName();

  public PeriodsToDateFunDef( FunDef dummyFunDef ) {
    super( dummyFunDef );
  }

  public Type getResultType( Validator validator, Exp[] args ) {
    if ( args.length == 0 ) {
      // With no args, the default implementation cannot
      // guess the hierarchy.
      RolapHierarchy defaultTimeHierarchy =
          ( (RolapCube) validator.getQuery().getCube() ).getTimeHierarchy( getName() );
      return new SetType( MemberType.forHierarchy( defaultTimeHierarchy ) );
    }

    if ( args.length >= 2 ) {
      Type hierarchyType = args[0].getType();
      MemberType memberType = (MemberType) args[1].getType();
      if ( memberType.getHierarchy() != null && hierarchyType.getHierarchy() != null && memberType
          .getHierarchy() != hierarchyType.getHierarchy() ) {
        throw Util.newError( "Type mismatch: member must belong to hierarchy " + hierarchyType.getHierarchy()
            .getUniqueName() );
      }
    }

    // If we have at least one arg, it's a level which will
    // tell us the type.
    return super.getResultType( validator, args );
  }

  public Calc compileCall( ResolvedFunCall call, ExpCompiler compiler ) {
    final LevelCalc levelCalc = call.getArgCount() > 0 ? compiler.compileLevel( call.getArg( 0 ) ) : null;
    final MemberCalc memberCalc = call.getArgCount() > 1 ? compiler.compileMember( call.getArg( 1 ) ) : null;
    final RolapHierarchy timeHierarchy =
        levelCalc == null ? ( (RolapCube) compiler.getEvaluator().getCube() ).getTimeHierarchy( getName() ) : null;

    return new AbstractListCalc( call, new Calc[] { levelCalc, memberCalc } ) {
      public TupleList evaluateList( Evaluator evaluator ) {
        evaluator.getTiming().markStart( TIMING_NAME );
        try {
          final Member member;
          final Level level;
          if ( levelCalc == null ) {
            member = evaluator.getContext( timeHierarchy );
            level = member.getLevel().getParentLevel();
          } else {
            level = levelCalc.evaluateLevel( evaluator );
            if ( memberCalc == null ) {
              member = evaluator.getContext( level.getHierarchy() );
            } else {
              member = memberCalc.evaluateMember( evaluator );
            }
          }
          return new UnaryTupleList( periodsToDate( evaluator, level, member ) );
        } finally {
          evaluator.getTiming().markEnd( TIMING_NAME );
        }
      }

      public boolean dependsOn( Hierarchy hierarchy ) {
        if ( super.dependsOn( hierarchy ) ) {
          return true;
        }
        if ( memberCalc != null ) {
          return false;
        } else if ( levelCalc != null ) {
          return levelCalc.getType().usesHierarchy( hierarchy, true );
        } else {
          return hierarchy == timeHierarchy;
        }
      }
    };
  }
}

// End PeriodsToDateFunDef.java
