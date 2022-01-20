/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2020 Hitachi Vantara..  All rights reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapHierarchy;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.eigenbase.util.property.StringProperty;

import java.util.Map;
import java.util.Set;

/**
 * Definition of the <code>&lt;Hierarchy&gt;.CurrentMember</code> MDX builtin function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
public class HierarchyCurrentMemberFunDef extends FunDefBase {
  private static final Logger LOGGER = LogManager.getLogger( HierarchyCurrentMemberFunDef.class );

  static final HierarchyCurrentMemberFunDef instance = new HierarchyCurrentMemberFunDef();

  private HierarchyCurrentMemberFunDef() {
    super( "CurrentMember", "Returns the current member along a hierarchy during an iteration.", "pmh" );
  }

  public Calc compileCall( ResolvedFunCall call, ExpCompiler compiler ) {
    final HierarchyCalc hierarchyCalc = compiler.compileHierarchy( call.getArg( 0 ) );
    final Hierarchy hierarchy = hierarchyCalc.getType().getHierarchy();
    if ( hierarchy != null ) {
      return new FixedCalcImpl( call, hierarchy );
    } else {
      return new CalcImpl( call, hierarchyCalc );
    }
  }

  /**
   * Compiled implementation of the Hierarchy.CurrentMember function that evaluates the hierarchy expression first.
   */
  public static class CalcImpl extends AbstractMemberCalc {
    private final HierarchyCalc hierarchyCalc;

    public CalcImpl( Exp exp, HierarchyCalc hierarchyCalc ) {
      super( exp, new Calc[] { hierarchyCalc } );
      this.hierarchyCalc = hierarchyCalc;
    }

    protected String getName() {
      return "CurrentMember";
    }

    public Member evaluateMember( Evaluator evaluator ) {
      Hierarchy hierarchy = hierarchyCalc.evaluateHierarchy( evaluator );
      validateSlicerMembers( hierarchy, evaluator );
      return evaluator.getContext( hierarchy );
    }

    public boolean dependsOn( Hierarchy hierarchy ) {
      return hierarchyCalc.getType().usesHierarchy( hierarchy, false );
    }
  }

  /**
   * Compiled implementation of the Hierarchy.CurrentMember function that uses a fixed hierarchy.
   */
  public static class FixedCalcImpl extends AbstractMemberCalc {
    // getContext works faster if we give RolapHierarchy rather than
    // Hierarchy
    private final RolapHierarchy hierarchy;

    public FixedCalcImpl( Exp exp, Hierarchy hierarchy ) {
      super( exp, new Calc[] {} );
      assert hierarchy != null;
      this.hierarchy = (RolapHierarchy) hierarchy;
    }

    protected String getName() {
      return "CurrentMemberFixed";
    }

    public Member evaluateMember( Evaluator evaluator ) {
      validateSlicerMembers( hierarchy, evaluator );
      return evaluator.getContext( hierarchy );
    }

    public boolean dependsOn( Hierarchy hierarchy ) {
      return this.hierarchy == hierarchy;
    }

    public void collectArguments( Map<String, Object> arguments ) {
      arguments.put( "hierarchy", hierarchy );
      super.collectArguments( arguments );
    }
  }

  private static void validateSlicerMembers( Hierarchy hierarchy, Evaluator evaluator ) {
    if ( evaluator instanceof RolapEvaluator ) {
      StringProperty alertProperty = MondrianProperties.instance().CurrentMemberWithCompoundSlicerAlert;
      String alertValue = alertProperty.get();

      if ( alertValue.equalsIgnoreCase( org.apache.logging.log4j.Level.OFF.toString() ) ) {
        return; // No validation
      }

      RolapEvaluator rev = (RolapEvaluator) evaluator;
      Map<Hierarchy, Set<Member>> map = rev.getSlicerMembersByHierarchy();
      Set<Member> members = map.get( hierarchy );

      if ( members != null && members.size() > 1 ) {
        MondrianException exception =
            MondrianResource.instance().CurrentMemberWithCompoundSlicer.ex( hierarchy.getUniqueName() );

        if ( alertValue.equalsIgnoreCase( org.apache.logging.log4j.Level.WARN.toString() ) ) {
          LOGGER.warn( exception.getMessage() );
        } else if ( alertValue.equalsIgnoreCase( org.apache.logging.log4j.Level.ERROR.toString() ) ) {
          throw MondrianResource.instance().CurrentMemberWithCompoundSlicer.ex( hierarchy.getUniqueName() );
        }
      }
    }
  }
}

// End HierarchyCurrentMemberFunDef.java
