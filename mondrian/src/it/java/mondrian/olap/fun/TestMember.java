/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package mondrian.olap.fun;

import mondrian.olap.Annotation;
import mondrian.olap.Dimension;
import mondrian.olap.DimensionType;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Id;
import mondrian.olap.Level;
import mondrian.olap.MatchType;
import mondrian.olap.Member;
import mondrian.olap.OlapElement;
import mondrian.olap.Property;
import mondrian.olap.Schema;
import mondrian.olap.SchemaReader;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Mock implementation of {@link Member} for testing.
 *
 * @author <a>Richard M. Emberson</a>
 */
public class TestMember implements Member {
  private final String identifer;

  public TestMember( String identifer ) {
    this.identifer = identifer;
  }

  public String toString() {
    return identifer;
  }

  public int compareTo( Object o ) {
    TestMember other = (TestMember) o;
    return this.identifer.compareTo( other.identifer );
  }

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

  @Override public boolean isOnSameHierarchyChain( Member otherMember ) {
    throw new UnsupportedOperationException();
  }

  public String getUniqueName() {
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
    return new MockDimension();
  }

  public Map<String, Annotation> getAnnotationMap() {
    throw new UnsupportedOperationException();
  }

  private static class MockDimension implements Dimension {
    public Hierarchy[] getHierarchies() {
      throw new UnsupportedOperationException();
    }

    public boolean isMeasures() {
      throw new UnsupportedOperationException();
    }

    public DimensionType getDimensionType() {
      throw new UnsupportedOperationException();
    }

    public Schema getSchema() {
      throw new UnsupportedOperationException();
    }

    public boolean isHighCardinality() {
      return false;
    }

    public String getUniqueName() {
      throw new UnsupportedOperationException();
    }

    public String getName() {
      throw new UnsupportedOperationException();
    }

    public String getDescription() {
      throw new UnsupportedOperationException();
    }

    public boolean isVisible() {
      throw new UnsupportedOperationException();
    }

    public OlapElement lookupChild(
      SchemaReader schemaReader,
      Id.Segment s, MatchType matchType ) {
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

    public Hierarchy getHierarchy() {
      throw new UnsupportedOperationException();
    }

    public Dimension getDimension() {
      throw new UnsupportedOperationException();
    }

    public Map<String, Annotation> getAnnotationMap() {
      throw new UnsupportedOperationException();
    }
  }
}

// End TestMember.java
