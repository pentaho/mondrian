/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.datatypes;



public interface DataTypesPackage
extends javax.jmi.reflect.RefPackage {

  public org.omg.java.cwm.objectmodel.core.CorePackage getCore();

  public org.omg.java.cwm.foundation.datatypes.EnumerationClass getEnumeration();

  public org.omg.java.cwm.foundation.datatypes.EnumerationLiteralClass getEnumerationLiteral();

  public org.omg.java.cwm.foundation.datatypes.QueryExpressionClass getQueryExpression();

  public org.omg.java.cwm.foundation.datatypes.TypeAliasClass getTypeAlias();

  public org.omg.java.cwm.foundation.datatypes.UnionClass getUnion();

  public org.omg.java.cwm.foundation.datatypes.UnionMemberClass getUnionMember();

  public org.omg.java.cwm.foundation.datatypes.UnionDiscriminator getUnionDiscriminator();

  public org.omg.java.cwm.foundation.datatypes.EnumerationLiterals getEnumerationLiterals();

  public org.omg.java.cwm.foundation.datatypes.ClassifierAlias getClassifierAlias();

}
