/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface TransformationPackage
extends javax.jmi.reflect.RefPackage {

  public org.omg.java.cwm.objectmodel.core.CorePackage getCore();

  public org.omg.java.cwm.foundation.expressions.ExpressionsPackage getExpressions();

  public org.omg.java.cwm.foundation.softwaredeployment.SoftwareDeploymentPackage getSoftwareDeployment();

  public org.omg.java.cwm.analysis.transformation.TransformationClass getTransformation();

  public org.omg.java.cwm.analysis.transformation.DataObjectSetClass getDataObjectSet();

  public org.omg.java.cwm.analysis.transformation.TransformationTaskClass getTransformationTask();

  public org.omg.java.cwm.analysis.transformation.TransformationStepClass getTransformationStep();

  public org.omg.java.cwm.analysis.transformation.TransformationActivityClass getTransformationActivity();

  public org.omg.java.cwm.analysis.transformation.PrecedenceConstraintClass getPrecedenceConstraint();

  public org.omg.java.cwm.analysis.transformation.TransformationUseClass getTransformationUse();

  public org.omg.java.cwm.analysis.transformation.TransformationMapClass getTransformationMap();

  public org.omg.java.cwm.analysis.transformation.TransformationTreeClass getTransformationTree();

  public org.omg.java.cwm.analysis.transformation.ClassifierMapClass getClassifierMap();

  public org.omg.java.cwm.analysis.transformation.FeatureMapClass getFeatureMap();

  public org.omg.java.cwm.analysis.transformation.StepPrecedenceClass getStepPrecedence();

  public org.omg.java.cwm.analysis.transformation.ClassifierFeatureMapClass getClassifierFeatureMap();

  public org.omg.java.cwm.analysis.transformation.CfmapFeature getCfmapFeature();

  public org.omg.java.cwm.analysis.transformation.CfmapClassifier getCfmapClassifier();

  public org.omg.java.cwm.analysis.transformation.FeatureMapSource getFeatureMapSource();

  public org.omg.java.cwm.analysis.transformation.FeatureMapTarget getFeatureMapTarget();

  public org.omg.java.cwm.analysis.transformation.ClassifierMapTarget getClassifierMapTarget();

  public org.omg.java.cwm.analysis.transformation.ClassifierMapSource getClassifierMapSource();

  public org.omg.java.cwm.analysis.transformation.ClassifierMapToCfmap getClassifierMapToCfmap();

  public org.omg.java.cwm.analysis.transformation.ClassifierMapToFeatureMap getClassifierMapToFeatureMap();

  public org.omg.java.cwm.analysis.transformation.TransformationTaskElement getTransformationTaskElement();

  public org.omg.java.cwm.analysis.transformation.DataObjectSetElement getDataObjectSetElement();

  public org.omg.java.cwm.analysis.transformation.InverseTransformationTask getInverseTransformationTask();

  public org.omg.java.cwm.analysis.transformation.TransformationStepTask getTransformationStepTask();

  public org.omg.java.cwm.analysis.transformation.TransformationTarget getTransformationTarget();

  public org.omg.java.cwm.analysis.transformation.TransformationSource getTransformationSource();

}
