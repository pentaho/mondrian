package org. omg. cwm. analysis. transformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. foundation. softwaredeployment.*;
public interface ClassifierFeatureMap extends ModelElement {


// class scalar attributes
public ProcedureExpression getFunction();
public void setFunction( ProcedureExpression input);
public String getFunctionDescription();
public void setFunctionDescription( String input);
public Boolean getClassifierToFeature();
public void setClassifierToFeature( Boolean input);


// class references
public void setClassifier( Collection input);
public Collection getClassifier();
public void addClassifier( Classifier input);
public void removeClassifier( Classifier input);
public void setFeature( Collection input);
public Collection getFeature();
public void addFeature( Feature input);
public void removeFeature( Feature input);
public void setClassifierMap( ClassifierMap input);
public ClassifierMap getClassifierMap();
// class operations
}


