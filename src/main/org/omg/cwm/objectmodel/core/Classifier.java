package org. omg. cwm. objectmodel. core;
import java. util. List; import java. util. Collection;


public abstract interface Classifier extends Namespace {
// class scalar attributes
public Boolean getIsAbstract();
public void setIsAbstract( Boolean input);


// class references
public void setFeature( Collection input);
public List getFeature();
public void removeFeature( Feature input);
public void moveFeatureBefore( Feature before, Feature input);
public void moveFeatureAfter( Feature before, Feature input);
// class operations
}


