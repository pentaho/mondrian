package org. omg. cwm. foundation. keysindexes;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Index extends ModelElement {


// class scalar attributes
public Boolean getIsPartitioning();
public void setIsPartitioning( Boolean input);
public Boolean getIsSorted();
public void setIsSorted( Boolean input);
public Boolean getIsUnique();
public void setIsUnique( Boolean input);


// class references
public void setIndexedFeature( Collection input);
public List getIndexedFeature();
public void removeIndexedFeature( IndexedFeature input);
public void moveIndexedFeatureBefore( IndexedFeature before, IndexedFeature input);


public void moveIndexedFeatureAfter( IndexedFeature before, IndexedFeature input);
public void setSpannedClass( org. omg. cwm. objectmodel. core. Class input);
public org. omg. cwm. objectmodel. core. Class getSpannedClass();
// class operations
}


