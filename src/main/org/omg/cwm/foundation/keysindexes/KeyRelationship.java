package org. omg. cwm. foundation. keysindexes; 
import java. util. List; import java. util. Collection; 
import org. omg. cwm. objectmodel. core.*; 
public interface KeyRelationship extends ModelElement { 


// class scalar attributes 
// class references 
public void setFeature( Collection input); 


public List getFeature(); 
public void addFeature( StructuralFeature input); 
public void removeFeature( StructuralFeature input); 
public void addFeatureBefore( StructuralFeature before, StructuralFeature input); 


public void addFeatureAfter( StructuralFeature before, StructuralFeature input); 
public void moveFeatureBefore( StructuralFeature before, StructuralFeature input); 
public void moveFeatureAfter( StructuralFeature before, StructuralFeature input); 
public void setUniqueKey( UniqueKey input);
public UniqueKey getUniqueKey(); 
// class operations 
} 

