package org. omg. cwm. objectmodel. core; 
import java. util. List; import java. util. Collection; 


public abstract interface StructuralFeature extends Feature {
// class scalar attributes 
public String getChangeability(); 
public void setChangeability( String input); 
public Multiplicity getMultiplicity(); 
public void setMultiplicity( Multiplicity input); 
public String getOrdering(); 
public void setOrdering( String input); 
public String getTargetScope(); 
public void setTargetScope( String input); 


// class references 
public void setType( Classifier input); 
public Classifier getType(); 
// class operations 
} 


