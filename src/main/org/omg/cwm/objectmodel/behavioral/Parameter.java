package org. omg. cwm. objectmodel. behavioral; 
import java. util. List; import java. util. Collection; 
import org. omg. cwm. objectmodel. core.*; 
public interface Parameter extends ModelElement { 


// class scalar attributes 
public Expression getDefaultValue(); 
public void setDefaultValue( Expression input); 
public String getKind(); 
public void setKind( String input); 


// class references 
public void setBehavioralFeature( BehavioralFeature input); 
public BehavioralFeature getBehavioralFeature(); 
public void setEvent( Event input); 
public Event getEvent(); 
public void setType( Classifier input); 
public Classifier getType(); 
// class operations 
} 


