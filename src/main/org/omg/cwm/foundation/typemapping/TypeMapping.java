package org. omg. cwm. foundation. typemapping; 
import java. util. List;
import java. util. Collection; import org. omg. cwm. objectmodel. core.*; 
public interface TypeMapping extends ModelElement { 
// class scalar attributes 
public Boolean getIsBestMatch(); 
public void setIsBestMatch( Boolean input); 
public Boolean getIsLossy(); 
public void setIsLossy( Boolean input); 


// class references 
public void setSourceType( Classifier input); 
public Classifier getSourceType(); 
public void setTargetType( Classifier input); 
public Classifier getTargetType(); 
// class operations 
} 


