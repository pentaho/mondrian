package org. omg. cwm. objectmodel. core;
import java. util. List; import java. util. Collection;


public interface MultiplicityRange extends Element {
// class scalar attributes
public Integer getLower();
public void setLower( Integer input);
public int getUpper();
public void setUpper( int input);


// class references
public void setMultiplicity( Multiplicity input);
public Multiplicity getMultiplicity();
// class operations
}
