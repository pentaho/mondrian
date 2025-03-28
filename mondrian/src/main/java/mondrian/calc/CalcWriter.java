/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.calc;

import org.apache.commons.collections.map.CompositeMap;

import java.io.PrintWriter;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Visitor which serializes an expression to text.
 *
 * @author jhyde
 * @since Dec 23, 2005
 */
public class CalcWriter {
  private static final int INDENT = 4;
  private static String BIG_STRING = "                ";

  private final PrintWriter writer;
  private final boolean profiling;
  private int linePrefixLength;
  private final Map<Calc, Map<String, Object>> parentArgMap = new IdentityHashMap<>();

  public CalcWriter( PrintWriter writer, boolean profiling ) {
    this.writer = writer;
    this.profiling = profiling;
  }

  public PrintWriter getWriter() {
    return writer;
  }

  public void visitChild( int ordinal, Calc calc ) {
    if ( calc == null ) {
      return;
    }
    indent();
    calc.accept( this );
    outdent();
  }

  public void visitCalc( Calc calc, String name, Map<String, Object> arguments, Calc[] childCalcs ) {
    writer.print( getLinePrefix() );
    writer.print( name );
    final Map<String, Object> parentArgs = parentArgMap.get( calc );
    if ( parentArgs != null && !parentArgs.isEmpty() ) {
      // noinspection unchecked
      arguments = new CompositeMap( arguments, parentArgs );
    }
    if ( !arguments.isEmpty() ) {
      writer.print( "(" );
      int k = 0;
      for ( Map.Entry<String, Object> entry : arguments.entrySet() ) {
        if ( k++ > 0 ) {
          writer.print( ", " );
        }
        writer.print( entry.getKey() );
        writer.print( "=" );
        writer.print( entry.getValue() );
      }
      writer.print( ")" );
    }
    writer.println();
    int k = 0;
    for ( Calc childCalc : childCalcs ) {
      visitChild( k++, childCalc );
    }
  }

  /**
   * Increases the indentation level.
   */
  public void indent() {
    linePrefixLength += INDENT;
  }

  /**
   * Decreases the indentation level.
   */
  public void outdent() {
    linePrefixLength -= INDENT;
  }

  private String getLinePrefix() {
    return spaces( linePrefixLength );
  }

  /**
   * Returns a string of N spaces.
   * 
   * @param n
   *          Number of spaces
   * @return String of N spaces
   */
  private static synchronized String spaces( int n ) {
    while ( n > BIG_STRING.length() ) {
      BIG_STRING = BIG_STRING + BIG_STRING;
    }
    return BIG_STRING.substring( 0, n );
  }

  public void setParentArgs( Calc calc, Map<String, Object> argumentMap ) {
    parentArgMap.put( calc, argumentMap );
  }

  /**
   * Whether to print out attributes relating to how a statement was actually executed. If false, client should only
   * send attributes relating to the plan.
   *
   * @return Whether client should send attributes about profiling
   */
  public boolean enableProfiling() {
    return profiling;
  }
}

// End CalcWriter.java
