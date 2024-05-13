/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2002-2024 Hitachi Vantara
 * All rights reserved.
 */

package mondrian.olap4j;

import mondrian.olap.Property;
import mondrian.rolap.RolapAggregator;
import mondrian.rolap.RolapMeasure;
import mondrian.rolap.RolapStoredMeasure;
import org.olap4j.metadata.Datatype;
import org.olap4j.metadata.Measure;

/**
 * Implementation of {@link org.olap4j.metadata.Measure} for the Mondrian OLAP engine, as a wrapper around a mondrian
 * {@link mondrian.rolap.RolapStoredMeasure}.
 *
 * @author jhyde
 * @since Dec 10, 2007
 */
class MondrianOlap4jMeasure extends MondrianOlap4jMember implements Measure {
  MondrianOlap4jMeasure( MondrianOlap4jSchema olap4jSchema, RolapMeasure measure ) {
    super( olap4jSchema, measure );
  }

  public Aggregator getAggregator() {
    if ( !( member instanceof RolapStoredMeasure ) ) {
      return Aggregator.UNKNOWN;
    }

    final RolapAggregator aggregator = ( (RolapStoredMeasure) member ).getAggregator();

    if ( aggregator == RolapAggregator.Avg ) {
      return Aggregator.AVG;
    } else if ( aggregator == RolapAggregator.Count ) {
      return Aggregator.COUNT;
    } else if ( aggregator == RolapAggregator.DistinctCount ) {
      return Aggregator.UNKNOWN;
    } else if ( aggregator == RolapAggregator.Max ) {
      return Aggregator.MAX;
    } else if ( aggregator == RolapAggregator.Min ) {
      return Aggregator.MIN;
    } else if ( aggregator == RolapAggregator.Sum ) {
      return Aggregator.SUM;
    } else {
      return Aggregator.UNKNOWN;
    }
  }

  public Datatype getDatatype() {
    final String datatype = (String) member.getPropertyValue( Property.DATATYPE.getName() );

    if ( datatype != null ) {
      if ( datatype.equals( "Integer" ) ) {
        return Datatype.INTEGER;
      } else if ( datatype.equals( "Numeric" ) ) {
        return Datatype.DOUBLE;
      }
    }

    return Datatype.STRING;
  }

  public String getDisplayFolder() {
    return (String) member.getPropertyValue( Property.DISPLAY_FOLDER.getName() );
  }
}
