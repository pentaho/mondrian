/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.spi;

import mondrian.rolap.*;

/**
 * An SPI to provide alternate ways of accessing source data.
 */
public interface DataServicesProvider {
    MemberReader getMemberReader(RolapCubeHierarchy hierarchy);
}
// End DataServicesProvider.java
