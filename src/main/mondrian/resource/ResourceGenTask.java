/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Oct 8, 2002
*/

package mondrian.resource;

import org.apache.tools.ant.Task;

/**
 * A <code>ResourceGenTask</code> is an ANT task to invoke the mondrian
 * Resource Generator.
 *
 * Example:
 *   <ResourceGen dest="source" package="com.foo">
 *     <include path="MyResource_en.xml"/>
 *     <include path="resources/OtherResource_fr.xml" package="com.bar"/>
 *   </ResourceGen>
 *
 * Generates
 *   source/com/foo/MyResource_en.properties
 *   source/com/foo/MyResource.java
 *   source/com/foo/MyResource_en.java
 *   source/com/bar/OtherResource_en.properties
 *   source/com/bar/OtherResource.java
 *   source/com/bar/OtherResource_fr.java
 *
 * Files are not generated if there is an existing newer one.
 *
 * The output path is determined by 'dest' and the package-name.
 *
 * <h2>Element &lt;resourceGen&gt;</h2>
 *
 * <table>
 * <tr>
 * <th>Attribute</th>
 * <th>Description</th>
 * <th>Required</th>
 * </tr>
 *
 * <tr>
 * <td>dest</td>
 * <td>Destination directory.</td>
 * <td>Yes.</td>
 * </tr>
 *
 * <tr>
 * <td>packageName</td>
 * <td>Default is "".</td>
 * </tr>
 * </table>
 *
 * <h2>Element &lt;include&gt;</h2>
 *
 * <table>
 * <tr>
 * <th>Attribute</th>
 * <th>Description</th>
 * <th>Required</th>
 * </tr>
 *
 * <tr>
 * <td>packageName</td>
 * <td>Overrides task's <code>packageName</code>.</td>
 * <td>No</td>
 * </tr>
 *
 * <tr>
 * <td>className</td>
 * <td>By default, the class name is derived from the source
 *     file name, for example <code>foo/MyResource_en.xml</code> will become
 *     <code>MyResource</code>.</td>
 * <td>No</td>
 * </tr>
 *
 * <tr>
 * <td>baseClassName</td>
 * <td>The fully-qualified name of the base class of the resource bundle.
 *     Defaults to "mondrian.resource.ShadowResourceBundle".
 *     todo: Obsolete BaflResourceList.baseClass.</td>
 * <td>No</td>
 * </tr>
 *
 * </table>
 *
 * @author jhyde
 * @since Oct 8, 2002
 * @version $Id$
 **/
class ResourceGenTask extends Task {
	ResourceGen generator;
}

// End ResourceGenTask.java