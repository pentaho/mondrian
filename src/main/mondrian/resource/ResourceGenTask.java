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
import org.apache.tools.ant.TaskContainer;
import org.apache.tools.ant.BuildException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;
import java.net.URL;

/**
 * A <code>ResourceGenTask</code> is an ANT task to invoke the mondrian
 * Resource Generator.
 *
 * Example:
 *   <ResourceGen srcdir="source" locale="en_US">
 *     <include name="happy/BirthdayResource.xml"/>
 *   </ResourceGen>
 *
 * Generates
 *   source/happy/BirthdayResource_en_US.properties
 *   source/happy/BirthdayResource.java
 *   source/happy/BirthdayResource_en_US.java
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
 * <td><a name="srcdir">srcdir</a></td>
 * <td>Source directory. The paths of resource files, and hence the
 *     package names of generated Java classes, are relative to this
 *     directory.</td>
 * <td>Yes</td>
 * </tr>
 *
 * <tr>
 * <td><a name="destdir">destdir</a></td>
 * <td>Destination directory. Classes and properties files are generated
 *     relative to this directory. If not specified, has the same value as
 *     <a href="#srcdir">srcdir</a>.</td>
 * <td>No</td>
 * </tr>
 *
 * <tr>
 * <td><a name="static">static</a></td>
 * <td>Whether to generate static or dynamic accessor methods. Default is
 *     true (generate static methods). Not yet implemented.</td>
 * <td>No</td>
 * </tr>
 *
 * </table>
 *
 * Nested element: &lt;{@link ResourceGen.Include include}&gt;.
 *
 * @author jhyde
 * @since Oct 8, 2002
 * @version $Id$
 **/
public class ResourceGenTask extends Task {
	private ArrayList resources = new ArrayList();
	File src;
	File dest;
	boolean statik = true;

	public ResourceGenTask() {
	}

	public void execute() throws BuildException {
		validate();
		try {
			new ResourceGen().run(this);
		} catch (IOException e) {
			throw new BuildException(e);
		}
	}

	/** Called by ANT. **/
	public void addInclude(Include resourceArgs) {
		resources.add(resourceArgs);
		resourceArgs.root = this;
	}
	void validate() {
		if (src == null) {
			throw new BuildException("You must specify 'srcdir'");
		}
		if (dest == null) {
			dest = src;
		}
		final Include[] args = getIncludes();
		for (int i = 0; i < args.length; i++) {
			args[i].validate();
		}
	}
	Include[] getIncludes() {
		return (Include[]) resources.toArray(new Include[0]);
	}

	/** Sets <a href="#srcdir">srcdir</a>. **/
	public void setSrcdir(File srcDir) {
		this.src = srcDir;
	}
	/** Sets <a href="#srcdir">srcdir</a>. **/
	public void setDestdir(File destDir) {
		this.dest = destDir;
	}
	/** Sets <a href="#static">static</a>. **/
	public void setStatic(boolean statik) throws BuildException {
		this.statik = statik;
		throw new BuildException(
				"The 'static' parameter is not implemented yet");
	}

	/**
	 * <code>Include</code> implements &lt;include&gt; element nested
	 * within a &lt;resgen&gt; task (see {@link ResourceGenTask}).
	 *
	 * <table>
	 * <tr>
	 * <th>Attribute</th>
	 * <th>Description</th>
	 * <th>Required</th>
	 * </tr>
	 *
	 * <tr>
	 * <td><a name="name">name</a></td>
	 * <td>The name, relative to <a href="#srcdir">, of the XML file which
	 *     defines the resources.</td>
	 * <td>Yes</td>
	 * </tr>
	 *
	 * <tr>
	 * <td><a name="className">className</a></td>
	 * <td>The name of the class to be generated, including the package, but
	 *     not including any locale suffix. By default, the class name is
	 *     derived from the name of the source file, for example
	 *     <code>happy/BirthdayResource_en_US.xml</code> becomes class
	 *     <code>happy.BirthdayResource</code>.</td>
	 * <td>No</td>
	 * </tr>
	 *
	 * <tr>
	 * <td><a name="baseClassName">baseClassName</a></td>
	 * <td>The fully-qualified name of the base class of the resource bundle.
	 *     Defaults to "mondrian.resource.ShadowResourceBundle".</td>
	 * <td>No</td>
	 * </tr>
	 *
	 * </table>
	 */
	public static class Include {
		public Include() {
		}
		ResourceGenTask root;
		/** Name of source file, relative to 'srcdir'. **/
		String fileName;
		/** Class name. **/
		String className;
		/** Base class. */
		String baseClassName;

		void validate() throws BuildException {
			if (fileName == null) {
				throw new BuildException("You must specify attribute 'name'");
			}
		}
		void process(ResourceGen generator) throws BuildException {
			ResourceGen.FileTask task;
			if (fileName.endsWith(".xml")) {
				task = generator.createXmlTask(this, fileName, className, baseClassName);
			} else if (fileName.endsWith(".properties")) {
				task = generator.createPropertiesTask(this, fileName);
			} else {
				throw new BuildException(
							"File '" + fileName + "' is not of a supported " +
							"type (.java or .properties)");
			}
			try {
				task.process(generator);
			} catch (IOException e) {
				e.printStackTrace();
				throw new BuildException(
						"Failed while processing '" + fileName + "'", e);
			}
		}
		/** Sets <a href="#name">name</a>. **/
		public void setName(String name) {
			this.fileName = name;
		}
		/** Sets <a href="#className">className</a>. **/
		public void setClassName(String className) {
			this.baseClassName = baseClassName;
		}
		/** Sets <a href="#baseClassName">baseClassName</a>. **/
		public void setBaseClassName(String baseClassName) {
			this.baseClassName = baseClassName;
		}
		String getBaseClassName() {
			return baseClassName;
		}
	}

}

// End ResourceGenTask.java
