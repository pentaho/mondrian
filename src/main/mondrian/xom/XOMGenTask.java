/*
// $Id$
// (C) Copyright 2002 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 1 April, 2002
*/
package mondrian.xom;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

import java.util.Vector;
import java.io.IOException;
import java.io.File;

/**
 * <code>XOMGenTask</code> is an ANT task with which to invoke {@link
 * MetaGenerator}.
 *
 * @author jhyde
 * @since 1 April, 2002
 * @version $Id$
 *
 * <hr/>
 * 
 * <h2><a name="XOMGen">XOMGen</a></h2>
 * <h3>Description</h3> 
 * <p>
 * 	 Invokes the {@link MetaGenerator}.
 * </p>
 * <p>
 * 	 This task only invokes XOMGen if the grammar file is newer than the
 *   generated Java files.
 * </p>
 * 
 * <h3>Parameters</h3>
 * <table border="1" cellpadding="2" cellspacing="0">
 * 	 <tr>
 * 	   <td valign="top"><b>Attribute</b></td>
 * 	   <td valign="top"><b>Description</b></td>
 * 	   <td align="center" valign="top"><b>Required</b></td>
 * 	 </tr>
 * 	 <tr>
 * 	   <td valign="top"><a name="model">model</a></td>
 * 	   <td valign="top">The name of the XML file which holds the XOM
 *       model.</td>
 * 	   <td valign="top" align="center">Yes</td>
 * 	 </tr>
 * 	 <tr>
 * 	   <td valign="top"><a name="destdir">destdir</a></td>
 * 	   <td valign="top">The name of the output directory. Default is the
 * 		 current directory.</td>
 * 	   <td valign="top" align="center">No</td>
 * 	 </tr>
 * 	 <tr>
 * 	   <td valign="top"><a name="classname">classname</a></td>
 * 	   <td valign="top">The full name of the class to generate.</td>
 * 	   <td valign="top" align="center">Yes</td>
 * 	 </tr>
 * 	 <tr>
 * 	   <td valign="top"><a name="dtdname">dtdname</a></td>
 * 	   <td valign="top">The name of the DTD file to generate. The path may be
 * 		 either absolute, or relative to <code>destdir</code>.</td>
 * 	   <td valign="top" align="center">Yes</td>
 * 	 </tr>
 * </table>
 * 
 * <h3>Example</h3>
 * <blockquote><pre>&lt;xomgen
 * 	   model=&quot;src/main/mondrian/xom/Meta.xml&quot;
 * 	   destdir=&quot;src/main&quot;
 * 	   classname=&quot;mondrian.xom.MetaDef&quot;/&gt;</pre></blockquote>
 * <p>
 * 	 This invokes XOMGen on the model file
 *   <code>src/main/mondrian/xom/Meta.xml</code>, and generates
 *   <code>src/main/mondrian/xom/MetaDef.java</code> and
 *   <code>src/main/mondrian/xom/meta.dtd</code>.
 * </p>
 *
 * <hr/>
 **/
public class XOMGenTask extends Task {
	String modelFileName;
	String destDir;
	String dtdFileName;
	String className;

	public XOMGenTask()
	{}

	public void execute() throws BuildException {
		if (modelFileName == null) {
			throw new BuildException("You must specify model.");
		}
		final boolean testMode = false;
		if (destDir == null) {
			destDir = project.getBaseDir().toString();
		}
		if (className == null) {
			throw new BuildException("You must specify className.");
		}
		String classFileName = classToFileName(destDir, className);
		try {
			File modelFile = new File(modelFileName),
				classFile = new File(classFileName),
				outputDir = classFile.getParentFile(),
				dtdFile = new File(outputDir, dtdFileName);
			if (modelFile.exists() &&
					classFile.exists() &&
				dtdFile.exists()) {
				long modelStamp = modelFile.lastModified(),
					classStamp = classFile.lastModified(),
					dtdStamp = dtdFile.lastModified();
				if (classStamp > modelStamp &&
					dtdStamp > modelStamp) {
					// files are up to date
					return;
				}
			}
			MetaGenerator generator = new MetaGenerator(
					modelFileName, testMode, className);
			generator.writeFiles(destDir, dtdFileName);
			generator.writeOutputs();
		} catch (XOMException e) {
			throw new BuildException("Generation of model failed: " + e);
		} catch (IOException e) {
			throw new BuildException("Generation of model failed: " + e);
		}			
	}

	// ------------------------------------------------------------------------
	// ANT attribute methods

	/** See parameter <code><a href="#model">model</a></code>. **/
	public void setModel(String model) {
		this.modelFileName = model;
	}

	/** See parameter <code><a href="#destdir">destdir</a></code>. **/
	public void setDestdir(String destdir) {
		this.destDir = destdir;
	}

	/** See parameter <code><a href="#classname">classname</a></code>. **/
	public void setClassname(String classname) {
		this.className = classname;
	}

	/** See parameter <code><a href="#dtdname">dtdname</a></code>. **/
	public void setDtdname(String dtdname) {
		this.dtdFileName = dtdname;
	}

	// ------------------------------------------------------------------------

	/**
     * Creates the name a java class will live in. For example,
     * <code>makeJavaFileName("com.myproj", "MyClass")</code> returns
     * "com/myproj/MyClass.java".
     **/
    static String classToFileName(String dir, String className) {
		char fileSep = System.getProperty("file.separator").charAt(0); // e.g. '/'
		if (dir == null) {
			dir = "";
		} else if (dir.equals("")) {
		} else {
			dir += fileSep;
		}
		return dir + className.replace('.', fileSep) + ".java";
    }

}


// End XOMGenTask.java
