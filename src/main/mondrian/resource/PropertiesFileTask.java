/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 December, 2001 (moved into its own file szuercher 10 February 2004)
*/

package mondrian.resource;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Locale;

class PropertiesFileTask extends ResourceGen.FileTask {
	Locale locale;

	PropertiesFileTask(ResourceGenTask.Include include, String fileName) {
		this.include = include;
		this.fileName = fileName;
		this.className = Util.fileNameToClassName(fileName, ".properties");
		this.locale = Util.fileNameToLocale(fileName, ".properties");
	}

	/**
	 * Given an existing properties file such as
	 * <code>happy/Birthday_fr_FR.properties</code>, generates the
	 * corresponding Java class happy.Birthday_fr_FR.java</code>.
	 *
	 * <p>todo: Validate.
	 */
	void process(ResourceGen generator) throws IOException {
		// e.g. happy/Birthday_fr_FR.properties
		String s = Util.fileNameSansLocale(fileName, ".properties");
		File file = new File(include.root.src, s + ".xml");
		URL url = Util.convertPathToURL(file);
		ResourceDef.ResourceBundle resourceList = Util.load(url);

    generateJava(generator, resourceList, locale);
	}

	protected void generateBaseJava(
			ResourceGen generator,
			ResourceDef.ResourceBundle resourceList, PrintWriter pw) {
		throw new UnsupportedOperationException();
	}

  protected void generateCpp(ResourceGen generator,
                             ResourceDef.ResourceBundle resourceList) {
		throw new UnsupportedOperationException();
	}
}

