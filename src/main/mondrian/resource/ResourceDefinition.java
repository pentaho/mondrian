/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 19 September, 2002
*/
package mondrian.resource;

import java.util.ResourceBundle;

public class ResourceDefinition {
	//int code;
	public String key;
	public String baseMessage;
	//int severity;

	public ResourceDefinition(String key, String baseMessage) {
		this.key = key;
		this.baseMessage = baseMessage;
	}
	/** Creates an instance of this definition with a set of parameters.
	 * Derived classes can override this factory method. **/
	public ResourceInstance instantiate(ResourceBundle bundle, Object[] args) {
		return new Instance(bundle, this, args);
	}

	/** Default implementation of {@link ResourceInstance}. **/
	private static class Instance implements ResourceInstance {
		ResourceDefinition definition;
		ResourceBundle bundle;
		Object[] args;
		public Instance(ResourceBundle bundle, ResourceDefinition definition, Object[] args) {
			this.definition = definition;
			this.bundle = bundle;
			this.args = args;
		}
		public String toString() {
			String message = bundle.getString(definition.key);
			return Util.formatError(message, args);
		}
	}
}

// End ResourceDefinition.java
