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

import java.util.Map;

/**
 * Role that generates an XML role definition dependent on the session context.
 *
 * <p>TODO: Implement test plan below, implement remaining features, and mark
 * {@link mondrian.util.Bug#BugMondrian1281Fixed} fixed.</p>
 */
public interface RoleGenerator {
    /** Generates an XML role definition. */
    String asXml(Map<String, Object> context);

/*
Test plan:

1. DeclarativeRole that generates invalid XML.
2. Error if NAMED and name not specified.
3. Error if SYSTEM_GENERATED and name is specified.
4. Error if NAMELESS and name is specified.
5. Role generator that returns...
5.1. Traditional role
5.2. Union role
5.3. <Role className=...> with class that implements Role
5.4. <Role><Script> ... that defines getAccess
5.5. <Role className=...> with class that implements RoleGenerator
5.6. <Role><Script> that defines asXml
6. Error if role generator returns another generated role.
7. Error if a Role has both className and Script (must give location in schema)
8. Error if a Role has both className and Union (must give location in schema)
9. Error if a Role has both Script and Union (must give location in schema)
10. Error if a Role has both className and SchemaGrant (must give location in
    schema)
11. Error if Role has className and class does not exist (must give location in
    schema)
12. Error if Role has className and class is neither a Role nor RoleGenerator
    (must give location in schema)

To implement:
1. RolapSchemaLoader.createScriptRole
2. Convert quite a few RuntimeExceptions to proper errors
3. Devise a way to pass in session state from user (provided by authentication
  plugin, not from connect string properties)
4. Document RoleGenerator with other SPIs, and document <Role className><Script>
  in schema doc.
*/
}

// End RoleGenerator.java
