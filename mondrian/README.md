## Welcome to Mondrian
Mondrian is an Online Analytical Processing (OLAP) server that enables business users to analyze large quantities of data in real-time.  Mondrian implements the Olap4J API.
### Building
#### Requirements
* JDK 1.8 or higher
* Maven 3.3 or higher

#### Quick Start
The following command will build the Mondrian jar, run the full integration test suite using an embedded MySQL server and install the jar file into your local Maven repository.
```
mvn -DrunITs install
```
Skip the integration tests by omitting `-DrunITs`

The embedded mysql server does not work on Windows.  If you are running on Windows you will need to configure foodmart as explained in the next section.

#### Alternate Foodmart
Mondrian's integration test suite runs queries against the Foodmart database.  By default, maven will start an embedded MySql database where the foodmart data will be loaded.  It is often needed to run the test suite against a different database.  You may choose different connection properties by creating a file `mondrian.properties` and specifying jdbc connection properties as follows:
```
mondrian.foodmart.jdbcURL=jdbc:mysql://yourServerName/yourDatabaseName
mondrian.foodmart.jdbcUser=yourUser
mondrian.foodmart.jdbcPassword=yourPassword
```
###### Getting JDBC Drivers
The Maven build has a test dependency on the MySql jdbc driver.  If you want to use a different DBMS you will need to add the jdbc driver to the test dependencies using a Maven profile.  The profile should be defined in your settings.xml file.  Example profile:
```
...
    <profile>
      <id>usePostgres</id>
      <dependencies>
        <dependency>
          <groupId>org.postgresql</groupId>
          <artifactId>postgresql</artifactId>
          <version>9.3-1102-jdbc4</version>
        </dependency>
      </dependencies>
    </profile>
...
```
    
###### Loading Foodmart
When using the embedded MySQL databse, Foodmart will be populated automatically during the integration-test phase of the build.  When you're using an alternate Foodmart you can populate your database before the test suite runs by activating the Maven profile `load-foodmart`.  The following command will compile Mondrian, build a jar file, load foodmart into the configured database and run the integration test suite.
```
mvn -DrunITs -Pload-foodmart verify 
```
Once Foodmart is loaded, you don't need to load it on subsequent builds, so you can omit activation of the load-foodmart profile.

#### CmdRunner
Mondrian provides a command line utility for executing MDX queries.  It can often be helpful for debugging.  CmdRunner requires that you have setup a mondrian.properties file.  At present, it cannot run with the embedded MySql server.  The required properties are:
```
mondrian.foodmart.jdbcURL=jdbc:mysql://yourServerName/yourDatabaseName
mondrian.foodmart.jdbcUser=yourUser
mondrian.foodmart.jdbcPassword=yourPassword
mondrian.catalogURL=../demo/FoodMart.xml
```
The Maven profile `cmdrunner` will build Mondrian, skip the test suite and execute CmdRunner.
```
mvn -Pcmdrunner
```
#### Advanced Configuration
Mondrian has many additional configuration options that can be specified in `mondrian.properties`.  The complete set of options as well as documentation on how to use them gets generated during the generate-sources phase of the Maven build.
```
mvn generate-sources
```
The `target` folder will now contain a file `mondrian.properties.template`.  All supported properties are explained there.  You can use any of them in your `mondrian.properties` file, but be aware that some tests will explicitly override some of these properties.
