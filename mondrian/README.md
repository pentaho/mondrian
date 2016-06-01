## Welcome to Mondrian
Mondrian is an Online Analytical Processing (OLAP) server that enables business users to analyze large quantities of data in real-time.  Mondrian implements the Olap4J API.
### Building
#### Requirements
* JDK 1.8 or higher
* Maven 3.3 or higher
* MySql or other supported DBMS

#### Preparing the Build
Mondrian's test suite runs queries against the Foodmart database.  By default, it will connect to a MySql database called foodmart on localhost using username/password of foodmart/foodmart.  Configure the MySql database and user on your system.  You may choose to use different connection properties by creating a file `mondrian.properties` and specifying properties as follows:
```
mondrian.foodmart.jdbcURL=jdbc:mysql://yourServerName/yourDatabaseName
mondrian.foodmart.jdbcUser=yourUser
mondrian.foodmart.jdbcPassword=yourPassword
```

#### Loading Foodmart
Populating the foodmart database before the test suite runs can be done by activating the Maven profile `load-foodmart`.  The following command will compile Mondrian, load foodmart into the configured database, run the test suite and build a jar file.
```
mvn -Pload-foodmart package 
```
#### Running the test suite
Once foodmart is loaded, you don't need to load it again, so you can omit activation of the load-foodmart profile.  Mondrian uses the typical Maven phases for building, testing, and packaging.  Run the test suite with:
```
mvn verify
```
#### CmdRunner
Mondrian provides a command line utility for executing MDX queries.  It can often be helpful for debugging.  CmdRunner will connect to the same database as is configured for the test suite.  The Maven profile `cmdrunner` will build Mondrian, skip the test suite and execute CmdRunner.
```
mvn -Pcmdrunner
```
#### Advanced Configuration
Mondrian has many additional configuration options that can be specified in `mondrian.properties`.  The complete set of options as well as documentation on how to use them gets generated during the generate-sources phase of the Maven build.
```
mvn generate-sources
```
The `target` folder will now contain a file `mondrian.properties.template`.  All supported properties are explained there.  You can use any of them in your `mondrian.properties` file, but be aware that some tests will explicitly override some of these properties.
#### Using a DBMS other than MySql.
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
