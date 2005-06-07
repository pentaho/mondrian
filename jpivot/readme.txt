JPivot for Mondrian

The Mondrian webapp contains JPivot. Because of the different release cycles of the two projects, we need
to keep a deployment of JPivot in Mondrian, with any changes required for compatibility with Mondrian, so
here it is. Additional Mondrian/JPivot contributions can be placed here, too.

6/6/05

The jpivot.war in /lib is based on JPivot 1.3.0 CVS snapshot of 5/2/05, which is included here as a zip
file. The build.xml in the JPivot source zip works against Tomcat. All the required JARs to build this 
version of JPivot are in jpivot.war - put them into a lib directory under the jpivot project root. The
build.xml and build.properties in this directory supercedes the ones in the source zip.

The war target in the Mondrian build.xml works with the jpivot.war as a starting point. The one thing to 
configure is the mondrian.webapp.connectString which is in mondrian.properties.

