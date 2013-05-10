package mondrian.util;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

import java.io.File;

/**
 * Run install deploy and test
 * @goal generate
 * @requiresDependencyResolution runtime
 */
public class PropertyUtilMojo extends AbstractMojo
{
    /**
     * The directory for the generated WAR.
     *
     * @parameter expression="${project.build.directory}"
     * @required
     */
    private String       outputDirectory;


    /**
     * @parameter expression="${settings.localRepository}"
     * @required
     */
    private String       localRepository;

    /**
     * @parameter expression="${project.version}"
     * @required
     */
    private String       version;


    /**
     * @parameter expression="${xmlFile}"
     */
    private File xmlFile;

    /**
     * @parameter expression="${javaFile}"
     */
    private File javaFile;

    /**
     * @parameter expression="${propertiesFile}"
     */
    private File propertiesFile;

    /**
     * @parameter expression="${htmlFile}"
     */
    private File htmlFile;

    public void execute() throws MojoExecutionException, MojoFailureException
    {
        PropertyUtil propertyUtil = new PropertyUtil();
        propertyUtil.generate(xmlFile, javaFile, propertiesFile, htmlFile);
    }
}