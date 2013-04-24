package it.openutils.mgnlaws.magnolia.init;

import info.magnolia.cms.beans.config.PropertiesInitializer;
import info.magnolia.cms.core.SystemProperty;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author molaschi
 * @version $Id: ClasspathPropertiesInitializer.java 10433 2012-07-23 11:04:39Z fabian.necci $
 */
public class ClasspathPropertiesInitializer extends PropertiesInitializer
{

    public static final String CLASSPATH_PREFIX = "classpath:";

    private Logger log = LoggerFactory.getLogger(ClasspathPropertiesInitializer.class);

    @Override
    public boolean loadPropertiesFile(String rootPath, String location)
    {
        if (StringUtils.startsWith(location, CLASSPATH_PREFIX))
        {
            String resource = StringUtils.substringAfter(location, CLASSPATH_PREFIX);
            InputStream propertiesInputStream = getClass().getResourceAsStream(resource);
            if (propertiesInputStream == null)
            {
                log.debug("Configuration file not found with classpath [{}]", resource); //$NON-NLS-1$
                return false;
            }
            try
            {
                SystemProperty.getProperties().load(propertiesInputStream);
                log.info("Loading configuration at {}", location);//$NON-NLS-1$
            }
            catch (Exception e)
            {
                log.error(e.getMessage(), e);
                return false;
            }
            finally
            {
                IOUtils.closeQuietly(propertiesInputStream);
            }
            return true;
        }
        else
        {
            return super.loadPropertiesFile(rootPath, location);
        }
    }

}
