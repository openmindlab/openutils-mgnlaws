/**
 *
 * Amazon AWS consciousness for Magnolia CMS (http://www.openmindlab.com/lab/products/mgnlaws.html)
 * Copyright(C) 2013-2012, Openmind S.r.l. http://www.openmindonline.it
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package it.openutils.mgnlaws.magnolia.init;

import info.magnolia.cms.beans.config.PropertiesInitializer;
import info.magnolia.cms.core.Path;
import info.magnolia.cms.core.SystemProperty;
import info.magnolia.cms.servlets.MgnlServletContextListener;
import info.magnolia.cms.util.ClasspathResourcesUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;


/**
 * @author molaschi
 * @version $Id: ClasspathMgnlServletContextListener.java 12555 2013-02-05 15:42:49Z manuel $
 */
public class ClasspathMgnlServletContextListener extends MgnlServletContextListener
{

    private static final String JAAS_PROPERTYNAME = "java.security.auth.login.config";

    private static final String MGNL_JAAS_PROPERTYNAME = "magnolia.jaas.file";

    private static final String SYSTEM_PROPERTY_PLACEHOLDER_PREFIX = "system/";

    private boolean jaasConfigFileFromContainer = false;

    @Override
    public void contextInitialized(ServletContextEvent sce)
    {
        SystemProperty.setProperty(
            PropertiesInitializer.class.getName(),
            ClasspathPropertiesInitializer.class.getName());
        jaasConfigFileFromContainer = StringUtils.isNotEmpty(System.getProperty(JAAS_PROPERTYNAME));
        super.contextInitialized(sce);
    }

    @Override
    protected void startServer(ServletContext context)
    {
        if (!jaasConfigFileFromContainer && StringUtils.isNotEmpty(SystemProperty.getProperty(MGNL_JAAS_PROPERTYNAME)))
        {
            String jaasConfigFile = SystemProperty.getProperty(MGNL_JAAS_PROPERTYNAME);
            if (jaasConfigFile.startsWith(ClasspathPropertiesInitializer.CLASSPATH_PREFIX))
            {
                jaasConfigFile = StringUtils.substringAfter(
                    jaasConfigFile,
                    ClasspathPropertiesInitializer.CLASSPATH_PREFIX);
                InputStream jaasConfigFileInputStream = null;
                FileOutputStream jaasConfigFileTmpOutputStream = null;
                try
                {
                    jaasConfigFileInputStream = ClasspathResourcesUtil.getStream(jaasConfigFile);
                    if (jaasConfigFileInputStream != null)
                    {
                        File jaasConfigFileTmp = File.createTempFile("jaas", ".config");
                        jaasConfigFileTmpOutputStream = new FileOutputStream(jaasConfigFileTmp);
                        IOUtils.copyLarge(jaasConfigFileInputStream, jaasConfigFileTmpOutputStream);
                        jaasConfigFile = jaasConfigFileTmp.getAbsolutePath();
                    }
                }
                catch (IOException ex)
                {
                    throw new RuntimeException("Cannot copy jaas config file to filesystem", ex);
                }
                finally
                {
                    IOUtils.closeQuietly(jaasConfigFileInputStream);
                    IOUtils.closeQuietly(jaasConfigFileTmpOutputStream);
                }
            }
            else
            {
                jaasConfigFile = Path.getAbsoluteFileSystemPath(jaasConfigFile);
            }
            System.setProperty(JAAS_PROPERTYNAME, jaasConfigFile);
        }
        super.startServer(context);
    }

    @Override
    protected String getPropertiesFilesString(ServletContext context, String servername, String webapp)
    {
        String propertiesFilesString = super.getPropertiesFilesString(context, servername, webapp);

        // Replacing system properties (${system/something})
        String[] systemPropertiesNames = getNamesBetweenPlaceholders(
            propertiesFilesString,
            SYSTEM_PROPERTY_PLACEHOLDER_PREFIX);
        if (systemPropertiesNames != null)
        {
            for (String propertyName : systemPropertiesNames)
            {
                if (propertyName != null)
                {
                    final String originalPlaceHolder = PropertiesInitializer.PLACEHOLDER_PREFIX
                        + SYSTEM_PROPERTY_PLACEHOLDER_PREFIX
                        + propertyName
                        + PropertiesInitializer.PLACEHOLDER_SUFFIX;
                    final String attrValue = System.getProperty(propertyName);
                    if (StringUtils.isNotEmpty(attrValue))
                    {
                        propertiesFilesString = propertiesFilesString.replace(originalPlaceHolder, attrValue);
                    }
                }
            }
        }
        return propertiesFilesString;
    }

    private static String[] getNamesBetweenPlaceholders(String propertiesFilesString, String contextNamePlaceHolder)
    {
        final String[] names = StringUtils.substringsBetween(
            propertiesFilesString,
            PropertiesInitializer.PLACEHOLDER_PREFIX + contextNamePlaceHolder,
            PropertiesInitializer.PLACEHOLDER_SUFFIX);
        return StringUtils.stripAll(names);
    }

}
