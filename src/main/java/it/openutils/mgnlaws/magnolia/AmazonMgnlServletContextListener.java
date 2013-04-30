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

package it.openutils.mgnlaws.magnolia;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Tag;

import it.openutils.mgnlaws.magnolia.init.ClasspathMgnlServletContextListener;


/**
 * @author molaschi
 * @version $Id: AmazonMgnlServletContextListener.java 12696 2013-02-14 13:55:22Z fabian.necci $
 */
public class AmazonMgnlServletContextListener extends ClasspathMgnlServletContextListener
{

    private final Logger log = LoggerFactory.getLogger(AmazonMgnlServletContextListener.class);

    private static final String JR_CLUSTERID = "org.apache.jackrabbit.core.cluster.node_id";

    private static final String EC2_TAG_CLUSTERID = "aws.instance.ClusterID";

    private static final String PARAM_AWS_ACCESS_KEY = "amazonAwsAccessKey";

    private static final String PARAM_AWS_SECRET_KEY = "amazonAwsSecretKey";

    private static final String PARAM_AWS_REGION = "amazonAwsRegion";

    private static final String PARAM_APP_VERSION_KEY = "app_version";

    private Instance ec2Instance;

    protected void initEc2(String accessKey, String secretKey, String endpoint) throws IOException,
        AmazonEc2InstanceNotFound
    {
        String ec2InstanceId;
        BufferedReader in = null;
        try
        {
            URL url = new URL("http://169.254.169.254/latest/meta-data/instance-id");
            URLConnection connection = url.openConnection();
            in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            ec2InstanceId = in.readLine();
            in.close();
        }
        finally
        {
            IOUtils.closeQuietly(in);
        }

        AmazonEC2Client client = new AmazonEC2Client(new BasicAWSCredentials(accessKey, secretKey));
        client.setEndpoint(endpoint);
        DescribeInstancesResult result = client.describeInstances(new DescribeInstancesRequest()
            .withInstanceIds(ec2InstanceId));
        if (result.getReservations().size() > 0 && result.getReservations().get(0).getInstances().size() > 0)
        {
            ec2Instance = result.getReservations().get(0).getInstances().get(0);
            if (ec2Instance == null)
            {
                // should never happen
                throw new AmazonEc2InstanceNotFound(ec2InstanceId);
            }
        }
        else
        {
            throw new AmazonEc2InstanceNotFound(ec2InstanceId);
        }

        for (Tag tag : ec2Instance.getTags())
        {
            if (StringUtils.startsWith(tag.getKey(), "__"))
            {
                System.setProperty(StringUtils.substring(tag.getKey(), 2), tag.getValue());
            }
            else
            {
                System.setProperty("aws.instance." + tag.getKey(), tag.getValue());
            }
        }

        String clusterId = System.getProperty(EC2_TAG_CLUSTERID);
        if (StringUtils.isNotEmpty(clusterId))
        {
            System.setProperty(JR_CLUSTERID, clusterId);
        }
    }

    @Override
    public void contextInitialized(ServletContextEvent sce)
    {
        ServletContext context = sce.getServletContext();
        String appVersion = context.getInitParameter(PARAM_APP_VERSION_KEY);
        if (!appVersion.equals("${project.version}") || StringUtils.endsWith(appVersion, "SNAPSHOT"))
        {
            String amazonAwsAccessKey = context.getInitParameter(PARAM_AWS_ACCESS_KEY);
            String amazonAwsSecretKey = context.getInitParameter(PARAM_AWS_SECRET_KEY);
            String amazonAwsRegion = context.getInitParameter(PARAM_AWS_REGION);

            System.setProperty("aws.credentials.accessKey", amazonAwsAccessKey);
            System.setProperty("aws.credentials.secretKey", amazonAwsSecretKey);
            System.setProperty("aws.region", amazonAwsRegion);

            if (StringUtils.isNotEmpty(amazonAwsAccessKey) && StringUtils.isNotEmpty(amazonAwsSecretKey))
            {
                try
                {
                    String ec2endpoint = "http://ec2." + amazonAwsRegion + ".amazonaws.com";
                    initEc2(amazonAwsAccessKey, amazonAwsSecretKey, ec2endpoint);
                }
                catch (IOException e)
                {
                    log.warn("Error getting instance id", e);
                }
                catch (AmazonEc2InstanceNotFound e)
                {
                    log.warn("Error getting instance details", e);
                }
            }
            else
            {
                log.warn("No Amazon credentials found");
            }
        }
        super.contextInitialized(sce);
    }
}
