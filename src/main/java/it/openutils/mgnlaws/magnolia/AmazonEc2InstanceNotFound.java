package it.openutils.mgnlaws.magnolia;

import java.text.MessageFormat;


/**
 * @author molaschi
 * @version $Id: AmazonEc2InstanceNotFound.java 12554 2013-02-05 15:38:55Z manuel $
 */
public class AmazonEc2InstanceNotFound extends Exception
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public AmazonEc2InstanceNotFound(String instanceId, Throwable cause)
    {
        super(getMessage(instanceId), cause);
    }

    public AmazonEc2InstanceNotFound(String instanceId)
    {
        super(getMessage(instanceId));
    }

    protected static String getMessage(String instanceId)
    {
        return MessageFormat.format("Amazon with ec2 instance id {0} not found", instanceId);
    }
}
