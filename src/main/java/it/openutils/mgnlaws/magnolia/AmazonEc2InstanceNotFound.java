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
