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

package it.openutils.mgnlaws.magnolia.datastore;

import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;


/**
 * @author molaschi
 * @version $Id: S3IdentifierIterator.java 12437 2013-01-30 17:34:41Z manuel $
 */
public class S3IdentifierIterator implements Iterator<DataIdentifier>
{

    private S3Iterator iterator;

    public S3IdentifierIterator(AmazonS3 client, String bucket, String prefix)
    {
        this(client, bucket, prefix, 100);
    }

    public S3IdentifierIterator(AmazonS3 client, String bucket, String prefix, int size)
    {
        iterator = new S3Iterator(client, bucket, prefix, size);
    }

    @Override
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    @Override
    public DataIdentifier next()
    {
        S3ObjectSummary summary = iterator.next();
        if (summary != null)
        {
            return new DataIdentifier(StringUtils.substringAfterLast(summary.getKey(), "/"));
        }
        return null;
    }

    @Override
    public void remove()
    {
        iterator.remove();
    }

}
