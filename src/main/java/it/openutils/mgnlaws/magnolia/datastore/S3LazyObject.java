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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;


/**
 * @author molaschi
 * @version $Id: S3LazyObject.java 12437 2013-01-30 17:34:41Z manuel $
 */
public class S3LazyObject implements Closeable
{

    private String key;

    private String bucket;

    private ObjectMetadata metadata;

    private AmazonS3 client;

    private S3Object internalObject;

    private InputStream internalInputStream;

    public S3LazyObject(AmazonS3 client, String bucket, String key)
    {
        this.key = key;
        this.client = client;
        this.bucket = bucket;
        this.metadata = client.getObjectMetadata(bucket, key);
    }

    public long getLastModified()
    {
        return metadata.getLastModified().getTime();
    }

    public long getSize()
    {
        return metadata.getContentLength();
    }

    public InputStream getInputStream()
    {
        if (internalInputStream == null)
        {
            if (internalObject == null)
            {
                internalObject = client.getObject(bucket, key);
            }
            internalInputStream = new BufferedInputStream(internalObject.getObjectContent());
        }
        return internalInputStream;
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeQuietly(internalInputStream);
    }

}
