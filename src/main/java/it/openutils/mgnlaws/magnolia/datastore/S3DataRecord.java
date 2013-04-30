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

/*
 * S3DataRecord.java
 */
package it.openutils.mgnlaws.magnolia.datastore;

import java.io.InputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.core.data.AbstractDataRecord;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class S3DataRecord extends AbstractDataRecord implements DataRecord
{

    private S3LazyObject object;

    public S3DataRecord(DataIdentifier identifier, S3LazyObject object)
    {
        super(identifier);
        this.object = object;
    }

    public S3DataRecord(AmazonS3 client, S3ObjectSummary summary)
    {
        this(new DataIdentifier(StringUtils.substringAfterLast(summary.getKey(), "/")), new S3LazyObject(
            client,
            summary.getBucketName(),
            summary.getKey()));
    }

    /**
     * {@inheritDoc}
     */
    public long getLastModified()
    {
        return object.getLastModified();
    }

    /**
     * {@inheritDoc}
     */
    public long getLength() throws DataStoreException
    {
        return object.getSize();
    }

    /**
     * {@inheritDoc}
     */
    public InputStream getStream() throws DataStoreException
    {
        return object.getInputStream();
    }
}