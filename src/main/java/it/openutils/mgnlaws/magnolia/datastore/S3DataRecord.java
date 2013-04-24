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