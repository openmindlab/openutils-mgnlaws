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
