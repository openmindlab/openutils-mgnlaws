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
