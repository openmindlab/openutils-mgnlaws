/*
 * S3Iterator.java
 */
package it.openutils.mgnlaws.magnolia.datastore;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;


//import org.jets3t.service.S3ServiceException;

public class S3Iterator implements Iterator<S3ObjectSummary>
{

    /** Indicates if iterator has been initialized */
    private boolean initDone;

    private AmazonS3 client;

    private ObjectListing currentListing;

    private Iterator<S3ObjectSummary> currentIterator;

    private ListObjectsRequest request;

    public S3Iterator(AmazonS3 client, String bucket, String prefix)
    {
        this(client, bucket, prefix, 100);
    }

    /**
     * Create a new S3Iterator
     * @param awsCredentials The credentials
     * @param bucketName The bucket name
     * @param size The chunk size
     */
    public S3Iterator(AmazonS3 client, String bucket, String prefix, int size)
    {
        this.client = client;
        this.initDone = false;
        request = new ListObjectsRequest().withBucketName(bucket).withPrefix(prefix).withMaxKeys(size);
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasNext()
    {
        if (!initDone)
        {
            init();
        }

        boolean hasNext = currentIterator.hasNext();
        if (!hasNext)
        {
            getNext();
            hasNext = currentIterator.hasNext();
            if (!hasNext)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public S3ObjectSummary next()
    {
        if (!hasNext())
        {
            throw new NoSuchElementException();
        }

        return currentIterator.next();
    }

    /**
     * {@inheritDoc}
     */
    public void remove()
    {
        throw new UnsupportedOperationException("Remove not supported.");
    }

    /**
     * Initialize the iterator
     */
    private void init()
    {
        getNext();
        initDone = true;
    }

    /**
     * Get next results
     * @param s3Service
     * @throws S3ServiceException
     */
    private void getNext()
    {
        if (currentListing == null || currentListing.isTruncated())
        {
            if (currentListing.isTruncated())
            {
                request.setMarker(currentListing.getNextMarker());
            }
            currentListing = client.listObjects(request);
            currentIterator = currentListing.getObjectSummaries().iterator();
        }
    }
}