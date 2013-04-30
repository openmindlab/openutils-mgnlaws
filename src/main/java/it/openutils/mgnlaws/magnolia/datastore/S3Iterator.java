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