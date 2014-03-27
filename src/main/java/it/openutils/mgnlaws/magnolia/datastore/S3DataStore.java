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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;

import javax.jcr.RepositoryException;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;


public class S3DataStore implements DataStore
{

    /**
     * The Log
     */
    private static Logger log = LoggerFactory.getLogger(S3DataStore.class);

    private static final String DIGEST = "SHA-1";

    private static final String PREFIX = "datastore";

    private static final String PREFIX_TMP = "temp/";

    /**
     * Config parameter awsAccessKey
     */
    protected String awsAccessKey;

    /**
     * Config parameter awsSecretKey
     */
    protected String awsSecretKey;

    /**
     * Config parameter minModifiedDate
     */
    protected long minModifiedDate;

    /**
     * Config parameter minRecordLength
     */
    protected int minRecordLength;

    /**
     * Config parameter bucket name
     */
    protected String bucket;

    /**
     * Base path for s3 files
     */
    protected String s3Path = "/";

    /**
     * Config parameter path for temp files on upload
     */
    protected String tmpPath;

    /**
     * Config parameter for s3 endpoint
     */
    protected String endpoint = "http://s3-eu-west-1.amazonaws.com";

    /**
     * Config parameter to enable cache
     */
    protected boolean useCache = true;

    /**
     * Config parameter for ehcache configuration
     */
    protected String cacheConfigFile = "classpath:/ehcache-s3datastore.xml";

    /**
     * Config parameter for ehcache cache name
     */
    protected String cacheName = "s3datastore-cache";

    /**
     * Config parameter for cache files
     */
    protected String cacheDirectoryPath;

    /**
     * S3 client
     */
    private AmazonS3 amazonS3;

    /**
     * Temp file local directory
     */
    private File tmpDirectory;

    /**
     * Transfer manager
     */
    private TransferManager transferManager;

    private File cacheDirectory;

    private CacheManager cacheManager;

    private Cache cache;

    /**
     * All data identifiers that are currently in use are in this set until they are garbage collected.
     */
    protected Map<DataIdentifier, WeakReference<DataIdentifier>> inUse = Collections
        .synchronizedMap(new WeakHashMap<DataIdentifier, WeakReference<DataIdentifier>>());

    /**
     * Create a new DataStore
     */
    public S3DataStore()
    {
        log.debug("Creating S3DataStore");
    }

    /**
     * {@inheritDoc}
     */
    public DataRecord addRecord(InputStream input) throws DataStoreException
    {
        File temporary = null;
        try
        {
            temporary = newTemporaryFile();
            DataIdentifier tempId = new DataIdentifier(temporary.getName());
            usesIdentifier(tempId);
            // Copy the stream to the temporary file and calculate the
            // stream length and the message digest of the stream
            MessageDigest digest = MessageDigest.getInstance(DIGEST);
            OutputStream output = new DigestOutputStream(new FileOutputStream(temporary), digest);
            try
            {
                IOUtils.copyLarge(input, output);
            }
            finally
            {
                IOUtils.closeQuietly(output);
            }
            DataIdentifier identifier = new DataIdentifier(digest.digest());
            // File file;
            String tmpKey = PREFIX_TMP + identifier.toString();
            String key = getKey(identifier);

            if (!objectExists(identifier))
            {
                Upload upload = transferManager.upload(bucket, tmpKey, temporary);
                try
                {
                    AmazonClientException e;
                    if ((e = upload.waitForException()) != null && upload.getState() != TransferState.Completed)
                    {
                        throw new DataStoreException("Error uploading file to s3", e);
                    }
                }
                catch (InterruptedException e)
                {
                    throw new DataStoreException("Upload interrupted", e);
                }
            }

            S3DataRecord record;
            synchronized (this)
            {
                // Check if the same record already exists, or
                // move the temporary file in place if needed
                usesIdentifier(identifier);
                if (!objectExists(identifier))
                {
                    amazonS3.copyObject(bucket, tmpKey, bucket, key);
                    amazonS3.deleteObject(bucket, tmpKey);
                }
                else
                {
                    // effettua un touch sul file
                    touch(key);
                }
                record = new S3DataRecord(identifier, new S3LazyObject(amazonS3, bucket, key));
                if (useCache)
                {
                    cache.put(new Element(identifier.toString(), new CachedS3DataRecord(
                        record,
                        cacheDirectory,
                        temporary)));
                    // no need to remove file
                    temporary = null;
                }
            }
            // this will also make sure that
            // tempId is not garbage collected until here
            inUse.remove(tempId);
            return record;
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new DataStoreException(DIGEST + " not available", e);
        }
        catch (IOException e)
        {
            throw new DataStoreException("Could not add record", e);
        }
        finally
        {
            if (temporary != null)
            {
                temporary.delete();
            }
        }
    }

    private File newTemporaryFile() throws IOException
    {
        // the directory is already created in the init method
        return File.createTempFile("s3ds", null, tmpDirectory);
    }

    private boolean objectExists(DataIdentifier identifier)
    {
        S3Object object = null;
        try
        {
            return (object = amazonS3.getObject(bucket, getKey(identifier))) != null;
        }
        catch (AmazonS3Exception ex)
        {
            return false;
        }
        finally
        {
            if (object != null)
            {
                IOUtils.closeQuietly(object.getObjectContent());
            }
        }
    }

    private void touch(DataIdentifier identifier)
    {
        touch(getKey(identifier));
    }

    private void touch(String key)
    {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setLastModified(new Date());
        CopyObjectRequest req = new CopyObjectRequest(bucket, key, bucket, key).withNewObjectMetadata(metadata);
        amazonS3.copyObject(req);
    }

    private String getKey(DataIdentifier identifier)
    {
        String string = identifier.toString();
        return new StringBuffer()
            .append(PREFIX)
            .append(s3Path)
            .append("/")
            .append(string.substring(0, 2))
            .append("/")
            .append(string.substring(2, 4))
            .append("/")
            .append(string.substring(4, 6))
            .append("/")
            .append(string)
            .toString();
    }

    public DataIdentifier getDataIdentifier(String key)
    {
        return new DataIdentifier(StringUtils.substringAfterLast(key, "/"));
    }

    /**
     * {@inheritDoc}
     */
    public void clearInUse()
    {
        inUse.clear();
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws DataStoreException
    {
        transferManager.shutdownNow();
        if (useCache && cacheManager != null)
        {
            cacheManager.shutdown();
        }
    }

    /**
     * {@inheritDoc}
     */
    public int deleteAllOlderThan(long min) throws DataStoreException
    {
        int deleted = 0;

        for (Iterator<S3ObjectSummary> i = new S3Iterator(amazonS3, bucket, PREFIX); i.hasNext();)
        {
            S3ObjectSummary rec = (S3ObjectSummary) i.next();
            if (rec.getLastModified().getTime() < min)
            {
                deleteRecord(rec.getKey());
                if (useCache)
                {
                    cache.remove(getDataIdentifier(rec.getKey()));
                }
                deleted++;
            }
        }

        return deleted;
    }

    /**
     * {@inheritDoc}
     */
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException
    {
        return new S3IdentifierIterator(amazonS3, bucket, PREFIX);
    }

    /**
     * {@inheritDoc}
     */
    public int getMinRecordLength()
    {
        return minRecordLength;
    }

    private DataRecord getRecord(DataIdentifier identifier, boolean checkExistence)
    {
        String key = identifier.toString();
        if (checkExistence && (useCache && !cache.isKeyInCache(key)) && !objectExists(identifier))
        {
            return null;
        }

        DataRecord record;
        if (useCache)
        {
            try
            {
                cache.acquireReadLockOnKey(key);
                Element cached = cache.get(key);
                if (cached != null)
                {
                    return (DataRecord) cached.getObjectValue();
                }
            }
            finally
            {
                cache.releaseReadLockOnKey(key);
            }
        }

        try
        {
            synchronized (this)
            {
                S3LazyObject object = new S3LazyObject(amazonS3, bucket, getKey(identifier));
                if (minModifiedDate != 0 && object.getLastModified() < minModifiedDate)
                {
                    touch(identifier);
                }
                usesIdentifier(identifier);
                record = new S3DataRecord(identifier, object);

                if (useCache)
                {
                    cache.acquireWriteLockOnKey(key);
                    try
                    {
                        record = new CachedS3DataRecord((S3DataRecord) record, cacheDirectory);
                        cache.put(new Element(key, record));
                    }
                    catch (DataStoreException e)
                    {
                        log.error("Error creating cached record", e);
                    }
                    catch (IOException e)
                    {
                        log.error("Error creating cached record", e);
                    }
                }
                return record;
            }
        }
        finally
        {
            if (useCache)
            {
                cache.releaseWriteLockOnKey(key);
            }
        }
    }

    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException
    {
        return getRecord(identifier, true);
    }

    /**
     * {@inheritDoc}
     */
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException
    {
        return getRecord(identifier, false);
    }

    /**
     * Delete data record
     * @param identifier The DataIdentifier
     */
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException
    {
        deleteRecord(getKey(identifier));
    }

    private void deleteRecord(String key) throws DataStoreException
    {
        amazonS3.deleteObject(bucket, key);
    }

    /**
     * {@inheritDoc}
     */
    public void init(String homeDir) throws RepositoryException
    {
        // init S3 client
        amazonS3 = new AmazonS3Client(new BasicAWSCredentials(awsAccessKey, awsSecretKey));
        // set endpoint
        if (StringUtils.isNotBlank(endpoint))
        {
            amazonS3.setEndpoint(endpoint);
        }
        // init transfer manager
        transferManager = new TransferManager(amazonS3);

        // initialize tmp directory
        if (StringUtils.isNotBlank(tmpPath))
        {
            tmpDirectory = new File(tmpPath);
            if (!tmpDirectory.exists())
            {
                tmpDirectory.mkdirs();
            }
        }
        if (tmpDirectory == null || !tmpDirectory.isDirectory())
        {
            tmpDirectory = new File(System.getProperty("java.io.tmpdir"));
        }

        if (useCache)
        {
            // initialize cache directory
            if (StringUtils.isNotBlank(cacheDirectoryPath))
            {
                cacheDirectory = new File(cacheDirectoryPath);
                if (!cacheDirectory.exists())
                {
                    cacheDirectory.mkdirs();
                }
            }
            if (cacheDirectory == null || !cacheDirectory.isDirectory())
            {
                cacheDirectory = new File(System.getProperty("java.io.tmpdir"), cacheName);
                if (!cacheDirectory.exists())
                {
                    cacheDirectory.mkdirs();
                }
            }

            // create cache manager
            CacheManager cacheManager;
            if (StringUtils.startsWith(cacheConfigFile, "classpath:"))
            {
                URL configurationFileURL = getClass().getResource(
                    StringUtils.substringAfter(cacheConfigFile, "classpath:"));
                cacheManager = CacheManager.newInstance(configurationFileURL);
            }
            else
            {
                cacheManager = CacheManager.newInstance(cacheConfigFile);
            }
            // get cache
            cache = cacheManager.getCache(cacheName);
            // register cache listener
            cache.getCacheEventNotificationService().registerListener(new S3CacheListener(cacheDirectory));
        }
    }

    /**
     * {@inheritDoc}
     */
    public void updateModifiedDateOnAccess(long before)
    {
        minModifiedDate = before;
    }

    /**
     * Add WeakReference to inUse Map
     * @param identifier The DataIdentifier
     */
    private void usesIdentifier(DataIdentifier identifier)
    {
        inUse.put(identifier, new WeakReference<DataIdentifier>(identifier));
    }

    /**
     * @return the awsAccessKey
     */
    public String getAwsAccessKey()
    {
        return awsAccessKey;
    }

    /**
     * @param awsAccessKey the awsAccessKey to set
     */
    public void setAwsAccessKey(String awsAccessKey)
    {
        this.awsAccessKey = awsAccessKey;
    }

    /**
     * @return the awsSecretKey
     */
    public String getAwsSecretKey()
    {
        return awsSecretKey;
    }

    /**
     * @param awsSecretKey the awsSecretKey to set
     */
    public void setAwsSecretKey(String awsSecretKey)
    {
        this.awsSecretKey = awsSecretKey;
    }

    /**
     * @return the minModifiedDate
     */
    public long getMinModifiedDate()
    {
        return minModifiedDate;
    }

    /**
     * @param minModifiedDate the minModifiedDate to set
     */
    public void setMinModifiedDate(long minModifiedDate)
    {
        this.minModifiedDate = minModifiedDate;
    }

    /**
     * @param minRecordLength the minRecordLength to set
     */
    public void setMinRecordLength(int minRecordLength)
    {
        this.minRecordLength = minRecordLength;
    }

    public String getBucket()
    {
        return bucket;
    }

    public void setBucket(String bucket)
    {
        this.bucket = bucket;
    }

    public String getTmpPath()
    {
        return tmpPath;
    }

    public void setTmpPath(String tmpPath)
    {
        this.tmpPath = tmpPath;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public void setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
    }

    public void setUseCache(boolean useCache)
    {
        this.useCache = useCache;
    }

    public void setCacheConfigFile(String cacheConfigFile)
    {
        this.cacheConfigFile = cacheConfigFile;
    }

    public void setCacheName(String cacheName)
    {
        this.cacheName = cacheName;
    }

    public void setCacheDirectoryPath(String cacheDirectoryPath)
    {
        this.cacheDirectoryPath = cacheDirectoryPath;
    }

    public String getS3Path()
    {
        return s3Path;
    }

    public void setS3Path(String s3Path)
    {
        this.s3Path = s3Path;
    }

}
