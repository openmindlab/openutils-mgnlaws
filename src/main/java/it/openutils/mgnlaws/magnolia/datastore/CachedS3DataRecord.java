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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.core.data.AbstractDataRecord;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStoreException;


/**
 * @author molaschi
 * @version $Id: CachedS3DataRecord.java 12478 2013-02-01 15:21:36Z manuel $
 */
public class CachedS3DataRecord extends AbstractDataRecord
{

    private long length;

    private long lastModified;

    private String fileToStreamAbsolutePath;

    private transient File fileToStream;

    public CachedS3DataRecord()
    {
        super(new DataIdentifier(StringUtils.EMPTY));
    }

    public CachedS3DataRecord(S3DataRecord record, File cacheDirectory) throws DataStoreException, IOException
    {
        super(record.getIdentifier());
        this.length = record.getLength();
        this.lastModified = record.getLastModified();

        fileToStream = File.createTempFile(this.getIdentifier().toString() + "-", null, cacheDirectory);
        fileToStreamAbsolutePath = fileToStream.getAbsolutePath();
        FileOutputStream fos = new FileOutputStream(fileToStream);
        try
        {
            IOUtils.copyLarge(record.getStream(), fos);
        }
        finally
        {
            IOUtils.closeQuietly(record.getStream());
            IOUtils.closeQuietly(fos);
        }
    }

    public CachedS3DataRecord(S3DataRecord record, File cacheDirectory, File temp)
        throws DataStoreException,
        IOException
    {
        super(record.getIdentifier());
        this.length = record.getLength();
        this.lastModified = record.getLastModified();

        fileToStream = File.createTempFile(this.getIdentifier().toString() + "-", null, cacheDirectory);
        temp.renameTo(fileToStream);
        fileToStreamAbsolutePath = fileToStream.getAbsolutePath();
        fileToStream = null;
    }

    @Override
    public long getLength() throws DataStoreException
    {
        return length;
    }

    @Override
    public InputStream getStream() throws DataStoreException
    {
        synchronized (this)
        {
            if (fileToStream == null)
            {
                fileToStream = new File(fileToStreamAbsolutePath);
                if (!fileToStream.exists())
                {
                    return null;
                }
            }
        }
        try
        {
            return new FileInputStream(fileToStream);
        }
        catch (FileNotFoundException e)
        {
            // should never happen
            return null;
        }
    }

    @Override
    public long getLastModified()
    {
        return lastModified;
    }

    public void delete()
    {
        synchronized (this)
        {
            if (fileToStream == null)
            {
                fileToStream = new File(fileToStreamAbsolutePath);
                if (!fileToStream.exists())
                {
                    return;
                }
            }
            fileToStream.delete();
        }
    }

}
