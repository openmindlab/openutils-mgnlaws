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

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.event.CacheEventListener;


/**
 * @author molaschi
 * @version $Id: S3CacheListener.java 12437 2013-01-30 17:34:41Z manuel $
 */
public class S3CacheListener implements CacheEventListener
{

    private File cacheDir;

    public S3CacheListener(File cacheDir)
    {
        this.cacheDir = cacheDir;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyElementRemoved(Ehcache cache, Element element) throws CacheException
    {
        CachedS3DataRecord record = (CachedS3DataRecord) element.getObjectValue();
        record.delete();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyElementPut(Ehcache cache, Element element) throws CacheException
    {
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyElementUpdated(Ehcache cache, Element element) throws CacheException
    {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyElementExpired(Ehcache cache, Element element)
    {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyElementEvicted(Ehcache cache, Element element)
    {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyRemoveAll(Ehcache cache)
    {
        String absPath = cacheDir.getAbsolutePath();
        cacheDir.renameTo(new File(cacheDir.getAbsolutePath() + "-del"));
        cacheDir.delete();
        cacheDir = new File(absPath);
        cacheDir.mkdirs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void dispose()
    {

    }

    public File getCacheDir()
    {
        return cacheDir;
    }

    @Override
    public Object clone() throws CloneNotSupportedException
    {
        return super.clone();
    }

}
