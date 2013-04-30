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

package it.openutils.mgnlaws.magnolia.init;

import java.util.Hashtable;
import java.util.Map;

import javax.jcr.RepositoryException;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.apache.commons.collections.map.ReferenceMap;
import org.apache.jackrabbit.core.jndi.BindableRepository;


/**
 * @author molaschi
 * @version $Id: ClasspathBindableRepositoryFactory.java 10433 2012-07-23 11:04:39Z fabian.necci $
 */
public class ClasspathBindableRepositoryFactory implements ObjectFactory
{

    /**
     * cache using <code>java.naming.Reference</code> objects as keys and storing soft references to
     * <code>BindableRepository</code> instances
     */
    private static final Map cache = new ReferenceMap();

    /**
     * {@inheritDoc}
     */
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable environment)
        throws RepositoryException
    {
        synchronized (cache)
        {
            Object instance = cache.get(obj);
            if (instance == null && obj instanceof Reference)
            {
                instance = new ClasspathBindableRepository((Reference) obj);
                cache.put(obj, instance);
            }
            return instance;
        }
    }

    /**
     * Invalidates the given reference in this factory's cache. Called by {@link BindableRepository#shutdown()} to
     * remove the old reference.
     * @param reference repository reference
     */
    static void removeReference(Reference reference)
    {
        synchronized (cache)
        {
            cache.remove(reference);
        }
    }

}