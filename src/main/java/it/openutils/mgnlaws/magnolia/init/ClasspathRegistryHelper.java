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

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.core.jndi.BindableRepository;


/**
 * JNDI helper functionality. This class contains static utility methods for binding and unbinding Jackrabbit
 * repositories to and from a JNDI context.
 */
public class ClasspathRegistryHelper
{

    /**
     * hidden constructor
     */
    private ClasspathRegistryHelper()
    {
    }

    /**
     * Binds a configured repository to the given JNDI context. This method creates a {@link BindableRepository
     * BindableRepository} instance using the given configuration information, and binds it to the given JNDI context.
     * @param ctx context where the repository should be registered (i.e. bound)
     * @param name the name to register the repository with
     * @param configFilePath path to the configuration file of the repository
     * @param repHomeDir repository home directory
     * @param overwrite if <code>true</code>, any existing binding with the given name will be overwritten; otherwise a
     * <code>NamingException</code> will be thrown if the name is already bound
     * @throws RepositoryException if the repository cannot be created
     * @throws NamingException if the repository cannot be registered in JNDI
     */
    public static void registerRepository(Context ctx, String name, String configFilePath, String repHomeDir,
        boolean overwrite) throws NamingException, RepositoryException
    {
        Reference reference = new Reference(
            Repository.class.getName(),
            ClasspathBindableRepositoryFactory.class.getName(),
            null); // no classpath defined
        reference.add(new StringRefAddr(BindableRepository.CONFIGFILEPATH_ADDRTYPE, configFilePath));
        reference.add(new StringRefAddr(BindableRepository.REPHOMEDIR_ADDRTYPE, repHomeDir));

        // always create instance by using BindableRepositoryFactory
        // which maintains an instance cache;
        // see http://issues.apache.org/jira/browse/JCR-411 for details
        Object obj = new ClasspathBindableRepositoryFactory().getObjectInstance(reference, null, null, null);
        if (overwrite)
        {
            ctx.rebind(name, obj);
        }
        else
        {
            ctx.bind(name, obj);
        }
    }

    /**
     * This method shutdowns a {@link BindableRepository BindableRepository} instance using the given configuration
     * information, and unbinds it from the given JNDI context.
     * @param ctx context where the repository should be unregistered (i.e. unbound)
     * @param name the name of the repository to unregister
     * @throws NamingException on JNDI errors
     */
    public static void unregisterRepository(Context ctx, String name) throws NamingException
    {
        ((JackrabbitRepository) ctx.lookup(name)).shutdown();
        ctx.unbind(name);
    }

}
