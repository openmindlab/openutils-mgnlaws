package it.openutils.mgnlaws.magnolia.init;

import java.io.InputStream;

import javax.jcr.RepositoryException;
import javax.naming.Reference;

import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.core.jndi.BindableRepository;


/**
 * @author molaschi
 * @version $Id: ClasspathBindableRepository.java 10433 2012-07-23 11:04:39Z fabian.necci $
 */
public class ClasspathBindableRepository extends BindableRepository
{

    /**
     * 
     */
    private static final long serialVersionUID = -4592794499813826391L;

    /**
     * @param reference
     * @throws RepositoryException
     */
    public ClasspathBindableRepository(Reference reference) throws RepositoryException
    {
        super(reference);
    }

    @Override
    protected JackrabbitRepository createRepository() throws RepositoryException
    {
        Reference reference = this.getReference();
        String configFilePath = reference.get(CONFIGFILEPATH_ADDRTYPE).getContent().toString();
        if (StringUtils.startsWith(configFilePath, ClasspathPropertiesInitializer.CLASSPATH_PREFIX))
        {
            InputStream resource = getClass().getResourceAsStream(
                StringUtils.substringAfter(configFilePath, ClasspathPropertiesInitializer.CLASSPATH_PREFIX));
            if (resource != null)
            {
                RepositoryConfig config = RepositoryConfig.create(resource, reference
                    .get(REPHOMEDIR_ADDRTYPE)
                    .getContent()
                    .toString());
                return RepositoryImpl.create(config);
            }
        }
        return super.createRepository();
    }

    @Override
    public void shutdown()
    {
        ClasspathBindableRepositoryFactory.removeReference(getReference());
        super.shutdown();
    }

}
