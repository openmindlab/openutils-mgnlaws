package it.openutils.mgnlaws.magnolia.datastore;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.db.DbDataStore;
import org.apache.jackrabbit.core.util.db.ConnectionFactory;
import org.apache.jackrabbit.core.util.db.DatabaseAware;


/**
 * @author molaschi
 * @version $Id: MigrateDataStoreDbToS3.java 12568 2013-02-05 22:58:11Z manuel $
 */
public class MigrateDataStoreDbToS3 extends S3DataStore implements DatabaseAware
{

    private DbDataStore dbDataStore;

    public MigrateDataStoreDbToS3()
    {
        dbDataStore = new DbDataStore();
    }

    @Override
    public void setConnectionFactory(ConnectionFactory connectionFactory)
    {
        dbDataStore.setConnectionFactory(connectionFactory);
    }

    @Override
    public void init(String homeDir) throws RepositoryException
    {
        super.init(homeDir);

        try
        {
            dbDataStore.init(homeDir);
        }
        catch (DataStoreException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException
    {
        return this.getRecordIfStored(identifier);
    }

    @Override
    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException
    {
        DataRecord record = null;
        try
        {
            record = super.getRecordIfStored(identifier);
        }
        finally
        {
            if (record == null)
            {
                return dbDataStore.getRecordIfStored(identifier);
            }
        }
        return record;
    }

    public void setDataSourceName(String dataSourceName)
    {
        dbDataStore.setDataSourceName(dataSourceName);
    }

    public void setDatabaseType(String databaseType)
    {
        dbDataStore.setDatabaseType(databaseType);
    }

    public void setCopyWhenReading(boolean copyWhenReading)
    {
        dbDataStore.setCopyWhenReading(copyWhenReading);
    }

    @Override
    public void setMinRecordLength(int minRecordLength)
    {
        super.setMinRecordLength(minRecordLength);
        dbDataStore.setMinRecordLength(minRecordLength);
    }
}
