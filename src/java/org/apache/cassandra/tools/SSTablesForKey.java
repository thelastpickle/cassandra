package org.apache.cassandra.tools;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;

import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;


import java.io.IOException;
import java.util.List;


/**
 * @author zznate
 */
public class SSTablesForKey {
    static
       {
           CassandraDaemon.initLog4j();
       }


       public static void main(String args[]) throws IOException
       {

           String keyspace = args[0];
           String cf = args[1];
           String key = args[2];

           try
           {
               System.out.println(String.format("SStables for key with keyspace: '%s' column family: '%s' and key: '%s'",keyspace, cf, key));

               // load keyspace descriptions.
               DatabaseDescriptor.loadSchemas();

               if (Schema.instance.getCFMetaData(keyspace, cf) == null)
                   throw new IllegalArgumentException(String.format("Unknown keyspace/columnFamily %s.%s",
                                                                    keyspace,
                                                                    cf));

               Table table = Table.open(keyspace);
               ColumnFamilyStore cfs = table.getColumnFamilyStore(cf);
               List<String> sstables = cfs.getSSTablesForKey(key);
               for (String sstable : sstables)
               {
                   System.out.println(sstable);
               }

           }
           catch (Exception e)
           {
               System.err.println(e.getMessage());

               System.exit(1);
           }
           System.exit(0);
       }


}
