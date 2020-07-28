CREATE KEYSPACE CatalogKeyspace
   WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
