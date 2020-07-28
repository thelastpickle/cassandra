CREATE KEYSPACE CQLKeyspace
   WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
