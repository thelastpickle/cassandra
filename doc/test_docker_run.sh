docker run -i -t \
  --volume $(pwd)/source/modules/cassandra/examples/TEXT/NODETOOL:/antora/cassandra/doc/source/modules/cassandra/examples/TEXT/NODETOOL \
  --volume $(pwd)/source/modules/cassandra/pages/tools/nodetool:/antora/cassandra/doc/source/modules/cassandra/pages/tools/nodetool \
  --volume $(pwd)/source/modules/cassandra/pages/configuration:/antora/cassandra/doc/source/modules/cassandra/pages/configuration \
  cassandra-docs:latest
