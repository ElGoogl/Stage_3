Terminal 1 (Start ingestion node 1):

$env:PORT="7001"
$env:ACTIVEMQ_URL="tcp://localhost:61616"
$env:ACTIVEMQ_QUEUE="books.ingested"
$env:HZ_MEMBERS="localhost:5701"
$env:NODE_ID="crawler1"
$env:REPLICATION_FACTOR="2"
$env:REPLICA_PEERS="http://localhost:7002"

java -DdataRepo=data_repository/datalake_node1 -jar ingestion_service/target/ingestion_service-1.0.0.jar

Terminal 2 (Start ingestion node 2):

$env:PORT="7002"
$env:ACTIVEMQ_URL="tcp://localhost:61616"
$env:ACTIVEMQ_QUEUE="books.ingested"
$env:HZ_MEMBERS="localhost:5701"
$env:NODE_ID="crawler2"
$env:REPLICATION_FACTOR="2"
$env:REPLICA_PEERS="http://localhost:7001"

java -DdataRepo=data_repository/datalake_node2 -jar ingestion_service/target/ingestion_service-1.0.0.jar