start zookeeper-server-start.bat ..\..\config\zookeeper.properties

TIMEOUT 10

start kafka-server-start.bat ..\..\config\server.properties
start kafka-server-start.bat ..\..\config\server-1.properties
start kafka-server-start.bat ..\..\config\server-2.properties

exit