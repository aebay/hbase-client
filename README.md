# hbase-client

Client for HBase 1.3 using Scala.  WIP - needs improvement.

## Testing

1. Open a terminal and navigate to the root of this project
2. Run this command to ensure that you're using the latest build of the Docker images:
```
$ docker-compose -f src/test/resources/docker/docker-compose.yml build
```

3. Start the docker container(s):
```
$ docker-compose -f src/test/resources/docker/docker-compose.yml up -d
```

4. Run the test suite.
5. Stop the docker container(s):
```
$ docker-compose -f src/test/resources/docker/docker-compose.yml down
```

## Notes

- To enter the HBase docker container once it's up and running, run this command:
```
$ docker exec -it <CONTAINER_ID> /bin/bash
```

- To run the HBase shell, start the HBase container, enter it and run this command:
```
$ hbase/bin/hbase shell
```