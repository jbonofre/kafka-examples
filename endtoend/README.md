# Apache Kafka end to end example

This is a full example of using Kafka between two applications.

It's composed by two application modules:

1. The `Reader` is reading a file (the path is provided as argument) and send a record in Kafka for each line of the file.
2. The `Writer` is parsing the records and write a file for each record.

The `Reader` uses the line ID as key, meaning that each line index of files with be a partition (for instance all `line_0` for the files will be in partition 0, all `line_1` in another partition, and so on).

The `Writer` uses an unique consumer group to consume each record.

## Reader

The reader provides `Main` class with the `main()` method.

You can launch it directly in an IDE or using `java`.

It takes a file to read as argument:

````
java -cp reader.jar:kafka-client.jar:... net.nanthrax.kafka.examples.endtoend.reader.Main /path/to/file
````

As said previously, it reads the file content and will create a record in Kafka for each line of the file.

## Writer

The writer also provides `Main` class with the `main()` method.

You can launch it directly in an IDE or using `java`.

It takes an output directory as argument:

````
java -cp writer.jar:kafka-client.jar:... net.nanthrax.kafka.examples.endtoend.writer.Main /path/to/output
````

It will create a file for each Kafka record, meaning one file per line.