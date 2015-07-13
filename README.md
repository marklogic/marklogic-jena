# marklogic-jena

## Version 1.0.0-SNAPSHOT

This library integrates MarkLogic Semantics feature into the Jena RDF
Framework as a persistence and query layer.

## Quick start (draft, for released version)

For gradle-based projects include this dependency in `build.gradle`
```
dependencies {
   compile 'com.marklogic:marklogic-jena:1.0.0'
}
```

Maven-based projects use this block in `pom.xml`:

```
<dependency>
    <groupId>com.marklogic</groupId>
    <artifactId>marklogic-jena</artifactId>
    <version>1.0.0</version>
</dependency>
```

### To build

Note: To use this library prior to the release of MarkLogic Server 8.0-4 and Java Client API 3.0.4, you must have contacted MarkLogic Product Management for access to an early version of the server.

To use this code as it's being initially developed, you need:

* A nightly build of MarkLogic 8, installed and running.  (See PM)
* A built and installed version of the SNAPSHOT Java Client API.  To build the
   Java Client API you'll need Maven 2.
```
git clone git@github.com:marklogic/java-client-api.git
cd java-client-api
git checkout develop
mvn -Dmaven.test.skip=true -Dmaven.javadoc.skip=true install
```
Now the prerequisites are available

* `./gradlew mlDeploy` installs the test harness on your local MarkLogic Server, on a new database and port.
* `./gradlew test` runs the tests.
 



