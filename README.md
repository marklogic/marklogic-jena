# marklogic-jena v3.0.6

## Introduction

This library integrates MarkLogic Semantics into the [Jena RDF
Framework](http://jena.apache.org) as a persistence and query layer.

### Before you start

#### Setup Marklogic

Ensure MarkLogic (8.0-6 or later) is installed and running.

### To use the develop branch

1) clone or download marklogic-jena _develop_ branch.

```
https://github.com/marklogic/marklogic-jena/tree/develop
```

2) Run the gradle target that provisions a testing database for this project.  The command and tests use values recorded in `./gradle.properties`.

```
./gradlew :marklogic-jena:mlDeploy
```

3) Build MarkLogic Jena.

```
./gradlew :marklogic-jena:test

```

To use `marklogic-jena` in your own projects, deploy into local maven repo or copy snapshot jars from /build directory.

```
./gradlew publishToMavenLocal

```

## Usage

### Quick start (Note: draft for future release)

For gradle-based projects include this dependency in `build.gradle`:
```
dependencies {
   implementation 'com.marklogic:marklogic-jena:4.0.0'
}
```

Maven-based projects use this block in `pom.xml`:

```
<dependency>
    <groupId>com.marklogic</groupId>
    <artifactId>marklogic-jena</artifactId>
    <version>4.0.0</version>
</dependency>
```

### Javadocs

http://marklogic.github.io/marklogic-jena/marklogic-jena/build/docs/javadoc/

### Examples

The project at [marklogic-jena-examples](marklogic-jena-examples) contains some
Java applications that exercise the functionality of MarkLogic using this
library.
