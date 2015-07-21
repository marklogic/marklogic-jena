# marklogic-jena v1.0.0-SNAPSHOT

_IMPORTANT_ - NO RELEASE HAS BEEN MADE YET

## Introduction

This library integrates MarkLogic Semantics feature into the [Jena RDF
Framework](http://jena.apache.org) as a persistence and query layer.

## Quick start (draft, for future release)

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

#### Setup Java API Client

These instructions are for pre-release builds of this project.

1) clone or download Java API client _develop_ branch

```
https://github.com/marklogic/java-client-api/tree/develop
```

2) build and deploy Java API client

```
 mvn -Dmaven.test.skip=true -Dmaven.javadoc.skip=true deploy
 ```

you should verify that Java API client has been deployed to your local maven repo.

#### Setup Marklogic

Ensure MarkLogic (Nightly) is installed and running;

1) run gradle target that provisions everything needed for MarkLogic

```
gradle mlDeploy
```


#### Setup  MarkLogic Jena

1) clone or download marklogic-jena _develop_ branch

```
https://github.com/marklogic/marklogic-jena/tree/develop
```

2) build MarkLogic Jena

```
gradle test

```

will build and run unit tests.

### Usage

To use in your own code, deploy into local maven repo or copy snapshot jars from /build directory.

```
gradle deploy

```

### Javadocs

http://marklogic.github.io/marklogic-jena/build/docs/javadoc/

### Examples

The following examples demonstrate idiomatic usage of the MarkLogic as a Jena DatasetGraph.

(TBD - provide /samples dir)




