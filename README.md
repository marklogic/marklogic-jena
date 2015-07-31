# marklogic-jena v1.0.0-SNAPSHOT

_IMPORTANT_ - NO RELEASE HAS BEEN MADE YET

## Introduction

This library integrates MarkLogic Semantics feature into the [Jena RDF
Framework](http://jena.apache.org) as a persistence and query layer.


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

Ensure MarkLogic (Nightly) is installed and running.

#### Setup  MarkLogic Jena

1) clone or download marklogic-jena _develop_ branch

```
https://github.com/marklogic/marklogic-jena/tree/develop
```

2) run gradle target that provisions a testing database for this project.  The command and tests use values recorded in `./gradle.properties`

```
gradle :marklogic-jena:mlDeploy
```

3) build MarkLogic Jena

```
gradle :marklogic-jena:test

```

To use in your own projects, deploy into local maven repo or copy snapshot jars from /build directory.

```
gradle install

```

## Usage

### Quick start (Note: draft for future release)

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

### Javadocs

http://marklogic.github.io/marklogic-jena/marklogic-jena/build/docs/javadoc/

### Examples

The following examples demonstrate idiomatic usage of the MarkLogic as a Jena DatasetGraph.

(TBD - provide /samples dir)




## Support
The markLogic-jena is maintained by MarkLogic Engineering and distributed under the [Apache 2.0 license](https://github.com/marklogic/java-client-api/blob/master/LICENSE). It is designed for use in production applications with MarkLogic Server. Everyone is encouraged to file bug reports, feature requests, and pull requests through GitHub. This input is critical and will be carefully considered, but we can’t promise a specific resolution or timeframe for any request. In addition, MarkLogic provides technical support for [release tags](https://github.com/marklogic/marklogic-jena/releases) of the Java Client API to licensed customers under the terms outlined in the [Support Handbook](http://www.marklogic.com/files/Mark_Logic_Support_Handbook.pdf). For more information or to sign up for support, visit [help.marklogic.com](http://help.marklogic.com).
