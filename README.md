# marklogic-jena v1.0.2

## Introduction

This library integrates MarkLogic Semantics feature into the [Jena RDF
Framework](http://jena.apache.org) as a persistence and query layer.


#### Setup Marklogic

Ensure MarkLogic (8.0-6 or later) is installed and running.

#### Setup  MarkLogic Jena

1) clone or download marklogic-jena _develop_ branch.

```
https://github.com/marklogic/marklogic-jena/tree/develop
```

2) Run the gradle target that provisions a testing database for this project.  The command and tests use values recorded in `./gradle.properties`.

```
gradle :marklogic-jena:mlDeploy
```

3) Build MarkLogic Jena.

```
gradle :marklogic-jena:test

```

To use `marklogic-jena` in your own projects, deploy into local maven repo or copy snapshot jars from /build directory.

```
gradle install

```

## Usage

### Quick start

For gradle-based projects include this dependency in `build.gradle`:
```
dependencies {
   compile 'com.marklogic:marklogic-jena:1.0.2'
}
```

Maven-based projects use this block in `pom.xml`:

```
<dependency>
    <groupId>com.marklogic</groupId>
    <artifactId>marklogic-jena</artifactId>
    <version>1.0.2</version>
</dependency>
```

### Javadocs

http://marklogic.github.io/marklogic-jena/marklogic-jena/build/docs/javadoc/

### Examples

The project at [marklogic-jena-examples](marklogic-jena-examples) contains some Java applications that exercise the functionality of MarkLogic using this library.


## Support

The markLogic-jena project is maintained by MarkLogic Engineering and 
distributed under the [Apache 2.0
license](https://github.com/marklogic/java-client-api/blob/master/LICENSE). It
is designed for use in production applications with MarkLogic Server. Everyone
is encouraged to file bug reports, feature requests, and pull requests through
GitHub. This input is critical and will be carefully considered, but we can’t
promise a specific resolution or timeframe for any request. 

In addition, MarkLogic provides technical support for [release
tags](https://github.com/marklogic/marklogic-jena/releases) of `marklogic-jena`
licensed customers under the terms outlined in the [Support
Handbook](http://www.marklogic.com/files/Mark_Logic_Support_Handbook.pdf). For
more information or to sign up for support, visit
[help.marklogic.com](http://help.marklogic.com).
