# D4M

[![Build Status](https://travis-ci.org/medined/d4m.svg?branch=master)](https://travis-ci.org/medined/d4m) - Travis CI
<br/>
[![Build Status](https://api.shippable.com/projects/5483b0d9d46935d5fbbf8b5f/badge?branchName=master)](https://app.shippable.com/projects/5483b0d9d46935d5fbbf8b5f/builds/latest) - Shippable
<br/>

This project provides a Java API to create the tables needed for the Accumulo 
D4M schema (described at https://github.com/medined/D4M_Schema).

# Getting It

```
<dependency>
  <groupId>com.codebits</groupId>
  <artifactId>d4m</artifactId>
  <version>1.0.2</version>
  <type>jar</type>
</dependency>
```

# How To Use

This project helps you to use the D4M Accumulo schema by providing methods to 
create the needed Accumulo tables. This work is done by the TableManager 
object. The code below shows how the object is used.

```
MiniAccumuloConfigImpl miniAccumuloConfig = new MiniAccumuloConfigImpl(new File("/accumulo"), "password");
miniAccumuloConfig.setNumTservers(20);

MiniAccumuloClusterImpl accumulo = new MiniAccumuloClusterImpl(miniAccumuloConfig);
accumulo.start();

Connector connector = accumulo.getConnector("root", "password");

TableManager tableManager = new TableManager(connector, tableOperations);
tableManager.createTables();
tableManager.addSplitsForSha1();
```
