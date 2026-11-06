# Documentation
If you have the option we recommend using [DataFrames](14_data_frames.md) instead of RDDs, they are also accessible from Java

## Using Connector in Java
This section describes how to access the functionality of Connector when 
you write your program in Java. It is assumed that you already 
familiarized yourself with the previous sections and you understand how 
Connector works. The Java API is included in the standard
`arizona-spark-connector` artifact.

### Prerequisites 

#### Spark Cassandra Connector < 2.0
In order to use Java API, you need to add the arizona-spark-connector to the list of dependencies:

```scala
libraryDependencies += "io.teknek.arizona.spark" %% "arizona-spark-connector" % "1.6.0" 
```

#### Spark Cassandra Connector >= 2.0
The Java API is now included in the standard Spark Cassandra Connector module, no additional dependencies are
required.


### Basic Usage

The best way to use Connector Java API is to import statically all the methods in `CassandraJavaUtil`. 
This utility class is the main entry point for Connector Java API.

```java
import static io.teknek.arizona.spark.connector.japi.CassandraJavaUtil.*;
```

The code snippets below work with a sample keyspace `ks` and table `people`. From the CQLSH shell, create 
the keyspace, table, and data with these commands:

```
create keyspace if not exists ks with replication = {'class':'SimpleStrategy', 'replication_factor':1};

create table if not exists ks.people (
  id int primary key,
  name text,
  birth_date timestamp
);

create index on ks.people (name);

insert into ks.people (id, name, birth_date) values (10, 'Catherine', '1987-12-02');
insert into ks.people (id, name, birth_date) values (11, 'Isadora', '2004-09-08');
insert into ks.people (id, name, birth_date) values (12, 'Anna', '1970-10-02');
```


### Accessing Cassandra tables in Java
`CassandraJavaRDD` is a `CassandraRDD` counterpart in Java. It allows 
to invoke easily Connector specific methods in order to enforce selection 
or projection on the database side. However, conversely to `CassandraRDD`, 
it extends `JavaRDD` which is much more suitable for the development of 
Spark applications in Java. 

In order to create `CassandraJavaRDD` you need to invoke one of the 
`cassandraTable` methods of a special wrapper around `SparkContext`. The 
wrapper can be easily obtained with use of one of the overloaded `javaFunctions`
method in `CassandraJavaUtil`. 

#### Example Reading a Cassandra Table In Java and Extracting a String Column into an RDD of Strings
```java
JavaRDD<String> cassandraRowsRDD = javaFunctions(sc).cassandraTable("ks", "people")
        .map(new Function<CassandraRow, String>() {
            @Override
            public String call(CassandraRow cassandraRow) throws Exception {
                return cassandraRow.toString();
            }
        });
System.out.println("Data as CassandraRows: \n" + StringUtils.join(cassandraRowsRDD.toArray(), "\n"));
```

In the above example, `cassandraTable` method has been used to create `CassandraJavaRDD` view of the data in `ks.people`.
The elements of the returned *RDD* are of `CassandraRow` type. If you want to produce an *RDD* of custom beans, you may
use `cassandraTable` method, which accepts a custom `RowReaderFactory` - (see
[Working with user-defined case classes and tuples](4_mapper.md) for more details).

#### Obtaining CassandraJavaRDD

Since version 1.1.x, Java API comes with several useful factory methods which can be used to create factories of row
readers of the two major kinds: type converter based and column mapper based.

The type converter based row reader uses a single `TypeConverter` to map a single column from a row to some type. It
doesn't matter how many columns are in projection because it always choose the first one. This kind of row reader is
useful when one wants to select a single column from a table and map it directly to an *RDD* of values of such types as
*String*, *Integer*, etc. For example, we may want to get an *RDD* of prices in order to calculate the average:


#### Example Reading column from a Cassandra Table into an RDD of Doubles using mapColumnTo
```java
JavaRDD<Double> pricesRDD = javaFunctions(sc).cassandraTable("ks", "people", mapColumnTo(Double.class)).select("price");
```

In the above example we explicitly select a single column (see the next subsection for details) and map it directly to
*Double*.

There are other overloaded versions of `mapColumnTo` methods which allow to map a column to one of the collection types,
or with use of an explicitly specified type converter.

The column mapper based row reader takes all the selected columns and maps them to some object with use of a given
`ColumnMapper`. The corresponding factories can be easily obtained by series of `mapRowTo` overloaded methods.

#### Example Reading rows from a Cassandra Table into an RDD of Bean Classes using mapRowTo
```java
// firstly, we define a bean class
public static class Person implements Serializable {
    private Integer id;
    private String name;
    private Date birthDate;

    // Remember to declare no-args constructor
    public Person() { }

    public Person(Integer id, String name, Date birthDate) {
        this.id = id;
        this.name = name;
        this.birthDate = birthDate;
    }

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Date getBirthDate() { return birthDate; }
    public void setBirthDate(Date birthDate) { this.birthDate = birthDate; }

    // other methods, constructors, etc.
}
```

```java
JavaRDD<Person> rdd = javaFunctions(sc).cassandraTable("ks", "people", mapRowTo(Person.class));
```

In this example, we created a `CassandraJavaRDD` of `Person` elements. While defining bean classes like 
`Person`, remember to define no-args constructor. Although, it is not required for it to be the only constructor 
of such a class.

By default, `mapRowTo` methods use `JavaBeanColumnMapper` with a default 
column name mapping logic. The column name translation can be customised 
by providing pairs for column name and attribute name which have to be overridden. 
There is also one overloaded `mapRowTo` methods which allows to specify 
a custom `ColumnMapper`. More details about column mapper can be found 
in [Working with user-defined case classes and tuples](4_mapper.md) and
[Customizing the mapping between Scala and Cassandra](6_advanced_mapper.md).

Since 1.2, it is possible to easily provide custom column name to property 
name translation by `select` method.

#### Example Reading a Cassandra Table with into a Bean Class with Differently Named Fields
Say we have a table `people2` with columns `id INT`, `last_name TEXT`, `date_of_birth TIMESTAMP` and
we want to map the rows of this table to objects of `Person` class.

```java
CassandraJavaRDD<Person> rdd = javaFunctions(sc).cassandraTable("ks", "people2", mapRowTo(Person.class)).select(
        column("id"),
        column("last_name").as("name"),
        column("date_of_birth").as("birthDate"));
```

`as` method can be used for any type of projected value: normal column, TTL or write time:

```java
javaFunctions(sc).cassandraTable("test", "table", mapRowTo(SomeClass.class)).select(
        column("no_alias"),
        column("simple").as("simpleProp"),
        ttl("simple").as("simplePropTTL"),
        writeTime("simple").as("simpleWriteTime"))
```

#### Obtaining CassandraJavaPairRDD

Since 1.1.0 one can directly obtain a *CassandraJavaPairRDD*, which is 
an extension of *JavaPairRDD*. This can be done easily by specifying two
row reader factories (vs one row reader factory in the previous examples). 
The corresponding row readers are responsible for resolving key and 
value from each row. The same methods `mapRowTo` and `mapColumnTo` can
be used to obtain the proper factories. However, one should keep in mind
 the following nuances:

Key row reader | Value row reader | Remarks
---------------|------------------|-----------
mapColumnTo    | mapColumnTo      | 1st column mapped to key, 2nd column mapped to value
mapColumnTo    | mapRowTo         | 1st column mapped to key, whole row mapped to value
mapRowTo       | mapColumnTo      | whole row mapped to key, 1st column mapped to value
mapRowTo       | mapRowTo         | whole row mapped to key, whole row mapped to value

#### Example Reading a Cassandra Table into a JavaPairRDD
```java
CassandraJavaPairRDD<Integer, String> rdd1 = javaFunctions(sc)
    .cassandraTable("ks", "people", mapColumnTo(Integer.class), mapColumnTo(String.class))
    .select("id", "name");

CassandraJavaPairRDD<Integer, Person> rdd2 = javaFunctions(sc)
    .cassandraTable("ks", "people", mapColumnTo(Integer.class), mapRowTo(Person.class))
    .select("id", "name", "birth_date");
```

### Using selection and projection on the database side
Once `CassandraJavaRDD` is created, you may apply selection and 
projection on that RDD by invoking `where` and `select` methods on it 
respectively. Their semantic is the same as the semantic of their counterparts
in `CassandraRDD`.

Note: See the [description of filtering](3_selection.md) to understand the limitations of the `where` method.

#### Example Using select to perform Server Side Column Pruning
```java
JavaRDD<String> rdd = javaFunctions(sc).cassandraTable("ks", "people")
        .select("id").map(new Function<CassandraRow, String>() {
            @Override
            public String call(CassandraRow cassandraRow) throws Exception {
                return cassandraRow.toString();
            }
        });
System.out.println("Data with only 'id' column fetched: \n" + StringUtils.join(rdd.toArray(), "\n"));
```

#### Example Using where to perform Server Side Filtering
```java
JavaRDD<String> rdd = javaFunctions(sc).cassandraTable("ks", "people")
        .where("name=?", "Anna").map(new Function<CassandraRow, String>() {
            @Override
            public String call(CassandraRow cassandraRow) throws Exception {
                return cassandraRow.toString();
            }
        });
System.out.println("Data filtered by the where clause (name='Anna'): \n" + StringUtils.join(rdd.toArray(), "\n"));
```

### Saving data to Cassandra

`javaFunctions` method can be also applied to any *RDD* in order to provide `writerBuilder` factory method.
In Spark Cassandra Connector prior to 1.1.0 there are a number of overloaded `saveToCassandra` methods because
of a lack of default values support for arguments and implicit conversions. Starting from version 1.1.0 they were
replaced by a builder object `RDDAndDStreamCommonJavaFunctions.WriterBuilder`, which can be obtained by invoking
`writerBuilder` method on the *RDD* wrapper. When the builder is eventually configured, one needs to call
`saveToCassandra` method on it to run writing job.

#### Example of Saving an RDD of Person objects to a Cassandra Table
In the following example, a `JavaRDD` of `Person` elements is saved to Cassandra table `ks.people` with a default
mapping and configuration.
```java
List<Person> people = Arrays.asList(
        new Person(1, "John", new Date()),
        new Person(2, "Troy", new Date()),
        new Person(3, "Andrew", new Date())
);
JavaRDD<Person> rdd = sc.parallelize(people);
javaFunctions(rdd).writerBuilder("ks", "people", mapToRow(Person.class)).saveToCassandra();
```

There are several `mapToRow` overloaded methods available to make it easier to get the proper `RowWriterFactory`
instance (which is the required third argument of `writerBuilder` method). In its simplest form, it takes the class
of *RDD* elements and uses a default `JavaBeanColumnMapper` to map those elements to Cassandra rows. Custom column name
to attribute translations can be specified in order to override the default logic. If `JavaBeanColumnMapper` is not an
option, a custom column mapper can be specified as well.

#### Example of Saving and RDD of Person object with Differently Named Fields
Say we have a table `people2` with columns `id INT`, `last_name TEXT`, `date_of_birth TIMESTAMP` and
we want to save RDD of `Person` class objects to this table. To do it we need to use overloaded `mapToRow(Class, Map<String, String>)` method.
```java
Map<String, String> fieldToColumnMapping = new HashMap<>();
fieldToColumnMapping.put("name", "last_name");
fieldToColumnMapping.put("birthDate", "date_of_birth");
javaFunctions(rdd).writerBuilder("ks", "people2", mapToRow(Person.class, fieldToColumnMapping)).saveToCassandra();
```
Another version of method `mapToRow(Class, Pair[])` can be considered much more handy for inline invocations.
```java
javaFunctions(rdd).writerBuilder("ks", "people2", mapToRow(
                Person.class,
                Pair.of("name", "last_name"),
                Pair.of("birthDate", "date_of_birth")))
        .saveToCassandra();
```
### Working with tuples

Since 1.3 there new methods to work with Scala tuples. 

To read a Cassandra table as an RDD of tuples, just use one of `mapRowToTuple` methods to create 
the appropriate `RowReaderFactory` instance. The arity of the tuple is determined by the number 
of parameters which are provided to the mentioned method. 

#### Example Saving a JavaRDD of Tuples to a Cassandra Table
```java
CassandraJavaRDD<Tuple3<String, Integer, Double>> rdd = javaFunctions(sc)
        .cassandraTable("ks", "tuples", mapRowToTuple(String.class, Integer.class, Double.class))
        .select("stringCol", "intCol", "doubleCol")
```

Remember to explicitly specify the columns to be selected because the values from the selected columns
are resolved by the column position rather than its name.

There are also new methods `mapTupleToRow` to create `RowWriterFactory` instance for tuples. 
Those methods require all the tuple arguments types to be provided. The number of them determines the
arity of tuples.

#### Example Saving a JavaRDD of Tuples with Custom Mapping to a Cassandra Table
```java
CassandraJavaUtil.javaFunctions(sc.makeRDD(Arrays.asList(tuple)))
        .writerBuilder("cassandra_java_util_spec", "test_table_4", mapTupleToRow(
                String.class,
                Integer.class,
                Double.class
        )).withColumnSelector(someColumns("stringCol", "intCol", "doubleCol"))
        .saveToCassandra()
```

Similarly to reading data as tuples, it is highly recommended to explicitly specify the columns which are
to be populated.

### Extensions for Spark Streaming

The main entry point for Spark Streaming in Java is `JavaStreamingContext` object. Like for `JavaSparkContext`, we 
can use `javaFunctions` method to access Connector specific functionality. For example, we can create an ordinary 
`CassandraJavaRDD` by invoking the same `cassandraTable` method as we do for `SparkContext`. There is nothing specific
to streaming in this case - these methods are provided only for convenience and they use `SparkContext` wrapped by 
`StreamingContext` under the hood. 

You may also save the data from `JavaDStream` to Cassandra. Again, you need to use `javaFunctions` method to create 
a special wrapper around `JavaDStream` and then invoke `writerBuilder` method and finally `saveToCassandra` on it.
*DStream* is a sequence of *RDDs* and when you invoke `saveToCassandra` on the builder, it will follow saving to
Cassandra all the *RDDs* in that *DStream*.

`javaFunctions` methods for Spark streaming related entities are provided in `CassandraStreamingJavaUtil`.

### Summary of changes between versions 1.0 and 1.1

- added the new functionality of the connector which has been introduced in v1.1
- removed multiple overloaded `cassandraTable` methods from the Java wrappers of `SparkContext` or `StreamingContext`
- introduced several static factory methods in `CassandraJavaUtil` for:
    - creating column based reader factories (`mapColumnTo` methods)
    - creating row based reader factories (`mapRowTo` methods)
    - creating writer factories (`mapToRow` methods)
    - creating type tags for arbitrary types and type parameters (`typeTag` methods)
    - resolving type converters for arbitrary types and type parameters (`typeConverter` methods)
- removed class argument from Java RDD wrappers factory methods
- deprecated `saveToCassandra` methods in Java RDD wrappers; the preferred way to save data to Cassandra is to use
  `writerBuilder` method, which returns `RDDAndDStreamCommonJavaFunctions.WriterBuilder` instance, which in turn has
  `saveToCassandra` method

### Further Examples

A longer example (with source code) of the Connector Java API is on the DataStax tech blog:
[Accessing Cassandra from Spark in Java](https://www.datastax.com/dev/blog/accessing-cassandra-from-spark-in-java).

[Next - Spark Streaming with Cassandra](8_streaming.md)
