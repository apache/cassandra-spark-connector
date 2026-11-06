# Documentation
If you have the option we recommend using [DataFrames](14_data_frames.md) instead of RDDs

## Customizing the mapping between Scala and Cassandra

While mapping rows to tuples and case classes work out-of-the box, 
in some cases you might need more control over Cassandra-Scala mapping. 
For example Java classes are likely to use the JavaBeans naming convention,
where accessors are named with `get`, `is` or `set` prefixes. 

To customize column-property mappings, 
you need to put an appropriate `ColumnMapper[YourClass]` implicit object in scope. 
Define such an object in a companion object of the class being mapped. 
A `ColumnMapper` affects both loading and saving data. A few special `ColumnMapper` 
implementations are included.

### Working with JavaBeans
To work with Java classes, use `JavaBeanColumnMapper`. 
Make sure your objects are `Serializable`, otherwise Spark won't be able to send them over the network.

#### Example of using a new implicit Column Mapper to map a JavaBean Like Class
```scala
import io.teknek.arizona.spark.connector.mapper.JavaBeanColumnMapper
class WordCount extends Serializable { 
    private var _word: String = ""
    private var _count: Int = 0
    def setWord(word: String) { _word = word }
    def setCount(count: Int) { _count = count }
    override def toString = _word + ":" + _count
}

object WordCount {
    implicit object Mapper extends JavaBeanColumnMapper[WordCount] 
}

sc.cassandraTable[WordCount]("test", "words").toArray
// Array(bar:20, foo:10)
```

To save objects of class `WordCount`, you'll need to define getters.

### Manually specifying the property-name to column-name relationship
If for some reason you wish to associate a column of a different name than the property, 
you may pass a column translation `Map` to a `DefaultColumnMapper` or `JavaBeanColumnMapper`:

#### Example of Using a Custom DefaultColumnMapper
```scala
case class WordCount(w: String, c: Int)

object WordCount { 
    implicit object Mapper extends DefaultColumnMapper[WordCount](
        Map("w" -> "word", "c" -> "count")) 
}

sc.cassandraTable[WordCount]("test", "words").toArray
// Array(WordCount(bar,20), WordCount(foo,10))

sc.parallelize(Seq(WordCount("baz", 30), WordCount("foobar", 40)))
  .saveToCassandra("test", "words", SomeColumns("word", "count"))
```

### Writing custom `ColumnMapper` implementations
To define column mappings for your classes, create an appropriate implicit object implementing 
`ColumnMapper[YourClass]` trait. This API is subject to future changes, so please refer to the current ScalaDoc.
 
### Using custom field types
To map a Cassandra column to a field of user-defined type, register custom `TypeConverter` implementations.
For example, imagine you want emails to be stored in custom `Email` class, wrapping a string:

```scala
case class EMail(email: String)
```
    
To tell the connector how to read and write fields of type `EMail`, you need to define two 
type converters - from `String` to `Email` and from `Email` to `String`:

```scala
import io.teknek.arizona.spark.connector.types._
import scala.reflect.runtime.universe._

object StringToEMailConverter extends TypeConverter[EMail] {
  def targetTypeTag = typeTag[EMail]
  def convertPF = { case str: String => EMail(str) }
}

object EMailToStringConverter extends TypeConverter[String] {
  def targetTypeTag = typeTag[String]
  def convertPF = { case EMail(str) => str }
}
    
TypeConverter.registerConverter(StringToEMailConverter)
TypeConverter.registerConverter(EMailToStringConverter)            
```
 
Now you can map any Cassandra text or ascii column to `EMail` instance.
The registration step must be performed before creating any RDDs you wish to
use the new converter for.

Additionally, defining the `StringToEMailConverter` as an implicit object 
allows to use generic `CassandraRow#get` with your custom `EMail` field type.
 
The following table specifies the relationship between a Cassandra column type and 
the type of needed `TypeConverter`.

Cassandra column type | Object type to convert from / to
----------------------|------------------------------------------
 `ascii`              | `java.lang.String`
 `bigint`             | `java.lang.Long`
 `blob`               | `java.nio.ByteBuffer`
 `boolean`            | `java.lang.Boolean`
 `counter`            | `java.lang.Long`
 `decimal`            | `java.math.BigDecimal`
 `double`             | `java.lang.Double`
 `float`              | `java.lang.Float`
 `inet`               | `java.net.InetAddress`
 `int`                | `java.lang.Integer`
 `smallint`           | `java.lang.Short`
 `text`               | `java.lang.String`
 `timestamp`          | `java.util.Date`
 `timeuuid`           | `java.util.UUID`
 `tinyint`            | `java.lang.Byte`
 `uuid`               | `java.util.UUID`
 `varchar`            | `java.lang.String`
 `varint`             | `java.math.BigInteger`
 `vector`             | `com.datastax.oss.driver.api.core.data.CqlVector`
 user defined         | `io.teknek.arizona.spark.connector.UDTValue`

Custom converters for collections are not supported.
 
When defining your own `TypeConverter` make sure it is `Serializable` and
works properly after being deserialized. For example, if you want to 
register a custom TypeConverter producing
Cassandra UDT values, you must register a converter converting to connector's
UDTValue class, not Java Driver's UDTValue. This is because many Java Driver's classes are not
Serializable and would not be possible for such converter to be properly serialized/deserialized.  

### Low-level control over mapping (Very Advanced Usage)
The `ColumnMapper` API cannot be used to express every possible mapping, e.g., for classes that do not expose
separate accessors for reading/writing every column. 

For converting a low-level `Row` object obtained from the Cassandra Java driver into an object stored in `RDD`, 
this Spark driver uses a `RowReader` instance. An appropriate `RowReader` is obtained from an implicit 
`RowReaderFactory` resolved based on the target RDD item type. You need to provide a custom implicit 
`RowReaderFactory` and `RowReader` for working with your class, and have it in scope when calling `cassandraTable`.
  
In the same way, when writing an `RDD` back to Cassandra, an appropriate implicit `RowWriterFactory` and 
`RowWriter` are used to extract column values from every RDD item and bind them to an INSERT `PreparedStatement`.
     
Please refer to the ScalaDoc for more details.

[Next - Using Connector in Java](7_java_api.md)
