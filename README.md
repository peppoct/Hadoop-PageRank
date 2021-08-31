# Hadoop-PageRank
An implementation of the **Page Rank algorithm** using Hadoop (Java) and Spark (Java & Python).

## Input Test
The inputs to the program are pages from the Simple English Wikipedia and each page is represented in XML as follows:
```xml
<title>page name</title>
...
<revision optionalVal="xxx">
    ...
    <text optionalVal="yyy">page content</text>
    ...
</revision>
```
Links to other Wikipedia articles are of the form *[[page name]]*.

The XML file can be downloaded
[here](https://drive.google.com/file/d/1yK99ou_3XcrnuaSpRqAgQFsuijjGfM_y/view).
Load your inputs file in the HDFS file system.

## Execution
>Execute:
>
>hadoop jar PageRank-1.0-SNAPSHOT.jar Driver \<input file> \<jump factor> \<number of iteration> \<number of reducers>

>Output:
>
>hadoop fs -cat PageRank/part-r-00000

