# Writing Data to Disk

## Issues

The discussion should be divided into three distinct issues:

1. Should I opt for a Row-Based or Column-Based approach?
2. Which format should be used for storing the data?
3. How should data be written to disk?

## Should I Choose a Row-Based or Column-Based Approach?

### Context
Databases utilize different formats to organize data, depending on the type of operation:
- **Row-Based:** Postgres, MySQL
- **Column-Based:** Redshift, BigQuery, Snowflake

### Decision
I chose a row-based database because StoneDb does not require the aggregation of data from multiple rows within a single column.

### Consequences
In the future, aggregating data will become slower, making it more challenging to analyze data.

### Sources of Knowledge
- [Row vs. Column-Oriented Databases](https://dataschool.com/data-modeling-101/row-vs-column-oriented-databases/)
- [Day10: Data Layout — Row-Based vs. Column-Based](https://www.linkedin.com/pulse/day10-data-layout-row-based-vs-column-based-farhan-khan/)
- [Deciding Between Row and Columnar Stores: Why We Chose Both](https://medium.com/bluecore-engineering/deciding-between-row-and-columnar-stores-why-we-chose-both-3a675dab4087)

## What Format Should Be Adopted for the Recorded Data?

### Proposals for a Solution
- Apache Avro (Row-Based)
- Apache Parquet (Column-Based)
- CSV (Row-Based)
- JSON
- Json lines

### Decision
#### Why Not Use JSON?
- Making changes in JSON requires overwriting the existing file.
- Large JSON files are slow in serialization and deserialization, according to experience.
- Serialization and deserialization require reading the entire file into memory.

#### Why Not Use Apache Parquet?
- This is a column-based orientation solution, so it doesn't fit our requirements.

#### Why Not Use CSV?
- It doesn't store data in binary format.
- I don't need to read data from Excel — in the future, I can create a dumper to CSV.
- It isn't projected for databases.

#### Why Use Apache Avro?
- It defines a schema.
- It uses a binary format.
- The library `avro_rs` provides writer and reader functions.
- It supports data compression.

## How to Write to Disk?

### Context
Data needs to be persisted to disk efficiently and effectively.

### Proposals for a Solution
- Utilize the writer function provided by the `avro_rs` library.
- Develop a custom function tailored to specific requirements.

### Decision
Initially, leverage the pre-existing function from the `avro_rs` library. However, remain adaptable to potentially developing a custom solution if the predefined function doesn't meet all the project's evolving needs or optimizations are required.

This approach ensures a balance between immediate implementation efficiency and the flexibility to adapt to future requirements.