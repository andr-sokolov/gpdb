# Managing Data

This collection of topics provides information about using SQL commands, Greenplum utilities, and advanced analytics integrations to manage data in your Greenplum Database cluster.

- [Defining Database Objects](./ddl/ddl.html)<br/>This topic covers data definition language (DDL) in Greenplum Database and how to create and manage database objects.
- [Working with External Data](./external/g-working-with-file-based-ext-tables.html)<br/>Both external and foreign tables provide access to data stored in data sources outside of Greenplum Database as if the data were stored in regular database tables. You can read data from and write data to external and foreign tables.
- [Loading and Unloading Data](./load/topics/g-loading-and-unloading-data.html)<br/>Greenplum Database supports high-performance parallel data loading and unloading, and for smaller amounts of data, single file, non-parallel data import and export. The topics in this section describe methods for loading and writing data into and out of a Greenplum Database, and how to format data files.
- [Querying Data](./query/topics/query.html)<br/>This topic provides information about using SQL queries to view, change, and analyze data in a database using the `psql` interactive SQL client and other client tools.
- [Advanced Analytics](../analytics/overview.html)<br/>Greenplum offers a unique combination of a powerful, massively parallel processing (MPP) database and advanced data analytics. This combination creates an ideal framework for data scientists, data architects and business decision makers to explore artificial intelligence (AI), machine learning, deep learning, text analytics, and geospatial analytics.
- [Inserting, Updating, and Deleting Data](./dml.html)<br/>This topic provides information about manipulating data and concurrent access in Greenplum Database.