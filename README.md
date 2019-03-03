## Architecture

![Architecture](./docs/images/Haystack_Tables.png)


### Getting Started?
Launch the table-allocator dropwizard app that exposes endpoint for creating and listing the views. 
The allocator uses kubernetes for running parquet-writers by default. If you are using minikube, make sure it is running and current k8s context points to it.

##### Create a new view:

```
curl -XPOST -H "Content-Type: application/json" -d '
{
  "view": "oms",
  "select": [
    "tags[errorcode]",
     "operationname"
  ],
  "where": {
    "servicename": "oms"
  }
}' "http://localhost:8080/sql"
```


##### List all views:

```
curl "http://localhost:8080/sql"
```

##### Delete a view:

```
curl -XDELETE "http://localhost:8080/sql/oms"
```

### S3 Data
Parquet writer runs independently for each requested view. They put the parquet data under a configured bucket name with following partitoning strategy:

`s3://bucket-name/sql/{view-name}/year=2019/month=02/day=03/hour=12/..`

The parquet files are named with the last kafka-offset value of the record in the file itself.

### Athena Tables
Allocator provides an endpoint `/athena/refresh` that takes following action for all the running views:
* Create partitioned table in Athena under haystack_tables database
* Repair the already existing table to add new s3 partitions

We run a cron job that hits this endpoint after every few minutes to make sure the tables are always upto date.

