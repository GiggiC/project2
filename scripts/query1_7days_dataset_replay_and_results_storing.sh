(java -cp ../target/project2-1.0.jar it.uniroma2.utils.DataSourceQuery1 | nc -l -N 9091) &
(nc -l -N 9001 | java -cp ../target/project2-1.0.jar it.uniroma2.utils.QueryResultsExporter query1_7days_results.csv)