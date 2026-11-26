The two Fluss 1.0 Arrow fixtures are unmodified FetchLog payloads from a stock
Apache Fluss 1.0.0 server (commit `5c07f88e50a8ff41b0ebc214a0458e5dba60be37`).
The official Java client (`org.apache.fluss:fluss-client:1.0.0`) appended
`(73, "java")` to two one-bucket ARROW tables with required INT `id` and
STRING `name` columns. Only the second table set `table.statistics.columns=id`.
The first batch uses magic 0; the second uses magic 1 with 49 statistics bytes.
