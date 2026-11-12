# Schema drift and ingestion metadata

Schema drift happens when a source changes what it sends, such as adding a field or changing a price from a number to text. Bronze keeps the source fields. Silver checks that the fields it needs are present and usable.

| Source change | Response | Example |
| --- | --- | --- |
| A new field appears | **Rescue** the value for review while accepting the valid price fields. | `High=2606` and `VendorNote="demo feed"` become a valid price row with a separate preview value of `{"VendorNote":"demo feed"}` in `_rescued_data`. |
| The team decides to use the new field | **Evolve the schema** by agreeing on its meaning and type, updating validation, and adding it to the table. | Add a nullable `vendor_note` text column. Earlier rows can keep a null until the source supplies the field or a backfill fills it. |
| A required value is unusable | **Reject** the batch or **quarantine** the affected rows. A quarantine keeps invalid data and the reason in a separate place for repair and replay. | A row with `High="unknown"` could go to quarantine with reason `High must be numeric`. If an entire file has no `High` column, the workshop validator rejects that batch. |

The [local daily notebook](../notebooks/01_historical_ingestion.ipynb) shows the rescue in Section 3a. It adds `VendorNote` to three fixture rows, displays their JSON values, and checks that the validated Silver rows match the original sample. Then it removes `High` to show the validation error. The demo's helper handles newly added fields. Databricks Auto Loader also supports rescuing certain type and case mismatches. [Rescued data](https://docs.databricks.com/gcp/en/ingestion/cloud-object-storage/auto-loader/schema) · [Schema evolution](https://docs.databricks.com/gcp/en/data-engineering/schema-evolution) · [Quarantine example](https://docs.databricks.com/aws/en/ldp/expectation-patterns)

## Read a Bronze row

| Field | Example value | Why keep it? |
| --- | --- | --- |
| `Date` | `2025-01-06` | Identifies the market session the observation describes. |
| `_ingested_at` | `2025-02-15 09:00 UTC` | Records when this snapshot was written. A replay can have the same market date and a later ingestion time. |
| `_source` | `fixture` | Identifies the input source so you can distinguish a workshop fixture from a provider download. |
| `_symbol` | `XAUUSD-DEMO` | Identifies the instrument associated with the row and snapshot. |

Together with the Parquet path, these fields let you trace a row back to its input and distinguish late arrivals from older observations. The notebook reads them from the Bronze file it just wrote.

## Apply the pattern to an S3 pipeline

Keep source files in Bronze and record the source object key and batch ID alongside the ingestion time. A staging table can retain unfamiliar fields in `_rescued_data` until the team decides how to use them. For a curated Iceberg table in S3, Athena supports explicit schema evolution when you decide to add an approved field. [Athena Iceberg schema evolution](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-evolving-table-schema.html)

If an S3 pipeline needs quarantine, store invalid rows with their validation reason, source object key, and batch ID. That gives the team enough information to repair the records and replay the affected input. Start the workshop by showing the validation error and the saved Bronze snapshot.
