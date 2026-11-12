# Student assignment

Adapt Market Data ML Pipeline to one source you can explain, such as weather observations, sports results or retail transactions. Submit a repository and a five-minute walkthrough. Include a small offline fixture and instructions a classmate can run from a clean checkout.

| Criterion | Points | Evidence |
| --- | ---: | --- |
| Source and Bronze | 15 | Fetch data with documented parameters and preserve one raw snapshot with metadata. |
| Validation | 15 | Define required fields, a record key and two domain checks. Show a useful error for a rejected batch. |
| Transformation | 15 | Use Polars to create a clean table and one meaningful derived value. Explain its inputs and any nulls. |
| Persistent store | 15 | Use PostgreSQL with a primary key, parameterized upserts and credentials outside Git. |
| Repeatability | 15 | Run the same window twice and show stable row and distinct-key counts. Provide one backfill command. |
| Orchestration | 15 | Show a Prefect run with visible tasks and logs, plus a recoverable or permanent failure example. |
| Documentation | 10 | Include setup, example commands, expected results and a known limitation. |

A predictive model belongs to a later session.
