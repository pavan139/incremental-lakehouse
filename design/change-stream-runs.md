# Change-stream runs

The default experiment used seed 17 and input hash `0408c38acbae20de0789b15142bd6733acb815f7f2b3df1fb554e4eaf72fa811`. It consumed 120 unique source events in 12 published batches. Changes include late versions, deletes, duplicate offsets, a versioned additive schema, invalid rows, partial staging and a failed publish.

| Check or outcome | Measured denominator/result |
| --- | --- |
| Independent full-rebuild comparisons | 12/12 passed |
| Source-cursor comparisons | 12/12 passed |
| Content-lineage comparisons | 12/12 passed |
| Published-batch replay comparisons | 12/12 passed |
| Crash visibility and invalid-publication checks | 5/5 passed |
| Total invariants | 53/53 passed |
| New consumed offsets | 120 |
| Exact duplicate events ignored | 11 |
| Quarantined new events | 7 |
| Valid older winner versions ignored | 26 |
| Coalesced visible source-key changes across batches | 77 |

Reproduce with:

```bash
PYTHONPATH=src python -m incremental_lakehouse evaluate --seed 17 --output artifacts/evaluation.json
```

[Raw evaluation](../fixtures/evaluation.json) includes each check and the committed source/model/lineage snapshot. [Demo output](../fixtures/demo.json) is a compact view of the same executed experiment. Unit tests repeat end-to-end comparisons at seeds 23 and 101.

These are correctness invariants over a controlled local workload. They are not latency/throughput measurements, source-delivery guarantees, distributed exactly-once claims or downstream AI accuracy. No network calls, models or paid services were involved.
