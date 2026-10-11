<!-- Canary marker: verify the exact required value from scripts/checks/check-canary.sh -->

## Task Metadata

- **Author:** Panita Vaishnavi Kanaguduru (panitakanaguduru.18@gmail.com) | individual
- **Category:** `Software`
- **Tags:** <code>python</code> <code>data_engineering</code> <code>event_processing</code> <code>deduplication</code> <code>recovery</code>
- **Expert time:** 2 hours
- **Agent timeout:** 0.5 hours
- **CPUs:** 2
- **Memory:** 2 GB

## Difficulty explanation

The main challenge in this task is making the event-processing pipeline reliable when the input contains multiple schema versions, duplicate events, invalid records, and delete operations. Resolving duplicate event IDs requires comparing revision numbers, ingestion timestamps, and operation types in a consistent order. Another challenge is ensuring that the output remains deterministic even when input records arrive in a different order. The processor also needs to calculate accurate metrics without including invalid or deleted events. Finally, atomic file writes and checkpoint recovery are important because an interrupted run must not leave the pipeline in an inconsistent state.

## Solution explanation

I approached the solution by separating event validation, duplicate resolution, output generation, and recovery into clear processing steps. The processor reads JSONL records from the available input files and validates them according to their schema versions. For duplicate event IDs, it selects the record with the highest revision, then uses the ingestion timestamp, operation type, and a deterministic tie-breaker when needed. Delete operations remove the selected events from the final output, while valid remaining events are sorted consistently and used to calculate event counts and unique-user metrics. I also implemented atomic output writes and checkpoint validation so the pipeline can recover from interrupted executions without trusting incomplete results.

## Verification explanation

I verified the event-processing pipeline using automated tests that cover schema validation, malformed JSON, missing fields, duplicate event IDs, revision conflicts, and delete operations. The tests check that records are selected consistently regardless of input order and that the final output follows deterministic sorting rules. I also verified event counts and unique-user metrics against the expected results. Additional tests cover corrupted checkpoints, missing output files, interrupted writes, and repeated executions to confirm that recovery works correctly. The Oracle evaluation passed with a reward of 1.0, and the latest test suite passed all 40 tests.

## Relevant experience

My background in Statistics and Data Science has given me practical experience working with Python, SQL, data validation, and analytics pipelines. At SuperWorld, I worked with more than 100,000 user and product interaction events, analyzing event tracking, conversion funnels, and data-quality issues. I investigated missing events, inconsistent tracking, and reporting discrepancies to improve the reliability of analytics. At the University of Central Florida, I also worked on database migration, data reconciliation, and reporting workflows. These experiences helped me understand the importance of accurate event processing, reproducible outputs, and reliable recovery when building data pipelines.

