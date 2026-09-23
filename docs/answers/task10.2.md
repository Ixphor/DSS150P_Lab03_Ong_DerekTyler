# Task 10.2

### Explain why the chosen cadence is appropriate.

Schedule: Running at 2:00 AM daily is optimal for batch jobs. It avoids locking database tables or consuming heavy resources during peak business hours, and ensures that all transactions from the previous day have fully settled before being loaded into the warehouse.

### Explain catch-up choice.

Catch-up Choice (catchup=False): Because our start date is Jan 1, 2026, setting this to True would cause Airflow to aggressively spawn hundreds of retroactive backfill jobs the second it booted up. We set it to False to maintain stability; backfills should be triggered deliberately and manually.