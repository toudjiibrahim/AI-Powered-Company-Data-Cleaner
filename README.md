# AI-Powered Company Data Enrichment Pipeline

## Overview
This project is an automated ETL (Extract, Transform, Load) solution built for a client processing 5,000+ company records daily. It runs inside Google Sheets, filtering raw input data, removing duplicates against a master database, and using **OpenAI's GPT model** to enrich and categorize company profiles.

## Key Features
- **Intelligent Deduplication:** Filters incoming data against a Master Archive to process only new records.
- **AI Enrichment:** Uses OpenAI API to:
  - Rewrite and summarize company descriptions.
  - Standardize "Industry" fields to match a validated domain list.
- **Robust Error Handling:** Implements **exponential backoff** algorithms to handle OpenAI API rate limits (429 errors).
- **Concurrency Control:** Uses `LockService` to prevent race conditions during automated triggers.
- **Batch Processing:** Processes data in chunks to adhere to Google Apps Script execution time limits.

## Technology Stack
- **Language:** Google Apps Script (JavaScript)
- **APIs:** OpenAI API (GPT-3.5/4), Google Sheets API
- **Logic:** LockService, UrlFetchApp, Triggers

## How It Works
1.  **Input:** Raw data lands in the "Input Companies" sheet.
2.  **Filter:** The script compares `company_id` against the "Master" sheet.
3.  **Process:**
    - Cleans company names and validates website URLs.
    - Sends descriptions/industries to OpenAI for standardization.
4.  **Output:** Validated, clean data is appended to the "New Companies" sheet for the production database.

## Code Snippet (Rate Limit Handling)
*The script handles API limits gracefully by retrying failed requests:*

```javascript
// Example of exponential backoff used in the script
if (code === 429) {
  Logger.log(`Rate limit exceeded, retrying... attempt ${attempts}`);
  Utilities.sleep(delay);
  delay *= 2; // Double the wait time for the next attempt
}
