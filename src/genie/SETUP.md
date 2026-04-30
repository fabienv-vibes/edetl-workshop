# Genie Space — manual setup (~2 min)

After the SDP pipeline has populated the gold tables, create a Genie Space in the workspace:

1. **Sidebar → Genie → New Space**
2. Pick the SQL warehouse (Serverless Starter Warehouse is fine)
3. **Tables** — add (replace `<CATALOG>` and `<SCHEMA>`):
   - `<CATALOG>.<SCHEMA>.gold_matter_summary`
   - `<CATALOG>.<SCHEMA>.gold_billable_hours_by_matter_month`
   - `<CATALOG>.<SCHEMA>.gold_doc_activity_by_matter`
   - `<CATALOG>.<SCHEMA>.gold_matter_lifecycle`
   - `<CATALOG>.<SCHEMA>.silver_matters_master`
   - `<CATALOG>.<SCHEMA>.silver_time_entries`
   - `<CATALOG>.<SCHEMA>.silver_matter_events`
   - `<CATALOG>.<SCHEMA>.silver_doc_audit`
4. **Title:** `edetl-workshop`
5. **Description:**
   > Natural-language exploration of legal practice data: matters, billable hours, and document activity. Backed by the edetl-workshop SDP gold layer.
6. **Sample questions** — paste these:
   - Which 10 matters generated the most billable revenue?
   - What's the breakdown of revenue by practice area?
   - How many doc events did we have last week?
   - Which matters were opened but never closed?
   - What's the average billable hours per matter for M&A vs Litigation?
   - Show me the top timekeepers by total revenue.
   - Which clients have the highest doc activity?
   - How does monthly billable revenue compare across regions?
7. Save.
