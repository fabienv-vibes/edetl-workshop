# Dashboard — manual setup (~1 min)

The `edetl_overview.lvdash.json` file in this folder is an exported AI/BI (Lakeview) dashboard. To import it into the workspace:

1. **Sidebar → Dashboards → Create dashboard**
2. Open the new dashboard. Top-right **⋯ menu → Import dashboard from file**
3. Select `edetl_overview.lvdash.json` from this folder
4. The queries reference unqualified table names. Open each dataset and prefix table names with your `<CATALOG>.<SCHEMA>.` (or use the dashboard's "Default catalog/schema" setting under ⚙️).
5. Pick a SQL warehouse (Serverless Starter Warehouse is fine).
6. Click **Refresh** on each tile.

If you'd rather script it later, see [`databricks-aibi-dashboards`](https://docs.databricks.com/dev-tools/bundles/resources#dashboards) for the DAB resource pattern, or use the `databricks-ai-dev-kit:databricks-aibi-dashboards` MCP `manage_dashboard` tool.
