Go to the link: https://lookerstudio.google.com/ \
Create -> Data source -> BigQuery \
Select: project -> dataset -> table “summary”; connect\
<i>You can notice many tables look like _bqc_*. Don’t worry, these are temporary tables created during data transformation via Spark. They will disappear in about 24 hours.</i>\
You’ll need one additional field: Add fileld -> Field name: month; Formula: MONTH(month_1st) -> Save\
Create report

Delete a created random table\
Add a chart -> Time series\
In the right panel: Metric: temperature; Breakdown dimension: city\
Add a chart -> Pie\
In the right panel: Dimension: conditions; Metric: record count\
You can also add titles to your charts: click “text” on the top panel.

Your dashboard is ready! You can view it and share – buttons are on the top panel.
