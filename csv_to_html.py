import csv
import sys
import os
import json

if len(sys.argv) < 2:
    print("Usage: python3 csv_to_html.py <input_csv_file>")
    sys.exit(1)

input_file = sys.argv[1]
output_file = input_file.replace('.csv', '.html')

# 1. Read Log File
run_dir = os.path.dirname(os.path.dirname(os.path.abspath(input_file)))
log_file_path = os.path.join(run_dir, "process.log")
log_content = "Log file not found."
if os.path.exists(log_file_path):
    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
            log_content = f.read()
    except Exception as e:
        log_content = f"Error reading log: {e}"
# Escape HTML for Log
log_content = log_content.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# 2. Read CSV Data into Python List of Dicts
data_rows = []
try:
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Pre-process logic (Color, Icons) here to keep JS light
            
            # Numeric formatting
            try:
                total = int(row.get('Total_Rows', 0))
                nulls = int(row.get('Null_Count', 0))
                null_pct = (nulls / total * 100) if total > 0 else 0
            except:
                total, nulls, null_pct = 0, 0, 0

            # Data Type Badge Logic
            dtype = row.get('DataType', '').lower()
            badge_class = "bg-secondary"
            if 'char' in dtype: badge_class = "bg-primary"
            elif 'int' in dtype or 'number' in dtype: badge_class = "bg-success"
            elif 'date' in dtype or 'time' in dtype: badge_class = "bg-info text-dark"
            dtype_html = f'<span class="badge {badge_class} badge-type">{row.get("DataType", "")}</span>'

            # Key Logic
            pk_val = row.get('PK', '')
            fk_val = row.get('FK', '')
            pk_icon = '<span class="key-icon" title="Primary Key">🔑</span>' if pk_val == 'YES' else ''
            fk_icon = ''
            if fk_val and fk_val != '':
                fk_icon = f'<span class="key-icon" title="FK: {fk_val}">🔗</span><div class="fk-detail">{fk_val}</div>'
            key_html = f'<div class="text-center">{pk_icon}{fk_icon}</div>'

            # Column Highlight
            col_name = row.get('Column', '')
            col_class = ""
            if pk_val == 'YES': col_class = "pk-col"
            elif fk_val != '': col_class = "fk-col"
            col_html = f'<span class="{col_class}">{col_name}</span>'

            # Progress Bar HTML
            bar_color = "bg-success"
            if null_pct > 50: bar_color = "bg-warning text-dark"
            if null_pct == 100: bar_color = "bg-danger"
            progress_html = f"""
            <div class="progress" style="position:relative">
                <div class="progress-bar {bar_color}" style="width:{null_pct}%"></div>
                <span style="position:absolute;width:100%;text-align:center;color:black;font-size:10px;line-height:16px">
                    {nulls:,} ({null_pct:.0f}%)
                </span>
            </div>
            """

            # Prepare row for JSON (Order matters for DataTables array or use Object)
            # We will use Object for clarity
            data_rows.append({
                "table": row.get('Table', ''),
                "column": col_html,
                "type": dtype_html,
                "key": key_html,
                "default": f'<span class="default-col">{row.get("Default", "")}</span>',
                "comment": f'<span class="comment-col">{row.get("Comment", "")}</span>',
                "rows": f'{total:,}', # Format number string
                "nulls": progress_html,
                "maxlen": row.get('Max_Length', ''),
                "distinct": row.get('Distinct_Values', ''),
                "sample": f'<div class="sample-data">{row.get("Sample_Values", "")}</div>',
                "is_warning": (null_pct == 100) # Helper for row styling
            })

except Exception as e:
    print(f"❌ Error processing CSV: {e}")
    sys.exit(1)

# 3. Convert Python List to JSON String
json_data = json.dumps(data_rows)

# 4. Generate HTML using Template
html_content = f"""
<!DOCTYPE html>
<html lang="th">
<head>
    <meta charset="UTF-8">
    <title>HIS Migration Report</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/5.3.0/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Sarabun:wght@300;400;600&display=swap" rel="stylesheet">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/5.3.0/js/bootstrap.bundle.min.js"></script>
    <style>
        body {{ font-family: 'Sarabun', sans-serif; background-color: #f8f9fa; padding: 20px; font-size: 14px; }}
        .container-fluid {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }}
        h2 {{ color: #0d6efd; font-weight: 600; margin-bottom: 20px; }}
        .badge-type {{ font-size: 0.75em; padding: 4px 6px; }}
        .progress {{ height: 16px; background-color: #e9ecef; }}
        
        th {{ resize: horizontal; overflow: auto; min-width: 50px; position: relative; background-color: #f8f9fa !important; }}
        
        /* Content Styles */
        .sample-data {{ font-family: 'Courier New', monospace; font-size: 0.85em; color: #444; white-space: pre-wrap; word-break: break-word; min-width: 300px; max-height: 100px; overflow-y: auto; }}
        .comment-col {{ font-style: italic; color: #6c757d; font-size: 0.85em; display:block; min-width: 150px; }}
        .key-icon {{ font-size: 1.2em; margin-right: 2px; cursor: help; }}
        .pk-col {{ color: #d63384; font-weight: bold; }}
        .fk-col {{ color: #0d6efd; font-weight: bold; }}
        .fk-detail {{ font-size: 0.75em; color: #0d6efd; font-family: monospace; white-space: nowrap; }}
        .default-col {{ font-family: monospace; color: #198754; font-size: 0.85em; word-break: break-all; }}
        
        /* Row Styles */
        tr.warning-row td {{ background-color: #fff5f5 !important; }}
        table.dataTable tbody td {{ vertical-align: top; }}
        
        /* Log Tab */
        .log-container {{ background-color: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 5px; height: 600px; overflow-y: auto; font-family: 'Courier New', monospace; font-size: 13px; }}
        .nav-tabs .nav-link.active {{ font-weight: bold; color: #0d6efd; border-top: 3px solid #0d6efd; }}
    </style>
</head>
<body>
<div class="container-fluid">
    <div class="d-flex justify-content-between align-items-center mb-3">
        <h2>🏥 HIS Database Analysis Report</h2>
        <span class="text-muted small">Source: {os.path.basename(input_file)}</span>
    </div>

    <ul class="nav nav-tabs" id="myTab" role="tablist">
        <li class="nav-item" role="presentation">
            <button class="nav-link active" id="report-tab" data-bs-toggle="tab" data-bs-target="#report" type="button">📊 Data Report</button>
        </li>
        <li class="nav-item" role="presentation">
            <button class="nav-link" id="log-tab" data-bs-toggle="tab" data-bs-target="#log" type="button">📝 Process Log</button>
        </li>
    </ul>

    <div class="tab-content pt-3" id="myTabContent">
        <!-- Report Tab -->
        <div class="tab-pane fade show active" id="report" role="tabpanel">
            <table id="hisTable" class="table table-hover table-bordered" style="width:100%">
                <thead class="table-light">
                    <tr>
                        <th>Table</th>
                        <th>Column</th>
                        <th>Type</th>
                        <th style="width:60px">Key</th>
                        <th>Default</th>
                        <th>Comment</th>
                        <th>Rows</th>
                        <th>Nulls</th>
                        <th>Max Len</th>
                        <th>Distinct</th>
                        <th>Sample Data</th>
                    </tr>
                </thead>
                <tbody>
                <!-- Data will be injected by JavaScript -->
                </tbody>
            </table>
        </div>

        <!-- Log Tab -->
        <div class="tab-pane fade" id="log" role="tabpanel">
            <div class="log-container">
                <pre>{log_content}</pre>
            </div>
        </div>
    </div>
</div>

<script src="https://code.jquery.com/jquery-3.7.0.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
<script>
    // Embed Data directly into JS variable (Fast & Single File)
    const tableData = {json_data};

    $(document).ready(function() {{
        var table = $('#hisTable').DataTable({{
            data: tableData,
            columns: [
                {{ data: 'table' }},
                {{ data: 'column' }},
                {{ data: 'type' }},
                {{ data: 'key', className: 'text-center' }},
                {{ data: 'default' }},
                {{ data: 'comment' }},
                {{ data: 'rows', className: 'text-end' }},
                {{ data: 'nulls' }},
                {{ data: 'maxlen', className: 'text-end' }},
                {{ data: 'distinct', className: 'text-end' }},
                {{ data: 'sample' }}
            ],
            createdRow: function(row, data, dataIndex) {{
                if (data.is_warning) {{
                    $(row).addClass('warning-row');
                }}
            }},
            pageLength: 25,
            lengthMenu: [ [10, 25, 50, 100, -1], [10, 25, 50, 100, "All"] ],
            order: [[ 0, "asc" ]],
            language: {{ "search": "🔍 Filter:" }},
            autoWidth: false 
        }});

        // Enable Tooltips
        var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'))
        var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {{
          return new bootstrap.Tooltip(tooltipTriggerEl)
        }})
    }});
</script>
</body>
</html>
"""

with open(output_file, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"✅ HTML Report Generated: {output_file}")