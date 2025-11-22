import csv
import sys
import os
import json
import re

if len(sys.argv) < 2:
    print("Usage: python3 csv_to_html.py <input_csv_file>")
    sys.exit(1)

input_file = sys.argv[1]
output_file = input_file.replace('.csv', '.html')

# --- 1. Path Setup ---
# CSV is in: RunDir/data_profile/file.csv
# Log is in: RunDir/process.log
# DDL is in: RunDir/ddl_schema/schema.sql
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(input_file)))
log_file_path = os.path.join(base_dir, "process.log")
ddl_file_path = os.path.join(base_dir, "ddl_schema", "schema.sql")

# --- 2. Load Log ---
log_content = "Log file not found."
if os.path.exists(log_file_path):
    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
            log_content = f.read()
    except Exception as e: log_content = str(e)
log_content = log_content.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# --- 3. Load & Parse DDL (Simple Parser) ---
ddl_map = {}
if os.path.exists(ddl_file_path):
    try:
        with open(ddl_file_path, 'r', encoding='utf-8', errors='replace') as f:
            sql_content = f.read()
            
            # Regex to find CREATE TABLE blocks (Works for MySQL/PG mostly)
            # Matches: CREATE TABLE [IF NOT EXISTS] [schema.]`?"?tablename`?"? ... ;
            # This is a heuristic parser, might need adjustment for complex schemas
            # Group 3 is table name
            regex = r'CREATE TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[a-zA-Z0-9_]+\.)?[`"]?([a-zA-Z0-9_]+)[`"]?\s*\((?:[^;]|\n)*?\);'
            matches = re.finditer(regex, sql_content, re.IGNORECASE | re.DOTALL)
            
            for match in matches:
                tbl_name = match.group(1)
                ddl_body = match.group(0)
                ddl_map[tbl_name] = ddl_body
    except Exception as e:
        print(f"⚠️ Warning parsing DDL: {e}")

# --- 4. Process CSV Data ---
detail_rows = []
table_stats = {} 

try:
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            t_name = row.get('Table', '')
            
            try:
                total = int(row.get('Total_Rows', 0))
                nulls = int(row.get('Null_Count', 0))
                null_pct = (nulls / total * 100) if total > 0 else 0
            except: total, nulls, null_pct = 0, 0, 0

            if t_name not in table_stats:
                table_stats[t_name] = {'rows': total, 'cols': 0, 'empty_cols': 0}
            table_stats[t_name]['cols'] += 1
            if null_pct == 100:
                table_stats[t_name]['empty_cols'] += 1

            badge_class = "bg-secondary"
            dtype = row.get('DataType', '').lower()
            if 'char' in dtype: badge_class = "bg-primary"
            elif 'int' in dtype or 'number' in dtype: badge_class = "bg-success"
            elif 'date' in dtype: badge_class = "bg-info text-dark"
            
            pk_icon = '🔑' if row.get('PK') == 'YES' else ''
            
            # FK Logic with Modal Link
            fk_raw = row.get("FK","")
            fk_icon = ''
            if fk_raw:
                # Extract table name from "-> table.col"
                ref_table = fk_raw.replace('-> ', '').split('.')[0].strip().replace('"', '')
                fk_icon = f'''
                <a href="#" class="text-decoration-none" onclick="showDDL('{ref_table}'); return false;" title="View {ref_table} Schema">
                    🔗 <span class="fk-detail">{fk_raw}</span>
                </a>'''
            
            bar_color = "bg-success"
            if null_pct > 50: bar_color = "bg-warning text-dark"
            if null_pct == 100: bar_color = "bg-danger"
            
            detail_rows.append({
                "table": t_name,
                "column": f'<span class="{"pk-col" if row.get("PK")=="YES" else ""}">{row.get("Column","")}</span>',
                "type": f'<span class="badge {badge_class} badge-type">{row.get("DataType","")}</span>',
                "key": f'{pk_icon} {fk_icon}',
                "default": f'<span class="default-col">{row.get("Default","")}</span>',
                "rows": f'{total:,}',
                "nulls": f'<div class="progress"><div class="progress-bar {bar_color}" style="width:{null_pct}%"></div><span style="position:absolute;width:100%;text-align:center;font-size:10px;color:black">{nulls:,} ({null_pct:.0f}%)</span></div>',
                "distinct": row.get('Distinct_Values', ''),
                "min": f'<span class="val-hl">{row.get("Min_Val","")}</span>',
                "max": f'<span class="val-hl">{row.get("Max_Val","")}</span>',
                "top5": f'<div class="sample-data" style="max-height:60px">{row.get("Top_5_Values","").replace("|", "<br>")}</div>',
                "sample": f'<div class="sample-data">{row.get("Sample_Values","")}</div>',
                "is_warning": (null_pct == 100)
            })

    overview_rows = []
    for t, stats in table_stats.items():
        quality = 100 - (stats['empty_cols'] / stats['cols'] * 100) if stats['cols'] > 0 else 0
        q_color = "text-success"
        if quality < 80: q_color = "text-warning"
        if quality < 50: q_color = "text-danger"
        
        # Table Name with Modal Link
        tbl_link = f'<a href="#" onclick="showDDL(\'{t}\'); return false;" class="fw-bold text-primary">{t}</a>'
        
        overview_rows.append({
            "table": tbl_link,
            "rows": f'{stats["rows"]:,}',
            "cols": stats['cols'],
            "empty": stats['empty_cols'],
            "quality": f'<b class="{q_color}">{quality:.1f}%</b>'
        })

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)

json_detail = json.dumps(detail_rows)
json_overview = json.dumps(overview_rows)
json_ddl = json.dumps(ddl_map)

html_content = f"""
<!DOCTYPE html>
<html lang="th">
<head>
    <meta charset="UTF-8">
    <title>HIS Migration Report</title>
    
    <!-- Bootstrap 5 -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/5.3.0/css/bootstrap.min.css">
    <!-- DataTables Core -->
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css">
    <!-- DataTables Buttons -->
    <link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.4.1/css/buttons.bootstrap5.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Sarabun:wght@300;400;600&display=swap" rel="stylesheet">
    
    <style>
        :root {{ --bs-primary-rgb: 13, 110, 253; }}
        body {{ font-family: 'Sarabun', sans-serif; background-color: #f8f9fa; padding: 20px; font-size: 14px; }}
        .container-fluid {{ background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.05); }}
        h2 {{ color: #0d6efd; font-weight: 600; margin-bottom: 0; }}
        .nav-tabs .nav-link.active {{ font-weight: bold; color: #0d6efd; border-top: 3px solid #0d6efd; background-color: #fff; }}
        
        /* Tables */
        th {{ background-color: #f1f4f9 !important; resize: horizontal; overflow: auto; min-width: 50px; vertical-align: middle !important; }}
        .progress {{ height: 18px; background-color: #e9ecef; position: relative; border-radius: 4px; }}
        .badge-type {{ font-size: 0.75em; padding: 5px 8px; border-radius: 6px; }}
        .sample-data {{ font-family: 'Courier New', monospace; font-size: 0.85em; color: #444; white-space: pre-wrap; min-width: 200px; max-height: 100px; overflow-y: auto; }}
        .val-hl {{ font-family: monospace; color: #d63384; font-weight: bold; }}
        .fk-detail {{ font-size: 0.8em; color: #0d6efd; font-family: monospace; }}
        tr.warning-row td {{ background-color: #fff5f5 !important; }}
        
        /* Button Styling (Outline & Hover) */
        .dt-buttons .btn-group {{ display: flex; flex-wrap: wrap; gap: 5px; }}
        .dt-button {{ 
            background-color: transparent !important; 
            border: 1px solid #6c757d !important; 
            color: #6c757d !important; 
            border-radius: 6px !important; 
            padding: 5px 12px !important;
            font-size: 0.9rem !important;
            transition: all 0.2s ease-in-out !important;
            background-image: none !important; /* Override DataTables default gradient */
        }}
        .dt-button:hover, .dt-button.active {{ 
            background-color: #6c757d !important; 
            color: white !important; 
            box-shadow: 0 2px 5px rgba(0,0,0,0.2) !important;
        }}
        .buttons-colvis {{ border-color: #0d6efd !important; color: #0d6efd !important; }}
        .buttons-colvis:hover {{ background-color: #0d6efd !important; }}

        /* Modal Code Block */
        pre.sql-code {{ background-color: #282c34; color: #abb2bf; padding: 15px; border-radius: 6px; font-size: 13px; max-height: 500px; overflow: auto; }}
        
        /* Log */
        .log-container {{ background-color: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 6px; height: 600px; overflow-y: auto; font-family: monospace; }}
    </style>
</head>
<body>
<div class="container-fluid">
    <div class="d-flex justify-content-between align-items-center mb-4 border-bottom pb-3">
        <div>
            <h2>🏥 HIS Database Analysis Report</h2>
            <div class="text-muted small mt-1">Analyzed Source: {os.path.basename(input_file)}</div>
        </div>
        <div class="text-end">
            <span class="badge bg-primary rounded-pill p-2">v5.1 Deep Analysis</span>
        </div>
    </div>

    <ul class="nav nav-tabs" id="myTab" role="tablist">
        <li class="nav-item"><button class="nav-link active" id="overview-tab" data-bs-toggle="tab" data-bs-target="#overview">📋 Overview</button></li>
        <li class="nav-item"><button class="nav-link" id="detail-tab" data-bs-toggle="tab" data-bs-target="#detail">🔍 Column Detail</button></li>
        <li class="nav-item"><button class="nav-link" id="log-tab" data-bs-toggle="tab" data-bs-target="#log">📝 Process Log</button></li>
    </ul>

    <div class="tab-content pt-4">
        <!-- Overview Tab -->
        <div class="tab-pane fade show active" id="overview">
            <table id="overviewTable" class="table table-hover table-bordered w-100">
                <thead class="table-light"><tr><th>Table Name (Click for Schema)</th><th>Total Rows</th><th>Columns</th><th>Empty Cols</th><th>Data Quality</th></tr></thead>
                <tbody></tbody>
            </table>
        </div>

        <!-- Detail Tab -->
        <div class="tab-pane fade" id="detail">
            <table id="detailTable" class="table table-hover table-bordered w-100">
                <thead class="table-light">
                    <tr>
                        <th>Table</th><th>Column</th><th>Type</th><th>Key / Ref</th><th>Default</th>
                        <th>Rows</th><th>Nulls</th><th>Dist.</th><th>Min</th><th>Max</th>
                        <th>Top 5 Freq</th><th>Sample</th>
                    </tr>
                </thead>
                <tbody></tbody>
            </table>
        </div>

        <!-- Log Tab -->
        <div class="tab-pane fade" id="log">
            <div class="log-container"><pre>{log_content}</pre></div>
        </div>
    </div>
</div>

<!-- DDL Modal -->
<div class="modal fade" id="ddlModal" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog modal-lg modal-dialog-centered">
    <div class="modal-content">
      <div class="modal-header bg-light">
        <h5 class="modal-title" id="ddlModalTitle">Table Schema</h5>
        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body">
        <pre class="sql-code" id="ddlModalBody">Loading...</pre>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
      </div>
    </div>
  </div>
</div>

<!-- Scripts -->
<script src="https://code.jquery.com/jquery-3.7.0.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/5.3.0/js/bootstrap.bundle.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.1/js/dataTables.buttons.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.1/js/buttons.bootstrap5.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.1/js/buttons.colVis.min.js"></script>

<script>
    const detailData = {json_detail};
    const overviewData = {json_overview};
    const ddlData = {json_ddl};
    let ddlModal;

    function showDDL(tableName) {{
        const content = ddlData[tableName] || "-- DDL not found for " + tableName + "\\n-- Note: Ensure schema.sql was generated successfully.";
        document.getElementById('ddlModalTitle').innerText = 'Schema: ' + tableName;
        document.getElementById('ddlModalBody').innerText = content;
        if(!ddlModal) ddlModal = new bootstrap.Modal(document.getElementById('ddlModal'));
        ddlModal.show();
    }}

    $(document).ready(function() {{
        // Overview Table
        $('#overviewTable').DataTable({{
            data: overviewData,
            columns: [
                {{ data: 'table' }}, {{ data: 'rows', className: 'text-end' }}, 
                {{ data: 'cols', className: 'text-end' }}, {{ data: 'empty', className: 'text-end' }}, 
                {{ data: 'quality', className: 'text-end' }}
            ],
            pageLength: 15, order: [[ 1, "desc" ]],
            dom: '<"d-flex justify-content-between mb-3"Bf>rtip',
            buttons: [ 
                {{ extend: 'pageLength', className: 'btn btn-outline-secondary' }} 
            ]
        }});

        // Detail Table
        $('#detailTable').DataTable({{
            data: detailData,
            columns: [
                {{ data: 'table' }}, {{ data: 'column' }}, {{ data: 'type' }}, {{ data: 'key' }}, {{ data: 'default' }},
                {{ data: 'rows', className: 'text-end' }}, {{ data: 'nulls' }}, {{ data: 'distinct', className: 'text-end' }},
                {{ data: 'min' }}, {{ data: 'max' }}, {{ data: 'top5' }}, {{ data: 'sample' }}
            ],
            dom: '<"d-flex flex-wrap gap-2 justify-content-between align-items-center mb-3"Bf>rtip',
            buttons: [
                {{
                    extend: 'colvis',
                    text: '👁️ Columns',
                    className: 'btn btn-outline-primary',
                    columns: ':not(:first-child)'
                }},
                {{ extend: 'pageLength', className: 'btn btn-outline-secondary' }}
            ],
            createdRow: function(row, data) {{ if(data.is_warning) $(row).addClass('warning-row'); }},
            pageLength: 25, lengthMenu: [[25, 50, 100, -1], [25, 50, 100, "All"]],
            language: {{ "search": "", "searchPlaceholder": "🔍 Search..." }}
        }});
    }});
</script>
</body>
</html>
"""

with open(output_file, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"✅ HTML Report Generated: {output_file}")