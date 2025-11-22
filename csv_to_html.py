import csv
import sys
import os
import json

if len(sys.argv) < 2:
    print("Usage: python3 csv_to_html.py <input_csv_file>")
    sys.exit(1)

input_file = sys.argv[1]
output_file = input_file.replace('.csv', '.html')

# 1. Load Log File
run_dir = os.path.dirname(os.path.dirname(os.path.abspath(input_file)))
log_file_path = os.path.join(run_dir, "process.log")
log_content = "Log file not found."
if os.path.exists(log_file_path):
    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
            log_content = f.read()
    except Exception as e: log_content = str(e)
log_content = log_content.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# 2. Process CSV Data
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
            fk_icon = f'🔗 <span class="fk-detail">{row.get("FK","")}</span>' if row.get('FK') else ''
            
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
        
        overview_rows.append({
            "table": f'<b>{t}</b>',
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
    <!-- DataTables Buttons (For Column Visibility) -->
    <link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.4.1/css/buttons.bootstrap5.min.css">
    
    <link href="https://fonts.googleapis.com/css2?family=Sarabun:wght@300;400;600&display=swap" rel="stylesheet">
    
    <style>
        body {{ font-family: 'Sarabun', sans-serif; background-color: #f8f9fa; padding: 20px; font-size: 14px; }}
        .container-fluid {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }}
        h2 {{ color: #0d6efd; font-weight: 600; margin-bottom: 20px; }}
        .nav-tabs .nav-link.active {{ font-weight: bold; color: #0d6efd; border-top: 3px solid #0d6efd; }}
        
        th {{ background-color: #f8f9fa !important; resize: horizontal; overflow: auto; min-width: 50px; }}
        .progress {{ height: 16px; background-color: #e9ecef; position: relative; }}
        .badge-type {{ font-size: 0.75em; }}
        .sample-data {{ font-family: 'Courier New', monospace; font-size: 0.8em; color: #444; white-space: pre-wrap; min-width: 200px; max-height: 100px; overflow-y: auto; }}
        .val-hl {{ font-family: monospace; color: #d63384; font-weight: bold; }}
        .fk-detail {{ font-size: 0.75em; color: #0d6efd; font-family: monospace; }}
        .log-container {{ background-color: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 5px; height: 600px; overflow-y: auto; font-family: monospace; }}
        tr.warning-row td {{ background-color: #fff5f5 !important; }}
        
        /* Button Customization */
        div.dt-buttons {{ margin-bottom: 10px; }}
        button.dt-button.buttons-colvis {{ background-color: #f8f9fa; border: 1px solid #ccc; color: #333; border-radius: 4px; font-size: 0.9em; }}
        button.dt-button.buttons-colvis:hover {{ background-color: #e2e6ea; }}
        div.dt-button-collection {{ width: 300px; }}
    </style>
</head>
<body>
<div class="container-fluid">
    <div class="d-flex justify-content-between align-items-center mb-3">
        <h2>🏥 HIS Database Analysis Report</h2>
        <span class="text-muted small">Source: {os.path.basename(input_file)}</span>
    </div>

    <ul class="nav nav-tabs" id="myTab" role="tablist">
        <li class="nav-item"><button class="nav-link active" id="overview-tab" data-bs-toggle="tab" data-bs-target="#overview">📋 Overview</button></li>
        <li class="nav-item"><button class="nav-link" id="detail-tab" data-bs-toggle="tab" data-bs-target="#detail">🔍 Column Detail</button></li>
        <li class="nav-item"><button class="nav-link" id="log-tab" data-bs-toggle="tab" data-bs-target="#log">📝 Process Log</button></li>
    </ul>

    <div class="tab-content pt-3">
        <!-- Overview Tab -->
        <div class="tab-pane fade show active" id="overview">
            <table id="overviewTable" class="table table-hover table-bordered" style="width:100%">
                <thead class="table-light"><tr><th>Table Name</th><th>Total Rows</th><th>Columns</th><th>Empty Cols</th><th>Data Quality</th></tr></thead>
                <tbody></tbody>
            </table>
        </div>

        <!-- Detail Tab -->
        <div class="tab-pane fade" id="detail">
            <table id="detailTable" class="table table-hover table-bordered" style="width:100%">
                <thead class="table-light">
                    <tr>
                        <th>Table</th><th>Column</th><th>Type</th><th>Key</th><th>Default</th>
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

<!-- JS Libraries -->
<script src="https://code.jquery.com/jquery-3.7.0.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/5.3.0/js/bootstrap.bundle.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>

<!-- DataTables Buttons -->
<script src="https://cdn.datatables.net/buttons/2.4.1/js/dataTables.buttons.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.1/js/buttons.bootstrap5.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.1/js/buttons.colVis.min.js"></script>

<script>
    const detailData = {json_detail};
    const overviewData = {json_overview};

    $(document).ready(function() {{
        // Overview Table
        $('#overviewTable').DataTable({{
            data: overviewData,
            columns: [
                {{ data: 'table' }}, {{ data: 'rows', className: 'text-end' }}, 
                {{ data: 'cols', className: 'text-end' }}, {{ data: 'empty', className: 'text-end' }}, 
                {{ data: 'quality', className: 'text-end' }}
            ],
            pageLength: 15, order: [[ 1, "desc" ]]
        }});

        // Detail Table with Column Visibility
        $('#detailTable').DataTable({{
            data: detailData,
            columns: [
                {{ data: 'table' }}, {{ data: 'column' }}, {{ data: 'type' }}, {{ data: 'key' }}, {{ data: 'default' }},
                {{ data: 'rows', className: 'text-end' }}, {{ data: 'nulls' }}, {{ data: 'distinct', className: 'text-end' }},
                {{ data: 'min' }}, {{ data: 'max' }}, {{ data: 'top5' }}, {{ data: 'sample' }}
            ],
            dom: 'Bfrtip', // Add Buttons to layout
            buttons: [
                {{
                    extend: 'colvis',
                    text: '👁️ Show/Hide Columns',
                    className: 'btn btn-sm btn-outline-primary',
                    columns: ':not(:first-child)' // Prevent hiding Table Name
                }},
                'pageLength'
            ],
            createdRow: function(row, data) {{ if(data.is_warning) $(row).addClass('warning-row'); }},
            pageLength: 25, 
            lengthMenu: [[25, 50, 100, -1], [25, 50, 100, "All"]],
            language: {{ "search": "🔍 Filter:" }}
        }});
    }});
</script>
</body>
</html>
"""

with open(output_file, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"✅ HTML Report Generated: {output_file}")