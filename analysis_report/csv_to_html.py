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

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(input_file)))
log_file_path = os.path.join(base_dir, "process.log")
ddl_file_path = os.path.join(base_dir, "ddl_schema", "schema.sql")

def safe_str(val):
    if val is None: return ""
    return str(val).strip()

log_content = "Log file not found."
if os.path.exists(log_file_path):
    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
            log_content = f.read()
    except Exception as e: log_content = str(e)
log_content = log_content.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

ddl_map = {}
if os.path.exists(ddl_file_path):
    try:
        with open(ddl_file_path, 'r', encoding='utf-8', errors='replace') as f:
            sql_content = f.read()
            regex = r'CREATE TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\[?[\w\d_]+\]?\.\[?)?([\w\d_\'\s]+)\]?\s*\((?:[^;]|\n)*?\);'
            matches = re.finditer(regex, sql_content, re.IGNORECASE | re.DOTALL)
            for match in matches:
                ddl_map[match.group(1)] = match.group(0)
    except Exception as e: print(f"Warning parsing DDL: {e}")

detail_rows = []
table_stats = {} 

try:
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            t_name = safe_str(row.get('Table', ''))
            
            # --- JUNK FILTER ---
            t_name_lower = t_name.lower()
            if not t_name or \
               t_name_lower.startswith("msg ") or \
               t_name_lower.startswith("level ") or \
               "changed database context" in t_name_lower or \
               "rows affected" in t_name_lower or \
               t_name_lower == "table":
                continue
            
            try: total = int(float(row.get('Total_Rows', 0) or 0))
            except: total = 0
            try: size = float(row.get('Table_Size_MB', 0) or 0)
            except: size = 0.0
            try: nulls = int(float(row.get('Null_Count', 0) or 0))
            except: nulls = 0
            try: empties = int(float(row.get('Empty_Count', 0) or 0))
            except: empties = 0
            try: zeros = int(float(row.get('Zero_Count', 0) or 0))
            except: zeros = 0

            bad_data_count = nulls + empties + zeros
            valid_data_count = total - bad_data_count
            
            null_pct = (nulls / total * 100) if total > 0 else 0
            empty_pct = (empties / total * 100) if total > 0 else 0
            zero_pct = (zeros / total * 100) if total > 0 else 0
            valid_pct = (valid_data_count / total * 100) if total > 0 else 0
            
            completeness_score = valid_pct

            if t_name not in table_stats:
                table_stats[t_name] = {
                    'rows': total, 
                    'cols': 0, 
                    'empty_cols': 0, 
                    'size': size,
                    'sum_completeness': 0
                }
            
            table_stats[t_name]['cols'] += 1
            table_stats[t_name]['sum_completeness'] += completeness_score
            if null_pct == 100:
                table_stats[t_name]['empty_cols'] += 1

            badge_class = "bg-secondary"
            dtype = safe_str(row.get('DataType', '')).lower()
            
            if 'char' in dtype: badge_class = "bg-primary"
            elif 'int' in dtype or 'number' in dtype: badge_class = "bg-success"
            elif 'date' in dtype: badge_class = "bg-info text-dark"
            
            pk_icon = '🔑' if safe_str(row.get('PK')).upper() == 'YES' else ''
            fk_raw = safe_str(row.get("FK",""))
            fk_icon = ''
            if fk_raw:
                ref_table = fk_raw.replace('-> ', '').split('.')[0].strip().replace('"', '').replace('[', '').replace(']', '')
                fk_icon = f'<a href="#" onclick="showDDL(\'{ref_table}\'); return false;" class="text-decoration-none">🔗 <span class="fk-detail">{fk_raw}</span></a>'
            
            def create_mini_bar(label, pct, color_class, count):
                opacity = "1" if count > 0 else "0.3"
                return f'''
                <div class="d-flex align-items-center" style="margin-bottom:2px; font-size:0.75em; opacity:{opacity}">
                    <div style="width:45px; color:#666;">{label}</div>
                    <div class="progress flex-grow-1" style="height:5px; background-color:#e9ecef; margin:0 6px;">
                        <div class="progress-bar {color_class}" role="progressbar" style="width: {pct}%"></div>
                    </div>
                    <div style="width:40px; text-align:right; font-family:monospace; color:#444;">{count:,}</div>
                </div>
                '''

            composition_html = f'''
            <div style="min-width:200px; padding:2px 0;">
                {create_mini_bar("Valid", valid_pct, "bg-success", valid_data_count)}
                {create_mini_bar("Nulls", null_pct, "bg-secondary", nulls)}
                {create_mini_bar("Empty", empty_pct, "bg-danger", empties)}
                {create_mini_bar("Zero", zero_pct, "bg-warning text-dark", zeros)}
            </div>
            '''

            detail_rows.append({
                "table": t_name,
                "column": f'<span class="{"pk-col" if safe_str(row.get("PK")).upper()=="YES" else ""}">{safe_str(row.get("Column",""))}</span>',
                "type": f'<span class="badge {badge_class} badge-type">{safe_str(row.get("DataType",""))}</span>',
                "key": f'{pk_icon} {fk_icon}',
                "default": f'<span class="default-col">{safe_str(row.get("Default",""))}</span>',
                "rows": f'{total:,}',
                "composition": composition_html,
                "distinct": safe_str(row.get('Distinct_Values', '')),
                "min": f'<span class="val-hl">{safe_str(row.get("Min_Val",""))}</span>',
                "max": f'<span class="val-hl">{safe_str(row.get("Max_Val",""))}</span>',
                "top5": f'<div class="sample-data" style="max-height:60px">{safe_str(row.get("Top_5_Values","")).replace("|", "<br>")}</div>',
                "sample": f'<div class="sample-data">{safe_str(row.get("Sample_Values",""))}</div>',
                "is_warning": (valid_pct < 100)
            })

    overview_rows = []
    for t, stats in table_stats.items():
        quality = stats['sum_completeness'] / stats['cols'] if stats['cols'] > 0 else 0
        q_color = "text-success"
        if quality < 95: q_color = "text-warning"
        if quality < 80: q_color = "text-danger"
        
        tbl_link = f'<a href="#" onclick="showDDL(\'{t}\'); return false;" class="fw-bold text-primary">{t}</a>'
        
        overview_rows.append({
            "table": tbl_link,
            "rows": f'{stats["rows"]:,}',
            "size": f'{stats["size"]:,.2f} MB',
            "cols": stats['cols'],
            "empty": stats['empty_cols'],
            "quality": f'<b class="{q_color}">{quality:.1f}%</b>'
        })

except Exception as e: print(f"Error: {e}"); sys.exit(1)

json_detail = json.dumps(detail_rows)
json_overview = json.dumps(overview_rows)
json_ddl = json.dumps(ddl_map)

html_content = f"""
<!DOCTYPE html>
<html lang="th">
<head>
    <meta charset="UTF-8">
    <title>HIS Migration Report</title>
    
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/5.3.0/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.4.1/css/buttons.bootstrap5.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Sarabun:wght@300;400;600&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css">
    
    <style>
        :root {{ --bs-primary-rgb: 13, 110, 253; }}
        body {{ font-family: 'Sarabun', sans-serif; background-color: #f8f9fa; padding: 20px; font-size: 14px; }}
        .container-fluid {{ background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.05); }}
        h2 {{ color: #0d6efd; font-weight: 600; margin-bottom: 0; }}
        .nav-tabs .nav-link.active {{ font-weight: bold; color: #0d6efd; border-top: 3px solid #0d6efd; background-color: #fff; }}
        
        th {{ background-color: #f1f4f9 !important; resize: horizontal; overflow: auto; min-width: 50px; vertical-align: middle !important; }}
        .progress {{ border-radius: 2px; box-shadow: inset 0 1px 2px rgba(0,0,0,.1); }}
        
        .badge-type {{ font-size: 0.75em; padding: 5px 8px; border-radius: 6px; }}
        .sample-data {{ font-family: 'Courier New', monospace; font-size: 0.85em; color: #444; white-space: pre-wrap; min-width: 200px; max-height: 100px; overflow-y: auto; }}
        .val-hl {{ font-family: monospace; color: #d63384; font-weight: bold; }}
        .fk-detail {{ font-size: 0.8em; color: #0d6efd; font-family: monospace; }}
        tr.warning-row td {{ background-color: #fff9e6 !important; }} 
        
        /* Buttons */
        .dt-buttons .btn-group {{ display: flex; flex-wrap: wrap; gap: 5px; }}
        .dt-button {{ border-radius: 6px !important; padding: 5px 12px !important; font-size: 0.9rem !important; transition: all 0.2s ease-in-out !important; background-image: none !important; }}
        .buttons-colvis {{ background-color: transparent !important; border: 1px solid #0d6efd !important; color: #0d6efd !important; }}
        .buttons-colvis:hover {{ background-color: #0d6efd !important; color: white !important; }}
        .btn-outline-secondary {{ background-color: #fff !important; color: #333 !important; border: 1px solid #6c757d !important; }}
        .btn-outline-secondary:hover, .btn-outline-secondary.active {{ background-color: #0d6efd !important; color: #fff !important; border-color: #0d6efd !important; box-shadow: 0 2px 5px rgba(0,0,0,0.2) !important; }}

        pre.sql-code {{ background-color: #282c34; color: #abb2bf; padding: 15px; border-radius: 6px; font-size: 13px; max-height: 500px; overflow: auto; }}
        .log-container {{ background-color: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 6px; height: 600px; overflow-y: auto; font-family: monospace; }}
        .doc-card {{ border-left: 4px solid #0d6efd; padding: 15px; background: #f8f9fa; margin-bottom: 15px; }}
        .strategy-card {{ background: #fff; border: 1px solid #eee; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.02); }}
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
            <span class="badge bg-primary rounded-pill p-2">v7.0 Robust</span>
        </div>
    </div>

    <ul class="nav nav-tabs" id="myTab" role="tablist">
        <li class="nav-item"><button class="nav-link active" id="overview-tab" data-bs-toggle="tab" data-bs-target="#overview">📋 Overview</button></li>
        <li class="nav-item"><button class="nav-link" id="detail-tab" data-bs-toggle="tab" data-bs-target="#detail">🔍 Column Detail</button></li>
        <li class="nav-item"><button class="nav-link" id="doc-tab" data-bs-toggle="tab" data-bs-target="#doc">📖 Formulas & Docs</button></li>
        <li class="nav-item"><button class="nav-link" id="log-tab" data-bs-toggle="tab" data-bs-target="#log">📝 Process Log</button></li>
    </ul>

    <div class="tab-content pt-4">
        <!-- Overview -->
        <div class="tab-pane fade show active" id="overview">
            <div class="alert alert-info py-2 mb-3 small">
                <i class="bi bi-info-circle"></i> <b>Data Quality Score:</b> Calculated based on the completeness of data across all columns (penalizing Nulls, Empty Strings, and Zeros).
            </div>
            <table id="overviewTable" class="table table-hover table-bordered w-100">
                <thead class="table-light"><tr><th>Table Name</th><th>Total Rows</th><th>Size (MB)</th><th>Columns</th><th>Empty Cols</th><th>Data Quality</th></tr></thead>
                <tbody></tbody>
            </table>
        </div>

        <!-- Detail -->
        <div class="tab-pane fade" id="detail">
            <table id="detailTable" class="table table-hover table-bordered w-100">
                <thead class="table-light">
                    <tr>
                        <th>Table</th><th>Column</th><th>Type</th><th>Key / Ref</th><th>Default</th>
                        <th>Rows</th>
                        <th style="min-width: 200px;">Data Composition</th>
                        <th>Dist.</th><th>Min</th><th>Max</th><th>Top 5 Freq</th><th>Sample</th>
                    </tr>
                </thead>
                <tbody></tbody>
            </table>
        </div>

        <!-- Docs -->
        <div class="tab-pane fade" id="doc">
            <div class="row">
                <div class="col-md-6">
                    <h4>1. สูตรคำนวณ Data Quality (Formulas)</h4>
                    <div class="doc-card">
                        <strong>Column Completeness Score:</strong> คำนวณจากสัดส่วนข้อมูลที่สมบูรณ์ในแต่ละคอลัมน์
                        <br><br>
                        $$ Score = \\frac{{Total - (Null + Empty + Zero)}}{{Total}} \\times 100 $$
                        <br>
                        <ul>
                            <li><span class="text-success">100%</span> : ข้อมูลสมบูรณ์ (ไม่มี Null, ว่าง, หรือ 0)</li>
                            <li><span class="text-warning">< 100%</span> : มีข้อมูลที่ไม่สมบูรณ์ปนอยู่</li>
                        </ul>
                        <hr>
                        <strong>Table Health Score:</strong> ค่าเฉลี่ยของคะแนนทุกคอลัมน์ในตารางนั้น
                    </div>
                </div>
                <div class="col-md-6">
                    <h4>2. การวิเคราะห์เพื่อการ Migrate (Analysis Strategy)</h4>
                    <div class="card p-3">
                        <p>ใช้ข้อมูลจาก Report นี้เพื่อจัดกลุ่มความยากง่ายในการย้ายข้อมูล:</p>
                        <ul class="list-unstyled">
                            <li class="mb-3">
                                <span class="badge bg-success">🟢 Group A: Direct Move</span><br>
                                <b>ลักษณะ:</b> Quality > 95%, Type ถูกต้อง<br>
                                <b>Action:</b> ย้ายข้อมูลได้โดยตรง (1-1 Mapping)
                            </li>
                            <li class="mb-3">
                                <span class="badge bg-warning text-dark">🟡 Group B: Transformation Needed</span><br>
                                <b>ลักษณะ:</b> มี Empty String/Zero ในฟิลด์สำคัญ<br>
                                <b>Action:</b> ต้องเขียน ETL Script เพื่อแปลงค่า (เช่น แปลง <code>""</code> เป็น <code>NULL</code>)
                            </li>
                            <li class="mb-3">
                                <span class="badge bg-info text-dark">🔵 Group C: Mapping Required</span><br>
                                <b>ลักษณะ:</b> Top 5 Values แสดงค่าที่ไม่เป็นมาตรฐาน (เช่น M, Male, ชาย)<br>
                                <b>Action:</b> สร้าง Dictionary หรือ Mapping Table เพื่อปรับให้เป็นมาตรฐานเดียว
                            </li>
                            <li>
                                <span class="badge bg-danger">🔴 Group D: Review / Drop</span><br>
                                <b>ลักษณะ:</b> Quality ต่ำมาก หรือเป็น Empty Column (100% Null)<br>
                                <b>Action:</b> พิจารณาตัดทิ้ง ไม่นำเข้าระบบใหม่
                            </li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>

        <!-- Log -->
        <div class="tab-pane fade" id="log">
            <div class="log-container"><pre>{log_content}</pre></div>
        </div>
    </div>
</div>

<!-- Modal -->
<div class="modal fade" id="ddlModal" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog modal-lg modal-dialog-centered">
    <div class="modal-content">
      <div class="modal-header bg-light">
        <h5 class="modal-title" id="ddlModalTitle">Table Schema</h5>
        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body"><pre class="sql-code" id="ddlModalBody">Loading...</pre></div>
      <div class="modal-footer"><button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button></div>
    </div>
  </div>
</div>

<script src="https://code.jquery.com/jquery-3.7.0.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/5.3.0/js/bootstrap.bundle.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.1/js/dataTables.buttons.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.1/js/buttons.bootstrap5.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.1/js/buttons.colVis.min.js"></script>
<script src="https://polyfill.io/v3/polyfill.min.js?features=es6"></script>
<script id="MathJax-script" async src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js"></script>

<script>
    const detailData = {json_detail};
    const overviewData = {json_overview};
    const ddlData = {json_ddl};
    let ddlModal;

    function showDDL(tableName) {{
        const content = ddlData[tableName] || "-- DDL not found for " + tableName;
        document.getElementById('ddlModalTitle').innerText = 'Schema: ' + tableName;
        document.getElementById('ddlModalBody').innerText = content;
        if(!ddlModal) ddlModal = new bootstrap.Modal(document.getElementById('ddlModal'));
        ddlModal.show();
    }}

    $(document).ready(function() {{
        $('#overviewTable').DataTable({{
            data: overviewData,
            columns: [
                {{ data: 'table' }}, {{ data: 'rows', className: 'text-end' }}, 
                {{ data: 'size', className: 'text-end' }}, 
                {{ data: 'cols', className: 'text-end' }}, {{ data: 'empty', className: 'text-end' }}, 
                {{ data: 'quality', className: 'text-end' }}
            ],
            pageLength: 15, order: [[ 1, "desc" ]],
            dom: '<"d-flex justify-content-between mb-3"Bf>rtip',
            buttons: [ {{ extend: 'pageLength', className: 'btn btn-outline-secondary' }} ]
        }});

        $('#detailTable').DataTable({{
            data: detailData,
            columns: [
                {{ data: 'table' }}, {{ data: 'column' }}, {{ data: 'type' }}, {{ data: 'key' }}, {{ data: 'default' }},
                {{ data: 'rows', className: 'text-end' }}, 
                {{ data: 'composition', className: 'text-start' }}, 
                {{ data: 'distinct', className: 'text-end' }},
                {{ data: 'min' }}, {{ data: 'max' }}, {{ data: 'top5' }}, {{ data: 'sample' }}
            ],
            dom: '<"d-flex flex-wrap gap-2 justify-content-between align-items-center mb-3"Bf>rtip',
            buttons: [
                {{ extend: 'colvis', text: '👁️ Columns', className: 'btn buttons-colvis', columns: ':not(:first-child)' }},
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