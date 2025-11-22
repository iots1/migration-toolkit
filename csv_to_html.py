import csv
import sys
import os

if len(sys.argv) < 2:
    print("Usage: python3 csv_to_html.py <input_csv_file>")
    sys.exit(1)

input_file = sys.argv[1]
output_file = input_file.replace('.csv', '.html')

html_head = """
<!DOCTYPE html>
<html lang="th">
<head>
    <meta charset="UTF-8">
    <title>HIS Migration Report</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/5.3.0/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Sarabun:wght@300;400;600&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Sarabun', sans-serif; background-color: #f8f9fa; padding: 20px; font-size: 14px; }
        .container-fluid { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }
        h2 { color: #0d6efd; font-weight: 600; margin-bottom: 20px; }
        .badge-type { font-size: 0.75em; padding: 4px 6px; }
        .progress { height: 16px; background-color: #e9ecef; }
        .sample-data { font-family: 'Courier New', monospace; font-size: 0.8em; color: #555; max-width: 250px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; cursor: help; }
        .warning-row { background-color: #fff5f5 !important; }
        .key-icon { font-size: 1.1em; margin-right: 2px; }
        .pk-col { color: #d63384; font-weight: bold; }
        .fk-col { color: #0d6efd; font-weight: bold; }
        .comment-col { font-style: italic; color: #6c757d; font-size: 0.85em; max-width: 200px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .default-col { font-family: monospace; color: #198754; font-size: 0.85em; }
    </style>
</head>
<body>
<div class="container-fluid">
    <div class="d-flex justify-content-between align-items-center">
        <h2>🏥 HIS Database Analysis Report</h2>
        <span class="text-muted small">File: """ + os.path.basename(input_file) + """</span>
    </div>
    <hr>
    <table id="hisTable" class="table table-hover table-bordered" style="width:100%">
        <thead class="table-light">
            <tr>
                <th>Table</th>
                <th>Column</th>
                <th>Type</th>
                <th title="Primary Key / Foreign Key" style="width:50px">Key</th>
                <th>Default</th>
                <th>Comment</th>
                <th>Rows</th>
                <th>Nulls (%)</th>
                <th>Max Len</th>
                <th>Distinct</th>
                <th>Sample Data</th>
            </tr>
        </thead>
        <tbody>
"""

html_footer = """
        </tbody>
    </table>
</div>
<script src="https://code.jquery.com/jquery-3.7.0.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
<script>
    $(document).ready(function() {
        $('#hisTable').DataTable({
            "pageLength": 25,
            "order": [[ 0, "asc" ]],
            "language": { "search": "🔍 Filter:" }
        });
        // Enable tooltips
        var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'))
        var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
          return new bootstrap.Tooltip(tooltipTriggerEl)
        })
    });
</script>
</body>
</html>
"""

print(f"Processing {input_file} ...")

try:
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        rows_html = []
        for row in reader:
            try:
                total = int(row.get('Total_Rows', 0))
                nulls = int(row.get('Null_Count', 0))
                null_pct = (nulls / total * 100) if total > 0 else 0
            except:
                total, nulls, null_pct = 0, 0, 0

            # --- Logic การแสดงผล ---
            
            # Progress Bar สีแดงถ้า Null 100%
            bar_class = "bg-success"
            if null_pct > 50: bar_class = "bg-warning text-dark"
            if null_pct == 100: bar_class = "bg-danger"

            # Badge สีตาม Type
            dtype = row.get('DataType', '').lower()
            badge_class = "bg-secondary"
            if 'char' in dtype: badge_class = "bg-primary"
            elif 'int' in dtype or 'number' in dtype: badge_class = "bg-success"
            elif 'date' in dtype or 'time' in dtype: badge_class = "bg-info text-dark"

            # Key Icons
            pk_html = '<span class="key-icon" title="Primary Key">🔑</span>' if row.get('PK') == 'YES' else ''
            fk_html = '<span class="key-icon" title="Foreign Key">🔗</span>' if row.get('FK') == 'YES' else ''
            key_display = f"{pk_html}{fk_html}"
            
            # Highlight Column Name if Key
            col_class = ""
            if row.get('PK') == 'YES': col_class = "pk-col"
            elif row.get('FK') == 'YES': col_class = "fk-col"

            # Row Class warning if empty table or col
            tr_class = "warning-row" if null_pct == 100 else ""

            # HTML Row Construction
            html_row = f"""
            <tr class="{tr_class}">
                <td class="fw-bold">{row.get('Table', '')}</td>
                <td class="{col_class}">{row.get('Column', '')}</td>
                <td><span class="badge {badge_class} badge-type">{row.get('DataType', '')}</span></td>
                <td class="text-center">{key_display}</td>
                <td class="default-col">{row.get('Default', '')}</td>
                <td class="comment-col" title="{row.get('Comment', '').replace('"', '&quot;')}">{row.get('Comment', '')}</td>
                <td class="text-end">{total:,}</td>
                <td>
                    <div class="progress" style="position:relative;">
                        <div class="progress-bar {bar_class}" style="width: {null_pct}%"></div>
                        <span style="position:absolute; width:100%; text-align:center; font-size:10px; line-height:16px; color:black;">
                            {nulls:,} ({null_pct:.0f}%)
                        </span>
                    </div>
                </td>
                <td class="text-end">{row.get('Max_Length', '')}</td>
                <td class="text-end">{row.get('Distinct_Values', '')}</td>
                <td class="sample-data" title="{row.get('Sample_Values', '').replace('"', '&quot;')}">{row.get('Sample_Values', '')}</td>
            </tr>
            """
            rows_html.append(html_row)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_head + "".join(rows_html) + html_footer)

    print(f"✅ HTML Report Generated: {output_file}")

except Exception as e:
    print(f"❌ Error: {e}")