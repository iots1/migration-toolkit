import csv
import sys
import os

if len(sys.argv) < 2:
    print("Usage: python3 csv_to_html.py <input_csv_file>")
    sys.exit(1)

input_file = sys.argv[1]
output_file = input_file.replace('.csv', '.html')

# Find Log file
run_dir = os.path.dirname(os.path.dirname(os.path.abspath(input_file)))
log_file_path = os.path.join(run_dir, "process.log")
log_content = "Log file not found at: " + log_file_path

if os.path.exists(log_file_path):
    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
            log_content = f.read()
    except Exception as e:
        log_content = f"Error reading log file: {e}"

# Escape HTML special characters in log content to prevent breaking the HTML
log_content = log_content.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

html_head = """
<!DOCTYPE html>
<html lang="th">
<head>
    <meta charset="UTF-8">
    <title>HIS Migration Report</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/5.3.0/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Sarabun:wght@300;400;600&display=swap" rel="stylesheet">
    <!-- Bootstrap JS for Tabs -->
    <script src="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/5.3.0/js/bootstrap.bundle.min.js"></script>
    <style>
        body { font-family: 'Sarabun', sans-serif; background-color: #f8f9fa; padding: 20px; font-size: 14px; }
        .container-fluid { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }
        h2 { color: #0d6efd; font-weight: 600; margin-bottom: 20px; }
        .badge-type { font-size: 0.75em; padding: 4px 6px; }
        .progress { height: 16px; background-color: #e9ecef; }
        
        th { resize: horizontal; overflow: auto; min-width: 50px; position: relative; }
        .sample-data { font-family: 'Courier New', monospace; font-size: 0.85em; color: #444; white-space: pre-wrap; word-break: break-word; min-width: 400px; }
        .comment-col { font-style: italic; color: #6c757d; font-size: 0.85em; white-space: normal; min-width: 150px; }
        .warning-row { background-color: #fff5f5 !important; }
        .key-icon { font-size: 1.1em; margin-right: 2px; cursor: help; }
        .pk-col { color: #d63384; font-weight: bold; }
        .fk-col { color: #0d6efd; font-weight: bold; }
        .fk-detail { font-size: 0.85em; color: #0d6efd; font-family: monospace; }
        .default-col { font-family: monospace; color: #198754; font-size: 0.85em; word-break: break-all; }
        table.dataTable tbody td { vertical-align: top; }
        th::-webkit-scrollbar { width: 5px; height: 5px; background: transparent; }
        th::-webkit-scrollbar-thumb { background: #ccc; border-radius: 5px; }

        /* Log Tab Styles */
        .log-container { background-color: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 5px; height: 600px; overflow-y: auto; font-family: 'Courier New', monospace; font-size: 13px; }
        .nav-tabs .nav-link.active { font-weight: bold; color: #0d6efd; border-top: 3px solid #0d6efd; }
    </style>
</head>
<body>
<div class="container-fluid">
    <div class="d-flex justify-content-between align-items-center mb-3">
        <h2>🏥 HIS Database Analysis Report</h2>
        <span class="text-muted small">Source: """ + os.path.basename(input_file) + """</span>
    </div>

    <!-- Tabs Nav -->
    <ul class="nav nav-tabs" id="myTab" role="tablist">
        <li class="nav-item" role="presentation">
            <button class="nav-link active" id="report-tab" data-bs-toggle="tab" data-bs-target="#report" type="button" role="tab">📊 Data Report</button>
        </li>
        <li class="nav-item" role="presentation">
            <button class="nav-link" id="log-tab" data-bs-toggle="tab" data-bs-target="#log" type="button" role="tab">📝 Process Log</button>
        </li>
    </ul>

    <!-- Tabs Content -->
    <div class="tab-content pt-3" id="myTabContent">
        
        <!-- Tab 1: Report Table -->
        <div class="tab-pane fade show active" id="report" role="tabpanel">
            <table id="hisTable" class="table table-hover table-bordered" style="width:100%">
                <thead class="table-light">
                    <tr>
                        <th>Table</th>
                        <th>Column</th>
                        <th>Type</th>
                        <th style="min-width:70px">Key / Ref</th>
                        <th>Default</th>
                        <th>Comment</th>
                        <th>Rows</th>
                        <th>Nulls (%)</th>
                        <th>Max Len</th>
                        <th>Distinct</th>
                        <th>Sample Data (Top 10)</th>
                    </tr>
                </thead>
                <tbody>
"""

# NOTE: Using standard string (not f-string) to avoid conflict with JavaScript braces {}
html_footer_template = """
                </tbody>
            </table>
        </div>

        <!-- Tab 2: System Log -->
        <div class="tab-pane fade" id="log" role="tabpanel">
            <div class="log-container">
                <pre>__LOG_CONTENT__</pre>
            </div>
        </div>
    </div>
</div>

<script src="https://code.jquery.com/jquery-3.7.0.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
<script>
    $(document).ready(function() {
        var table = $('#hisTable').DataTable({
            "pageLength": 25,
            "lengthMenu": [ [10, 25, 50, 100, -1], [10, 25, 50, 100, "All"] ],
            "order": [[ 0, "asc" ]],
            "language": { "search": "🔍 Filter:" }
        });

        // Prevent sorting when resizing
        $('#hisTable thead th').on('mousedown', function () {
            $(this).data('initialWidth', $(this).outerWidth());
        });
        $('#hisTable thead th').on('click', function (e) {
            var currentWidth = $(this).outerWidth();
            var initialWidth = $(this).data('initialWidth');
            if (initialWidth && currentWidth !== initialWidth) {
                e.stopPropagation();
                return false;
            }
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

            bar_class = "bg-success"
            if null_pct > 50: bar_class = "bg-warning text-dark"
            if null_pct == 100: bar_class = "bg-danger"

            dtype = row.get('DataType', '').lower()
            badge_class = "bg-secondary"
            if 'char' in dtype: badge_class = "bg-primary"
            elif 'int' in dtype or 'number' in dtype: badge_class = "bg-success"
            elif 'date' in dtype or 'time' in dtype: badge_class = "bg-info text-dark"

            pk_val = row.get('PK', '')
            fk_val = row.get('FK', '')
            pk_html = '<span class="key-icon" title="Primary Key">🔑</span>' if pk_val == 'YES' else ''
            fk_html = ''
            if fk_val and fk_val != '':
                fk_html = f'<span class="key-icon" title="Foreign Key: {fk_val}">🔗</span><br><span class="fk-detail">{fk_val}</span>'
            key_display = f"{pk_html}{fk_html}"
            
            col_class = ""
            if pk_val == 'YES': col_class = "pk-col"
            elif fk_val != '': col_class = "fk-col"

            tr_class = "warning-row" if null_pct == 100 else ""

            html_row = f"""
            <tr class="{tr_class}">
                <td class="fw-bold">{row.get('Table', '')}</td>
                <td class="{col_class}">{row.get('Column', '')}</td>
                <td><span class="badge {badge_class} badge-type">{row.get('DataType', '')}</span></td>
                <td>{key_display}</td>
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
                <td class="sample-data">{row.get('Sample_Values', '')}</td>
            </tr>
            """
            rows_html.append(html_row)

    # Inject log content into footer
    html_footer = html_footer_template.replace("__LOG_CONTENT__", log_content)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_head + "".join(rows_html) + html_footer)

    print(f"✅ HTML Report Generated: {output_file}")

except Exception as e:
    print(f"❌ Error: {e}")