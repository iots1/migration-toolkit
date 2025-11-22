import random
import json
from faker import Faker
from datetime import datetime, timedelta

# ตั้งค่า Locale เป็นภาษาไทย
fake = Faker('th_TH')

# ==========================================
# ⚙️ CONFIGURATION (ปรับจำนวนข้อมูลตรงนี้)
# ==========================================
FILENAME = "full_his_mockup.sql"
NUM_PATIENTS = 500       # จำนวนผู้ป่วย
NUM_DOCTORS = 20         # จำนวนหมอ
NUM_OPD_VISITS = 1000    # จำนวน Visit ผู้ป่วยนอก
NUM_IPD_CASES = 200      # จำนวน Case ผู้ป่วยใน
NUM_REFER_HOSPITALS = 20 # จำนวนโรงพยาบาลเครือข่าย

# ==========================================
# 📝 FILE WRITER SETUP
# ==========================================
f = open(FILENAME, "w", encoding="utf-8")

def write_sql(sql):
    f.write(sql + "\n")

print(f"🚀 กำลังสร้างไฟล์ {FILENAME} สำหรับ PostgreSQL...")
print("   - รวมระบบ: OPD, IPD, Queue, Refer, Orders, Lab (JSONB)")

# ==========================================
# 1. DATABASE CLEANUP & DDL (สร้างตาราง)
# ==========================================
write_sql("BEGIN;")
write_sql("-- Cleanup Tables (ลบตารางเก่าถ้ามี)")
write_sql("DROP TABLE IF EXISTS lab_results CASCADE;")
write_sql("DROP TABLE IF EXISTS referrals CASCADE;")
write_sql("DROP TABLE IF EXISTS daily_queues CASCADE;")
write_sql("DROP TABLE IF EXISTS visit_orders CASCADE;")
write_sql("DROP TABLE IF EXISTS diagnosis CASCADE;")
write_sql("DROP TABLE IF EXISTS ipd_admissions CASCADE;")
write_sql("DROP TABLE IF EXISTS appointments CASCADE;")
write_sql("DROP TABLE IF EXISTS opd_visits CASCADE;")
write_sql("DROP TABLE IF EXISTS doctors CASCADE;")
write_sql("DROP TABLE IF EXISTS patients CASCADE;")
write_sql("DROP TABLE IF EXISTS wards CASCADE;")
write_sql("DROP TABLE IF EXISTS refer_hospitals CASCADE;")
write_sql("\n")

write_sql("-- 1. Master Tables")
write_sql("""
CREATE TABLE patients (
    hn VARCHAR(20) PRIMARY KEY,
    cid VARCHAR(13) UNIQUE,
    full_name VARCHAR(200),
    birthdate DATE,
    gender VARCHAR(1),
    blood_group VARCHAR(5),
    phone VARCHAR(20)
);
""")

write_sql("""
CREATE TABLE doctors (
    doctor_id SERIAL PRIMARY KEY,
    doctor_name VARCHAR(200),
    department VARCHAR(50), -- MED, SUR, PED, ORTHO
    license_no VARCHAR(20)
);
""")

write_sql("""
CREATE TABLE wards (
    ward_id SERIAL PRIMARY KEY,
    ward_name VARCHAR(100),
    room_rate NUMERIC(10,2)
);
""")

write_sql("""
CREATE TABLE refer_hospitals (
    hcode VARCHAR(10) PRIMARY KEY,
    hname VARCHAR(200),
    htype VARCHAR(50)
);
""")

write_sql("-- 2. Transaction Tables (OPD Flow)")
write_sql("""
CREATE TABLE opd_visits (
    vn VARCHAR(20) PRIMARY KEY,
    hn VARCHAR(20) REFERENCES patients(hn),
    doctor_id INT REFERENCES doctors(doctor_id),
    visit_date TIMESTAMP,
    symptom TEXT,
    bp_systolic INT,
    bp_diastolic INT
);
""")

write_sql("""
CREATE TABLE daily_queues (
    q_id SERIAL PRIMARY KEY,
    vn VARCHAR(20) REFERENCES opd_visits(vn),
    q_number VARCHAR(10),
    issue_time TIMESTAMP,  -- กดคิว
    call_time TIMESTAMP,   -- เรียกตรวจ
    finish_time TIMESTAMP, -- ตรวจเสร็จ
    wait_minutes INT       -- KPI: เวลารอ
);
""")

write_sql("""
CREATE TABLE diagnosis (
    diag_id SERIAL PRIMARY KEY,
    vn VARCHAR(20) REFERENCES opd_visits(vn),
    icd10 VARCHAR(10),
    diag_name VARCHAR(255),
    diag_type VARCHAR(20) -- PRINCIPAL, COMORBIDITY
);
""")

write_sql("""
CREATE TABLE visit_orders (
    order_id SERIAL PRIMARY KEY,
    vn VARCHAR(20) REFERENCES opd_visits(vn),
    order_type VARCHAR(20), -- DRUG, LAB, XRAY
    item_name VARCHAR(200),
    target_dept VARCHAR(50),
    qty INT,
    order_time TIMESTAMP
);
""")

write_sql("""
CREATE TABLE referrals (
    ref_id SERIAL PRIMARY KEY,
    vn VARCHAR(20) REFERENCES opd_visits(vn),
    type VARCHAR(10), -- IN, OUT
    hcode VARCHAR(10) REFERENCES refer_hospitals(hcode),
    reason TEXT,
    urgency VARCHAR(20)
);
""")

write_sql("-- 3. IPD Tables")
write_sql("""
CREATE TABLE ipd_admissions (
    an VARCHAR(20) PRIMARY KEY,
    hn VARCHAR(20) REFERENCES patients(hn),
    ward_id INT REFERENCES wards(ward_id),
    admit_date TIMESTAMP,
    discharge_date TIMESTAMP,
    discharge_status VARCHAR(50) -- IMPROVED, DEAD, REFER
);
""")

write_sql("""
CREATE TABLE lab_results (
    lab_id SERIAL PRIMARY KEY,
    ref_no VARCHAR(20), -- Link to VN or AN
    test_group VARCHAR(50),
    result_json JSONB, -- Postgres JSONB Feature
    report_time TIMESTAMP
);
""")

# ==========================================
# 2. DATA SEEDING (สร้างข้อมูล)
# ==========================================

# Lists to keep track of generated IDs for relationships
gen_hns = []
gen_doc_ids = []
gen_hcode = []
DEPTS = ['MED (อายุรกรรม)', 'SUR (ศัลยกรรม)', 'PED (เด็ก)', 'ORTHO (กระดูก)', 'ER (ฉุกเฉิน)']

# --- Seed Wards ---
WARDS = [('ICU', 5000), ('Male Medical', 1500), ('Female Medical', 1500), ('Pediatric', 2000), ('Private VIP', 4500)]
for w in WARDS:
    write_sql(f"INSERT INTO wards (ward_name, room_rate) VALUES ('{w[0]}', {w[1]});")

# --- Seed Refer Hospitals ---
for i in range(NUM_REFER_HOSPITALS):
    hc = str(random.randint(10000, 99999))
    gen_hcode.append(hc)
    hnm = f"โรงพยาบาล{fake.city()}"
    write_sql(f"INSERT INTO refer_hospitals VALUES ('{hc}', '{hnm}', 'General Hospital');")

# --- Seed Doctors ---
for i in range(1, NUM_DOCTORS + 1):
    gen_doc_ids.append(i)
    nm = f"นพ./พญ. {fake.first_name()} {fake.last_name()}"
    dp = random.choice(DEPTS)
    lic = f"ว.{random.randint(10000, 60000)}"
    write_sql(f"INSERT INTO doctors (doctor_name, department, license_no) VALUES ('{nm}', '{dp}', '{lic}');")

# --- Seed Patients ---
write_sql("\n-- Generating Patients...")
for i in range(1, NUM_PATIENTS + 1):
    hn = f"HN{str(i).zfill(6)}"
    gen_hns.append(hn)
    nm = f"{fake.first_name()} {fake.last_name()}"
    cid = fake.numerify('#############')
    bd = fake.date_of_birth(minimum_age=1, maximum_age=90)
    g = random.choice(['M', 'F'])
    bl = random.choice(['A', 'B', 'O', 'AB'])
    ph = fake.phone_number()
    write_sql(f"INSERT INTO patients VALUES ('{hn}', '{cid}', '{nm}', '{bd}', '{g}', '{bl}', '{ph}');")

# --- Seed OPD Visits & Related Flow ---
write_sql("\n-- Generating OPD Flow (Visits, Queues, Orders)...")
for i in range(1, NUM_OPD_VISITS + 1):
    vn = f"VN{datetime.now().year}{str(i).zfill(6)}"
    hn = random.choice(gen_hns)
    doc = random.choice(gen_doc_ids)
    v_date = fake.date_time_between(start_date='-6M', end_date='now')
    
    # 1. OPD Visit
    symp = random.choice(['ปวดศีรษะ', 'ไข้สูง หนาวสั่น', 'ปวดท้อง ท้องเสีย', 'ไอ เจ็บคอ', 'เวียนศีรษะ บ้านหมุน'])
    sbp = random.randint(100, 160)
    write_sql(f"INSERT INTO opd_visits VALUES ('{vn}', '{hn}', {doc}, '{v_date}', '{symp}', {sbp}, {random.randint(60, 95)});")
    
    # 2. Queue Logic
    wait = random.randint(5, 90) # รอนาน 5-90 นาที
    call = v_date + timedelta(minutes=wait)
    finish = call + timedelta(minutes=15)
    q_prefix = random.choice(['A', 'B', 'C'])
    q_num = f"{q_prefix}{random.randint(1, 100):03d}"
    write_sql(f"INSERT INTO daily_queues (vn, q_number, issue_time, call_time, finish_time, wait_minutes) VALUES ('{vn}', '{q_num}', '{v_date}', '{call}', '{finish}', {wait});")
    
    # 3. Diagnosis
    icd = random.choice(['J00', 'E11.9', 'I10', 'A09', 'K29.7'])
    diag_nm = "Mock Diagnosis Name"
    write_sql(f"INSERT INTO diagnosis (vn, icd10, diag_name, diag_type) VALUES ('{vn}', '{icd}', '{diag_nm}', 'PRINCIPAL');")
    
    # 4. Orders (Lab/Xray/Drug) - Randomly assign
    # -- Drug --
    if random.random() > 0.2:
        drug = random.choice(['Paracetamol 500mg', 'Amoxicillin 500mg', 'Omeprazole 20mg'])
        write_sql(f"INSERT INTO visit_orders (vn, order_type, item_name, target_dept, qty, order_time) VALUES ('{vn}', 'DRUG', '{drug}', 'Pharmacy', 20, '{finish}');")
    
    # -- Lab --
    if random.random() > 0.5:
        write_sql(f"INSERT INTO visit_orders (vn, order_type, item_name, target_dept, qty, order_time) VALUES ('{vn}', 'LAB', 'CBC + Platelet', 'Laboratory', 1, '{call}');")

    # 5. Refer Out (5% chance)
    if random.random() < 0.05:
        thc = random.choice(gen_hcode)
        write_sql(f"INSERT INTO referrals (vn, type, hcode, reason, urgency) VALUES ('{vn}', 'OUT', '{thc}', 'Over Capability', 'URGENT');")

# --- Seed IPD & JSON Lab ---
write_sql("\n-- Generating IPD Cases & JSON Lab...")
for i in range(1, NUM_IPD_CASES + 1):
    an = f"AN{datetime.now().year}{str(i).zfill(5)}"
    hn = random.choice(gen_hns)
    ward = random.randint(1, 5) # Ward 1-5
    admit = fake.date_time_between(start_date='-3M', end_date='now')
    disc = admit + timedelta(days=random.randint(2, 14))
    
    write_sql(f"INSERT INTO ipd_admissions VALUES ('{an}', '{hn}', {ward}, '{admit}', '{disc}', 'IMPROVED');")
    
    # Mock JSON Lab Result
    res_dict = {
        "Hb": round(random.uniform(10, 16), 1),
        "WBC": random.randint(4000, 12000),
        "Platelet": random.randint(150000, 400000),
        "Note": "ผลปกติ"
    }
    json_str = json.dumps(res_dict, ensure_ascii=False)
    # Note: ใน SQL String ต้อง Escape Single Quote ถ้ามี แต่ JSON dumps จะใช้ Double Quote เป็นหลัก
    write_sql(f"INSERT INTO lab_results (ref_no, test_group, result_json, report_time) VALUES ('{an}', 'CBC', '{json_str}', '{disc}');")

write_sql("COMMIT;")
f.close()
print(f"✅ เสร็จสิ้น! ไฟล์ '{FILENAME}' ถูกสร้างเรียบร้อยแล้ว")