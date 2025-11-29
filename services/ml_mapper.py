import streamlit as st
import pandas as pd
from sentence_transformers import SentenceTransformer, util
import numpy as np
import re

class SmartMapper:
    """
    AI Service for semantic column matching using Sentence Transformers + HIS Dictionary.
    """
    
    def __init__(self, model_name='paraphrase-multilingual-MiniLM-L12-v2'):
        self.model_name = model_name
        
        # --- HIS / Medical Dictionary ---
        # คำศัพท์เฉพาะทางที่ AI อาจจะเดาไม่ถูก หรือเราอยากบังคับจับคู่
        # Format: "คำที่เจอบ่อยใน Source": ["คำที่เป็นไปได้ใน Target", ...]
        self.his_dictionary = {
            "hn": ["hn", "hospital_number", "mrn", "patient_code"],
            "vn": ["vn", "visit_number", "visit_no"],
            "an": ["an", "admission_number", "admit_no"],
            "cid": ["cid", "national_id", "card_id", "citizen_id", "id_card"],
            "dob": ["dob", "birth_date", "birthdate", "date_of_birth"],
            "pname": ["prefix", "title", "pname"],
            "fname": ["firstname", "first_name", "name"],
            "lname": ["lastname", "last_name", "surname"],
            "sex": ["gender", "sex"],
            "diag": ["diagnosis", "icd10", "diag_code"],
            "bp": ["blood_pressure", "bp_sys", "bp_dia"],
            "bw": ["body_weight", "weight"],
            "ht": ["height"],
            "cc": ["chief_complaint", "symptom"]
        }

    @st.cache_resource
    def load_model(_self):
        """Loads the model and caches it to avoid reloading on every interaction."""
        return SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')

    def suggest_mapping(self, source_cols, target_cols, threshold=0.4):
        """
        Matches source columns to target columns based on:
        1. Exact/Dictionary Match (Priority)
        2. Semantic Similarity (AI)
        """
        if not source_cols or not target_cols:
            return {}

        model = self.load_model()
        suggestions = {}
        
        # Pre-compute Target Embeddings once
        tgt_embeddings = model.encode(target_cols, convert_to_tensor=True)

        for src_col in source_cols:
            src_lower = str(src_col).lower().strip()
            found_match = None
            
            # --- STEP 1: Check HIS Dictionary & Rules First ---
            # ลองหาว่า Source นี้ตรงกับ key ไหนใน Dictionary หรือไม่
            for key, possible_targets in self.his_dictionary.items():
                # ถ้า Source มีคำว่า key นี้ผสมอยู่ (เช่น 'patient_hn' มีคำว่า 'hn')
                # หรือถ้า Source เป็นตัวย่อตรงๆ
                if src_lower == key or src_lower in possible_targets:
                    # ลองหาว่าใน Target List มีคำที่มีความหมายเดียวกันไหม
                    for tgt in target_cols:
                        tgt_lower = tgt.lower().strip()
                        # ถ้า Target ก็อยู่ในกลุ่มคำเดียวกัน ให้จับคู่เลย
                        if tgt_lower == key or tgt_lower in possible_targets:
                            found_match = tgt
                            break
                    if found_match: break
            
            # Direct text match fallback (e.g. "CreateDate" == "create_date")
            if not found_match:
                simple_src = re.sub(r'[^a-z0-9]', '', src_lower)
                for tgt in target_cols:
                    simple_tgt = re.sub(r'[^a-z0-9]', '', tgt.lower())
                    if simple_src == simple_tgt:
                        found_match = tgt
                        break

            if found_match:
                suggestions[src_col] = found_match
                continue

            # --- STEP 2: Use AI (Semantic) if no dictionary match ---
            src_embedding = model.encode(src_col, convert_to_tensor=True)
            cosine_scores = util.cos_sim(src_embedding, tgt_embeddings)[0]
            
            best_score_idx = np.argmax(cosine_scores.cpu().numpy())
            best_score = cosine_scores[best_score_idx].item()
            
            if best_score >= threshold:
                suggestions[src_col] = target_cols[best_score_idx]
            else:
                suggestions[src_col] = None

        return suggestions

    def analyze_column_content(self, col_values):
        """
        Analyzes a list of values from a single column to suggest transformers and ignore status.
        """
        result = {"transformers": [], "should_ignore": False}
        
        # 1. Filter out None and empty strings
        valid_values = [v for v in col_values if v is not None and str(v).strip() != ""]
        
        # If no data -> Ignore
        if not valid_values:
            result["should_ignore"] = True
            return result
            
        # 2. Analyze content patterns (Sample first 20)
        sample_str = [str(v) for v in valid_values[:20]] 
        
        # Check for Thai Date (e.g., 2566, 2567)
        # Regex: ปี พ.ศ. 25xx
        has_thai_year = any(re.search(r'25[5-9]\d', s) for s in sample_str)
        if has_thai_year:
            result["transformers"].append("BUDDHIST_TO_ISO")
            
        # Check for Whitespace issues (Leading/Trailing spaces)
        has_whitespace = any(s != s.strip() for s in sample_str)
        if has_whitespace:
            result["transformers"].append("TRIM")
            
        # Check for numeric only (Example: remove formatting from ID)
        # simple heuristic...
            
        return result

# Create Singleton Instance
ml_mapper = SmartMapper()