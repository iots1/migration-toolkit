import pandas as pd
from sentence_transformers import SentenceTransformer, util
import numpy as np
import re

class SmartMapper:
    """
    AI Service for semantic column matching using Sentence Transformers + HIS Dictionary.

    Pure Python service - no Streamlit dependencies.
    Model caching is handled via simple lazy loading (singleton pattern).
    """

    def __init__(self, model_name='paraphrase-multilingual-MiniLM-L12-v2'):
        self.model_name = model_name
        self._model = None  # Lazy-loaded model cache

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

    def load_model(self):
        """
        Load the model (lazy loading with caching).

        Model is loaded once and cached in self._model.
        Subsequent calls return the cached model.

        Returns:
            SentenceTransformer: Loaded model instance
        """
        if self._model is None:
            self._model = SentenceTransformer(self.model_name)
        return self._model

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

    def analyze_column_with_sample(self, source_col_name, target_col_name, sample_values):
        """
        Advanced column analysis using sample data to suggest transformers and validation.

        Args:
            source_col_name: Name of source column
            target_col_name: Tentatively mapped target column name
            sample_values: List of sample values (top 20 distinct values)

        Returns:
            {
                "confidence_score": float (0.0-1.0),
                "is_match": bool,
                "transformers": list,
                "should_ignore": bool,
                "reason": str
            }
        """
        result = {
            "confidence_score": 0.5,
            "is_match": True,
            "transformers": [],
            "should_ignore": False,
            "reason": "Standard mapping"
        }

        # 1. Filter and validate sample values
        valid_values = [v for v in sample_values if v is not None and str(v).strip() not in ["", "NaN", "None", "null"]]

        # If completely empty data -> suggest ignore
        if not valid_values:
            result["should_ignore"] = True
            result["confidence_score"] = 0.9
            result["reason"] = "All values are null/empty - suggested to ignore"
            return result

        # 2. Convert to strings for pattern analysis
        sample_str = [str(v).strip() for v in valid_values[:20]]

        # 3. Date/Time Analysis
        date_score = self._analyze_date_patterns(sample_str)
        if date_score["detected"]:
            result["transformers"].extend(date_score["transformers"])
            result["confidence_score"] = max(result["confidence_score"], date_score["confidence"])
            result["reason"] = date_score["reason"]

        # 4. String Quality Analysis
        string_score = self._analyze_string_quality(sample_str)
        if string_score["has_issues"]:
            result["transformers"].extend(string_score["transformers"])
            if "reason" not in result or result["reason"] == "Standard mapping":
                result["reason"] = string_score["reason"]

        # 5. Numeric/ID Analysis
        numeric_score = self._analyze_numeric_patterns(sample_str, source_col_name)
        if numeric_score["detected"]:
            result["transformers"].extend(numeric_score["transformers"])
            if numeric_score.get("should_ignore"):
                result["should_ignore"] = True
                result["reason"] = numeric_score["reason"]

        # 6. Healthcare-specific patterns (HN, VN, AN, CID)
        his_score = self._analyze_his_patterns(source_col_name, target_col_name, sample_str)
        if his_score["detected"]:
            result["confidence_score"] = max(result["confidence_score"], his_score["confidence"])
            result["is_match"] = his_score["is_match"]
            result["reason"] = his_score["reason"]

        # 7. Remove duplicates from transformers
        result["transformers"] = list(dict.fromkeys(result["transformers"]))

        return result

    def _analyze_date_patterns(self, sample_str):
        """Detect date patterns and suggest appropriate transformers."""
        result = {"detected": False, "transformers": [], "confidence": 0.5, "reason": ""}

        # Thai Buddhist Year pattern (25xx)
        thai_year_pattern = r'25[5-9]\d'
        thai_matches = sum(1 for s in sample_str if re.search(thai_year_pattern, s))

        if thai_matches > len(sample_str) * 0.5:  # More than 50% have Thai year
            result["detected"] = True
            result["transformers"].append("BUDDHIST_TO_ISO")
            result["confidence"] = min(0.9, thai_matches / len(sample_str))
            result["reason"] = f"Detected Thai Buddhist year (25xx) in {thai_matches}/{len(sample_str)} samples"
            return result

        # ISO Date pattern (YYYY-MM-DD or similar)
        iso_pattern = r'\d{4}[-/]\d{1,2}[-/]\d{1,2}'
        iso_matches = sum(1 for s in sample_str if re.search(iso_pattern, s))

        if iso_matches > len(sample_str) * 0.7:
            result["detected"] = True
            result["confidence"] = 0.8
            result["reason"] = f"Detected ISO date format in {iso_matches}/{len(sample_str)} samples"
            # No transformer needed - already in good format
            return result

        # Mixed date formats - need normalization
        date_indicators = sum(1 for s in sample_str if re.search(r'\d{2,4}[-/]\d{1,2}', s))
        if date_indicators > len(sample_str) * 0.5:
            result["detected"] = True
            result["transformers"].append("ENG_DATE_TO_ISO")
            result["confidence"] = 0.6
            result["reason"] = "Mixed date formats detected - normalization recommended"

        return result

    def _analyze_string_quality(self, sample_str):
        """Detect string quality issues."""
        result = {"has_issues": False, "transformers": [], "reason": ""}

        # Leading/Trailing whitespace
        whitespace_count = sum(1 for s in sample_str if s != s.strip())
        if whitespace_count > 0:
            result["has_issues"] = True
            result["transformers"].append("TRIM")
            result["reason"] = f"Leading/trailing whitespace in {whitespace_count}/{len(sample_str)} samples"

        # Multiple consecutive spaces
        multi_space_count = sum(1 for s in sample_str if re.search(r'\s{2,}', s))
        if multi_space_count > len(sample_str) * 0.3:
            result["has_issues"] = True
            result["transformers"].append("CLEAN_SPACES")
            if result["reason"]:
                result["reason"] += f"; multiple spaces in {multi_space_count} samples"

        # Empty/whitespace-only strings → suggest NULL
        empty_count = sum(1 for s in sample_str if not s.strip())
        if empty_count > 0:
            result["has_issues"] = True
            if "TRIM" not in result["transformers"]:
                result["transformers"].append("TRIM")
            result["transformers"].append("REPLACE_EMPTY_WITH_NULL")
            reason_part = f"empty/whitespace-only in {empty_count}/{len(sample_str)} samples"
            result["reason"] = f"{result['reason']}; {reason_part}" if result["reason"] else reason_part

        # JSON-like structures
        json_indicators = sum(1 for s in sample_str if s.startswith('{') or s.startswith('['))
        if json_indicators > len(sample_str) * 0.5:
            result["has_issues"] = True
            result["transformers"].append("PARSE_JSON")
            result["reason"] = "JSON/array structures detected"

        return result

    def _analyze_numeric_patterns(self, sample_str, col_name):
        """Analyze numeric patterns and ID formats."""
        result = {"detected": False, "transformers": [], "should_ignore": False, "reason": ""}

        # Float with .0 pattern (like "123.0" for IDs)
        float_zero_pattern = r'^\d+\.0+$'
        float_matches = sum(1 for s in sample_str if re.search(float_zero_pattern, s))

        if float_matches > len(sample_str) * 0.7:
            result["detected"] = True
            result["transformers"].append("FLOAT_TO_INT")
            result["reason"] = f"Detected float format IDs ({float_matches}/{len(sample_str)} samples) - converting to integer"
            return result

        # All zeros (might indicate missing data)
        all_zeros = all(str(s).strip() in ['0', '0.0', '00'] for s in sample_str)
        if all_zeros and not any(keyword in col_name.lower() for keyword in ['count', 'flag', 'status']):
            result["detected"] = True
            result["should_ignore"] = True
            result["reason"] = "All values are zero - likely missing/placeholder data"
            return result

        # Numeric-only values that should be treated as strings (like IDs with leading zeros)
        if any(keyword in col_name.lower() for keyword in ['id', 'code', 'number']):
            has_leading_zeros = any(s.startswith('0') and s.isdigit() and len(s) > 1 for s in sample_str)
            if has_leading_zeros:
                result["detected"] = True
                result["reason"] = "ID with leading zeros - should preserve as string"
                # No transformer - keep as string

        return result

    def _analyze_his_patterns(self, source_col, target_col, sample_str):
        """Analyze healthcare-specific patterns (HN, VN, AN, CID, etc.)."""
        result = {"detected": False, "is_match": True, "confidence": 0.5, "reason": ""}

        src_lower = source_col.lower()
        tgt_lower = target_col.lower() if target_col else ""

        # Hospital Number (HN) - typically 6-10 digits
        if any(kw in src_lower for kw in ['hn', 'hospital_number', 'mrn']):
            result["detected"] = True
            # Check if samples match expected HN format
            hn_pattern = r'^\d{6,10}$'
            matches = sum(1 for s in sample_str if re.match(hn_pattern, s))
            result["confidence"] = matches / len(sample_str)

            if any(kw in tgt_lower for kw in ['hn', 'hospital_number', 'mrn']):
                result["is_match"] = True
                result["reason"] = f"HN pattern matched ({matches}/{len(sample_str)} valid)"
            else:
                result["is_match"] = False
                result["reason"] = "Source is HN but target seems different"
            return result

        # National ID (CID) - 13 digits
        if any(kw in src_lower for kw in ['cid', 'national_id', 'citizen_id', 'id_card']):
            result["detected"] = True
            cid_pattern = r'^\d{13}$'
            matches = sum(1 for s in sample_str if re.match(cid_pattern, s))
            result["confidence"] = matches / len(sample_str)

            if any(kw in tgt_lower for kw in ['cid', 'national_id', 'citizen_id']):
                result["is_match"] = True
                result["reason"] = f"CID pattern matched ({matches}/{len(sample_str)} valid 13-digit)"
            return result

        # Visit Number (VN) or Admission Number (AN)
        if any(kw in src_lower for kw in ['vn', 'visit', 'an', 'admission']):
            result["detected"] = True
            result["confidence"] = 0.7
            result["reason"] = "Healthcare visit/admission identifier detected"
            return result

        return result

# Create Singleton Instance
ml_mapper = SmartMapper()