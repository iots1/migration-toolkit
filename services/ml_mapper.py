import streamlit as st
import pandas as pd
from sentence_transformers import SentenceTransformer, util
import numpy as np
import re

class SmartMapper:
    """
    AI Service for semantic column matching using Sentence Transformers.
    """
    
    def __init__(self, model_name='paraphrase-multilingual-MiniLM-L12-v2'):
        # paraphrase-multilingual-MiniLM-L12-v2: รองรับหลายภาษา (ไทย/อังกฤษ) และทำงานเร็ว
        self.model_name = model_name

    @st.cache_resource
    def load_model(_self):
        """Loads the model and caches it to avoid reloading on every interaction."""
        return SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')

    def suggest_mapping(self, source_cols, target_cols, threshold=0.4):
        """
        Matches source columns to target columns based on semantic similarity.
        """
        if not source_cols or not target_cols:
            return {}

        model = self.load_model()

        # 1. Compute Embeddings (Convert text to vector)
        src_embeddings = model.encode(source_cols, convert_to_tensor=True)
        tgt_embeddings = model.encode(target_cols, convert_to_tensor=True)

        # 2. Compute Cosine Similarity Matrix
        cosine_scores = util.cos_sim(src_embeddings, tgt_embeddings)

        suggestions = {}
        
        # 3. Find best match for each source column
        for i, src_col in enumerate(source_cols):
            best_score_idx = np.argmax(cosine_scores[i].cpu().numpy())
            best_score = cosine_scores[i][best_score_idx].item()
            
            if best_score >= threshold:
                suggested_col = target_cols[best_score_idx]
                suggestions[src_col] = suggested_col
            else:
                suggestions[src_col] = None

        return suggestions

    def analyze_column_content(self, col_values):
        """
        Analyzes a list of values from a single column to suggest transformers and ignore status.
        
        Returns:
            dict: { "transformers": list, "should_ignore": bool }
        """
        result = {"transformers": [], "should_ignore": False}
        
        # 1. Filter out None and empty strings
        valid_values = [v for v in col_values if v is not None and str(v).strip() != ""]
        
        # If no data -> Ignore
        if not valid_values:
            result["should_ignore"] = True
            return result
            
        # 2. Analyze content patterns
        sample_str = [str(v) for v in valid_values[:20]] # Check first 20 valid samples
        
        # Check for Thai Date (e.g., 2566, 2567)
        has_thai_year = any(re.search(r'25[5-9]\d', s) for s in sample_str)
        if has_thai_year:
            result["transformers"].append("BUDDHIST_TO_ISO")
            
        # Check for Whitespace issues
        has_whitespace = any(s != s.strip() for s in sample_str)
        if has_whitespace:
            result["transformers"].append("TRIM")
            
        return result

# Create Singleton Instance
ml_mapper = SmartMapper()