import pandas as pd
import numpy as np
from datetime import datetime
import os

class DuplicateRemover:
    def __init__(self, file_path=None, encoding='utf-8'):
        self.file_path = file_path
        self.encoding = encoding
        self.df = None
        self.original_row_count = 0
        self.encodings_to_try = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
        
    def load_file(self, file_path=None):
        """Load CSV file with automatic encoding detection"""
        if file_path:
            self.file_path = file_path
            
        for encoding in self.encodings_to_try:
            try:
                self.df = pd.read_csv(self.file_path, encoding=encoding)
                self.encoding = encoding
                self.original_row_count = len(self.df)
                return True
            except UnicodeDecodeError:
                continue
        
        raise UnicodeDecodeError(f"Could not decode file with any of these encodings: {', '.join(self.encodings_to_try)}")
    
    def analyze_file(self):
        """Returns columns, row count, data types, and sample data"""
        if self.df is None:
            self.load_file()
            
        analysis = {
            'columns': list(self.df.columns),
            'row_count': len(self.df),
            'column_types': {col: str(dtype) for col, dtype in self.df.dtypes.items()},
            'sample_data': self.df.head(5).to_dict('records'),
            'file_size_mb': os.path.getsize(self.file_path) / (1024 * 1024) if self.file_path else 0
        }
        
        return analysis
    
    def find_duplicates(self, check_columns):
        """Returns count and preview of duplicates"""
        if self.df is None:
            self.load_file()
            
        # Find all rows that have duplicates (including the first occurrence)
        duplicated_mask = self.df.duplicated(subset=check_columns, keep=False)
        duplicate_rows = self.df[duplicated_mask]
        
        # Count unique duplicate groups
        duplicate_groups = duplicate_rows.groupby(check_columns).size()
        
        result = {
            'total_duplicate_rows': len(duplicate_rows),
            'duplicate_groups': len(duplicate_groups),
            'rows_to_remove': len(duplicate_rows) - len(duplicate_groups),  # Keeping one from each group
            'preview': []
        }
        
        # Get preview of first 5 duplicate groups
        preview_count = 0
        for cols, group in duplicate_rows.groupby(check_columns):
            if preview_count >= 5:
                break
            
            group_data = {
                'duplicate_values': {col: group.iloc[0][col] for col in check_columns},
                'occurrences': len(group),
                'sample_rows': group.head(3).to_dict('records')
            }
            result['preview'].append(group_data)
            preview_count += 1
            
        return result
    
    def remove_duplicates(self, check_columns, keep_strategy='first', strategy_column=None):
        """Apply deduplication logic and return cleaned dataframe"""
        if self.df is None:
            self.load_file()
            
        # Create a copy to work with
        df_clean = self.df.copy()
        
        if keep_strategy in ['first', 'last']:
            # Simple keep first or last
            df_clean = df_clean.drop_duplicates(subset=check_columns, keep=keep_strategy)
        
        elif keep_strategy == 'not_empty' and strategy_column:
            # Sort by non-null values in strategy column (non-null first)
            df_clean['_has_value'] = ~df_clean[strategy_column].isna()
            df_clean = df_clean.sort_values(['_has_value'] + check_columns, 
                                           ascending=[False] + [True] * len(check_columns))
            df_clean = df_clean.drop_duplicates(subset=check_columns, keep='first')
            df_clean = df_clean.drop('_has_value', axis=1)
            
        elif keep_strategy == 'max_value' and strategy_column:
            # Keep row with highest value in strategy column
            # Convert to numeric, coercing errors to NaN
            df_clean['_numeric_value'] = pd.to_numeric(df_clean[strategy_column], errors='coerce')
            df_clean = df_clean.sort_values(['_numeric_value'] + check_columns, 
                                           ascending=[False] + [True] * len(check_columns))
            df_clean = df_clean.drop_duplicates(subset=check_columns, keep='first')
            df_clean = df_clean.drop('_numeric_value', axis=1)
            
        elif keep_strategy == 'most_recent' and strategy_column:
            # Keep row with most recent date
            # Try to parse dates
            df_clean['_parsed_date'] = pd.to_datetime(df_clean[strategy_column], errors='coerce')
            df_clean = df_clean.sort_values(['_parsed_date'] + check_columns, 
                                           ascending=[False] + [True] * len(check_columns))
            df_clean = df_clean.drop_duplicates(subset=check_columns, keep='first')
            df_clean = df_clean.drop('_parsed_date', axis=1)
            
        # Calculate statistics
        rows_removed = self.original_row_count - len(df_clean)
        
        return {
            'cleaned_df': df_clean,
            'original_rows': self.original_row_count,
            'cleaned_rows': len(df_clean),
            'rows_removed': rows_removed,
            'removal_percentage': (rows_removed / self.original_row_count) * 100 if self.original_row_count > 0 else 0
        }
    
    def save_cleaned_file(self, cleaned_df, output_path):
        """Save the cleaned dataframe to a CSV file"""
        cleaned_df.to_csv(output_path, index=False, encoding=self.encoding)
        return output_path