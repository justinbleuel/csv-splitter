import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings

class CSVMerger:
    def __init__(self, encoding='utf-8'):
        self.files = {}  # Dictionary to store file info and dataframes
        self.encoding = encoding
        self.encodings_to_try = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
        self.merge_type = 'vertical'  # vertical or horizontal
        self.total_rows = 0
        self.total_size_mb = 0
        
    def add_file(self, file_path, file_id=None):
        """Add a CSV file to the merge queue"""
        if file_id is None:
            file_id = f"file_{len(self.files) + 1}"
            
        # Try different encodings
        df = None
        encoding_used = None
        
        for encoding in self.encodings_to_try:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                encoding_used = encoding
                break
            except UnicodeDecodeError:
                continue
                
        if df is None:
            raise UnicodeDecodeError(f"Could not decode file with any of these encodings: {', '.join(self.encodings_to_try)}")
        
        file_info = {
            'path': file_path,
            'name': os.path.basename(file_path),
            'df': df,
            'encoding': encoding_used,
            'rows': len(df),
            'columns': list(df.columns),
            'size_mb': os.path.getsize(file_path) / (1024 * 1024),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
        }
        
        self.files[file_id] = file_info
        self.total_rows += file_info['rows']
        self.total_size_mb += file_info['size_mb']
        
        return file_id, file_info
    
    def analyze_files(self):
        """Analyze all added files for merging compatibility"""
        if not self.files:
            return {'error': 'No files added for merging'}
            
        # Collect all unique columns
        all_columns = set()
        common_columns = None
        
        for file_id, file_info in self.files.items():
            columns_set = set(file_info['columns'])
            all_columns.update(columns_set)
            
            if common_columns is None:
                common_columns = columns_set
            else:
                common_columns = common_columns.intersection(columns_set)
        
        # Analyze column compatibility
        column_analysis = {
            'total_unique_columns': len(all_columns),
            'common_columns': list(common_columns),
            'all_columns': list(all_columns),
            'column_coverage': {}  # Which files have which columns
        }
        
        # Check which files have which columns
        for col in all_columns:
            column_analysis['column_coverage'][col] = []
            for file_id, file_info in self.files.items():
                if col in file_info['columns']:
                    column_analysis['column_coverage'][col].append(file_id)
        
        # Analyze data types for common columns
        dtype_conflicts = {}
        for col in common_columns:
            dtypes = set()
            for file_id, file_info in self.files.items():
                if col in file_info['dtypes']:
                    dtypes.add(file_info['dtypes'][col])
            
            if len(dtypes) > 1:
                dtype_conflicts[col] = list(dtypes)
        
        analysis = {
            'file_count': len(self.files),
            'total_rows': self.total_rows,
            'total_size_mb': self.total_size_mb,
            'files': {fid: {
                'name': info['name'],
                'rows': info['rows'],
                'columns': len(info['columns']),
                'size_mb': info['size_mb']
            } for fid, info in self.files.items()},
            'column_analysis': column_analysis,
            'dtype_conflicts': dtype_conflicts,
            'merge_feasibility': {
                'vertical': len(common_columns) > 0 or len(self.files) == 1,
                'horizontal': len(common_columns) > 0  # Need at least one common column for joining
            }
        }
        
        return analysis
    
    def preview_merge(self, merge_type='vertical', options=None):
        """Generate a preview of the merge operation"""
        if not self.files:
            return {'error': 'No files added for merging'}
            
        options = options or {}
        preview_rows = options.get('preview_rows', 10)
        
        if merge_type == 'vertical':
            return self._preview_vertical_merge(options, preview_rows)
        elif merge_type == 'horizontal':
            return self._preview_horizontal_merge(options, preview_rows)
        else:
            return {'error': f'Unknown merge type: {merge_type}'}
    
    def _preview_vertical_merge(self, options, preview_rows):
        """Preview vertical (append) merge"""
        include_source = options.get('include_source', False)
        columns_mode = options.get('columns_mode', 'union')  # union or intersection
        
        # Get sample rows from each file
        preview_dfs = []
        
        for file_id, file_info in self.files.items():
            df_sample = file_info['df'].head(min(5, preview_rows)).copy()
            
            if include_source:
                df_sample['_source_file'] = file_info['name']
                
            preview_dfs.append(df_sample)
        
        # Determine columns based on mode
        if columns_mode == 'intersection':
            # Only common columns
            common_cols = set(self.files[list(self.files.keys())[0]]['columns'])
            for file_info in self.files.values():
                common_cols = common_cols.intersection(set(file_info['columns']))
            
            if include_source:
                common_cols.add('_source_file')
                
            preview_dfs = [df[list(common_cols)] for df in preview_dfs]
        
        # Concatenate preview
        try:
            preview_df = pd.concat(preview_dfs, ignore_index=True, sort=False)
            preview_data = preview_df.head(preview_rows).to_dict('records')
            
            # Calculate statistics
            if columns_mode == 'union':
                total_columns = len(preview_df.columns)
            else:
                total_columns = len(common_cols)
                
            stats = {
                'total_rows_after_merge': self.total_rows,
                'total_columns': total_columns,
                'columns_mode': columns_mode,
                'include_source': include_source
            }
            
            return {
                'preview_data': preview_data,
                'stats': stats,
                'columns': list(preview_df.columns)
            }
            
        except Exception as e:
            return {'error': f'Preview generation failed: {str(e)}'}
    
    def _preview_horizontal_merge(self, options, preview_rows):
        """Preview horizontal (join) merge"""
        join_columns = options.get('join_columns', [])
        join_type = options.get('join_type', 'inner')  # inner, left, right, outer
        
        if not join_columns:
            return {'error': 'Join columns must be specified for horizontal merge'}
        
        if len(self.files) != 2:
            return {'error': 'Horizontal merge currently supports exactly 2 files'}
        
        file_ids = list(self.files.keys())
        df1 = self.files[file_ids[0]]['df'].head(preview_rows)
        df2 = self.files[file_ids[1]]['df'].head(preview_rows)
        
        try:
            # Perform the join
            preview_df = pd.merge(
                df1, df2,
                on=join_columns,
                how=join_type,
                suffixes=('_file1', '_file2')
            )
            
            preview_data = preview_df.head(preview_rows).to_dict('records')
            
            stats = {
                'join_type': join_type,
                'join_columns': join_columns,
                'estimated_rows': 'varies based on join matches',
                'total_columns': len(preview_df.columns)
            }
            
            return {
                'preview_data': preview_data,
                'stats': stats,
                'columns': list(preview_df.columns)
            }
            
        except Exception as e:
            return {'error': f'Preview generation failed: {str(e)}'}
    
    def execute_merge(self, merge_type='vertical', options=None, output_path=None):
        """Execute the merge operation"""
        if not self.files:
            return {'error': 'No files added for merging'}
            
        options = options or {}
        self.merge_type = merge_type
        
        try:
            if merge_type == 'vertical':
                merged_df = self._execute_vertical_merge(options)
            elif merge_type == 'horizontal':
                merged_df = self._execute_horizontal_merge(options)
            else:
                return {'error': f'Unknown merge type: {merge_type}'}
            
            # Save if output path provided
            if output_path:
                merged_df.to_csv(output_path, index=False, encoding=self.encoding)
            
            return {
                'success': True,
                'merged_df': merged_df,
                'rows': len(merged_df),
                'columns': len(merged_df.columns),
                'output_path': output_path
            }
            
        except Exception as e:
            return {'error': f'Merge execution failed: {str(e)}'}
    
    def _execute_vertical_merge(self, options):
        """Execute vertical (append) merge"""
        include_source = options.get('include_source', False)
        columns_mode = options.get('columns_mode', 'union')
        
        dfs_to_merge = []
        
        for file_id, file_info in self.files.items():
            df = file_info['df'].copy()
            
            if include_source:
                df['_source_file'] = file_info['name']
                
            dfs_to_merge.append(df)
        
        # Handle column mode
        if columns_mode == 'intersection':
            # Only keep common columns
            common_cols = set(self.files[list(self.files.keys())[0]]['columns'])
            for file_info in self.files.values():
                common_cols = common_cols.intersection(set(file_info['columns']))
            
            if include_source:
                common_cols.add('_source_file')
                
            dfs_to_merge = [df[list(common_cols)] for df in dfs_to_merge]
        
        # Perform the merge
        merged_df = pd.concat(dfs_to_merge, ignore_index=True, sort=False)
        
        return merged_df
    
    def _execute_horizontal_merge(self, options):
        """Execute horizontal (join) merge"""
        join_columns = options.get('join_columns', [])
        join_type = options.get('join_type', 'inner')
        
        if not join_columns:
            raise ValueError('Join columns must be specified for horizontal merge')
        
        if len(self.files) != 2:
            raise ValueError('Horizontal merge currently supports exactly 2 files')
        
        file_ids = list(self.files.keys())
        df1 = self.files[file_ids[0]]['df']
        df2 = self.files[file_ids[1]]['df']
        
        # Perform the join
        merged_df = pd.merge(
            df1, df2,
            on=join_columns,
            how=join_type,
            suffixes=('_file1', '_file2')
        )
        
        return merged_df
    
    def save_merge_report(self, merge_result, report_path):
        """Save a detailed merge report"""
        report = {
            'merge_timestamp': datetime.now().isoformat(),
            'merge_type': self.merge_type,
            'files_merged': [
                {
                    'name': info['name'],
                    'rows': info['rows'],
                    'columns': len(info['columns']),
                    'size_mb': info['size_mb']
                }
                for info in self.files.values()
            ],
            'result': {
                'total_rows': merge_result.get('rows', 0),
                'total_columns': merge_result.get('columns', 0),
                'output_file': merge_result.get('output_path', '')
            }
        }
        
        with open(report_path, 'w') as f:
            import json
            json.dump(report, f, indent=2)
        
        return report_path