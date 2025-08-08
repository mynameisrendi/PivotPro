import pandas as pd
import sys
import os

class PivotTransformer:
    def __init__(self):
        self.data = None
        self.last_loaded_file = None  # Track the last loaded file name
    
    def load_csv(self, filepath, delimiter=';'):
        """Load CSV file with specified delimiter"""
        try:
            # Use 'sep' parameter instead of 'delimiter' for pandas
            self.data = pd.read_csv(filepath, sep=delimiter)
            # Store the filename (without path and extension) for default naming
            self.last_loaded_file = os.path.splitext(os.path.basename(filepath))[0]
            print(f"✓ Successfully loaded: {filepath}")
            print(f"  Shape: {self.data.shape}")
            print(f"  Columns: {list(self.data.columns)}")
            return True
        except Exception as e:
            print(f"✗ Error loading {filepath}: {e}")
            return False
    
    def row_to_column_format(self, project_col='project', designator_col='designator', value_col='volume'):
        """Convert from row format to column format (pivot)"""
        if self.data is None:
            print("✗ No data loaded. Please load a CSV file first.")
            return None
        
        try:
            # Print data info for debugging
            print(f"  Columns in data: {list(self.data.columns)}")
            print(f"  Data types: {self.data.dtypes.to_dict()}")
            print(f"  Sample data:")
            print(self.data.head(3).to_string(index=False))
            
            # Check if required columns exist
            if project_col not in self.data.columns:
                print(f"✗ Column '{project_col}' not found. Available columns: {list(self.data.columns)}")
                return None
            if designator_col not in self.data.columns:
                print(f"✗ Column '{designator_col}' not found. Available columns: {list(self.data.columns)}")
                return None
            if value_col not in self.data.columns:
                print(f"✗ Column '{value_col}' not found. Available columns: {list(self.data.columns)}")
                return None
            
            # Clean the data - strip whitespace and handle data types
            data_clean = self.data.copy()
            data_clean[project_col] = data_clean[project_col].astype(str).str.strip()
            data_clean[designator_col] = data_clean[designator_col].astype(str).str.strip()
            
            # Convert volume to numeric, handling any non-numeric values
            data_clean[value_col] = pd.to_numeric(data_clean[value_col], errors='coerce')
            
            # Check for any NaN values after conversion
            if data_clean[value_col].isna().any():
                print("⚠ Warning: Some volume values couldn't be converted to numbers. Setting them to 0.")
                data_clean[value_col] = data_clean[value_col].fillna(0)
            
            # Create pivot table
            pivoted = data_clean.pivot(index=designator_col, columns=project_col, values=value_col)
            
            # Reset index to make designator a column
            pivoted = pivoted.reset_index()
            
            # Fill NaN values with 0 if any
            pivoted = pivoted.fillna(0)
            
            # Convert to int only if all values are whole numbers
            try:
                # Check if all values can be safely converted to int
                numeric_columns = pivoted.select_dtypes(include=['float64', 'int64']).columns
                for col in numeric_columns:
                    if col != designator_col:
                        pivoted[col] = pivoted[col].astype(int)
            except:
                print("⚠ Warning: Keeping values as float (some values have decimals)")
            
            print("✓ Successfully converted to column format")
            return pivoted
        
        except Exception as e:
            print(f"✗ Error during row-to-column transformation: {e}")
            print("Debug info:")
            if hasattr(self, 'data') and self.data is not None:
                print(f"  Data shape: {self.data.shape}")
                print(f"  Unique values in each column:")
                for col in self.data.columns:
                    unique_count = self.data[col].nunique()
                    print(f"    {col}: {unique_count} unique values")
                    if unique_count <= 10:
                        print(f"      Values: {list(self.data[col].unique())}")
            return None
    
    def column_to_row_format(self, designator_col='designator'):
        """Convert from column format to row format (melt)"""
        if self.data is None:
            print("✗ No data loaded. Please load a CSV file first.")
            return None
        
        try:
            # Print data info for debugging
            print(f"  Columns in data: {list(self.data.columns)}")
            print(f"  Data shape: {self.data.shape}")
            
            # Check if designator column exists
            if designator_col not in self.data.columns:
                print(f"✗ Column '{designator_col}' not found. Available columns: {list(self.data.columns)}")
                return None
            
            # Get all columns except the designator column
            project_columns = [col for col in self.data.columns if col != designator_col]
            
            if not project_columns:
                print("✗ No project columns found for melting.")
                return None
            
            print(f"  Found {len(project_columns)} project columns: {project_columns}")
            
            # Clean the data
            data_clean = self.data.copy()
            
            # Melt the dataframe
            melted = pd.melt(
                data_clean, 
                id_vars=[designator_col], 
                value_vars=project_columns,
                var_name='project', 
                value_name='volume'
            )
            
            # Clean up the melted data
            melted['project'] = melted['project'].astype(str).str.strip()
            melted[designator_col] = melted[designator_col].astype(str).str.strip()
            
            # Convert volume to numeric
            melted['volume'] = pd.to_numeric(melted['volume'], errors='coerce').fillna(0)
            
            # Remove rows with zero volume (optional)
            melted = melted[melted['volume'] != 0]
            
            # Sort by project and designator for better readability
            melted = melted.sort_values(['project', designator_col]).reset_index(drop=True)
            
            print("✓ Successfully converted to row format")
            return melted
        
        except Exception as e:
            print(f"✗ Error during column-to-row transformation: {e}")
            print("Debug info:")
            if hasattr(self, 'data') and self.data is not None:
                print(f"  Data shape: {self.data.shape}")
                print(f"  Data columns: {list(self.data.columns)}")
                print(f"  Data types: {self.data.dtypes.to_dict()}")
            return None
    
    def save_csv(self, data, output_path, delimiter=';'):
        """Save dataframe to CSV with specified delimiter"""
        if data is None:
            print("✗ No data to save")
            return False
        
        try:
            # Handle empty or directory-only paths
            output_path = output_path.strip()
            
            # If path ends with / or is empty, it's a directory path
            if output_path.endswith('/') or output_path.endswith('\\') or output_path == '':
                # Generate default filename
                if self.last_loaded_file:
                    default_filename = f"PivotPro_{self.last_loaded_file}.csv"
                else:
                    default_filename = "PivotPro_output.csv"
                
                if output_path:
                    # Combine directory path with default filename
                    output_path = os.path.join(output_path, default_filename)
                else:
                    # Use current directory with default filename
                    output_path = default_filename
            else:
                # Check if it's a directory path without trailing slash
                if os.path.isdir(output_path):
                    if self.last_loaded_file:
                        default_filename = f"PivotPro_{self.last_loaded_file}.csv"
                    else:
                        default_filename = "PivotPro_output.csv"
                    output_path = os.path.join(output_path, default_filename)
                else:
                    # Add .csv extension if not provided
                    if not output_path.lower().endswith('.csv'):
                        output_path += '.csv'
            
            # Use 'sep' parameter instead of 'delimiter' for pandas
            data.to_csv(output_path, sep=delimiter, index=False)
            print(f"✓ Successfully saved: {output_path}")
            return True
        except Exception as e:
            print(f"✗ Error saving {output_path}: {e}")
            return False
    
    def preview_data(self, data=None, rows=5):
        """Preview the data"""
        if data is None:
            data = self.data
        
        if data is None:
            print("✗ No data to preview")
            return
        
        print(f"\nData Preview (first {rows} rows):")
        print("=" * 50)
        print(data.head(rows).to_string(index=False))
        print("=" * 50)

def main():
    transformer = PivotTransformer()
    
    while True:
        print("\n" + "="*60)
        print("           CSV PIVOT TRANSFORMER")
        print("="*60)
        print("1. Load CSV file")
        print("2. Convert Row → Column format (pivot)")
        print("3. Convert Column → Row format (melt)")
        print("4. Preview current data")
        print("5. Save current data")
        print("6. Exit")
        print("="*60)
        
        choice = input("\nSelect option (1-6): ").strip()
        
        if choice == '1':
            filepath = input("Enter CSV file path: ").strip()
            delimiter = input("Enter delimiter (default ';'): ").strip() or ';'
            transformer.load_csv(filepath, delimiter)
            
        elif choice == '2':
            result = transformer.row_to_column_format()
            if result is not None:
                transformer.data = result
                transformer.preview_data()
                
        elif choice == '3':
            result = transformer.column_to_row_format()
            if result is not None:
                transformer.data = result
                transformer.preview_data()
                
        elif choice == '4':
            transformer.preview_data()
            
        elif choice == '5':
            if transformer.data is not None:
                # Suggest default path with filename
                if transformer.last_loaded_file:
                    suggested_name = f"PivotPro_{transformer.last_loaded_file}.csv"
                    print(f"Suggested filename: {suggested_name}")
                    output_path = input(f"Enter output file path (press Enter for '{suggested_name}'): ").strip()
                    if not output_path:
                        output_path = suggested_name
                else:
                    output_path = input("Enter output file path: ").strip()
                
                delimiter = input("Enter delimiter (default ';'): ").strip() or ';'
                transformer.save_csv(transformer.data, output_path, delimiter)
            else:
                print("✗ No data to save. Please load and transform data first.")
                
        elif choice == '6':
            print("Goodbye!")
            break
            
        else:
            print("✗ Invalid option. Please select 1-6.")

def create_multi_project_demo():
    """Create demo data with multiple projects to test scalability"""
    print("Creating demo data with 5 projects...")
    
    # Sample data with 5 different projects
    demo_data = [
        ['MD2025-PTK-FDL-PT3-KOMPLEK PERMATA SARI', 'SC-OF-SM-24', 2],
        ['MD2025-PTK-FDL-PT3-KOMPLEK PERMATA SARI', 'OS-SM-1', 14],
        ['MD2025-PTK-FDL-PT3-KOMPLEK PERMATA SARI', 'PC-APC/UPC-657-A1', 13],
        ['MD2025-PTK-FDU-PT3-Gang Arafah', 'SC-OF-SM-24', 1],
        ['MD2025-PTK-FDU-PT3-Gang Arafah', 'OS-SM-1', 13],
        ['MD2025-PTK-FDU-PT3-Gang Arafah', 'PC-APC/UPC-657-A1', 13],
        ['MD2025-PTK-BTN-PT4-Jalan Mawar', 'SC-OF-SM-24', 3],
        ['MD2025-PTK-BTN-PT4-Jalan Mawar', 'OS-SM-1', 20],
        ['MD2025-PTK-BTN-PT4-Jalan Mawar', 'PC-APC/UPC-657-A1', 15],
        ['MD2025-PTK-BTN-PT4-Jalan Mawar', 'AC-OF-SM-ADSS-12D', 450],
        ['MD2025-PTK-SLT-PT5-Kompleks Indah', 'SC-OF-SM-24', 4],
        ['MD2025-PTK-SLT-PT5-Kompleks Indah', 'OS-SM-1', 18],
        ['MD2025-PTK-SLT-PT5-Kompleks Indah', 'PC-APC/UPC-657-A1', 12],
        ['MD2025-PTK-SLT-PT5-Kompleks Indah', 'AC-OF-SM-ADSS-12D', 720],
        ['MD2025-PTK-KPG-PT6-Gang Melati', 'SC-OF-SM-24', 1],
        ['MD2025-PTK-KPG-PT6-Gang Melati', 'OS-SM-1', 8],
        ['MD2025-PTK-KPG-PT6-Gang Melati', 'PC-APC/UPC-657-A1', 10],
        ['MD2025-PTK-KPG-PT6-Gang Melati', 'AC-OF-SM-ADSS-12D', 280]
    ]
    
    # Create DataFrame
    df = pd.DataFrame(demo_data, columns=['project', 'designator', 'volume'])
    
    # Save demo file
    df.to_csv('demo_multi_projects_row.csv', sep=';', index=False)
    print("✓ Created demo_multi_projects_row.csv with 5 projects")
    
    return df

def demo_with_multiple_projects():
    """Demo function showing scalability with multiple projects"""
    print("DEMO: Testing with Multiple Projects")
    print("="*50)
    
    # Create demo data
    demo_df = create_multi_project_demo()
    
    transformer = PivotTransformer()
    
    # Load the demo data
    transformer.data = demo_df
    print(f"\nLoaded demo data:")
    print(f"  Projects: {demo_df['project'].nunique()}")
    print(f"  Unique designators: {demo_df['designator'].nunique()}")
    print(f"  Total rows: {len(demo_df)}")
    
    # Convert to column format
    print(f"\nConverting to column format...")
    result_column = transformer.row_to_column_format()
    if result_column is not None:
        print(f"Result: {result_column.shape[0]} designators × {result_column.shape[1]-1} projects")
        transformer.preview_data(result_column)
        transformer.save_csv(result_column, 'demo_multi_projects_column.csv')
    
    # Convert back to row format
    print(f"\nConverting back to row format...")
    transformer.data = result_column
    result_row = transformer.column_to_row_format()
    if result_row is not None:
        print(f"Result: {len(result_row)} rows")
        transformer.preview_data(result_row)
        transformer.save_csv(result_row, 'demo_multi_projects_back_to_row.csv')

def demo_with_your_files():
    """Demo function using your specific files"""
    print("DEMO: Converting your files")
    print("="*40)
    
    transformer = PivotTransformer()
    
    # Demo 1: Row to Column
    print("\n1. Converting pivot_row.csv to column format...")
    if transformer.load_csv('pivot_row.csv'):
        result = transformer.row_to_column_format()
        if result is not None:
            transformer.preview_data(result)
            transformer.save_csv(result, 'output_column_format.csv')
    
    # Demo 2: Column to Row
    print("\n2. Converting pivot_column.csv to row format...")
    if transformer.load_csv('pivot_column.csv'):
        result = transformer.column_to_row_format()
        if result is not None:
            transformer.preview_data(result)
            transformer.save_csv(result, 'output_row_format.csv')

if __name__ == "__main__":
    # Uncomment one of the lines below to run different demos:
    # demo_with_your_files()          # Demo with your 2 projects
    # demo_with_multiple_projects()   # Demo with 5 projects
    
    # Run the interactive application
    main()