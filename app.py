import pandas as pd
import sys
import os

class PivotTransformer:
    def __init__(self):
        self.data = None
    
    def load_csv(self, filepath, delimiter=';'):
        """Load CSV file with specified delimiter"""
        try:
            self.data = pd.read_csv(filepath, delimiter=delimiter)
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
            # Create pivot table
            pivoted = self.data.pivot(index=designator_col, columns=project_col, values=value_col)
            
            # Reset index to make designator a column
            pivoted = pivoted.reset_index()
            
            # Fill NaN values with 0 if any
            pivoted = pivoted.fillna(0).astype(int)
            
            print("✓ Successfully converted to column format")
            return pivoted
        
        except Exception as e:
            print(f"✗ Error during row-to-column transformation: {e}")
            return None
    
    def column_to_row_format(self, designator_col='designator'):
        """Convert from column format to row format (melt)"""
        if self.data is None:
            print("✗ No data loaded. Please load a CSV file first.")
            return None
        
        try:
            # Get all columns except the designator column
            project_columns = [col for col in self.data.columns if col != designator_col]
            
            # Melt the dataframe
            melted = pd.melt(
                self.data, 
                id_vars=[designator_col], 
                value_vars=project_columns,
                var_name='project', 
                value_name='volume'
            )
            
            # Remove rows with zero volume (optional)
            melted = melted[melted['volume'] != 0]
            
            # Sort by project and designator for better readability
            melted = melted.sort_values(['project', designator_col]).reset_index(drop=True)
            
            print("✓ Successfully converted to row format")
            return melted
        
        except Exception as e:
            print(f"✗ Error during column-to-row transformation: {e}")
            return None
    
    def save_csv(self, data, output_path, delimiter=';'):
        """Save dataframe to CSV with specified delimiter"""
        if data is None:
            print("✗ No data to save")
            return False
        
        try:
            data.to_csv(output_path, delimiter=delimiter, index=False)
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