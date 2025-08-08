import streamlit as st
import pandas as pd
import io
from datetime import datetime

# Configure the page
st.set_page_config(
    page_title="PivotPro - CSV Transformer",
    page_icon="🔄",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        text-align: center;
        color: #1e3a8a;
        margin-bottom: 2rem;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d1fae5;
        border: 1px solid #059669;
        color: #059669;
    }
    .error-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #fee2e2;
        border: 1px solid #dc2626;
        color: #dc2626;
    }
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #dbeafe;
        border: 1px solid #2563eb;
        color: #1d4ed8;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'data' not in st.session_state:
    st.session_state.data = None
if 'original_filename' not in st.session_state:
    st.session_state.original_filename = None
if 'transformation_type' not in st.session_state:
    st.session_state.transformation_type = None

class StreamlitPivotTransformer:
    @staticmethod
    def row_to_column_format(data, project_col='project', designator_col='designator', value_col='volume'):
        """Convert from row format to column format (pivot)"""
        try:
            # Clean the data
            data_clean = data.copy()
            data_clean[project_col] = data_clean[project_col].astype(str).str.strip()
            data_clean[designator_col] = data_clean[designator_col].astype(str).str.strip()
            data_clean[value_col] = pd.to_numeric(data_clean[value_col], errors='coerce').fillna(0)
            
            # Create pivot table
            pivoted = data_clean.pivot(index=designator_col, columns=project_col, values=value_col)
            pivoted = pivoted.reset_index().fillna(0)
            
            # Convert to int if possible
            numeric_columns = pivoted.select_dtypes(include=['float64', 'int64']).columns
            for col in numeric_columns:
                if col != designator_col:
                    pivoted[col] = pivoted[col].astype(int)
            
            return pivoted, None
        except Exception as e:
            return None, str(e)
    
    @staticmethod
    def column_to_row_format(data, designator_col='designator'):
        """Convert from column format to row format (melt)"""
        try:
            # Get project columns
            project_columns = [col for col in data.columns if col != designator_col]
            
            # Melt the dataframe
            melted = pd.melt(
                data,
                id_vars=[designator_col],
                value_vars=project_columns,
                var_name='project',
                value_name='volume'
            )
            
            # Clean and filter
            melted['project'] = melted['project'].astype(str).str.strip()
            melted[designator_col] = melted[designator_col].astype(str).str.strip()
            melted['volume'] = pd.to_numeric(melted['volume'], errors='coerce').fillna(0)
            melted = melted[melted['volume'] != 0]
            melted = melted.sort_values(['project', designator_col]).reset_index(drop=True)
            
            return melted, None
        except Exception as e:
            return None, str(e)

# Main App
def main():
    # Header
    st.markdown("<h1 class='main-header'>🔄 PivotPro - CSV Transformer</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #6b7280; font-size: 1.2rem;'>Transform your CSV data between row and column formats with ease</p>", unsafe_allow_html=True)
    
    # Create two columns for layout
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📁 Upload Your CSV File")
        
        # File uploader
        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type=['csv'],
            help="Upload your CSV file with semicolon (;) separator"
        )
        
        # Delimiter selection
        delimiter = st.selectbox(
            "Select delimiter",
            options=[';', ',', '\t', '|'],
            index=0,
            help="Choose the delimiter used in your CSV file"
        )
        
        if uploaded_file is not None:
            try:
                # Read the uploaded file
                data = pd.read_csv(uploaded_file, sep=delimiter)
                st.session_state.data = data
                st.session_state.original_filename = uploaded_file.name.split('.')[0]
                
                st.markdown("<div class='success-box'>✅ File uploaded successfully!</div>", unsafe_allow_html=True)
                
                # Display file info
                st.info(f"""
                **File Info:**
                - Shape: {data.shape[0]} rows × {data.shape[1]} columns
                - Columns: {', '.join(data.columns.tolist())}
                """)
                
                # Preview data
                st.subheader("📊 Data Preview")
                st.dataframe(data.head(10), use_container_width=True)
                
            except Exception as e:
                st.markdown(f"<div class='error-box'>❌ Error reading file: {str(e)}</div>", unsafe_allow_html=True)
    
    with col2:
        st.subheader("🔄 Transform Your Data")
        
        if st.session_state.data is not None:
            # Transformation options
            transformation = st.radio(
                "Select transformation type:",
                options=["Row → Column (Pivot)", "Column → Row (Melt)"],
                help="Choose how you want to transform your data"
            )
            
            # Column configuration
            with st.expander("⚙️ Column Configuration", expanded=True):
                columns = st.session_state.data.columns.tolist()
                
                if transformation == "Row → Column (Pivot)":
                    project_col = st.selectbox("Project Column", columns, 
                                             index=0 if 'project' not in columns else columns.index('project'))
                    designator_col = st.selectbox("Designator Column", columns,
                                                index=1 if len(columns) > 1 else 0)
                    value_col = st.selectbox("Volume Column", columns,
                                           index=2 if len(columns) > 2 else 0)
                else:
                    designator_col = st.selectbox("Designator Column", columns,
                                                index=0 if 'designator' in columns else 0)
            
            # Transform button
            if st.button("🚀 Transform Data", type="primary", use_container_width=True):
                transformer = StreamlitPivotTransformer()
                
                with st.spinner('Transforming your data...'):
                    if transformation == "Row → Column (Pivot)":
                        result, error = transformer.row_to_column_format(
                            st.session_state.data, project_col, designator_col, value_col
                        )
                        st.session_state.transformation_type = "pivot"
                    else:
                        result, error = transformer.column_to_row_format(
                            st.session_state.data, designator_col
                        )
                        st.session_state.transformation_type = "melt"
                    
                    if error:
                        st.markdown(f"<div class='error-box'>❌ Transformation failed: {error}</div>", unsafe_allow_html=True)
                    else:
                        st.session_state.transformed_data = result
                        st.markdown("<div class='success-box'>✅ Data transformed successfully!</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div class='info-box'>👆 Please upload a CSV file first</div>", unsafe_allow_html=True)
    
    # Results section
    if 'transformed_data' in st.session_state:
        st.markdown("---")
        st.subheader("📋 Transformation Results")
        
        col3, col4 = st.columns([2, 1])
        
        with col3:
            st.dataframe(st.session_state.transformed_data, use_container_width=True)
            
            # Stats
            st.info(f"""
            **Result Info:**
            - Shape: {st.session_state.transformed_data.shape[0]} rows × {st.session_state.transformed_data.shape[1]} columns
            - Transformation: {st.session_state.transformation_type.title()}
            """)
        
        with col4:
            st.subheader("💾 Download Results")
            
            # Generate default filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_filename = f"PivotPro_{st.session_state.original_filename}_{st.session_state.transformation_type}_{timestamp}.csv"
            
            # Convert to CSV
            csv_buffer = io.StringIO()
            st.session_state.transformed_data.to_csv(csv_buffer, sep=delimiter, index=False)
            csv_data = csv_buffer.getvalue()
            
            # Download button
            st.download_button(
                label="📥 Download CSV",
                data=csv_data,
                file_name=default_filename,
                mime='text/csv',
                type="primary",
                use_container_width=True
            )
            
            st.success(f"File will be saved as:\n`{default_filename}`")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #6b7280; margin-top: 2rem;'>
        <p>🔄 <strong>PivotPro</strong> - Making data transformation simple and fast</p>
        <p>Built with ❤️ using Streamlit and Pandas</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()