import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.database import DatabaseManager
from core.importers.csv_importer import CSVImporter
import streamlit as st
from pathlib import Path
import pandas as pd
from core.database import DatabaseManager
from core.importers.csv_importer import CSVImporter

# Page configuration
st.set_page_config(
    page_title="Data Entry Automation",
    page_icon="📊",
    layout="wide"
)

def main():
    st.title("📊 Data Entry Automation Tool")
    st.markdown("Import CSV files into your database with validation and error handling.")

    # Initialize session state
    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager()
        st.session_state.db_manager.connect()
        st.session_state.db_manager.create_tables()

    # File upload section
    uploaded_file = st.file_uploader(
        "Upload your CSV file",
        type=["csv"],
        help="CSV should contain columns: name, email, phone, company"
    )

    if uploaded_file:
        # Preview data
        st.subheader("Data Preview")
        df = pd.read_csv(uploaded_file)
        st.dataframe(df.head())

        # Import options
        if st.button("✨ Import Data"):
            with st.spinner("Processing..."):
                # Save temp file
                temp_path = Path("temp_upload.csv")
                with open(temp_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())

                # Process import
                importer = CSVImporter(st.session_state.db_manager)
                results = importer.import_from_file(temp_path)
                temp_path.unlink()  # Delete temp file

                # Show results
                st.success(f"✅ Import complete! Processed {results['total']} records")

                cols = st.columns(3)
                cols[0].metric("Inserted", results['inserted'])
                cols[1].metric("Skipped", results['skipped'])
                cols[2].metric("Errors", len(results['errors']))

                if results['errors']:
                    st.subheader("⚠️ Import Errors")
                    error_df = pd.DataFrame(results['errors'])
                    st.dataframe(error_df)

                    # Export errors button
                    csv_error = error_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "Download Error Report",
                        data=csv_error,
                        file_name="import_errors.csv",
                        mime="text/csv"
                    )

    # Sidebar with database info
    st.sidebar.title("Database Info")
    st.sidebar.markdown(f"**Database file:** `contacts.db`")

    if st.session_state.db_manager.connection:
        cursor = st.session_state.db_manager.execute("SELECT COUNT(*) FROM contacts")
        count = cursor.fetchone()[0] if cursor else 0
        st.sidebar.metric("Total Contacts", count)

    st.sidebar.markdown("---")
    st.sidebar.info(
        "Need a template? [Download sample CSV](https://example.com/sample.csv)"
    )

if __name__ == "__main__":
    main()
