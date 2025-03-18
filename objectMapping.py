import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from io import BytesIO

# Database connection
DB_URI = "postgresql://postgres:2Ellbelt!@localhost:5432/obj"
engine = create_engine(DB_URI)

# Initialize session state
if "refresh_page" not in st.session_state:
    st.session_state.refresh_page = False

# Database helper functions
def get_new_id(table, column):
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COALESCE(MAX({column}), 0) + 1 FROM {table}")).fetchone()
        return result[0] if result else 1

def get_all_processes():
    query = text("SELECT DISTINCT name FROM process ORDER BY name;")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df["name"].tolist() if not df.empty else []

def get_objects_for_process(process_name):
    query = text("""
    SELECT DISTINCT o.name 
    FROM object o
    JOIN process_object po ON o.id = po.object_id
    JOIN process p ON p.id = po.process_id
    WHERE p.name ILIKE :process_name
    ORDER BY o.name;
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"process_name": process_name})
    return df["name"].tolist() if not df.empty else []

def get_pivot_table(process_name, selected_objects):
    if not selected_objects:
        return pd.DataFrame(columns=["frame"])  

    column_headers = ", ".join([f'"{obj}" TEXT' for obj in selected_objects])
    
    query = text(f"""
    SELECT * FROM crosstab(
        $$SELECT 
            f.name AS frame, 
            o.name AS object, 
            CASE WHEN fo.frame_id IS NOT NULL THEN '✔' ELSE '❌' END AS assigned
          FROM frame f
          JOIN process_frame pf ON pf.frame_id = f.id  
          JOIN process p ON p.id = pf.process_id
          JOIN object o ON o.id IN (
              SELECT object_id FROM process_object WHERE process_id = p.id
          )
          LEFT JOIN frame_object fo ON f.id = fo.frame_id AND o.id = fo.object_id
          WHERE p.name ILIKE :process_name  
          ORDER BY 1, 2$$
    ) AS ct (
        frame TEXT,
        {column_headers}
    );
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"process_name": process_name})

    df.columns = df.columns.str.lower()
    return df

# Data modification functions
def add_new_process(process_name):
    with engine.begin() as conn:
        existing = conn.execute(text("SELECT id FROM process WHERE name ILIKE :name"), {"name": process_name}).fetchone()
        if not existing:
            new_id = get_new_id("process", "id")
            conn.execute(text("INSERT INTO process (id, name) VALUES (:id, :name)"), {"id": new_id, "name": process_name})
            st.success(f"✅ Process '{process_name}' added.")
            st.rerun()

def add_object_to_process(process_name, object_name):
    with engine.begin() as conn:
        process_id = conn.execute(text("SELECT id FROM process WHERE name ILIKE :name"), {"name": process_name}).fetchone()
        if process_id:
            process_id = process_id[0]
            object_id = conn.execute(text("SELECT id FROM object WHERE name ILIKE :name"), {"name": object_name}).fetchone()
            
            if not object_id:
                new_object_id = get_new_id("object", "id")
                conn.execute(text("INSERT INTO object (id, name) VALUES (:id, :name)"), {"id": new_object_id, "name": object_name})
                object_id = new_object_id
            else:
                object_id = object_id[0]

            conn.execute(text("""
                INSERT INTO process_object (process_id, object_id) 
                VALUES (:process_id, :object_id)
                ON CONFLICT (process_id, object_id) DO NOTHING;
            """), {"process_id": process_id, "object_id": object_id})

            st.success(f"✅ Linked object '{object_name}' to process '{process_name}'.")
            st.rerun()

def update_database(frame, object_name, new_value, process_name):
    assigned_value = True if new_value == "✔" else False

    with engine.begin() as conn:
        object_id = conn.execute(text("SELECT id FROM object WHERE name ILIKE :name"), {"name": object_name}).fetchone()
        frame_id = conn.execute(text("SELECT id FROM frame WHERE name ILIKE :name"), {"name": frame}).fetchone()
        
        if not object_id or not frame_id:
            st.error(f"❌ ERROR: Could not find object or frame ({object_name}, {frame}).")
            return
            
        object_id, frame_id = object_id[0], frame_id[0]

        if assigned_value:
            conn.execute(text("""
                INSERT INTO frame_object (frame_id, object_id) 
                VALUES (:frame_id, :object_id)
                ON CONFLICT (frame_id, object_id) DO NOTHING;
            """), {"frame_id": frame_id, "object_id": object_id})
        else:
            conn.execute(text("""
                DELETE FROM frame_object WHERE frame_id = :frame_id AND object_id = :object_id;
            """), {"frame_id": frame_id, "object_id": object_id})

    st.session_state.refresh_page = True

# Import/Export functions
def import_excel_to_database():
    st.subheader("Upload Excel File to Import Data")
    uploaded_file = st.file_uploader("Choose an Excel file eg: objLoad", type=["xlsx"])
    
    if not uploaded_file:
        return

    try:
        tables = ["process", "frame", "object", "process_frame", "process_object", "frame_object"]
        dataframes = {table: pd.read_excel(uploaded_file, sheet_name=table) for table in tables}
        
        # Validate required columns
        required_columns = {
            "process": ["id", "name"],
            "frame": ["id", "name"],
            "object": ["id", "name"],
            "process_frame": ["process_id", "frame_id"],
            "process_object": ["process_id", "object_id"],
            "frame_object": ["frame_id", "object_id"]
        }
        
        for table, columns in required_columns.items():
            if not all(col in dataframes[table].columns for col in columns):
                st.error(f"❌ Missing required columns in sheet `{table}`")
                return
                
        # Truncate and import
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE process RESTART IDENTITY CASCADE;"))
            
            for table, df in dataframes.items():
                df.to_sql(table, con=engine, if_exists="append", index=False)
                
        st.success("✅ Data imported successfully into the database.")
        st.rerun()
        
    except Exception as e:
        st.error(f"❌ Error: {e}")

def export_all_processes_to_excel():
    st.subheader("Export All Processes to Excel")
    
    with engine.connect() as conn:
        processes = pd.read_sql(text("SELECT DISTINCT name FROM process ORDER BY name;"), conn)
    
    if processes.empty:
        st.warning("⚠️ No processes found in the database.")
        return
        
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for process_name in processes["name"]:
            query = text("""
                WITH RankedObjects AS (
                    SELECT 
                        f.id as frame_id,
                        f.name as frame_name, 
                        o.name AS object_name,
                        p.name AS process_name,
                        ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY o.name) AS obj_rank
                    FROM frame f
                    JOIN process_frame pf ON pf.frame_id = f.id
                    JOIN process p ON p.id = pf.process_id
                    JOIN frame_object fo ON fo.frame_id = f.id
                    JOIN object o ON o.id = fo.object_id
                    WHERE p.name ILIKE :process_name 
                )
                SELECT 
                    frame_id,
                    frame_name,
                    COALESCE(MAX(CASE WHEN obj_rank = 1 THEN object_name END), '') AS "Obj 1",
                    COALESCE(MAX(CASE WHEN obj_rank = 2 THEN object_name END), '') AS "Obj 2",
                    COALESCE(MAX(CASE WHEN obj_rank = 3 THEN object_name END), '') AS "Obj 3",
                    COALESCE(MAX(CASE WHEN obj_rank = 4 THEN object_name END), '') AS "Obj 4",
                    COALESCE(MAX(CASE WHEN obj_rank = 5 THEN object_name END), '') AS "Obj 5"
                FROM RankedObjects
                GROUP BY frame_id, frame_name
                ORDER BY frame_name;
            """)
            with engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"process_name": process_name})
                
            if not df.empty:
                df.to_excel(writer, sheet_name=process_name[:31], index=False)
                
    output.seek(0)
    st.download_button(
        label="📥 Download Excel File",
        data=output,
        file_name="all_processes_export.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# Main App
st.title("Process, Object, and Frame Management")

# Process selection
process_names = get_all_processes()
selected_process = st.selectbox("Select Process:", process_names, key="process_select")

# Process management
with st.expander("➕ Manage Processes"):
    new_process_name = st.text_input("Enter New Process Name:")
    if st.button("Add Process") and new_process_name.strip():
        add_new_process(new_process_name.strip())

# Object management
with st.expander("➕ Manage Objects"):
    new_object_name = st.text_input("Enter New Object Name:")
    if st.button("Add Object") and new_object_name.strip():
        add_object_to_process(selected_process, new_object_name.strip())

# Object selection
available_objects = get_objects_for_process(selected_process)
selected_objects = st.multiselect("Choose Objects to Display:", available_objects, default=available_objects[:10])

# Display grid
df = get_pivot_table(selected_process, selected_objects)
df = df.reset_index(drop=True)

gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_default_column(editable=True, singleClickEdit=True)
for col in df.columns[1:]:
    gb.configure_column(col, cellEditor="agSelectCellEditor", cellEditorParams={"values": ["✔", "❌"]})
grid_options = gb.build()

grid_response = AgGrid(df, gridOptions=grid_options, update_mode=GridUpdateMode.VALUE_CHANGED)

# Process changes
if grid_response["data"] is not None:
    new_df = pd.DataFrame(grid_response["data"]).reset_index(drop=True)
    changes = df.compare(new_df)

    if not changes.empty:
        for row_idx in changes.index:
            for col_name in df.columns[1:]:
                if col_name in changes.columns.get_level_values(1):
                    old_value = df.at[row_idx, col_name]
                    new_value = new_df.at[row_idx, col_name]
                    if old_value != new_value:
                        frame = df.at[row_idx, "frame"]
                        update_database(frame, col_name, new_value, selected_process)

# Import/Export buttons
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("📤 Import Excel File"):
        import_excel_to_database()
with col2:
    if st.button("📥 Export Pivoted Excel"):
        export_all_processes_to_excel()

# Refresh UI
if st.session_state.refresh_page:
    st.session_state.refresh_page = False
    st.rerun()