import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
from io import BytesIO

# Database connection string
DB_URI = "postgresql://postgres:2Ellbelt!@localhost:5432/obj"

# Create SQLAlchemy engine
engine = create_engine(DB_URI)

# Initialize session state for page refresh control
if "refresh_page" not in st.session_state:
    st.session_state.refresh_page = False

# Function to get max ID and generate new one
def get_new_id(table_name, id_column):
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COALESCE(MAX({id_column}), 0) + 1 FROM {table_name}")).fetchone()
        return result[0] if result else 1

# Function to fetch all processes
def get_all_processes():
    query = text("SELECT DISTINCT name FROM process ORDER BY name;")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df["name"].tolist() if not df.empty else []

# Function to add a new process
def add_new_process(process_name):
    with engine.begin() as conn:
        existing_process = conn.execute(text("SELECT id FROM process WHERE name ILIKE :name"), {"name": process_name}).fetchone()
        if not existing_process:
            new_id = get_new_id("process", "id")
            conn.execute(text("INSERT INTO process (id, name) VALUES (:id, :name)"), {"id": new_id, "name": process_name})
            st.success(f"✅ Process '{process_name}' added.")
            st.rerun()

# Function to fetch objects linked to a process
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

# Function to add a new object and link it to a process
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

# Function to update `frame_objects` table based on AgGrid changes
def update_database(frame, object_name, new_value, process_name):
    assigned_value = True if new_value == "✔" else False

    with engine.begin() as conn:
        # Get object ID

        object_id_result = conn.execute(text("SELECT id FROM object WHERE name ILIKE :name"), {"name": object_name}).fetchone()
        object_id = object_id_result[0] if object_id_result else None

        # Get frame ID
        frame_id_result = conn.execute(text("SELECT id FROM frame WHERE name ILIKE :name"), {"name": frame}).fetchone()
        frame_id = frame_id_result[0] if frame_id_result else None

        if object_id is None or frame_id is None:
            st.error(f"❌ ERROR: Could not find object or frame ({object_name}, {frame}).")
            return

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

# **Select Process**
st.title("Process, Object, and Frame Management")
process_names = get_all_processes()
selected_process = st.selectbox("Select Process:", process_names, key="process_select")

# **Manage Processes**
with st.expander("➕ Manage Processes"):
    new_process_name = st.text_input("Enter New Process Name:")
    if st.button("Add Process"):
        if new_process_name.strip():
            add_new_process(new_process_name.strip())

# **Manage Objects**
with st.expander("➕ Manage Objects"):
    new_object_name = st.text_input("Enter New Object Name:")
    if st.button("Add Object"):
        if new_object_name.strip():
            add_object_to_process(selected_process, new_object_name.strip())
            st.rerun()

# **Select Objects to Display**
available_objects = get_objects_for_process(selected_process)
selected_objects = st.multiselect("Choose Objects to Display:", available_objects, default=available_objects[:10])

def import_excel_to_database():
    """Prompts user for an Excel file, truncates `process`, `frame`, and `object` tables, and imports data into the database."""
    
    # **Step 1: Upload Excel File**
    st.subheader("Upload Excel File to Import Data")
    uploaded_file = st.file_uploader("Choose an Excel file eg: objLoad", type=["xlsx"])
    
    if uploaded_file is None:
        st.warning("⚠️ Please upload a valid Excel file.")
        return

    # **Step 2: Read Excel File**
    try:
        df_process = pd.read_excel(uploaded_file, sheet_name="process")
        df_frame = pd.read_excel(uploaded_file, sheet_name="frame")
        df_object = pd.read_excel(uploaded_file, sheet_name="object")
        df_process_frame = pd.read_excel(uploaded_file, sheet_name="process_frame")
        df_process_object = pd.read_excel(uploaded_file, sheet_name="process_object")
        df_frame_object = pd.read_excel(uploaded_file, sheet_name="frame_object")
    except Exception as e:
        st.error(f"❌ Error reading Excel file: {e}")
        return

    # **Step 3: Validate Sheets**
    required_columns = {
        "process": ["id", "name"],
        "frame": ["id", "name"],
        "object": ["id", "name"],
        "process_frame": ["process_id", "frame_id"],
        "process_object": ["process_id", "object_id"],
        "frame_object": ["frame_id", "object_id"]
    }
    
    for sheet_name, required_cols in required_columns.items():
        if sheet_name == "process":
            df = df_process
        elif sheet_name == "frame":
            df = df_frame
        elif sheet_name == "object":
            df = df_object
        elif sheet_name == "process_frame":
            df = df_process_frame
        elif sheet_name == "process_object":
            df = df_process_object
        elif sheet_name == "frame_object":
            df = df_frame_object

        if not all(col in df.columns for col in required_cols):
            st.error(f"❌ Missing required columns in sheet `{sheet_name}`. Expected: {required_cols}")
            return

    # **Step 4: Truncate Existing Tables**
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE process RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE TABLE frame RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE TABLE object RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE TABLE process_frame RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE TABLE process_object RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE TABLE frame_object RESTART IDENTITY CASCADE;"))
        st.success("✅ Tables truncated successfully.")

    # **Step 5: Import Data**
    try:
        df_process.to_sql("process", con=engine, if_exists="append", index=False)
        df_frame.to_sql("frame", con=engine, if_exists="append", index=False)
        df_object.to_sql("object", con=engine, if_exists="append", index=False)
        df_process_frame.to_sql("process_frame", con=engine, if_exists="append", index=False)
        df_process_object.to_sql("process_object", con=engine, if_exists="append", index=False)
        df_frame_object.to_sql("frame_object", con=engine, if_exists="append", index=False)
        st.success("✅ Data imported successfully into the database.")
    except Exception as e:
        st.error(f"❌ Error importing data: {e}")

def export_all_processes_to_excel():
    """Fetches frame-object assignments for all processes and exports them into separate sheets in an Excel file."""
    
    st.subheader("Export All Processes to Excel")

    # **Step 1: Fetch All Process Names**
    with engine.connect() as conn:
        processes = pd.read_sql(text("SELECT DISTINCT name FROM process ORDER BY name;"), conn)

    if processes.empty:
        st.warning("⚠️ No processes found in the database.")
        return

    output = BytesIO()

    # **Step 2: Write Each Process to Its Own Sheet**
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
                df.to_excel(writer, sheet_name=process_name[:31], index=False)  # Excel sheet names are limited to 31 chars

    output.seek(0)

    # **Step 3: Allow User to Download the File**
    st.download_button(
        label="📥 Download Excel File",
        data=output,
        file_name="all_processes_pivoted.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

def export_pivoted_database_to_excel():
    """Fetches data from the database, pivots it by process, and allows users to download as an Excel file."""
    
    st.subheader("Export Database (Pivoted by Process) to Excel")
    
    # **Step 1: Fetch Data from the Database**
    with engine.connect() as conn:
        df_processes = pd.read_sql(text("SELECT id, name FROM process"), conn)
        df_frame_objects = pd.read_sql(text("""
            SELECT 
                f.name as frame_name, 
                p.name AS process_name, 
                o.name AS object_name, 
                CASE WHEN fo.frame_id IS NOT NULL THEN '✔' ELSE '❌' END AS assigned
            FROM frame f
            JOIN process_frame pf ON pf.frame_id = f.id
            JOIN process p ON p.id = pf.process_id
            JOIN object o ON o.id IN (
                SELECT object_id FROM process_object WHERE process_id = p.id
            )
            LEFT JOIN frame_object fo ON f.id = fo.frame_id AND o.id = fo.object_id
            ORDER BY p.name, f.name, o.name;
        """), conn)

    # **Step 2: Write to Excel in Memory**
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        
        for process in df_processes["name"].unique():
            # Filter data for the specific process
            df_filtered = df_frame_objects[df_frame_objects["process_name"] == process]
            
            # Pivot the data (Frame Names as Rows, Object Names as Columns)
            df_pivot = df_filtered.pivot(index="frame_name", columns="object_name", values="assigned").fillna("❌")
            
            # Write to a separate sheet
            df_pivot.to_excel(writer, sheet_name=process, index=True)
        
    output.seek(0)

    # **Step 3: Allow User to Download the File**
    st.download_button(
        label="📥 Download Pivoted Excel File",
        data=output,
        file_name="database_pivoted_export.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# **Fetch Pivot Table**
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

df = get_pivot_table(selected_process, selected_objects)
df = df.reset_index(drop=True)

# **Configure AgGrid**
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_default_column(editable=True, singleClickEdit=True)
for col in df.columns[1:]:
    gb.configure_column(col, cellEditor="agSelectCellEditor", cellEditorParams={"values": ["✔", "❌"]})
grid_options = gb.build()

# **Display Grid**
grid_response = AgGrid(df, gridOptions=grid_options, update_mode=GridUpdateMode.VALUE_CHANGED)

# **Process AgGrid Changes**
if grid_response["data"] is not None:
    new_df = pd.DataFrame(grid_response["data"]).reset_index(drop=True)
    changes = df.compare(new_df)

    if not changes.empty:
        for row_idx in changes.index:
            for col_name in df.columns[1:]:
                old_value = df.at[row_idx, col_name]
                new_value = new_df.at[row_idx, col_name]
                if old_value != new_value:
                    frame = df.at[row_idx, "frame"]
                    update_database(frame, col_name, new_value, selected_process)

if st.button("📤 Import Excel File to Database"):
    import_excel_to_database()

if st.button("📥 Export Pivoted Excel File"):
    export_pivoted_database_to_excel()

if st.button("📥 Export Pivoted Excel File (v2)"):
    export_all_processes_to_excel()


# **Refresh UI**
if st.session_state.refresh_page:
    st.session_state.refresh_page = False
    st.rerun()
