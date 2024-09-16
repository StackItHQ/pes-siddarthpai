import streamlit as st
import pandas as pd
import time
from backend import load_data, save_data, delete_record, get_last_update_times, poll_for_changes, has_data_changed, reset_data_changed, sync_data
from datetime import datetime
import altair as alt

@st.cache_data(ttl=1)  # cache for 1 second
def get_data():
    try:
        return load_data()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(columns=['id', 'first_name', 'last_name', 'email', 'department', 'salary'])

def main():
    st.title("Database Manipulation")

    if 'sync_logs' not in st.session_state:
        st.session_state.sync_logs = []

    df = get_data()

    st.sidebar.header("Actions")
    action = st.sidebar.radio("Choose an action", ["View Data", "Add Employee", "Edit Employee", "Delete Employee", "Logs"])

    if st.sidebar.button("Refresh Data"):
        sync_data()
        st.cache_data.clear()
        st.rerun()

    if action == "View Data":
        st.header("Employee Data")
        st.dataframe(df)

    elif action == "Add Employee":
        st.header("Add New Employee")
        new_id = st.number_input("ID", min_value=1, step=1, value=df['id'].astype(int).max() + 1 if not df.empty else 1)
        first_name = st.text_input("First Name")
        last_name = st.text_input("Last Name")
        email = st.text_input("Email")
        department = st.text_input("Department")
        salary = st.number_input("Salary", min_value=0.0, step=100.0, max_value=99999999999999.99)

        if st.button("Add Employee"):
            new_row = pd.DataFrame({
                "id": [new_id],
                "first_name": [first_name],
                "last_name": [last_name],
                "email": [email],
                "department": [department],
                "salary": [salary]
            })
            updated_df = pd.concat([df, new_row], ignore_index=True)
            try:
                save_data(updated_df)
                st.success("Employee added successfully!")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Error adding employee: {e}")

    elif action == "Edit Employee":
        st.header("Edit Employee")
        employee_id = st.selectbox("Select Employee ID", df['id'].tolist())
        employee = df[df['id'] == employee_id].iloc[0]

        first_name = st.text_input("First Name", employee['first_name'])
        last_name = st.text_input("Last Name", employee['last_name'])
        email = st.text_input("Email", employee['email'])
        department = st.text_input("Department", employee['department'])
        salary = st.number_input("Salary", min_value=0.0, step=100.0, value=float(employee['salary']), max_value=99999999999999.99)

        if st.button("Update Employee"):
            df.loc[df['id'] == employee_id, ['first_name', 'last_name', 'email', 'department', 'salary']] = [first_name, last_name, email, department, salary]
            try:
                save_data(df)
                st.success("Employee updated successfully!")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Error updating employee: {e}")

    elif action == "Delete Employee":
        st.header("Delete Employee")
        employee_id = st.selectbox("Select Employee ID to Delete", df['id'].tolist())

        if st.button("Delete Employee"):
            try:
                delete_record(employee_id)
                st.success("Employee deleted successfully!")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Error deleting employee: {e}")

    elif action == "Logs":
        st.header("Synchronization Logs")
        
        # Create a line chart of sync times
        if st.session_state.sync_logs:
            log_df = pd.DataFrame(st.session_state.sync_logs, columns=['timestamp', 'sheet_update', 'db_update'])
            log_df['timestamp'] = pd.to_datetime(log_df['timestamp'])
            
            chart = alt.Chart(log_df).mark_line().encode(
                x='timestamp:T',
                y='sheet_update:Q',
                color=alt.value("blue")
            ).properties(
                width=600,
                height=300,
                title='Synchronization Times'
            ) + alt.Chart(log_df).mark_line().encode(
                x='timestamp:T',
                y='db_update:Q',
                color=alt.value("red")
            )
            
            st.altair_chart(chart)
        else:
            st.write("No synchronization logs available yet.")

    last_sheet_update, last_db_update = get_last_update_times()
    st.sidebar.write(f"Last Sheet Sync: {datetime.fromtimestamp(last_sheet_update) if last_sheet_update else 'Never'}")
    st.sidebar.write(f"Last DB Sync: {datetime.fromtimestamp(last_db_update) if last_db_update else 'Never'}")

    # Add current sync times to logs
    st.session_state.sync_logs.append([datetime.now(), last_sheet_update, last_db_update])
    if len(st.session_state.sync_logs) > 100:  # Keep only the last 100 logs
        st.session_state.sync_logs = st.session_state.sync_logs[-100:]

if __name__ == "__main__":
    main()