import streamlit as st
import pandas as pd
import time
import threading
from backend import load_data, save_data, delete_record, get_last_update_times, poll_for_changes
from datetime import datetime

@st.cache_data(ttl=10)  # caching for 10 seconds
def get_data():
    try:
        return load_data()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(columns=['id', 'first_name', 'last_name', 'email', 'department', 'salary'])

def main():
    st.title("Database Manipulation")

    if 'refresh' not in st.session_state:
        st.session_state['refresh'] = False

    if st.session_state['refresh']:
        st.session_state['refresh'] = False
        st.experimental_rerun()

    if 'sync_thread' not in st.session_state:
        st.session_state.sync_thread = threading.Thread(target=poll_for_changes, daemon=True)
        st.session_state.sync_thread.start()

    df = get_data()

    st.sidebar.header("Actions")
    action = st.sidebar.radio("Choose an action", ["View Data", "Add Employee", "Edit Employee", "Delete Employee"])

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
                st.rerun()
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
                st.rerun()
            except Exception as e:
                st.error(f"Error updating employee: {e}")

    elif action == "Delete Employee":
        st.header("Delete Employee")
        employee_id = st.selectbox("Select Employee ID to Delete", df['id'].tolist())

        if st.button("Delete Employee"):
            try:
                delete_record(employee_id)
                st.success("Employee deleted successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Error deleting employee: {e}")

    if st.sidebar.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    last_sheet_update, last_db_update = get_last_update_times()
    st.sidebar.write(f"Last Sync: {datetime.fromtimestamp(last_sheet_update) if last_sheet_update else 'Never'}")

if __name__ == "__main__":
    main()