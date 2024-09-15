import streamlit as st
import pandas as pd
import time
import threading
from backend import load_data, save_data, delete_record, get_last_update_times

def auto_refresh():
    while True:
        time.sleep(20)
        st.session_state['refresh'] = True

@st.cache_data(ttl=60)
def get_data():
    return load_data()

def main():
    st.title("Database Manipulation")

    if 'refresh' not in st.session_state:
        st.session_state['refresh'] = False

    if st.session_state['refresh']:
        st.session_state['refresh'] = False
        st.rerun()

    if 'auto_refresh_started' not in st.session_state:
        st.session_state['auto_refresh_started'] = True
        threading.Thread(target=auto_refresh, daemon=True).start()

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
        salary = st.number_input("Salary", min_value=0.0, step=100.0)

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
            save_data(updated_df)
            st.success("Employee added successfully!")
            st.rerun()

    elif action == "Edit Employee":
        st.header("Edit Employee")
        employee_id = st.selectbox("Select Employee ID", df['id'].tolist())
        employee = df[df['id'] == employee_id].iloc[0]

        first_name = st.text_input("First Name", employee['first_name'])
        last_name = st.text_input("Last Name", employee['last_name'])
        email = st.text_input("Email", employee['email'])
        department = st.text_input("Department", employee['department'])
        salary = st.number_input("Salary", min_value=0.0, step=100.0, value=float(employee['salary']))

        if st.button("Update Employee"):
            df.loc[df['id'] == employee_id, ['first_name', 'last_name', 'email', 'department', 'salary']] = [first_name, last_name, email, department, salary]
            save_data(df)
            st.success("Employee updated successfully!")
            st.rerun()

    elif action == "Delete Employee":
        st.header("Delete Employee")
        employee_id = st.selectbox("Select Employee ID to Delete", df['id'].tolist())

        if st.button("Delete Employee"):
            delete_record(employee_id)
            st.success("Employee deleted successfully!")
            st.rerun()

    if st.sidebar.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    last_sheet_update, last_db_update = get_last_update_times()
    st.sidebar.write(f"Last Sheet Update: {pd.to_datetime(last_sheet_update, unit='s') if last_sheet_update else 'Never'}")
    st.sidebar.write(f"Last DB Update: {pd.to_datetime(last_db_update, unit='s') if last_db_update else 'Never'}")

if __name__ == "__main__":
    main()