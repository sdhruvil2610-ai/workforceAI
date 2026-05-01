import pandas as pd
import numpy as np
import os

print("🚀 Starting Aivena Quantitative KPI Diagnostic...")

# --- 1. LOAD DATA ---
# Using your specific path structure
try:
    df_stores = pd.read_csv('data/input/stores.csv')
    df_traffic = pd.read_csv('data/input/traffic_forecast.csv')
    df_rules = pd.read_csv('data/input/staffing_rules.csv') 
    df_schedule = pd.read_csv('data/input/current_schedule.csv')
except FileNotFoundError as e:
    print(f"❌ Error: Missing file {e.filename}. Please run your generator script first.")
    exit()

# --- 2. HOURLY DEMAND & TARGET CALIBRATION ---
# We use a cross-join to calculate requirements for every role at every hour
df_demand = pd.merge(df_traffic, df_rules, how='cross') 

# Required = Customers / Productivity Ratio
df_demand['required_staff'] = np.ceil(df_demand['forecast_customers'] / df_demand['customers_per_hour'])
df_demand['required_staff'] = np.maximum(df_demand['required_staff'], df_demand['min_staff'])

# --- 3. HOURLY CAPACITY (Breakdown 8-hour shifts) ---
# Transforming shift blocks into hourly data points for comparison
hourly_rows = []
for _, row in df_schedule.iterrows():
    for h in range(int(row['shift_start']), int(row['shift_end'])):
        hourly_rows.append({
            'store_id': row['store_id'],
            'employee_id': row['employee_id'],
            'date': row['date'],
            'role': row['role'],
            'hour': h,
            'wage': row['hourly_wage_mxn']
        })
df_hourly_cap = pd.DataFrame(hourly_rows)
df_cap_grouped = df_hourly_cap.groupby(['store_id', 'date', 'hour', 'role']).size().reset_index(name='scheduled_staff')

# --- 4. COVERAGE & SERVICE LEVEL MATH ---
df_coverage = pd.merge(df_demand, df_cap_grouped, on=['store_id', 'date', 'hour', 'role'], how='left').fillna(0)

# Met Demand is the lesser of what we have vs what we need
df_coverage['met_demand'] = np.minimum(df_coverage['scheduled_staff'], df_coverage['required_staff'])
df_coverage['understaffed_gap'] = np.maximum(df_coverage['required_staff'] - df_coverage['scheduled_staff'], 0)
df_coverage['overstaffed_waste'] = np.maximum(df_coverage['scheduled_staff'] - df_coverage['required_staff'], 0)

# --- 5. EMPLOYEE-LEVEL MICRO-ECONOMICS ---
# Aggregate hours and costs per unique employee to identify OT leakage
emp_diag = df_schedule.groupby(['employee_id', 'role', 'store_id']).agg(
    total_hours=('scheduled_hours', 'sum'),
    total_labor_cost_mxn=('labor_cost_mxn', 'sum')
).reset_index()

emp_diag['regular_hours'] = emp_diag['total_hours'].clip(upper=48)
emp_diag['overtime_hours'] = (emp_diag['total_hours'] - 48).clip(lower=0)

# Export for the Streamlit Micro-Economics Tab
# Saving to data/input to keep your structure consistent
emp_diag.to_csv('data/input/employee_level_diagnostics.csv', index=False)
print("✅ Created data/input/employee_level_diagnostics.csv")

# --- 6. EXECUTIVE SUMMARY OUTPUT ---
total_cost = df_schedule['labor_cost_mxn'].sum()
total_req_hours = df_coverage['required_staff'].sum()
total_sched_hours = df_coverage['scheduled_staff'].sum()
total_met_demand = df_coverage['met_demand'].sum()

# Key Macro Metrics
avg_utilization = (total_req_hours / total_sched_hours) * 100
avg_service_level = (total_met_demand / total_req_hours) * 100

print("="*60)
print(" 📊 LATIN LEAP: QUANTITATIVE BASELINE DIAGNOSTIC ")
print("="*60)

print(f"\n1. NETWORK EFFICIENCY")
print(f"   - Avg Labor Utilization: {avg_utilization:.1f}%")
print(f"   - Avg Service Level:     {avg_service_level:.1f}%")
print(f"   - Total Wasted Hours:    {df_coverage['overstaffed_waste'].sum():,.0f} hrs")

print(f"\n2. FINANCIAL LEAKAGE")
print(f"   - Total Baseline Cost:   ${total_cost:,.2f} MXN")
print(f"   - OT Workers:            {len(emp_diag[emp_diag['overtime_hours'] > 0])} employees")

print(f"\n3. TOP 3 INEFFICIENCY HOTSPOTS (By Waste Hours)")
hotspots = df_coverage.groupby('store_id')['overstaffed_waste'].sum().nlargest(3)
for sid, waste in hotspots.items():
    print(f"   - Store {sid}: {waste:,.0f} hours of idle payroll")

print("\n" + "="*60)
print(f" 🎯 8% SAVINGS TARGET: Below ${(total_cost * 0.92):,.2f} MXN")
print("="*60)