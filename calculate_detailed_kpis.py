import pandas as pd
import numpy as np
import os

print("--- Starting Granular Store & Employee Operational Calculation ---")

# 1. LOAD DATA
try:
    df_stores = pd.read_csv('data/input/stores.csv')
    df_employees = pd.read_csv('data/input/employees.csv')
    df_traffic = pd.read_csv('data/input/traffic_forecast.csv')
    df_rules = pd.read_csv('data/input/staffing_rules.csv')
    df_schedule = pd.read_csv('data/input/current_schedule.csv')
except FileNotFoundError:
    print("Error: Missing input files. Please ensure data is generated.")
    exit()

# 2. MINUTE DEMAND CALIBRATION (Hour-by-Hour, Role-by-Role)
print("Calibrating True Demand (Required Staff) per hour...")
df_traffic['key'] = 1
df_rules['key'] = 1
df_demand = pd.merge(df_traffic, df_rules, on='key').drop('key', axis=1)

df_demand['required_staff'] = np.ceil(df_demand['forecast_customers'] / df_demand['customers_per_hour'])
df_demand['required_staff'] = np.maximum(df_demand['required_staff'], df_demand['min_staff'])

# 3. MINUTE CAPACITY PLANNING (Exploding shifts into hourly blocks)
print("Mapping Scheduled Capacity to Hourly Blocks...")
hourly_schedule = []
for _, row in df_schedule.iterrows():
    for h in range(int(row['shift_start']), int(row['shift_end'])):
        hourly_schedule.append({
            'store_id': row['store_id'],
            'date': row['date'],
            'role': row['role'],
            'hour': h,
            'scheduled_staff': 1
        })
df_capacity = pd.DataFrame(hourly_schedule)
df_capacity_grouped = df_capacity.groupby(['store_id', 'date', 'hour', 'role'])['scheduled_staff'].sum().reset_index()

# 4. HOURLY GAP ANALYSIS
print("Calculating Hourly Service Gaps and Labor Waste...")
df_coverage = pd.merge(df_demand, df_capacity_grouped, on=['store_id', 'date', 'hour', 'role'], how='left').fillna(0)
df_coverage['staffing_gap'] = df_coverage['scheduled_staff'] - df_coverage['required_staff']

df_coverage['understaffed_hours'] = np.where(df_coverage['staffing_gap'] < 0, abs(df_coverage['staffing_gap']), 0)
df_coverage['overstaffed_hours'] = np.where(df_coverage['staffing_gap'] > 0, df_coverage['staffing_gap'], 0)
df_coverage['peak_understaffed_hours'] = np.where((df_coverage['is_peak_hour'] == 1) & (df_coverage['staffing_gap'] < 0), abs(df_coverage['staffing_gap']), 0)

# 5. DETAILED EMPLOYEE LEDGER (Overtime and Penalty Breakdown)
print("Computing granular employee unit costs and overtime penalties...")
# Group by employee to get total weekly hours, bringing in role and wage
weekly_hrs = df_schedule.groupby(['employee_id', 'store_id', 'role', 'hourly_wage_mxn'])['scheduled_hours'].sum().reset_index()

# Merge in employee names for the detailed ledger
weekly_hrs = weekly_hrs.merge(df_employees[['employee_id', 'employee_name']], on='employee_id', how='left')

# Apply the strict 48-hour threshold logic
weekly_hrs['regular_hours'] = np.minimum(weekly_hrs['scheduled_hours'], 48)
weekly_hrs['overtime_hours'] = np.maximum(weekly_hrs['scheduled_hours'] - 48, 0)

# Calculate Exact Costs
weekly_hrs['regular_cost_mxn'] = weekly_hrs['regular_hours'] * weekly_hrs['hourly_wage_mxn']
weekly_hrs['overtime_penalty_cost_mxn'] = weekly_hrs['overtime_hours'] * (weekly_hrs['hourly_wage_mxn'] * 2.0)
weekly_hrs['total_labor_cost_mxn'] = weekly_hrs['regular_cost_mxn'] + weekly_hrs['overtime_penalty_cost_mxn']

# Format the Employee Ledger
employee_ledger = weekly_hrs[['employee_id', 'employee_name', 'store_id', 'role', 'hourly_wage_mxn', 
                              'scheduled_hours', 'regular_hours', 'overtime_hours', 
                              'regular_cost_mxn', 'overtime_penalty_cost_mxn', 'total_labor_cost_mxn']]

# 6. ROLLUP TO STORE-LEVEL MICRO-ECONOMICS
print("Aggregating metrics to the Store Level...")
store_financials = weekly_hrs.groupby('store_id').agg(
    total_labor_cost=('total_labor_cost_mxn', 'sum'),
    total_regular_cost=('regular_cost_mxn', 'sum'),
    total_overtime_penalty_cost=('overtime_penalty_cost_mxn', 'sum'),
    total_overtime_hours=('overtime_hours', 'sum'),
    employees_on_ot=('overtime_hours', lambda x: (x > 0).sum())
).reset_index()

store_ops = df_coverage.groupby('store_id').agg(
    total_required_hours=('required_staff', 'sum'),
    total_scheduled_hours=('scheduled_staff', 'sum'),
    waste_overstaffed_hours=('overstaffed_hours', 'sum'),
    total_understaffed_hours=('understaffed_hours', 'sum'),
    peak_understaffed_hours=('peak_understaffed_hours', 'sum')
).reset_index()

store_master = pd.merge(df_stores[['store_id', 'city', 'format']], store_ops, on='store_id')
store_master = pd.merge(store_master, store_financials, on='store_id')

store_master['labor_utilization_pct'] = (store_master['total_required_hours'] / store_master['total_scheduled_hours']) * 100

cols = ['store_id', 'city', 'format', 'total_labor_cost', 'total_regular_cost', 'total_overtime_penalty_cost', 
        'total_overtime_hours', 'employees_on_ot', 'total_scheduled_hours', 'total_required_hours', 
        'labor_utilization_pct', 'waste_overstaffed_hours', 'total_understaffed_hours', 'peak_understaffed_hours']
store_master = store_master[cols]

# Calculate True Labor Utilization % (Efficiency)
store_master['labor_utilization_pct'] = (store_master['total_required_hours'] / store_master['total_scheduled_hours']) * 100

# NEW: Calculate Service Level % (Effectiveness)
store_master['service_level_pct'] = ((store_master['total_required_hours'] - store_master['total_understaffed_hours']) / store_master['total_required_hours']) * 100

# Reorder for readability (Added service_level_pct)
cols = ['store_id', 'city', 'format', 'total_labor_cost', 'total_regular_cost', 'total_overtime_penalty_cost', 
        'total_overtime_hours', 'employees_on_ot', 'total_scheduled_hours', 'total_required_hours', 
        'labor_utilization_pct', 'service_level_pct', 'waste_overstaffed_hours', 'total_understaffed_hours', 'peak_understaffed_hours']
store_master = store_master[cols]

# 7. EXPORT DIAGNOSTICS
os.makedirs('data/output', exist_ok=True)
store_export_path = 'data/output/store_level_diagnostics.csv'
employee_export_path = 'data/output/employee_level_diagnostics.csv'

store_master.to_csv(store_export_path, index=False)
employee_ledger.to_csv(employee_export_path, index=False)

print(f"\n--- SUCCESS! TWO files exported ---")
print(f"1. Store Master: {store_export_path}")
print(f"2. Employee Ledger: {employee_export_path}")
print("\nTop 3 Employees Bleeding the Most Overtime Cash:")
print(employee_ledger.sort_values('overtime_penalty_cost_mxn', ascending=False)[['employee_id', 'role', 'store_id', 'overtime_hours', 'overtime_penalty_cost_mxn']].head(3).to_string(index=False))

# Calculate the service gap (Actual vs Target)
# This shows if we are meeting the CEO's quality standards
store_master['service_gap_pct'] = store_master['service_level_pct'] - 90 # Using 90% as a global benchmark