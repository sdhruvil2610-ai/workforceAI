import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta
import os

os.makedirs('data/input', exist_ok=True)
os.makedirs('data/output', exist_ok=True)
fake = Faker('es_MX')

print("Starting End-to-End Data Generation (Strict 48-Hour Current State)...")

# --- STEP 1: GENERATE STORES ---
print("1. Generating 50 Retail Stores...")
cities = ['CDMX', 'Monterrey', 'Guadalajara', 'Queretaro', 'Merida', 'Puebla', 'Cancun', 'Tijuana']
stores_data = [{'store_id': f"S{i:03}", 'city': random.choice(cities), 'format': random.choice(['Small', 'Medium', 'Large']), 'open_hour': 8, 'close_hour': 22} for i in range(1, 51)]
df_stores = pd.DataFrame(stores_data)
df_stores.to_csv('data/input/stores.csv', index=False)

# --- STEP 2: GENERATE EMPLOYEES (CURRENT 48-HOUR LAW) ---
print("2. Generating 4,000 Employees (Current Legal Cap: 48 Hours)...")
roles = ['Cashier', 'Floor', 'Stock', 'Customer Service', 'Supervisor']
wages = {'Cashier': 75, 'Floor': 70, 'Stock': 65, 'Customer Service': 85, 'Supervisor': 120}

employees_data = []
emp_counter = 1
for store in df_stores['store_id']:
    for _ in range(80):
        role = random.choices(roles, weights=[30, 30, 20, 10, 10])[0]
        employees_data.append({
            'employee_id': f"E{emp_counter:05}",
            'store_id': store,
            'employee_name': fake.name(),
            'role': role,
            'hourly_wage_mxn': wages[role],
            'max_weekly_hours': 48 # Current law before reform
        })
        emp_counter += 1
df_employees = pd.DataFrame(employees_data)
df_employees.to_csv('data/input/employees.csv', index=False)

# --- STEP 3: GENERATE TRAFFIC FORECAST ---
print("3. Generating Hourly Traffic Forecast...")
start_date = datetime(2026, 5, 3) 
dates = [start_date + timedelta(days=i) for i in range(7)]

traffic_data = []
for store, format_type in zip(df_stores['store_id'], df_stores['format']):
    base_traffic = {'Small': 50, 'Medium': 100, 'Large': 150}[format_type]
    for current_date in dates:
        day_of_week = current_date.strftime('%A')
        day_factor = 1.3 if day_of_week in ['Saturday', 'Sunday'] else 0.9 
        for hour in range(8, 22):
            is_peak = 1 if 17 <= hour <= 20 else 0
            hour_factor = 1.8 if is_peak else random.uniform(0.5, 1.2)
            customers = int(base_traffic * day_factor * hour_factor * random.uniform(0.9, 1.1))
            traffic_data.append({
                'store_id': store, 'date': current_date.strftime('%Y-%m-%d'),
                'day_of_week': day_of_week, 'hour': hour,
                'forecast_customers': customers, 'is_peak_hour': is_peak
            })
df_traffic = pd.DataFrame(traffic_data)
df_traffic.to_csv('data/input/traffic_forecast.csv', index=False)

# --- STEP 4: GENERATE STAFFING RULES ---
# --- STEP 4: GENERATE STAFFING RULES (WITH TARGETS) ---
print("4. Generating Staffing Rules with Optimal Service Targets...")
rules_data = [
    {'role': 'Cashier', 'customers_per_hour': 40, 'min_staff': 1, 'target_service_level': 0.95},
    {'role': 'Floor', 'customers_per_hour': 50, 'min_staff': 2, 'target_service_level': 0.90},
    {'role': 'Stock', 'customers_per_hour': 100, 'min_staff': 1, 'target_service_level': 0.85},
    {'role': 'Customer Service', 'customers_per_hour': 80, 'min_staff': 1, 'target_service_level': 0.92},
    {'role': 'Supervisor', 'customers_per_hour': 200, 'min_staff': 1, 'target_service_level': 0.98}
]
df_rules = pd.DataFrame(rules_data)
df_rules.to_csv('data/input/staffing_rules.csv', index=False)

# --- STEP 5: GENERATE QUANTITATIVE BASELINE SCHEDULE ---
print("5. Generating Demand-Driven Schedule (Optimal Service Level Logic)...")

def find_optimal_staffing(demands, target_level):
    """Calculates min staff k needed to meet target service level over a shift."""
    total_demand = sum(demands)
    if total_demand == 0: return 0
    for k in range(1, 30): # Checking capacity from 1 to 30 staff
        met_demand = sum(min(k, d) for d in demands)
        if (met_demand / total_demand) >= target_level:
            return k
    return 1 # Fallback to min

# 1. Calculate Hourly Demand
df_traffic['key'] = 1
df_rules['key'] = 1
demand_calc = pd.merge(df_traffic, df_rules, on='key').drop('key', axis=1)
demand_calc['hourly_req'] = np.ceil(demand_calc['forecast_customers'] / demand_calc['customers_per_hour'])
demand_calc['hourly_req'] = np.maximum(demand_calc['hourly_req'], demand_calc['min_staff'])

# 2. Define Shift Containers
shift_defs = [
    {'name': 'Morning', 'start': 8, 'end': 16},
    {'name': 'Evening', 'start': 14, 'end': 22}
]

schedule_data = []
emp_running_hrs = {emp_id: 0 for emp_id in df_employees['employee_id']}

# 3. Solve for each Store, Date, and Shift
for date in demand_calc['date'].unique():
    for store in df_stores['store_id']:
        for shift in shift_defs:
            # Filter demand for this specific 8-hour window
            shift_data = demand_calc[(demand_calc['date'] == date) & 
                                     (demand_calc['store_id'] == store) & 
                                     (demand_calc['hour'] >= shift['start']) & 
                                     (demand_calc['hour'] < shift['end'])]
            
            for role in df_rules['role']:
                role_demands = shift_data[shift_data['role'] == role]['hourly_req'].tolist()
                target_level = df_rules[df_rules['role'] == role]['target_service_level'].values[0]
                
                # Determine how many employees are needed for this 8-hour block
                needed_k = find_optimal_staffing(role_demands, target_level)
                
                # Assign specific employees from the store/role pool
                potential_staff = df_employees[(df_employees['store_id'] == store) & 
                                               (df_employees['role'] == role)]
                
                assigned = 0
                for _, emp in potential_staff.iterrows():
                    emp_id = emp['employee_id']
                    # Managers prioritize staff with available hours (<48) 
                    # but will go up to 56 to hit service targets
                    if assigned < needed_k and emp_running_hrs[emp_id] < 56:
                        schedule_data.append({
                            'store_id': store,
                            'employee_id': emp_id,
                            'role': role,
                            'date': date,
                            'shift_start': shift['start'],
                            'shift_end': shift['end'],
                            'scheduled_hours': 8,
                            'hourly_wage_mxn': emp['hourly_wage_mxn']
                        })
                        emp_running_hrs[emp_id] += 8
                        assigned += 1

df_schedule = pd.DataFrame(schedule_data)

# --- RECALCULATE COSTS WITH OVERTIME PENALTIES ---
# Calculate total weekly hours per employee
weekly_summary = df_schedule.groupby('employee_id')['scheduled_hours'].sum().reset_index()
weekly_summary.columns = ['employee_id', 'total_weekly_hrs']

df_schedule = df_schedule.merge(weekly_summary, on='employee_id')

# Apply Mexican Overtime Logic:
# Total Cost = (Regular Hours * Wage) + (Overtime Hours * Wage * 2.0)
def calculate_shift_cost(row):
    # This is a simplification: if they are over 48 for the week, 
    # we distribute the penalty across their shifts.
    reg_hrs = min(row['total_weekly_hrs'], 48)
    ot_hrs = max(row['total_weekly_hrs'] - 48, 0)
    
    total_weekly_cost = (reg_hrs * row['hourly_wage_mxn']) + (ot_hrs * row['hourly_wage_mxn'] * 2.0)
    
    # Cost per shift = Total Weekly Cost / Number of shifts worked
    num_shifts = row['total_weekly_hrs'] / 8
    return total_weekly_cost / num_shifts

df_schedule['labor_cost_mxn'] = df_schedule.apply(calculate_shift_cost, axis=1)
df_schedule = df_schedule.drop(columns=['total_weekly_hrs'])

# Save to CSV
df_schedule.to_csv('data/input/current_schedule.csv', index=False)