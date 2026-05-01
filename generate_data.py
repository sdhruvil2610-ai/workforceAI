import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta
import os

print("--- STARTING DATA GENERATOR ---")

# Create the folders safely
os.makedirs('data/input', exist_ok=True)
os.makedirs('data/output', exist_ok=True)

fake = Faker('es_MX')

# 1. GENERATE STORES
print("1/5: Generating 50 Retail Stores...")
cities = ['CDMX', 'Monterrey', 'Guadalajara', 'Queretaro', 'Merida', 'Puebla', 'Cancun', 'Tijuana']
stores_data = []
for i in range(1, 51):
    stores_data.append({
        'store_id': f"S{i:03}",
        'city': random.choice(cities),
        'format': random.choice(['Small', 'Medium', 'Large']),
        'open_hour': 8,
        'close_hour': 22
    })
df_stores = pd.DataFrame(stores_data)
df_stores.to_csv('data/input/stores.csv', index=False)

# 2. GENERATE EMPLOYEES
print("2/5: Generating 4,000 Employees...")
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
            'max_weekly_hours': 40 # Legal Cap
        })
        emp_counter += 1
df_employees = pd.DataFrame(employees_data)
df_employees.to_csv('data/input/employees.csv', index=False)

# 3. GENERATE TRAFFIC
print("3/5: Generating Hourly Traffic Forecast...")
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

# 4. GENERATE RULES
print("4/5: Generating Staffing Rules...")
rules_data = [
    {'role': 'Cashier', 'customers_per_hour': 40, 'min_staff': 1},
    {'role': 'Floor', 'customers_per_hour': 50, 'min_staff': 2},
    {'role': 'Stock', 'customers_per_hour': 100, 'min_staff': 1},
    {'role': 'Customer Service', 'customers_per_hour': 80, 'min_staff': 1},
    {'role': 'Supervisor', 'customers_per_hour': 200, 'min_staff': 1}
]
df_rules = pd.DataFrame(rules_data)
df_rules.to_csv('data/input/staffing_rules.csv', index=False)

# 5. GENERATE BASELINE SCHEDULE
print("5/5: Generating the 48-Hour Inefficient Baseline Schedule...")
shifts = [(8, 16), (14, 22)] 
schedule_data = []
for idx, emp in df_employees.iterrows():
    days_to_work = 6 if random.random() < 0.80 else 5
    work_days = random.sample(dates, days_to_work)
    for day in work_days:
        shift_start, shift_end = random.choice(shifts)
        schedule_data.append({
            'store_id': emp['store_id'], 'employee_id': emp['employee_id'],
            'role': emp['role'], 'date': day.strftime('%Y-%m-%d'),
            'shift_start': shift_start, 'shift_end': shift_end,
            'scheduled_hours': 8, 'hourly_wage_mxn': emp['hourly_wage_mxn']
        })
df_schedule = pd.DataFrame(schedule_data)

def calculate_baseline_cost(group):
    total_hours = group['scheduled_hours'].sum()
    wage = group['hourly_wage_mxn'].iloc[0]
    if total_hours > 40:
        regular_cost = 40 * wage
        overtime_cost = (total_hours - 40) * (wage * 2.0)
        group['labor_cost_mxn'] = (regular_cost + overtime_cost) / len(group)
    else:
        group['labor_cost_mxn'] = (total_hours * wage) / len(group)
    return group

df_schedule = df_schedule.groupby('employee_id', group_keys=False).apply(calculate_baseline_cost)
df_schedule.to_csv('data/input/current_schedule.csv', index=False)

print("--- SUCCESS! All 5 files are saved in 'data/input/' ---")