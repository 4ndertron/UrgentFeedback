workdays_per_quarter = 66
training_days = 1
project_time_reduction_rate = 0.80
affected_agents = {
    'default': {
        'count': 18,
        'minute_rate': 13 / 60,
        'process_minutes': 7.5 * 60,
        'train_minutes': 0.5 * 60
    },
    'd3': {
        'count': 6,
        'minute_rate': 15 / 60,
        'process_minutes': 5 * 60,
        'train_minutes': 0.5 * 60
    },
    'pre-default': {
        'count': 200,
        'minute_rate': 12 / 60,
        'process_minutes': 0.08 * 60,
        'train_minutes': 0.5 * 60
    }
}

maintenance_cost = 0
training_cost = 0

for group in affected_agents:
    org = affected_agents[group]
    maintenance_cost += org['count'] * org['minute_rate'] * org['process_minutes'] * workdays_per_quarter
    training_cost += org['count'] * org['minute_rate'] * org['train_minutes'] * training_days

print('The cost to train agents on the new process would be ${}'.format(training_cost))
print('The cost to maintain the project on a quarterly basis would be ${}'.format(maintenance_cost))

old_process_cost = 0
new_process_cost = 0



cost_recution = old_process_cost - new_process_cost
