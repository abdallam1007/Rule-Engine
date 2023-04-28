import time
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

import rule_engine

'''
def scheduler(mongo_db, user_db, rule, updates_fp):
    # init the user database by adding data for the first round
    round = 0
    database_engine.insert_new_updates(user_db, round, updates_fp)

    while (True):
        sql_query = rule["sql_query"]
        rows = rule_engine.apply_rule_on_database(user_db, sql_query)
        rule_engine.process_rule_matches_by_row(mongo_db, rows, rule)
        database_engine.update_database(user_db, round, updates_fp)
        time.sleep(rule["cron"])
'''

def schedule_rule(scheduler, mongo_db, rule):
    # Parse the cron to extract hours, minutes and seconds
    cron = rule["cron"]
    hours, minutes, seconds = map(int, cron.split())

    # Create a new job that will run every h:m:s starting from the current time
    scheduler.add_job(rule_engine.apply_rule, 'interval',
                      hours=hours,
                      minutes=minutes,
                      seconds=seconds,
                      args=[mongo_db, rule])

def init_scheduler(mongo_db, rules):
    scheduler = BackgroundScheduler()
    
    # Create a job for each rule
    for rule in rules:
        schedule_rule(scheduler, mongo_db, rule)

    scheduler.start()


# * Notes:
# * This code works only for one rule, to make it work for multiple rules:
#       * change the round to be part of the rule
#       * instead of looping figure out how to use a crone