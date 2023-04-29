# Rule Engine MVP
#### This is an MVP implementation of the rule_engine program.
​
## Assumptions
#### The following assumptions have been made for this implementation:
​
1. The user database is of type PostgreSQL.
2. The database used in the code is of type MongoDB.
3. The user cannot apply a rule on the entire returned set from the query (on table), nor can they apply a rule on a condition. These two features are not implemented yet. Currently, rules can only be applied on a row-by-row basis. This is because when applying on the table or a condition, the entire system functionality (like severity levels and DDT checks) are disabled, so it did not make sense to include it in the MVP.
​
## Usage
The code takes one parameter, which is a JSON file that contains a list of JSON dictionaries. Each index in the list contains a JSON object that defines all the fields of a rule mentioned in the database schema design.
​
### The code flow is as follows:
- We read the rules and create the rule objects.
- We schedule the rules as jobs using Apscheduler that calls the function rule_engine.apply_rule on schedule for each rule.
- From there, the rule is applied on the database and processed by checking the
validity of the match, depending on if the DDT time passed or not. Later, the severity level is updated based on the expiration period and the severity level in the last added match for this (user, rule) pair. Then, the actions are performed and logged in the database.
​
## Notes on User Database Usage and Updates
The file database_engine is only created as a temporary component to simulate a database that gets updated with some information about the users that can be used to detect violations. Everything related to this component should be deleted upon connecting a real live database to this code.
​
Since we added the component that takes care of updating the database, we had to add the following three fields in the rule definition (these additions are just temporary and should be removed once connected to a real live database):
​
- "table_name": "drivers_order_response"
- "updates_fp": "data/updates_DDO.json"
- "round": 0
​
The data/updates_RULEACRYNM.json is a file that has the updates that should be added to the database at each round for this rule. The round variable keeps track of which round the rule is in now. The table indicates which table to add data to in the database.
​
## Limitations
#### The code has the following limitation:
​
When specifying the cron and DDT for a rule, do not make them a multiple of each other, as that does not give any marginal error time for the scheduler to be late on calling a specific rule to be applied. They should be at least one second in difference.