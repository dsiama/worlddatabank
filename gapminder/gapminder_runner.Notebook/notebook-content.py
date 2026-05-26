# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "eb1de4c1-a5f7-4384-a481-f546d17e9c96",
# META       "default_lakehouse_name": "e_ext_ingestions_support_lh",
# META       "default_lakehouse_workspace_id": "be4cf150-0cbc-4153-abd4-999a080c1e9d",
# META       "known_lakehouses": [
# META         {
# META           "id": "eb1de4c1-a5f7-4384-a481-f546d17e9c96"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "979E8E16-F420-4D8A-A074-B75EE612B6FF",
# META       "workspaceId": "be4cf150-0cbc-4153-abd4-999a080c1e9d"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

#%%configure -f
#{"conf": {"spark.fabric.notebook.session.timeout": "72000"}}

local_updates = [{
        "group_order": 1,
        "task_order": 1,
        "key": "projects",
        "new_value": []
        }
        ]

nb_settings={
    "config_file":"gapminder",
    "local_updates":local_updates
}


running_local = True
log_correlation = {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if type(nb_settings) == type("") :
    nb_settings=json.loads(nb_settings)
    log_correlation = json.loads(log_correlation)

import sys 
import json
sys.path.append("/lakehouse/default/Files/Global_Libs")
from dap_ELogger import ELogger
from dap_fabric_semantic_tools import semantic_model_refresh_wait
from dap_fabric_config_tools import read_config_pipelines,handleError,build_call_nb_params,update_fabric_metadata,run_notebook_with_params,get_execution_plan


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    nb = "gapminder_runner"
    group = "main"

    errorState = None
    errorId = 0

    config_file = nb_settings["config_file"]
    local_updates = nb_settings["local_updates"]
    main_set = read_config_pipelines(config_file)
except Exception as e:
    handleError(10 , e, running_local, {"step": "pipeline config"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for upd in local_updates:
    g_ord = upd.get('group_order');t_ord = upd.get('task_order');k = upd.get('key');v = upd.get('new_value')
    main_set = update_fabric_metadata(main_set, key=k, new_value=v, group_order=g_ord, task_order=t_ord)
    print(f"✅ Aplicado: {k} -> {v} (G:{g_ord}, T:{t_ord})")

# main_set

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try: 
    pipeline_id = main_set["pipeline_id"]
    ws = main_set["workspace"]
    
    logLevel = main_set["log_Level"]["logLevel"] 
    printOnScreen= (main_set["log_Level"]["printOnScreen"] == 1)


except Exception as e:
    handleError(20 , e, running_local, {"step": "initial config"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  workspace, logLevel = 2, correlation_id="", log_path=None,spark=None,printOnScreen=False

elogger = None
if running_local:
    print(2)
    elogger = ELogger(workspace=ws,pipeline_id=pipeline_id,group=group, nb=nb ,logLevel=logLevel,printOnScreen=printOnScreen)
    elogger.log_event_start(message=f"group:{group}")
else: 
    elogger = ELogger(correlation=log_correlation, workspace=ws, nb=nb)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plan =  get_execution_plan(main_set)
for exec in plan:
    ge = exec['group_enabled'];te = exec['task_enabled'];go = exec['group_order'];to = exec['task_order'];gn = exec['group_name'];nb = exec['notebooks_name']
    gwnd = exec['group_run_withouth_new_data'];tt = exec['task_type'];ds = exec['dataset_name'];ws = exec['workspace_id'];timeout = exec['timeout']
    #print(f"{go:4d} {to:4d} {gn[:15]:<15} {nb}{ge}  {te}{tt}{ds}{gwnd}")
    print(f"{ge}{te} {go:4d} {to:4d} group:{gn[:15]:<15}  type:{tt[:15]:<15} name:{(nb + ds)[:30]:<30} new_data:{gwnd}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_data = 0
for exec in plan:
    ge = exec['group_enabled'];te = exec['task_enabled'];go = exec['group_order'];to = exec['task_order'];gn = exec['group_name'];nb = exec['notebooks_name']
    gwnd = exec['group_run_withouth_new_data'];tt = exec['task_type'];ds = exec['dataset_name'];ws = exec['workspace_id'];timeout = exec['timeout']

    msg= f"run {ge}{te} {go:4d} {to:4d} group:{gn[:15]:<15}  type:{tt[:15]:<15} name:{(nb + ds)[:30]:<30} new_data:{gwnd}"
    elogger.log_event_info(step_name = f"{'_' * 4} Start Group {gn}", message = f"{msg}")    
    print(f"{'*' * 5}  {msg}")

    if  ge != 1 or te !=1:
        print(f"  SKIP Group:{group} Task {nb}{ds} - {ge}{te}")
        continue

    if not (new_data == 1 or gwnd == 1):
        print(f"{'*' * 10} SKIP Task  {msg}  (no new data)")
        continue

    if tt == "notebook":            
        nb_params = build_call_nb_params(main_set,go,to)
        call_parameters = { "main_set":json.dumps(nb_params),"running_local":False, "log_correlation":json.dumps(elogger.get_correlation()) }                    
        print(json.dumps(nb_params, indent=4))
        run_result = run_notebook_with_params(nb, timeout, call_parameters)
        elogger.log_event_info(step_name = f"{'_' * 8} end notebook:{nb}", message = f"{run_result}")
        new_data = run_result.get("new_data",1) 
    
    if tt == "sm_refresh":
        x = semantic_model_refresh_wait(ds,ws,3,8)
        print(x)
        elogger.log_event_info(step_name = f"{'_' * 8} end sm_refresh sm:{ds} {ws} ", message = f"{''}")

elogger.log_event_Finish()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
