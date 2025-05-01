def list_pannes(data):
    return data[1]['pannes'] if data and len(data) > 1 else []

def filter_by_cause(list):
    causes = ['21', '22', '24', '25', '26']
    return [panne for panne in list if panne[7] in causes]

def main(ti):
    data = ti.xcom_pull(task_ids='run_task_1', key='data')
    list = list_pannes(data)
    list = filter_by_cause(list)