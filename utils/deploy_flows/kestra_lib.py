import requests
from requests.auth import HTTPBasicAuth
import os
import yaml


def urljoin(*args, ispath=True):
    """
    Joins given arguments into an url. Trailing but not leading slashes are
    stripped for each argument.
    """

    res = "/".join(map(lambda x: str(x).strip('/'), args))
    if ispath and len(res) > 0:
        if res[-1] != '/':
            res = res + '/'
            
    return res

def update_flow(flow_path):
    inst_addr = os.environ['KESTRA_ADDR']
    kestra_user = os.environ['KESTRA_USER']
    kestra_pwd = os.environ['KESTRA_PWD']

    flow_content = ""

    with open(flow_path, 'rt') as f:
        flow_content = f.read()


    flow = yaml.load(flow_content, Loader=yaml.Loader)
    flow_id = flow['id']
    flow_namespace = flow['namespace']
    
    resp = requests.put(
        url=urljoin(inst_addr, f'/api/v1/flows/{flow_namespace}/{flow_id}', ispath=False), 
        data=flow_content, 
        auth=HTTPBasicAuth(kestra_user, kestra_pwd),
        headers= {
            "Content-Type": "application/x-yaml"
        }
    )

    if resp.status_code == 200:
        print(f'Flow {flow_path} succesfully updated!')
    else:
        print(f'Failed to update flow {flow_path}! Status code: {resp.status_code}')
        print('Response text:')
        print(resp.text)




def deploy_flow(flow_path):
    inst_addr = os.environ['KESTRA_ADDR']
    kestra_user = os.environ['KESTRA_USER']
    kestra_pwd = os.environ['KESTRA_PWD']

    flow_content = ""

    with open(flow_path, 'rt') as f:
        flow_content = f.read()
    
    print(flow_content)
        
    resp = requests.post(
        url=urljoin(inst_addr, '/api/v1/flows/company.team/', ispath=False), 
        data=flow_content, 
        auth=HTTPBasicAuth(kestra_user, kestra_pwd),
        headers= {
            "Content-Type": "application/x-yaml"
        }
    )

    if resp.status_code == 200:
        print(f'Flow {flow_path} succesfully deployed!')
    else:
        print(f'Failed to deploy flow {flow_path}! Status code: {resp.status_code}')
        print('Response text:')
        print(resp.text)

        print(resp.status_code)
        print(resp.text)

