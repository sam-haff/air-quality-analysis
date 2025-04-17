
from dotenv import load_dotenv
import kestra_lib as kestra 

load_dotenv()

flows_dir = './../../kestra/flows/'
flow_to_deploy = flows_dir + input('Enter the flow file name(shold be present in airq/kestra/flows): \n')

kestra.deploy_flow(flow_to_deploy)
