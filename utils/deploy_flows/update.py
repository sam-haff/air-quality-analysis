from dotenv import load_dotenv
import kestra_lib as kestra 

load_dotenv()

flows_dir = './../../kestra/flows/'
flow_to_update = flows_dir + input('Enter the flow file name(shold be present in airq/kestra/flows): \n')

kestra.update_flow(flow_to_update)
