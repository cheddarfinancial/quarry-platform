from chassis.models import Workflow
from runner import runWorkflow
wf = Workflow.query.filter(Workflow.id == 2).first()
runWorkflow(wf)
