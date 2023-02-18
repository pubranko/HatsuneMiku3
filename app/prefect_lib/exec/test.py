from prefect.agent.local import LocalAgent
from time import sleep

LocalAgent(
    labels=["crawler-container"],
    hostname_label=False,
).start()

# LocalAgent()._stop_agent_api_server()

LocalAgent().on_shutdown()
LocalAgent(
    labels=["crawler-container"],
    hostname_label=False,
).on_shutdown()
# LocalAgent().on_startup()
# LocalAgent().

# LocalAgent().agent_config()