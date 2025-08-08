Steps for the agents:

How a query reaches the agent:

1. **UI (`src/web/web.py`)**: The Streamlit interface captures the user's query.  
2. **Web Server (`src/web/web.py`)**: The `ask_agent` function sends the query to the FastAPI backend.  
3. **API Client (`src/web/agent_runtime_client.py`)**: The `FastAPIEngineRuntime` class makes an HTTP request to the `/run_sse` endpoint on the FastAPI server.  
4. **API Server (`src/web/fast_api_app.py`)**: The FastAPI application receives the request at the `/run_sse` endpoint.  
5. **Agent Runner (`src/web/fast_api_runner.py` and `src/web/fast_api_app.py`)**: The runner identifies that the agent's code is in the `src/agents/data_agent` directory. It then loads the `agent.py` file from that directory and finds the `root_agent` object.  
6. **Agent (`src/agents/data_agent/agent.py`)**: The `root_agent`, which is an `LlmAgent`, receives the query and begins processing it using its defined tools and instructions.

The system follows a sequential workflow. Here's the agent-by-agent calling sequence:

1. **`root_agent` \-\> `crm_business_analyst_agent`**: The `root_agent` first consults the `crm_business_analyst_agent` to define metrics and data requirements based on the user's question.  
2. **`root_agent` \-\> `data_engineer`**: The `root_agent` then takes the output from the `crm_business_analyst_agent` and passes it to the `data_engineer` to write and execute a SQL query.  
3. **`root_agent` \-\> `bi_engineer_tool`**: Finally, the `root_agent` takes the SQL query from the `data_engineer` and passes it to the `bi_engineer_tool` to execute the query, return the data, and create a visualization.

After the `bi_engineer_tool` returns the data and visualization, the `root_agent` synthesizes the information and provides a final answer to the user.

This workflow is a linear chain of calls, with the `root_agent` acting as the orchestrator between the specialized agents. Each agent in the chain performs a specific task, and the output of one agent becomes the input for the next.

