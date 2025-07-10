"""
Demonstrates the publish-subscribe capabilities of the AgentProtocol.

This example sets up a FastMCP server with multiple components that
communicate with each other by publishing messages to topics and subscribing
to those topics, without needing direct references to each other.

Workflow:
1. A "SupervisorAgent" subscribes to all topics ('*').
2. A "WorkerAgent" subscribes only to the 'tasks.start' topic.
3. A client calls the "start_new_task" tool.
4. The tool publishes a message to the 'tasks.start' topic.
5. Both the Supervisor and the Worker receive and process the message,
   logging their actions to the console.
"""
import asyncio
import logging

from mcp.server.fastmcp import Context, FastMCP

from src.communication_protocol.agent_protocol import Message

# --- Agent and Server Setup ---

# 1. Configure logging to see the output from agents and tools.
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("PubSubExample")


# 2. Define the handler for our "WorkerAgent".
async def worker_agent_handler(message: Message):
    """This agent performs the actual work when a task is started."""
    log.info(
        f"[WorkerAgent] Received a new task! Processing message on topic '{message.topic}'.\n"
        f"  - Task Details: {message.payload}\n"
        f"  - Sender: {message.sender}"
    )
    # In a real application, this handler would perform some long-running task.
    await asyncio.sleep(1)
    log.info(f"[WorkerAgent] Task '{message.payload.get('task_name')}' completed.")


# 3. Define the handler for our "SupervisorAgent".
async def supervisor_agent_handler(message: Message):
    """This agent logs all activity in the system for monitoring purposes."""
    log.info(
        f"[SupervisorAgent] Monitoring activity. Saw message on topic '{message.topic}'.\n"
        f"  - Message ID: {message.message_id}\n"
        f"  - Sender: {message.sender}"
    )


# 4. Create the FastMCP server instance.
mcp_server = FastMCP(name="pubsub-example-server")

# 5. Subscribe the handlers to topics in the AgentProtocol.
# The WorkerAgent only cares about starting tasks.
mcp_server.agent_protocol.subscribe("tasks.start", worker_agent_handler)
# The SupervisorAgent uses a wildcard '*' to listen to all messages.
mcp_server.agent_protocol.subscribe("*", supervisor_agent_handler)


# --- Tool Definition ---


@mcp_server.tool(
    name="start_new_task",
    description="Starts a new task by publishing a message to the 'tasks.start' topic.",
)
async def start_new_task_tool(task_name: str, task_description: str, ctx: Context) -> str:
    """
    This tool acts as a publisher. It creates a message and uses the context
    to access the server's agent_protocol to publish it.
    """
    log.info(f"[Tool] Publishing a new task: '{task_name}'")

    message_to_publish = Message(
        sender="start_new_task_tool",
        topic="tasks.start",
        payload={"task_name": task_name, "description": task_description},
        priority=0.8,
    )

    # Access the protocol via the context and publish the message.
    # Note: `ctx.fastmcp` gives access to the main server instance.
    await ctx.fastmcp.agent_protocol.publish(message_to_publish)

    return f"Task '{task_name}' has been published successfully. Check server logs for agent activity."


# --- Server Execution ---

if __name__ == "__main__":
    # To test this example:
    # 1. Run this file from the project root: `python -m examples.fastmcp.pubsub_example`
    # 2. In a separate terminal, use the MCP client to call the tool:
    #    `mcp call --uri "http://127.0.0.1:8000" start_new_task --args '{"task_name": "process_invoices", "task_description": "Process all pending invoices for the month"}'`
    # 3. Observe the log output in the server terminal. You will see messages from the tool,
    #    the SupervisorAgent, and the WorkerAgent, demonstrating the pub/sub flow.
    mcp_server.run() 