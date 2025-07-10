# communication_protocol.py
import asyncio
import json
import logging
import uuid
import os
import tempfile
from typing import Dict, Any, Optional, Union, Callable, Coroutine
from datetime import datetime

# Basic message structure (can be expanded later)
class Message(dict):
    def __init__(self, sender: str, receiver: str, message_type: str, payload: Dict, 
                 conversation_id: Optional[str] = None, message_id: Optional[str] = None, 
                 in_reply_to: Optional[str] = None, timestamp: Optional[float] = None):
        super().__init__()
        self['message_id'] = message_id or str(uuid.uuid4())
        self['conversation_id'] = conversation_id
        self['sender'] = sender
        self['receiver'] = receiver
        self['message_type'] = message_type
        self['payload'] = payload
        self['in_reply_to'] = in_reply_to
        self['timestamp'] = timestamp or asyncio.get_event_loop().time()

    def to_json(self):
        return json.dumps(self, default=str)

# Simplified Protocol - focuses on message passing
class CommunicationProtocol:
    def __init__(self, assistant_agent_handler: Callable[[Message], Coroutine[Any, Any, None]]):
        """Initializes the communication protocol.

        Args:
            assistant_agent_handler: An async function (coroutine) that will process 
                                       incoming messages routed to the 'Assistance' agent.
                                       This handler is responsible for LLM routing.
        """
        # The handler for the main routing/assistant agent
        self.assistant_agent_handler = assistant_agent_handler
        self.message_queue = asyncio.Queue()
        self.running = False
        self._message_handlers: Dict[str, Callable[[Message], Coroutine[Any, Any, None]]] = {}

        # Register the core assistant handler
        self.register_handler("Assistance", self.assistant_agent_handler)
        # We might register other specific handlers if needed, but routing primarily goes via Assistance
        self.register_handler("CommunicationProtocol", self._handle_internal_message) 

        logging.info("Communication Protocol Initialized.")

    def register_handler(self, receiver_id: str, handler: Callable[[Message], Coroutine[Any, Any, None]]):
        """Registers a handler for a specific receiver ID."""
        logging.info(f"Registering handler for receiver: {receiver_id}")
        self._message_handlers[receiver_id.lower()] = handler

    async def send_message(self, message: Message):
        """Adds a message to the queue for processing."""
        if not isinstance(message, Message):
            logging.error(f"Invalid message type passed to send_message: {type(message)}")
            # Optionally convert dict to Message or raise error
            # For now, just log and enqueue if it looks like a dict
            if isinstance(message, dict):
                logging.warning("Attempting to enqueue dict as message, prefer using Message class.")
            else:
                 return

        logging.debug(f"Enqueueing message: {message.get('message_id')} for {message.get('receiver')}")
        await self._save_debug_message(message, "enqueued")
        await self.message_queue.put(message)

    async def _route_message(self, message: Message):
        """Routes a message to the appropriate registered handler."""
        receiver = str(message.get("receiver", "")).lower()
        handler = self._message_handlers.get(receiver)

        if handler:
            logging.debug(f"Routing message {message.get('message_id')} to handler for {receiver}")
            try:
                await handler(message)
            except Exception as e:
                logging.exception(f"Error executing handler for receiver {receiver} on message {message.get('message_id')}: {e}")
                # Optionally send an error message back to the original sender
                if message.get('sender'):
                    error_payload = {"error": f"Handler failed: {str(e)}", "original_message_id": message.get('message_id')}
                    error_msg = Message(sender="CommunicationProtocol", receiver=message['sender'], 
                                          message_type="error", payload=error_payload, 
                                          conversation_id=message.get('conversation_id'),
                                          in_reply_to=message.get('message_id'))
                    await self.send_message(error_msg)
        else:
            # Default to Assistant if no specific handler found? Or log error?
            # For now, let's assume specific registration is required or it's an error.
            logging.warning(f"No handler registered for receiver: {receiver}. Message ID: {message.get('message_id')}")
            # Optionally, route to assistant as a fallback?
            # assistant_handler = self._message_handlers.get("assistance")
            # if assistant_handler:
            #     logging.info(f"Routing message {message.get('message_id')} to Assistance handler as fallback.")
            #     await assistant_handler(message)

    async def _handle_internal_message(self, message: Message):
        """Handles messages directed specifically to the CommunicationProtocol itself."""
        logging.debug(f"CommunicationProtocol received internal message: {message}")
        # Add logic here if the protocol needs to handle specific commands itself (e.g., shutdown)
        pass

    async def run(self):
        """Starts the message processing loop."""
        self.running = True
        logging.info("Communication Protocol Runner starting...")
        while self.running:
            try:
                message = await self.message_queue.get()
                logging.debug(f"Dequeued message: {message.get('message_id')} for {message.get('receiver')}")
                await self._save_debug_message(message, "dequeued")
                await self._route_message(message)
                self.message_queue.task_done()
            except asyncio.CancelledError:
                logging.info("Message processing loop cancelled.")
                self.running = False
                break
            except Exception as e:
                logging.exception(f"Critical error in message processing loop: {e}")
                # Avoid busy-looping on critical errors
                await asyncio.sleep(1)
        logging.info("Communication Protocol Runner stopped.")

    def stop(self):
        """Signals the message processing loop to stop."""
        logging.info("Stopping Communication Protocol Runner...")
        self.running = False

    async def _save_debug_message(self, message: Union[Dict, Message], prefix: str = "message"):
        filename_for_error_log = "unknown_debug_file"
        try:
            debug_dir = os.path.join(tempfile.gettempdir(), "vector_debug_bouncebooks")
            os.makedirs(debug_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            msg_id_val = message.get('message_id', 'no_id')
            filename_for_error_log = f"{prefix}_{timestamp}_{msg_id_val}.json"
            filepath = os.path.join(debug_dir, filename_for_error_log)
            
            def robust_json_default(o):
                if isinstance(o, (datetime, uuid.UUID)):
                    return str(o)
                if hasattr(o, 'to_dict') and callable(o.to_dict):
                    return o.to_dict()
                if isinstance(o, set):
                    return list(o)
                # Fallback for other types that json.dumps might struggle with directly
                return f"<Unserializable: {type(o).__name__}> {repr(o)[:100]}..." 

            dict_to_dump = {k: v for k, v in message.items()} # Basic conversion to dict if it's a Message obj

            with open(filepath, 'w') as f:
                try:
                    json.dump(dict_to_dump, f, indent=2, default=robust_json_default)
                except TypeError as te_dump:
                    if "Circular reference detected" in str(te_dump):
                        logging.warning(f"Circular reference detected for {filename_for_error_log}. Dumping with simplified payload.")
                        simplified_dict = {}
                        for key, value in dict_to_dump.items():
                            if key == 'payload':
                                try:
                                    # Attempt to dump payload separately with robust default
                                    # This won't be written to file here, just a check for complexity
                                    json.dumps(value, default=robust_json_default) 
                                    simplified_dict[key] = value # It seems serializable enough with default
                                except TypeError:
                                    simplified_dict[key] = f"<Payload (circular or complex, stringified)>: {str(value)[:1000]}..."
                            else:
                                simplified_dict[key] = value
                        # Re-open in write mode to overwrite potentially partial file
                        with open(filepath, 'w') as f_safer:
                            json.dump(simplified_dict, f_safer, indent=2, default=robust_json_default)
                        logging.info(f"Successfully saved debug message {filename_for_error_log} with simplified payload after circular ref.")
                    else:
                        logging.error(f"TypeError during json.dump for {filename_for_error_log}: {te_dump}. Data: {dict_to_dump}", exc_info=False)
                        # Fallback: write a very basic representation if dump still fails
                        f.write(f"{{\n  \"error\": \"JSON dump failed with TypeError\",\n  \"message_id\": \"{msg_id_val}\",\n  \"details\": \"{str(te_dump)}\",\n  \"message_repr\": \"{repr(dict_to_dump)[:1000]}...\"\n}}")
            
        except Exception as e:
            logging.error(f"Failed to save debug message (filename: {filename_for_error_log}): {e}", exc_info=False)

# Example Usage (Conceptual - would be in your main setup)
async def example_assistant_handler(message: Message):
    print(f"Assistant Agent received message: {message}")
    # Here the assistant agent would use its LLM router to decide the next step
    # and potentially send new messages via `protocol.send_message`
    pass

async def main():
    # Configure logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    protocol = CommunicationProtocol(assistant_agent_handler=example_assistant_handler)
    
    # Start the protocol runner in the background
    runner_task = asyncio.create_task(protocol.run())
    
    # Example: Simulate sending a message from a 'Frontend' component
    initial_message = Message(
        sender="Frontend",
        receiver="Assistance", # Route to the main assistant
        message_type="user_query",
        payload={"query": "Analyze my data"},
        conversation_id="conv_123"
    )
    await protocol.send_message(initial_message)
    
    # Let it run for a bit
    await asyncio.sleep(2)
    
    # Stop the protocol
    protocol.stop()
    await runner_task # Wait for the runner to finish

# if __name__ == "__main__":
#     asyncio.run(main()) # Uncomment to run example