"""
A publish-subscribe communication protocol for agents.

This protocol allows for decoupled communication between different components
(agents, tools) of the system. Instead of sending messages to a specific
receiver, components publish messages to a "topic". Other components can
"subscribe" to these topics to receive the messages.
"""

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from typing import Any, Awaitable, Callable, Coroutine, Dict, List, Optional

from pydantic import BaseModel, Field


class Message(BaseModel):
    """
    A standardized message for the publish-subscribe protocol.

    This message is designed to be validated by Pydantic and contains
    metadata for routing and context management.
    """

    sender: str
    """The name of the component sending the message."""

    topic: str
    """The topic to which the message is published (e.g., 'user.query', 'tool.result')."""

    payload: Dict[str, Any]
    """The actual content of the message."""

    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    """A unique identifier for the message."""

    conversation_id: Optional[str] = None
    """An optional ID to group messages belonging to the same conversation."""

    in_reply_to: Optional[str] = None
    """The ID of the message this is a reply to, if applicable."""

    timestamp: float = Field(default_factory=time.time)
    """The timestamp of when the message was created."""

    # Metadata for context management, as suggested.
    priority: float = Field(default=0.5, ge=0.0, le=1.0)
    """A priority score (0.0 to 1.0) for the message."""

    token_count: Optional[int] = None
    """The optional token count of the payload."""

    summary: Optional[str] = None
    """An optional one-sentence summary of the message."""


class AgentProtocol:
    """
    Manages the publish-subscribe communication between agents and tools.
    """

    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Callable[[Message], Coroutine[Any, Any, None]]]] = defaultdict(list)
        self._message_queue: asyncio.Queue[Message] = asyncio.Queue()
        self._is_running = False
        self._runner_task: asyncio.Task[None] | None = None

    def subscribe(self, topic: str, handler: Callable[[Message], Coroutine[Any, Any, None]]):
        """
        Subscribes a handler to a specific topic.

        Args:
            topic: The topic to subscribe to.
            handler: An async function that will be called with messages for this topic.
        """
        logging.info(f"Registering handler for topic: {topic}")
        self._subscribers[topic].append(handler)

    def unsubscribe(self, topic: str, handler: Callable[[Message], Coroutine[Any, Any, None]]):
        """Removes a handler from a topic."""
        if handler in self._subscribers.get(topic, []):
            logging.info(f"Unregistering handler for topic: {topic}")
            self._subscribers[topic].remove(handler)

    async def publish(self, message: Message):
        """Publishes a message to the protocol's queue."""
        logging.debug(f"Publishing message {message.message_id} to topic '{message.topic}'")
        await self._message_queue.put(message)

    async def _dispatch(self, message: Message):
        """
        Dispatches a message to all subscribers of its topic.
        Includes a wildcard '*' subscription for all messages.
        """
        subscribers = self._subscribers.get(message.topic, [])
        wildcard_subscribers = self._subscribers.get("*", [])

        all_handlers = subscribers + wildcard_subscribers
        if not all_handlers:
            logging.warning(f"No subscribers for topic: '{message.topic}'. Message ID: {message.message_id}")
            return

        for handler in all_handlers:
            try:
                await handler(message)
            except Exception:
                logging.exception(f"Error in handler for topic {message.topic} on message {message.message_id}")

    async def _run_loop(self):
        """The main loop that dequeues and dispatches messages."""
        self._is_running = True
        logging.info("Agent protocol runner starting...")
        while self._is_running:
            try:
                message = await self._message_queue.get()
                logging.debug(f"Dequeued message: {message.message_id} for topic '{message.topic}'")
                await self._dispatch(message)
                self._message_queue.task_done()
            except asyncio.CancelledError:
                logging.info("Agent protocol runner received cancellation request.")
                break
        self._is_running = False
        logging.info("Agent protocol runner has stopped.")

    def run(self):
        """Starts the protocol runner in a background task."""
        if not self._is_running and self._runner_task is None:
            self._runner_task = asyncio.create_task(self._run_loop())
        else:
            logging.warning("Protocol runner is already running.")

    def stop(self):
        """Signals the protocol runner to stop."""
        logging.info("Stopping agent protocol runner...")
        self._is_running = False
        if self._runner_task:
            self._runner_task.cancel()
            self._runner_task = None 