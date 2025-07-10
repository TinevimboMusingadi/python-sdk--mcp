"""
Tests for the AgentProtocol publish-subscribe system.
"""

import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from src.communication_protocol.agent_protocol import AgentProtocol, Message

# Mark all tests in this file as asyncio tests
pytestmark = pytest.mark.anyio


@pytest.fixture
def protocol() -> AgentProtocol:
    """Provides a fresh instance of AgentProtocol for each test."""
    return AgentProtocol()


async def test_subscribe_and_publish(protocol: AgentProtocol):
    """
    Tests that a handler subscribed to a specific topic receives a message
    published to that topic.
    """
    # Use AsyncMock to create a mock coroutine function
    handler = AsyncMock()
    topic = "test.topic"
    message = Message(sender="test_sender", topic=topic, payload={"data": "value"})

    protocol.subscribe(topic, handler)
    protocol.run()

    await protocol.publish(message)
    await asyncio.sleep(0.01)  # Allow time for the message to be dispatched

    handler.assert_called_once_with(message)
    protocol.stop()


async def test_wildcard_subscription(protocol: AgentProtocol):
    """
    Tests that a handler subscribed to the wildcard topic '*' receives messages
    from any topic.
    """
    handler = AsyncMock()
    topic1 = "specific.topic.1"
    topic2 = "specific.topic.2"
    message1 = Message(sender="test", topic=topic1, payload={})
    message2 = Message(sender="test", topic=topic2, payload={})

    protocol.subscribe("*", handler)
    protocol.run()

    await protocol.publish(message1)
    await protocol.publish(message2)
    await asyncio.sleep(0.01)

    assert handler.call_count == 2
    handler.assert_any_call(message1)
    handler.assert_any_call(message2)
    protocol.stop()


async def test_multiple_subscribers(protocol: AgentProtocol):
    """
    Tests that multiple handlers subscribed to the same topic all receive the
    message.
    """
    handler1 = AsyncMock()
    handler2 = AsyncMock()
    topic = "shared.topic"
    message = Message(sender="test", topic=topic, payload={})

    protocol.subscribe(topic, handler1)
    protocol.subscribe(topic, handler2)
    protocol.run()

    await protocol.publish(message)
    await asyncio.sleep(0.01)

    handler1.assert_called_once_with(message)
    handler2.assert_called_once_with(message)
    protocol.stop()


async def test_no_subscribers(protocol: AgentProtocol, caplog):
    """
    Tests that a warning is logged when a message is published to a topic
    with no subscribers.
    """
    message = Message(sender="test", topic="unsubscribed.topic", payload={})
    protocol.run()

    with caplog.at_level(logging.WARNING):
        await protocol.publish(message)
        await asyncio.sleep(0.01)

    assert "No subscribers for topic: 'unsubscribed.topic'" in caplog.text
    protocol.stop()


async def test_unsubscribe(protocol: AgentProtocol):
    """
    Tests that a handler does not receive messages after unsubscribing.
    """
    handler = AsyncMock()
    topic = "test.topic"
    message = Message(sender="test", topic=topic, payload={})

    protocol.subscribe(topic, handler)
    protocol.unsubscribe(topic, handler)
    protocol.run()

    await protocol.publish(message)
    await asyncio.sleep(0.01)

    handler.assert_not_called()
    protocol.stop()


async def test_protocol_lifecycle(protocol: AgentProtocol):
    """
    Tests the run and stop methods to ensure the background task is managed
    correctly.
    """
    assert protocol._runner_task is None
    assert not protocol._is_running

    protocol.run()
    assert protocol._runner_task is not None
    assert protocol._is_running
    # Give the loop a chance to start
    await asyncio.sleep(0)

    protocol.stop()
    await asyncio.sleep(0.01)  # Allow time for the task to process cancellation

    assert protocol._runner_task is None
    assert not protocol._is_running
    # Verify the task was actually cancelled
    assert protocol._runner_task is None or protocol._runner_task.done()

async def test_dispatch_to_correct_subscriber(protocol: AgentProtocol):
    """
    Ensures that a message for a specific topic is not sent to a subscriber
    of a different topic.
    """
    handler_A = AsyncMock()
    handler_B = AsyncMock()
    topic_A = "topic.a"
    topic_B = "topic.b"
    message_A = Message(sender="test", topic=topic_A, payload={})

    protocol.subscribe(topic_A, handler_A)
    protocol.subscribe(topic_B, handler_B)
    protocol.run()

    await protocol.publish(message_A)
    await asyncio.sleep(0.01)

    handler_A.assert_called_once_with(message_A)
    handler_B.assert_not_called()
    protocol.stop() 