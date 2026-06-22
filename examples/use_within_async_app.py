"""
异步服务内接入企业微信智能机器人 SDK 示例。

外部只需要调用 start() / stop()：
- start(): 启动机器人长连接后台任务
- stop(): 关闭机器人长连接并清理任务

以FastAPI接入为例：

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:

    import os
    from dotenv import load_dotenv
    load_dotenv()

    bot_id = os.getenv("WECHAT_BOT_ID")
    bot_secret = os.getenv("WECHAT_BOT_SECRET")
    target_id = os.getenv("WECHAT_BOT_TARGET_ID", "")
    notifier_id = os.getenv("WECHAT_BOT_NOTIFIER_ID", "")
    at_notifier_hook = os.getenv("WECHAT_BOT_AT_NOTIFIER_HOOK", "")
    if not bot_id or not bot_secret:
        raise RuntimeError("missing required env WECHAT_BOT_ID or WECHAT_BOT_SECRET")

    channel = WeComAiBotChannel(
        bot_id=bot_id,
        bot_secret=bot_secret,
        target_id=target_id,
        notifier_id=notifier_id,
        at_notifier_hook=at_notifier_hook,
    )

    await channel.start()
    try:
        yield
    finally:
        await channel.stop()
"""
from __future__ import annotations
import asyncio
import os
import signal
import sys
import logging
from datetime import datetime
from collections.abc import Awaitable, Callable
from contextlib import suppress
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from aibot import WSClient, WSClientOptions, generate_req_id

from types import SimpleNamespace

print_logger = SimpleNamespace(
    info=print,
    warn=print,
    error=print,
    debug=print,
)


def get_logger(name: str) -> logging.Logger:
    logger: logging.Logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    handler: logging.StreamHandler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
    )

    logger.addHandler(handler)
    logger.propagate = False

    return logger


biz_logger = get_logger("wecom async demo")
logger = biz_logger

AsyncEventHandler = Callable[..., Awaitable[None]]


def get_current_time():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


async def send_wecom_message(WEBHOOK_URL: str, content: str) -> bool:
    """使用webhook发送消息。可以用来实现@效果"""
    HAS_httpx = True
    try:
        import httpx
    except ImportError:
        HAS_httpx = False
    if not HAS_httpx:
        return False
    payload = {
        "msgtype": "text",
        "text": {
            "content": content,
        },
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            WEBHOOK_URL,
            json=payload,
        )

    response.raise_for_status()

    result = response.json()
    if result.get("errcode") != 0:
        raise RuntimeError(f"发送失败: {result}")

    return True


def parse_command(content: str) -> str | None:
    parts = content.strip().split()

    match parts:
        case [mention, command] if mention.startswith("@"):
            return command

        case _:
            return None


async def handle(content: str, quote_text: str = None) -> str:
    """方便后续接入 健康检查/环境检查/刷新缓存、配置 等功能。这种语法对py大版本有要求"""
    command = parse_command(content)
    match command:
        case "性能":
            return "暂未开放"
        case "环境":
            return "暂未开放"
        case "刷新配置":
            return "暂未开放"
        case "看看这个":
            return f"这个是 {quote_text}"
        case _:
            return "暂不支持该指令"


class WeComAiBotChannel:
    """
    企业微信智能机器人通道。
    接入业务中可能需要一个适配层来处理企业微信机器人、飞书机器人、webhook等各种渠道，因此因此这里用Channel来进行演示，演示真实接入
    """

    def __init__(
            self,
            bot_id: str,
            bot_secret: str,
            target_id: str = "",
            chat_type: str = "group",
            notifier_id: str = "",
            at_notifier_hook: str = "",
    ) -> None:
        """
        初始化微信机器人接入渠道
        :param bot_id: 机器人ID。必须
        :param bot_secret: 机器人密钥。必须
        :param target_id: 对应推送目标（群聊/个人）
        :param chat_type: 对话类型（同上）
        :param notifier_id: 兜底负责查看通知的成员。用于@
        :param at_notifier_hook: aibot不支持@，使用webhook机器人实现@
        """
        self._chat_type = chat_type
        self._target_id = target_id
        self._notifier_id = notifier_id
        self._at_notifier_hook = at_notifier_hook

        self._client = WSClient(
            WSClientOptions(
                bot_id=bot_id,
                secret=bot_secret,
                logger=logger
            )
        )

        # 嵌入在异步app环境中的事件绑定和触发都会有一些问题。这里做了异步兼容
        self._runner_task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None
        self._callback_tasks: set[asyncio.Task[Any]] = set()

        self._bind_events()

    @property
    def is_running(self) -> bool:
        return self._runner_task is not None and not self._runner_task.done()

    async def start(self) -> None:
        """启动机器人后台长连接。"""

        if self.is_running:
            logger.info("wecom aibot already started")
            return

        self._stop_event = asyncio.Event()
        self._runner_task = asyncio.create_task(
            self._run_until_stopped(),
            name="wecom-aibot-runner",
        )

        logger.info("wecom aibot started")

    async def stop(self) -> None:
        """停止机器人后台长连接。"""

        logger.info("wecom aibot stopping")

        if self._stop_event is not None:
            self._stop_event.set()

        self._client.disconnect()

        if self._runner_task is not None:
            try:
                await asyncio.wait_for(self._runner_task, timeout=5)
            except TimeoutError:
                logger.warn("wecom aibot runner stop timeout, cancelling")
                self._runner_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._runner_task

        await self._cancel_callback_tasks()

        self._runner_task = None
        self._stop_event = None

        logger.info("wecom aibot stopped")

    async def _run_until_stopped(self) -> None:
        if self._stop_event is None:
            logger.warn("wecom aibot run skipped reason=stop_event_missing")
            return

        try:
            logger.info("wecom client connecting")
            await self._client.connect()
            logger.info(f"wecom client connected connected={self._client.is_connected}")

            await self._stop_event.wait()
            logger.info("wecom stop event received")

        except asyncio.CancelledError:
            logger.warn("wecom aibot runner cancelled")
            raise

        except Exception as e:
            logger.error(f"wecom aibot runner failed error={e!r}")

        finally:
            self._client.disconnect()
            logger.info(f"wecom client disconnected connected={self._client.is_connected}")

    def _bind_events(self) -> None:
        @self._async_on("authenticated")
        async def on_authenticated() -> None:
            logger.info("wecom aibot authenticated")

            # 更推荐在认证成功后发启动消息。
            if self._target_id:
                await self.send_message(f"BOT started 时间:{get_current_time()}")

        @self._async_on("connected")
        async def on_connected() -> None:
            logger.info("wecom aibot connected")

        @self._async_on("disconnected")
        async def on_disconnected(reason: str) -> None:
            logger.warn(f"wecom aibot disconnected reason={reason!r}")

        @self._async_on("reconnecting")
        async def on_reconnecting(attempt: int) -> None:
            logger.warn(f"wecom aibot reconnecting attempt={attempt}")

        @self._async_on("error")
        async def on_error(error: Exception) -> None:
            logger.error(f"wecom aibot error error={error!r}")

        @self._async_on("message.text")
        async def on_text(frame: dict[str, Any]) -> None:
            await self._handle_text_message(frame)

    def _async_on(self, event_name: str) -> Callable[[AsyncEventHandler], AsyncEventHandler]:
        """把 async 事件处理器包装成 SDK 可接受的同步回调。"""

        def decorator(handler: AsyncEventHandler) -> AsyncEventHandler:
            @self._client.on(event_name)
            def wrapper(*args: Any, **kwargs: Any) -> None:
                self._create_callback_task(
                    handler(*args, **kwargs),
                    name=f"wecom-aibot-callback-{event_name}",
                )

            return handler

        return decorator

    def _create_callback_task(
            self,
            coro: Awaitable[Any],
            *,
            name: str,
    ) -> None:
        """统一调度异步回调，避免 SDK 直接执行 async handler。"""

        task = asyncio.create_task(coro, name=name)
        self._callback_tasks.add(task)

        def on_done(t: asyncio.Task[Any]) -> None:
            self._callback_tasks.discard(t)

            try:
                t.result()
            except asyncio.CancelledError:
                logger.warn(f"{name} cancelled")
            except Exception as e:
                logger.error(f"{name} failed error={e!r}")

        task.add_done_callback(on_done)

    async def _cancel_callback_tasks(self) -> None:
        if not self._callback_tasks:
            return

        logger.warn(f"wecom aibot cancelling callback tasks count={len(self._callback_tasks)}")

        tasks = list(self._callback_tasks)

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        self._callback_tasks.clear()

    async def _handle_text_message(self, frame: dict[str, Any]) -> None:
        body = frame.get("body", {})
        content = body.get("text", {}).get("content", "")
        quote_text = body.get("quote", {}).get("text", {}).get("content", "")
        req_id = frame.get("headers", {}).get("req_id")

        logger.info(f"wecom aibot text message received req_id={req_id!r} content={content!r}")

        stream_id = generate_req_id("stream")

        await self._client.reply_stream(
            frame,
            stream_id,
            "正在处理中...",
            False,
        )
        reply_text = await handle(content, quote_text=quote_text)
        await self.send_at_message()
        await self._client.reply_stream(
            frame,
            stream_id,
            reply_text,
            True,
        )

        logger.info(f"wecom aibot text message replied req_id={req_id!r} stream_id={stream_id!r}")

    async def send_at_message(self, user_id: str = None) -> None:
        user_id = user_id or self._notifier_id or ""
        if user_id and self._at_notifier_hook:
            at_msg = f"<@{self._notifier_id}> ️请查收"
            await send_wecom_message(self._at_notifier_hook, at_msg)

    async def send_message(self, message: str) -> None:
        if not self._target_id:
            logger.warn("wecom aibot send skipped reason=target_id_missing")
            return
        await self.send_at_message()
        body = {
            "chat_type": self._chat_type,
            "msgtype": "markdown",
            "mentioned_mobile_list": ["15290173171", "@all"],
            "mentioned_list": [self._notifier_id, "@all"],
            "markdown": {
                "content": f"**主动推送**\n{message}",
            },
        }

        await self._client.send_message(self._target_id, body)


async def main() -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    load_dotenv()

    bot_id = os.getenv("WECHAT_BOT_ID")
    bot_secret = os.getenv("WECHAT_BOT_SECRET")
    target_id = os.getenv("WECHAT_BOT_TARGET_ID", "")
    notifier_id = os.getenv("WECHAT_BOT_NOTIFIER_ID", "")
    at_notifier_hook = os.getenv("WECHAT_BOT_AT_NOTIFIER_HOOK", "")
    if not bot_id or not bot_secret:
        raise RuntimeError("missing required env WECHAT_BOT_ID or WECHAT_BOT_SECRET")

    channel = WeComAiBotChannel(
        bot_id=bot_id,
        bot_secret=bot_secret,
        target_id=target_id,
        notifier_id=notifier_id,
        at_notifier_hook=at_notifier_hook,
    )
    stop_event = asyncio.Event()

    def request_stop() -> None:
        logger.info("shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, request_stop)

    await channel.start()

    try:
        await stop_event.wait()
    finally:
        await channel.stop()


if __name__ == "__main__":
    asyncio.run(main())
