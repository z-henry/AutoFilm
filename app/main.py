#!/usr/bin/env python3
# encoding: utf-8
import asyncio
from sys import path
from os.path import dirname
path.append(dirname(dirname(__file__)))

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from contextlib import asynccontextmanager

from app.core import settings, logger
from app.extensions import LOGO
from app.modules import Alist2Strm, Ani2Alist

# FastAPI 导入
from fastapi import FastAPI,Request
import uvicorn

#任务队列
task_queue = asyncio.Queue()
scheduler = AsyncIOScheduler()

# 包装任务执行逻辑
async def task_worker():
    while True:
        task = await task_queue.get()
        try:
            await task()
        finally:
            task_queue.task_done()

# 将任务添加到队列
async def queue_task(task):
    await task_queue.put(task)

# 定义生命周期管理函数
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("启动任务工作协程...")
    asyncio.create_task(task_worker())

    logger.info("初始化 APScheduler...")

    # 添加 Alist2Strm 定时任务
    if settings.AlistServerList:
        logger.info("检测到 Alist2Strm 模块配置，正在添加至后台任务")
        for server in settings.AlistServerList:
            cron = server.get("cron")
            if cron:
                scheduler.add_job(
                    queue_task,
                    args=[Alist2Strm(**server).run],
                    trigger=CronTrigger.from_crontab(cron)
                )
                logger.info(f'{server["id"]} 已被添加至后台任务')
            else:
                logger.warning(f'{server["id"]} 未设置 Cron')
    else:
        logger.warning("未检测到 Alist2Strm 模块配置")

    # 添加 Ani2Alist 定时任务
    if settings.Ani2AlistList:
        logger.info("检测到 Ani2Alist 模块配置，正在添加至后台任务")
        for server in settings.Ani2AlistList:
            cron = server.get("cron")
            if cron:
                scheduler.add_job(
                    lambda s=server: asyncio.create_task(queue_task(Ani2Alist(**s).run())),
                    trigger=CronTrigger.from_crontab(cron)
                )
                logger.info(f'{server["id"]} 已被添加至后台任务')
            else:
                logger.warning(f'{server["id"]} 未设置 Cron')
    else:
        logger.warning("未检测到 Ani2Alist 模块配置")

    # 启动调度器
    scheduler.start()
    logger.info("APScheduler 调度器已启动")

    yield  # 应用运行期间

    # 关闭事件逻辑（如果需要）
    logger.info("应用程序即将关闭...")
    scheduler.shutdown()
    logger.info("APScheduler 调度器已关闭")
    
# 实例化 FastAPI，并传入 lifespan
app = FastAPI(lifespan=lifespan)

# API：定义一个用于立即触发任务的 POST 接口
@app.post("/trigger")
async def trigger_job(request: Request):
    data = await request.json()
    updates = data.get('Updates')
    if not updates or not isinstance(updates, list):
        reason = "'Updates' 不合法"
        logger.info(reason)
        return {"status": reason}, 400
    
    unique_servers = {}
    # 遍历 dst 中的每个元素，收集匹配的服务器，并按 'source_dir' 去重
    for item in updates:
        path_value = item.get('Path')
        if not path_value:
            logger.info(f'item 中缺少 "path" 字段，跳过此项')
            continue
        matched = False
        for server in settings.AlistServerList:
            if server["source_dir"] in path_value:
                matched = True
                logger.info(f'找到与 "{path_value}" 匹配的 Server {server["id"]}')
                # 如果该 'source_dir' 尚未添加到 unique_servers，添加之
                if server["source_dir"] not in unique_servers:
                    unique_servers[server["source_dir"]] = server
        if not matched:
            unmatched_items.append(path_value)
            logger.warn(f'未找到与 "{path_value}" 匹配的 Server')
    
    # 遍历去重后的服务器列表，执行任务
    for server in unique_servers.values():
        try:
            await queue_task(Alist2Strm(**server).run)
            logger.info(f'任务 {server["id"]} 已成功触发')
        except Exception as e:
            logger.error(f'执行任务 {server["id"]} 时发生错误：{e}')

    return {"status": "处理完成"}, 200

if __name__ == "__main__":    
    print(LOGO + str(settings.APP_VERSION).center(65, "="))
    logger.info(f"AutoFilm {settings.APP_VERSION}启动中...")
    
     # 启动 FastAPI 应用
    logger.info("启动 FastAPI 服务器...")
    try:
        uvicorn.run(app, host="0.0.0.0", port=8648)
    except (KeyboardInterrupt, SystemExit):
        logger.info("AutoFilm程序退出！")
