#!/usr/bin/env python3
# encoding: utf-8

from typing import Final
from datetime import datetime

from json import loads
from aiohttp import ClientSession
from feedparser import parse

from app.core import logger
from app.utils import structure_to_dict, dict_to_structure, retry
from app.modules.alist import AlistClient, AlistStorage

VIDEO_MINETYPE: Final = frozenset(("video/mp4", "video/x-matroska"))
SUBTITLE_MINETYPE: Final = frozenset(("application/octet-stream",))
ZIP_MINETYPE: Final = frozenset(("application/zip",))
FILE_MINETYPE: Final = VIDEO_MINETYPE | SUBTITLE_MINETYPE | ZIP_MINETYPE

ANI_SEASION: Final = frozenset((1, 4, 7, 10))


class Ani2Alist:
    """
    将 ANI Open 项目的视频通过地址树的方式挂载在 Alist服务器上
    """

    def __init__(
        self,
        url: str = "http://localhost:5244",
        username: str = "",
        password: str = "",
        target_dir: str = "/Anime",
        latest: bool = True,
        year: int | None = None,
        month: int | None = None,
        src_domain: str = "aniopen.an-i.workers.dev",
        rss_domain: str = "api.ani.rip",
        key_word: str | None = None,
        **_,
    ) -> None:
        """
        实例化 Ani2Alist 对象

        :param origin: Alist 服务器地址，默认为 "http://localhost:5244"
        :param username: Alist 用户名，默认为空
        :param password: Alist 密码，默认为空
        :param target_dir: 挂载到 Alist 服务器上目录，默认为 "/"
        :param latest: 使用RSS追更最新番剧，默认为 True
        :param year: 动画年份，默认为空
        :param month: 动画季度，默认为空
        :param src_domain: ANI Open 项目地址，默认为 "aniopen.an-i.workers.dev"，可自行反代
        :param rss_domain ANI Open 项目 RSS 地址，默认为 "api.ani.rip"，可自行反代
        :param key_word: 自定义关键字，默认为空
        """
        self.__url = url
        self.__username = username
        self.__password = password
        self.__target_dir = "/" + target_dir.strip("/")
        self.__latest = latest

        self.__year = None
        self.__month = None
        self.__key_word = None

        if self.__latest:
            if self.__is_valid(year, month, key_word):
                self.__year = year
                self.__month = month
                self.__key_word = key_word
                if key_word is None:
                    logger.debug(f"传入时间{year}-{month}")
                else:
                    logger.debug(f"传入关键字{key_word}")
            else:
                if year is None and month is None:
                    logger.debug("未传入时间，将使用当前时间")
                else:
                    logger.warning(f"传入时间{year}-{month}不合理，将使用当前时间")

        self.__src_domain = src_domain.strip()
        self.__rss_domain = rss_domain.strip()

    async def run(self) -> None:
        def merge_dicts(target_dict: dict, source_dict: dict) -> dict:
            for key, value in source_dict.items():
                if key not in target_dict:
                    target_dict[key] = value
                elif isinstance(target_dict[key], dict) and isinstance(value, dict):
                    merge_dicts(target_dict[key], value)
                else:
                    target_dict[key] = value
            return target_dict

        current_season = self.__get_ani_season
        logger.info(f"开始更新ANI Open{current_season}季度番剧")

        if self.__latest:
            anime_dict = await self.get_rss_anime_dict
        else:
            anime_dict = await self.get_season_anime_list

        async with AlistClient(self.__url, self.__username, self.__password) as client:
            storage: AlistStorage | None = next(
                (
                    s
                    for s in await client.async_api_admin_storage_list()
                    if s.mount_path == self.__target_dir
                ),
                None,
            )
            if not storage:
                logger.debug(
                    f"在Alist服务器上未找到存储器{self.__target_dir}，开始创建存储器"
                )
                storage = AlistStorage(driver="UrlTree", mount_path=self.__target_dir)
                await client.async_api_admin_storage_create(storage)
                storage: AlistStorage | None = next(
                    (
                        s
                        for s in await client.async_api_admin_storage_list()
                        if s.mount_path == self.__target_dir
                    ),
                    None,
                )

            if storage:
                logger.debug(f"发现存储器{self.__target_dir}，开始更新番剧")
                addition_dict = storage.addition
                url_dict = structure_to_dict(addition_dict.get("url_structure", {}))

                if url_dict.get(current_season) is None:
                    url_dict[current_season] = {}

                url_dict[current_season] = merge_dicts(
                    url_dict[current_season], anime_dict
                )

                addition_dict["url_structure"] = dict_to_structure(url_dict)
                storage.change_addition(addition_dict)

                await client.sync_api_admin_storage_update(storage)
                logger.info(f"ANI Open{current_season}季度更新完成")
            else:
                logger.error(f"创建存储器后未找到存储器：{self.__target_dir}")

    def __is_valid(
        self, year: int | None, month: int | None, key_word: str | None
    ) -> bool:
        """
        判断传入的年月是否比当前时间更早

        :param year: 传入的年份
        :param month: 传入的月份
        :return: 如果传入的年月比当前时间更早，则返回 True；否则返回 False
        """
        if key_word:
            return True
        else:
            current_date = datetime.now()

            if year is None or month is None:
                return False
            if (year, month) == (2019, 4):
                logger.warning("2019-4季度暂无数据")
                return False
            elif (year, month) < (2019, 1):
                logger.warning("ANI Open项目仅支持2019年1月及其之后的数据")
                return False
            elif (year, month) > (current_date.year, current_date.month):
                logger.warning("传入的年月晚于当前时间")
                return False
            else:
                return True

    @property
    def __get_ani_season(self) -> str:
        """
        根据 self.__year 和 self.__month 以及关键字 self.__key_word 判断更新的季度
        """
        if self.__key_word:
            return self.__key_word

        current_date = datetime.now()
        if isinstance(self.__year, int) and isinstance(self.__month, int):
            year = self.__year
            month = self.__month
        else:
            year = current_date.year
            month = current_date.month

        for _month in range(month, 0, -1):
            if _month in ANI_SEASION:
                return f"{year}-{_month}"

    @property
    async def get_season_anime_list(self) -> dict:
        """
        获取指定季度的动画列表
        """
        current_season = self.__get_ani_season
        logger.debug(f"开始获取ANI Open{current_season}季度动画列表")
        url = f"https://{self.__src_domain}/{current_season}/"

        async with ClientSession() as session:

            @retry(Exception, tries=3, delay=3, backoff=2, logger=logger, ret={})
            async def parse_data(_url: str = url) -> dict:
                logger.debug(f"请求地址：{_url}")
                async with session.post(_url, json={}) as _resp:
                    if _resp.status != 200:
                        raise Exception(f"请求发送失败，状态码：{_resp.status}")
                    _result = loads(await _resp.text())

                    _anime_dict = {}
                    for file in _result["files"]:
                        mimeType: str = file["mimeType"]
                        name: str = file["name"]

                        if mimeType in FILE_MINETYPE:
                            size: int = file["size"]
                            logger.debug(f"获取文件：{name}，文件大小：{size}")
                            _anime_dict[name] = [
                                size,
                                f"{_url}{name}?d=true",
                            ]
                        elif mimeType == "application/vnd.google-apps.folder":
                            logger.debug(f"获取目录：{name}")
                            __url = _url + name + "/"
                            _anime_dict[name] = await parse_data(__url)
                        else:
                            raise RuntimeError(
                                f"无法识别类型：{mimeType}，文件详情：\n{file}"
                            )
                    return _anime_dict

            logger.debug(f"获取ANI Open{current_season}季度动画列表成功")
            return await parse_data()

    @property
    @retry(Exception, tries=3, delay=3, backoff=2, logger=logger, ret={})
    async def get_rss_anime_dict(self) -> dict:
        """
        获取 RSS 动画列表
        """

        def convert_size_to_bytes(size_str: str) -> int:
            """
            将带单位的大小转换为字节
            """
            units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
            number, unit = [string.strip() for string in size_str.split()]
            return int(float(number) * units[unit])

        logger.debug(f"开始获取ANI Open RSS动画列表")
        url = f"https://{self.__rss_domain}/ani-download.xml"
        rss_anime_dict = {}

        async with ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"请求发送失败，状态码：{resp.status}")
                feeds = parse(await resp.text())
                
        for item in feeds.entries:
            rss_anime_dict[item.title] = [
                convert_size_to_bytes(item.anime_size),
                item.link,
            ]

        logger.debug(f"获取RSS动画列表成功")
        return rss_anime_dict
