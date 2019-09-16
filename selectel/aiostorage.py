# -*- coding: utf-8 -*-


import hashlib
from datetime import datetime, timedelta
import aiohttp
import aiofiles
import asyncio

from typing import ByteString, Tuple, Union


class Storage(object):
    SUPPORTED_ARCHIVES = ("tar", "tar.gz", "tar.bz2")

    def update_expired(fn):
        async def wrapper(storage, *args, **kwargs):
            auth = storage.auth
            if auth.expired():
                await storage.authenticate()
            try:
                return await fn(storage, *args, **kwargs)
            except aiohttp.client_exceptions.ClientResponseError as err:
                if err.status == 401:
                    await storage.authenticate()
                    return await fn(storage, *args, **kwargs)
                else:
                    raise err
        return wrapper

    class Auth(object):
        THRESHOLD = 300

        def __init__(self, token: str, storage: str, expires: str) -> None:
            self.token = token
            self.storage = storage[:-1]
            self.expires = datetime.now() + timedelta(seconds=int(expires))

        def expired(self) -> bool:
            left = self.expires - datetime.now()
            return (left.total_seconds() < self.THRESHOLD)

    def __init__(self, user: str, key: str) -> None:
        self.url = "https://auth.selcdn.ru/"
        self.user = user
        self.key = key

        if not hasattr(asyncio, 'get_running_loop'):
            ioloop = asyncio.get_event_loop()
        else:
            ioloop = asyncio.get_running_loop()
        ioloop.run_until_complete(self.authenticate())

    async def authenticate(self) -> None:
        headers = {"X-Auth-User": self.user, "X-Auth-Key": self.key}
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            resp = await session.get(self.url, headers=headers)
            if resp.status != 204:
                raise Exception("Selectel: Unexpected status code: %s" %
                                r.status)
            headers = resp.headers

        auth = self.Auth(headers["X-Auth-Token"],
                         headers["X-Storage-Url"],
                         headers["X-Expire-Auth-Token"])
        self.auth = auth
        headers = {"X-Auth-Token": self.auth.token}
        session = aiohttp.ClientSession(headers=headers, raise_for_status=True)
        self.session = session

    @update_expired
    async def list(self,
                   container: object,
                   path: str = None,
                   prefix: str = None) -> dict:
        url = "%s/%s" % (self.auth.storage, container)
        params = {"format": "json"}
        if path is not None:
            if path.startswith("/"):
                path = path[1:]
            params["path"] = path
            if path == "":
                params["delimiter"] = "/"
        if prefix:
            params["prefix"] = prefix

        def mapper(obj):
            dt = datetime.strptime(obj["last_modified"] + " GMT",
                                   "%Y-%m-%dT%H:%M:%S.%f %Z")
            result = {
                "content-type": obj["content_type"],
                "content-length": obj["bytes"],
                "hash": obj["hash"],
                "last-modified": dt
            }
            return result

        clause = (lambda x: path != "" or "subdir" not in x)

        async with self.session.get(url, params=params) as resp:
            rjson = await resp.json()

        return {
            "/" + x["name"]: mapper(x) for x in rjson
            if clause(x)
        }

    @update_expired
    async def get(self,
                  container: object,
                  path: str, headers: str = None) -> str:
        url = "%s/%s%s" % (self.auth.storage, container, path)
        if headers is None:
            headers = {}
        async with self.session.get(url, headers=headers) as resp:
            rcont = await resp.text()
        return rcont

    @update_expired
    async def get_stream(self, container: object,
                         path: str, headers: str=None,
                         chunk_size: int=2**20):
        url = "%s/%s%s" % (self.auth.storage, container, path)
        if headers is None:
            headers = {}
        async with self.session.get(url, headers=headers) as resp:
            content = resp.content
            while True:
                chunk = await resp.content.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    @update_expired
    async def put(self, container: object,
                  path: str, content: ByteString,
                  headers: str=None,
                  extract: str=None) -> Union[None, Tuple[str, str]]:
        url = "%s/%s%s" % (self.auth.storage, container, path)
        if headers is None:
            headers = {}
        if extract in self.SUPPORTED_ARCHIVES:
            url += "?extract-archive=%s" % extract
            headers["Accept"] = "application/json"
        if not extract:
            headers["ETag"] = hashlib.md5(content).hexdigest()
        async with self.session.put(url, data=content, headers=headers) as resp:
            if extract in self.SUPPORTED_ARCHIVES:
                assert resp.status == 201
                answer = await resp.json()
                return (answer["Number Files Created"], answer["Errors"])
            else:
                assert resp.status == 201

    @update_expired
    async def put_stream(self, container: object,
                         path: str, descriptor,
                         headers: str=None, chunk=2**20,
                         extract: str=None) -> Union[None, Tuple[str, str]]:
        url = "%s/%s%s" % (self.auth.storage, container, path)
        if headers is None:
            headers = {}
        if extract in self.SUPPORTED_ARCHIVES:
            url += "?extract-archive=%s" % extract
            headers["Accept"] = "application/json"

        async def gen():
            data = descriptor.read(chunk)
            while data:
                yield data
                data = descriptor.read(chunk)

        async with self.session.put(url, data=gen(), headers=headers) as resp:
            if extract in self.SUPPORTED_ARCHIVES:
                assert resp.status == 200
                answer = await resp.json()
                return (answer["Number Files Created"], answer["Errors"])
            else:
                assert resp.status == 201

    @update_expired
    async def put_file(self, container: object,
                       path: str, filename: str,
                       headers: str=None,
                       extract: str=None) -> Union[None, Tuple[str, str]]:
        url = "%s/%s%s" % (self.auth.storage, container, path)
        chunk_size = 2 ** 20
        if headers is None:
            headers = {}
        if extract in self.SUPPORTED_ARCHIVES:
            url += "?extract-archive=%s" % extract
            headers["Accept"] = "application/json"

        async def file_sender():
            async with aiofiles.open(filename, 'rb') as fl:
                chunk = await fl.read(chunk_size)
                while chunk:
                    yield chunk
                    chunk = await fl.read(chunk_size)

        resp = await self.session.put(url, data=file_sender(),
                                        headers=headers)

        if extract in self.SUPPORTED_ARCHIVES:
            assert resp.status == 200
            answer = await resp.json()
            return (answer["Number Files Created"], answer["Errors"])
        else:
            assert resp.status == 201

    @update_expired
    async def remove(self, container, path: str, force: bool=False):
        url = "%s/%s%s" % (self.auth.storage, container, path)
        resp = await self.session.delete(url)
        if force:
            if resp.status == 404:
                return resp.headers
        assert resp.status == 204
        return resp.headers

    @update_expired
    async def copy(self, container, src, dst, headers=None) -> None:
        dst = "%s/%s%s" % (self.auth.storage, container, dst)
        src = "%s%s" % (container, src)
        if headers is None:
            headers = {}
        headers["X-Copy-From"] = src
        resp = await self.session.put(dst, headers=headers)
        assert resp.status == 201

    @update_expired
    async def info(self, container, path=None):
        if path is None:
            url = "%s/%s" % (self.auth.storage, container)
        else:
            url = "%s/%s%s" % (self.auth.storage, container, path)
        resp = await self.session.head(url)
        # XXX: according to documentation code should be 204
        assert resp.status == (200 if path else 204)
        if path is None:
            result = {
                "count": int(resp.headers["X-Container-Object-Count"]),
                "usage": int(resp.headers["X-Container-Bytes-Used"]),
                "public": (resp.headers.get("X-Container-Meta-Type") == "public"),
                "tx": int(resp.headers.get("X-Transfered-Bytes", 0)),
                "rx": int(resp.headers.get("X-Received-Bytes", 0))
            }
        else:
            dt = datetime.strptime(resp.headers["Last-Modified"],
                                   "%a, %d %b %Y %H:%M:%S %Z")
            result = {
                "content-length": int(resp.headers["Content-Length"]),
                "last-modified": dt,
                "hash": resp.headers["ETag"],
                "content-type": resp.headers["Content-Type"],
                "downloads": int(resp.headers.get("X-Object-Downloads", 0))
            }
        return result

    @update_expired
    async def create(self, container, public=False, headers=None):
        url = "%s/%s" % (self.auth.storage, container)
        if headers is None:
            headers = {}
        if public:
            headers["X-Container-Meta-Type"] = "public"
        else:
            headers["X-Container-Meta-Type"] = "private"
        resp = await self.session.put(url, headers=headers)
        assert resp.status in (201, 202)

    @update_expired
    async def drop(self, container, force=False, recursive=False):
        url = "%s/%s" % (self.auth.storage, container)
        if recursive:
            for filename in self.list(container):
                self.remove(container, filename, force=force)
        resp = await self.session.delete(url)
        if force:
            if resp.status == 404:
                pass
        assert resp.status == 204


class Container(object):
    METHODS = ["list", "get", "get_stream", "put",
               "put_stream", "put_file", "remove",
               "copy", "info"]

    def __init__(self, auth, key, name):
        self.name = name
        self.storage = Storage(auth, key)

        def make_method(name):
            async def method(*args, **kwargs):
                fn = getattr(self.storage, name)
                return await fn(self.name, *args, **kwargs)
            return method

        for name in self.METHODS:
            setattr(self, name, make_method(name))
