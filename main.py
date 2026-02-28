import asyncio, aiohttp.web, pathlib, builtins, uuid, io, posixpath, tarfile, math

async def main():
    app = aiohttp.web.Application()
    app.add_routes([aiohttp.web.static('/', pathlib.Path(__file__).resolve().parent, show_index=True)])
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, port=8080)
    await site.start()
    print(1)
    '''async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout()) as client:
        async with client.get(f'https://auth.docker.io/token?service=registry.docker.io&scope=repository:traffmonetizer/cli_v2:pull') as response:
            token = (await response.json()).get('token')
            async with client.get(f'https://registry-1.docker.io/v2/traffmonetizer/cli_v2/manifests/sha256:139266229af2341eb8dc8fb31bd44aab9f201c6e7403c85113943a08cbda7838', headers={'authorization':'Bearer ' + token, 'accept':'application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json'}) as manifests:
                async with client.get(posixpath.join('https://registry-1.docker.io/v2/traffmonetizer/cli_v2/blobs', (await manifests.json()).get('layers')[0].get('digest')), headers={'authorization':'Bearer ' + token}) as response:
                    tar = tarfile.open(mode='r:gz', fileobj=io.BytesIO(await response.content.read()))
                    member = tar.getmember('usr/local/bin/cli')
                    member.name = pathlib.Path(member.name).name
                    tar.extract(member, path=pathlib.Path(__file__).resolve().parent)
        await asyncio.create_subprocess_exec(pathlib.Path(__file__).resolve().parent.joinpath('cli'), 'start', 'accept', '--token', 'ELGPy/DEQYDtARslA6HnkrbPIF6JQi+qYLCre5LBe58=')
        await asyncio.create_subprocess_exec(pathlib.Path(__file__).resolve().parent.joinpath('bitpingd'))
        await asyncio.create_subprocess_exec(pathlib.Path(__file__).resolve().parent.joinpath('antgain'), '--api-key', 'R35Du7JnCiYCuW0Mk40XKltf8DyUmbVLIYCWlOLU31HMilYFrngVILF6XnJOPuz0', 'run', '-d')
        asyncio.create_task(wizardgain.run_client(builtins.str(uuid.uuid4()), 'chaowen.guo1@gmail.com', 'https://connector.wizardgain.com'))
        async with client.get('https://nodejs.org/dist/v24.13.1/node-v24.13.1-linux-x64.tar.xz') as node:
            tar = tarfile.open(mode='r:xz', fileobj=io.BytesIO(await node.content.read())) 
            for _ in tar.getmembers(): _.name = builtins.str(pathlib.Path('node', *pathlib.Path(_.name).parts[1:]))
            tar.extractall(path=pathlib.Path(__file__).resolve().parent)
        async with client.get('https://app-updates.sock.sh/peerclient/script/script.js') as script: pathlib.Path(__file__).resolve().parent.joinpath('script.js').write_bytes(await script.content.read())
        while True:
            async with client.get('https://app-updates.sock.sh/peerclient/script/version.txt') as version:
                node = await asyncio.create_subprocess_exec(pathlib.Path(__file__).resolve().parent.joinpath('node/bin/node'), pathlib.Path(__file__).resolve().parent.joinpath('script.js'), '--homeIp', 'point-of-presence.sock.sh', '--homePort', '443', '--id', 'wispbytecom', '--version', await version.text(), '--clientKey', 'proxyrack-pop-client', '--clientType', 'PoP')
                await node.wait()'''
    await asyncio.sleep(math.inf)

asyncio.run(main())
