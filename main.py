import asyncio
import scrap

async def main(periodicidade: int) -> None:
    """

    """
    while True:
        scrap.start()
        
        await asyncio.sleep(periodicidade * 3600)


asyncio.run(main(24))