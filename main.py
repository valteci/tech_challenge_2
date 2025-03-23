import asyncio
import scrap

async def main(periodicidade: int) -> None:
    """
    Executa periodicamente uma tarefa de scraping de dados.

    Esta função inicia o processo de scraping por meio da função `scrap.start()`
    e repete essa execução de forma contínua a cada intervalo especificado (em horas).

    Parâmetros:
    -----------
    periodicidade : int
        O intervalo de tempo (em horas) entre cada execução da função de scraping.

    Exemplo de uso:
    ---------------
    Para executar o scraping a cada 24 horas:
    
        asyncio.run(main(24))

    Observação:
    ------------
    Essa função é projetada para ser executada de forma assíncrona e contínua,
    não havendo uma condição de parada interna.
    """
    while True:
        scrap.start()
        
        await asyncio.sleep(periodicidade * 3600)


asyncio.run(main(24))