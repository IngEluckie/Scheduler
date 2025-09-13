# Automata.py

# Import librarires
from multiprocessing import Process
from flask import Flask
import os

# Import modules
from routers.utilities.terminalTools import Logger, CsvManager
from routers.utilities.recurrentes2 import Scheduler
from personalServer import run_server
from discordBot import run_bot

log: CsvManager = CsvManager("log")
logger: Logger = Logger(log)

# Automata main
class Automata:

    def __init__(self, name: str= "Kallen") -> None:
        self.name: str= name
        self.flask_process = None
        self.discord_process = None
        (f"Instancia {self.name} se ha iniciado/reiniciado")

    def __start_flask(self) -> None:
        self.flask_process = Process(target=run_server)
        self.flask_process.start()  # ✅ Esta línea es esencial
        logger.newLog(f"{self.name} ha iniciado el servidor Flask.")

    def __start_discordBot(self) -> None:
        self.discord_process = Process(target=run_bot)
        self.discord_process.start()
        logger.newLog(f"{self.name} se está iniciando en Discord.")


    def main(self):
        # Start Flask instance
        self.__start_flask()
        self.__start_discordBot()
        try:
            # Mantener el proceso principal vivo mientras Flask esté corriendo
            self.flask_process.join()
            self.discord_process.join()
        except KeyboardInterrupt:
            logger.newLog("Automata detenido por el usuario.")
            logger.newLog("Discord Bot detenido por el usuario.")
            #self.shutdown()

        # Stard Discord bot

        # Start Schedule instance
        pass

if __name__ == "__main__":
    a = Automata()
    a.main()
    
# Short doc
# flask --app personalServer run

# flask --app personalServer run --debug 

#flask run --host=0.0.0.0 # Externally Visible Server
