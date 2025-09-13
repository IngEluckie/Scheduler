# recurrentes.py

# Import libraries
from dataclasses import dataclass, field
import asyncio
import json
from datetime import datetime, time
from enum import Enum
from typing import Any, Optional, List, Dict, Callable, Awaitable
try:
    # Requiere Python 3.9+
    from zoneinfo import ZoneInfo
    MX_TZ = ZoneInfo("America/Mexico_City")
except Exception:
    MX_TZ = None

# Import modules
from routers.utilities.terminalTools import CsvManager, Logger
from routers.utilities.ollamaCalls import rapidaUmigus

# Variables
log: CsvManager = CsvManager("log") # Para log de errores, mensajes y demás...
mlog: Logger = Logger(log) # message log

activityLog: CsvManager = CsvManager("ActivityLog") # Log de actividades de este módulo
record: Logger = Logger(activityLog)

# --------- Helpers ---------
def now_mx() -> datetime:
    """Fecha/hora actual con zona de America/Mexico_City (o naive si no disponible)."""
    if MX_TZ:
        return datetime.now(MX_TZ)
    return datetime.now()

def ensure_tz(dt: Optional[datetime]) -> Optional[datetime]:
    """Asegura que la fecha tenga zona horaria de MX si existe y viene naive."""
    if dt is None:
        return None
    if MX_TZ and dt.tzinfo is None:
        return dt.replace(tzinfo=MX_TZ)
    return dt

def gen_id(prefix: str = "tsk") -> str:
    """ID corto estable por timestamp (suficiente para logs y correlación)."""
    ts = now_mx().strftime("%Y%m%d%H%M%S%f")
    return f"{prefix}_{ts}"

# --------- Dominios / Enums ---------
class Mode(str, Enum):
    AUTO = "AUTO"
    USER = "USER"

class Priority(str, Enum):
    LOW = "LOW"
    MED = "MED"
    HIGH = "HIGH"

class Status(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"

class Kind(str, Enum):
    TASK = "TASK"
    RECURRENT = "RECURRENT"

class Frequency(str, Enum):
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    EVERY_N_DAYS = "EVERY_N_DAYS"


# --------- FechaHora (helper para logs legibles) ---------
@dataclass
class FechaHora:
    """
    Helper para imprimir marcas de tiempo legibles en logs.
    Ejemplo: [11/Aug/2025 20:41:36]
    """
    registro: str = field(init=False)
    timestamp: datetime = field(default_factory=now_mx)

    def __post_init__(self):
        self.registro = self.timestamp.strftime("[%d/%b/%Y]")

# --------- Task puntual ---------
@dataclass
class Task:
    """
    Representa una tarea puntual (no recurrente).
    """
    title: str
    mode: Mode = Mode.USER
    due: Optional[datetime] = None
    priority: Priority = Priority.MED
    status: Status = Status.PENDING

    # Metadatos / gestión
    id: str = field(default_factory=gen_id)
    source: str = "manual"                   # p. ej., "obsidian:semana", "discord", "api"
    labels: List[str] = field(default_factory=list)
    raw_line: Optional[str] = None           # línea fuente (para trazabilidad)
    project_key: Optional[str] = None        # si pertenece a un Project

    # Control de ejecución automática
    retries: int = 0
    max_retries: int = 0

    # Auditoría
    created_at: datetime = field(default_factory=now_mx)
    updated_at: datetime = field(default_factory=now_mx)

    def __post_init__(self):
        # Normaliza zona horaria
        self.due = ensure_tz(self.due)
        self.created_at = ensure_tz(self.created_at) or now_mx()
        self.updated_at = ensure_tz(self.updated_at) or now_mx()

        # Validaciones leves
        if not isinstance(self.mode, Mode):
            # Permite inicializar con str y convertir
            try:
                self.mode = Mode(str(self.mode).upper())
            except Exception:
                self.mode = Mode.USER

        if not isinstance(self.priority, Priority):
            try:
                self.priority = Priority(str(self.priority).upper())
            except Exception:
                self.priority = Priority.MED

        if not isinstance(self.status, Status):
            try:
                self.status = Status(str(self.status).upper())
            except Exception:
                self.status = Status.PENDING


# --------- Recurrent (extiende Task) ---------
@dataclass
class Recurrent(Task):
    """
    Actividad recurrente. Genera instancias puntuales según frecuencia y parámetros.
    """
    frequency: Frequency = Frequency.DAILY
    interval: int = 1                   # cada N días (para EVERY_N_DAYS) o multiplicador de frecuencia
    byday: Optional[List[int]] = None   # 0=Lun ... 6=Dom (para WEEKLY)
    at: Optional[time] = None           # hora del día (ejecución)
    window_tolerance_min: int = 5       # tolerancia de ventana de ejecución

    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None

    def __post_init__(self):
        super().__post_init__()
        # Normaliza zona para next/last
        self.next_run = ensure_tz(self.next_run)
        self.last_run = ensure_tz(self.last_run)

        # Validación de dominio
        if not isinstance(self.frequency, Frequency):
            try:
                self.frequency = Frequency(str(self.frequency).upper())
            except Exception:
                self.frequency = Frequency.DAILY

        if self.interval < 1:
            self.interval = 1

        if self.byday is not None:
            # limpia valores inválidos
            self.byday = [d for d in self.byday if isinstance(d, int) and 0 <= d <= 6]


# --------- Project (agrupador) ---------
@dataclass
class Project:
    """
    Agrupa tareas bajo un mismo objetivo.
    """
    key: str                               # identificador corto (p. ej., "CENA_NAVIDAD")
    name: Optional[str] = None             # nombre legible
    task_ids: List[str] = field(default_factory=list)
    notes: str = ""

    created_at: datetime = field(default_factory=now_mx)
    updated_at: datetime = field(default_factory=now_mx)

    def __post_init__(self):
        self.created_at = ensure_tz(self.created_at) or now_mx()
        self.updated_at = ensure_tz(self.updated_at) or now_mx()


# --------- Registro (fila de ActivityLog) ---------
@dataclass
class Registro:
    """
    Fila estructurada para ActivityLog (CSV).
    NOTA: 'id' puede ser asignado por CsvManager (autoincrement); por eso es opcional.
    """
    kind: Kind                              # TASK | RECURRENT
    event: str                              # CREATED | START | END | DONE | FAILED | NOTIFIED | SKIPPED | SCHEDULED
    task_id: str
    when: datetime = field(default_factory=now_mx)
    message: str = ""
    data: Dict[str, Any] = field(default_factory=dict)

    id: Optional[int] = None                # asignado por el CSV si aplica

    # Compatibilidad con tu idea original de "tipo"
    @property
    def tipo(self) -> str:
        return self.kind.value

    def __post_init__(self):
        self.when = ensure_tz(self.when) or now_mx()
        if not isinstance(self.kind, Kind):
            try:
                self.kind = Kind(str(self.kind).upper())
            except Exception:
                self.kind = Kind.TASK

# Tools: functions and classes

class ObsidianTask:
    """
    Esta clase:
    - Busca un archivo "sem00al11mmyyyy.md" en un Vault Obsidian
    almacenado localmente en mi laptop.
    
    Este archivo es mi bitácora
    personal de actividades. Las actividades marcadas "- [x]" 
    (realizadas) no se incluirán en la lista de retorno.

    - Analiza el archivo y retorna una lista de Task's
    """

    _mac_path: str = ""
    #_w_path: str = ""

    pass

class ObsidianRecurrent:
    """
    Esta clase:
    - Busca un archivo "Recurrentes.md" en un Vault Obsidian
    almacenado localmente en mi laptop.

    En este archivo guardo las actividades recurrentes o
    recursivas que debo realizar. Si necesito información adicional,
    ahí mismo lo guardo. 

    - Analiza el archivo y retorna una lista de Task's
    """


    _mac_path: str = ""
    #_w_path: str = ""

    pass

# Scheduler

forced_tasks: dict = {
    "publicacionFbUmigus": {
        "funcion": rapidaUmigus,
        "mode": "AUTO",
        "due": "DAILY",
        "priority": "MED",
        "labels": ["forced", "umigus"],
        "source": "scheduler:forced",
    },
    "publicacionFbGenesys": "ñu"
}

# Inicializar lista de actividades del día
# Inicializar lista de actividades recurrentes del día
class Scheduler:

    """
    ESTA CLASE SINGLETON DEBE:

    - Al iniciar, siempre debe de recibir una lista de Task's y otra de Recurrent's.

    - Verificar en el doc activityLog cuales ya se han realizado para eliminarlas de
    las listas.

    - TODA ESTA LÓGICA TENDRÁ QUE SER DESARROLLADA MÁS ADELANTE: De las listas,
    separar las actividades que debo hacer yo (y mandar mensaje por Discord)
    de las actividades automáticas que la computadora debe realizar (y comenzar
    rutina de ejución).
    """

    # 1.- Solicitamos los registros con la fecha del día
    def _searchRegistries(self, field: str | FechaHora, exact: bool = False) -> list[Task]:
        result: list[list[str,]] = activityLog.searchRows(field)

        """
        EJEMPLO DE RESULTADO:
        registries = [['', 'TASK', 'SCHEDULED', 'tsk_20250911174030825396', '2025-09-11T17:40:30.825427-06:00', 'Programada desde test1()', '{"title": "Ir al veterinario"', 'mode: "USER"', 'due: "2025-09-11T15:00:00-06:00"', 'priority: "MED"', 'status: "PENDING"', 'labels: ["demo"', 'vet]', 'source: "test1"}']]
        """

        registries: list[Registro] = []
        for row in result: 
            #if row[2] == "DONE":
            #    continue
            try:
                # row[0] = id (puede estar vacío)
                reg_id = int(row[0]) if row[0].isdigit() else None

                # row[1] = kind
                kind = Kind(row[1])

                # row[2] = event
                event = row[2]

                # row[3] = task_id
                task_id = row[3]

                # row[4] = fecha/hora
                when = datetime.fromisoformat(row[4])

                # row[5] = message
                message = row[5]

                # row[6:] = diccionario serializado
                data_str = " ".join(row[6:])
                try:
                    data = json.loads(data_str.replace("'", '"'))
                except Exception:
                    data = {"raw": row[6:]}
                
                # Crear objeto Registro
                registro = Registro(
                    kind=kind,
                    event=event,
                    task_id=task_id,
                    when=when,
                    message=message,
                    data=data,
                    id=reg_id
                )
                registries.append(registro)

            except Exception as e:
                print(f"[WARN] No pude parsear fila: {row} ({e})")

        return registries
    
    def _getTasks(self, registries: list[Registro]) -> list[Task]:
        import re, json
        from datetime import datetime

        def _coerce_dt(v) -> Optional[datetime]:
            if isinstance(v, datetime):
                return ensure_tz(v)
            if isinstance(v, str) and v.strip():
                try:
                    return ensure_tz(datetime.fromisoformat(v))
                except Exception:
                    return None
            return None

        def _coerce_list(v) -> list[str]:
            if v is None:
                return []
            if isinstance(v, list):
                return [str(x).strip().strip('"').strip("'") for x in v if str(x).strip()]
            if isinstance(v, str):
                s = v.strip()
                # intenta JSON
                if (s.startswith("[") and s.endswith("]")) or ('","' in s):
                    try:
                        arr = json.loads(s.replace("'", '"'))
                        return _coerce_list(arr)
                    except Exception:
                        pass
                # separa por coma
                parts = [p.strip().strip('"').strip("'") for p in s.split(",")]
                return [p for p in parts if p]
            return [str(v)]

        def _from_raw(raw_tokens: list[str]) -> dict:
            """
            Intenta recuperar pares clave:valor de una lista sin JSON estricto.
            Soporta: title, mode, due, priority, status, labels, source, project_key, retries, max_retries
            """
            if not raw_tokens:
                return {}
            text = " ".join(str(t) for t in raw_tokens)

            out: dict = {}

            def grab_str(key: str) -> Optional[str]:
                # key: "VAL"
                m = re.search(rf'\b{key}\b\s*:\s*"([^"]+)"', text)
                if m:
                    return m.group(1).strip()
                # key: VAL (sin comillas)
                m = re.search(rf'\b{key}\b\s*:\s*([^\s,\}}]+)', text)
                if m:
                    return m.group(1).strip()
                return None

            def grab_int(key: str) -> Optional[int]:
                s = grab_str(key)
                if s is None:
                    return None
                try:
                    return int(s)
                except Exception:
                    return None

            # strings
            for k in ("title", "mode", "due", "priority", "status", "source", "project_key"):
                v = grab_str(k)
                if v is not None:
                    out[k] = v

            # ints
            for k in ("retries", "max_retries"):
                v = grab_int(k)
                if v is not None:
                    out[k] = v

            # labels: [ ... ]
            ml = re.search(r'labels\s*:\s*\[([^\]]+)\]', text)
            if ml:
                inside = ml.group(1)
                # divide por coma, limpia comillas/espacios
                labels = [p.strip().strip('"').strip("'") for p in inside.split(",") if p.strip()]
                out["labels"] = labels

            return out

        def _normalize_data(d: dict) -> dict:
            if not isinstance(d, dict):
                return {}
            # si viene {"raw": [...]}, intenta reconstruir
            if "raw" in d and isinstance(d["raw"], list):
                recovered = _from_raw(d["raw"])
                d = {**d, **recovered}
            # normalizaciones mínimas de tipos
            if "labels" in d:
                d["labels"] = _coerce_list(d.get("labels"))
            if "due" in d:
                d["due"] = _coerce_dt(d.get("due"))
            return d

        # 1) Ordena por tiempo para preservar created_at y quedarse con el estado más reciente
        regs = sorted(registries, key=lambda r: r.when)

        agg: dict[str, dict] = {}
        for r in regs:
            tid = r.task_id
            d = _normalize_data(r.data or {})
            if tid not in agg:
                agg[tid] = {
                    "first_when": ensure_tz(r.when) or now_mx(),
                    "last_when": ensure_tz(r.when) or now_mx(),
                    "data": dict(d),
                    "message": r.message,
                    "kind": r.kind,
                    "last_event": r.event,
                }
            else:
                agg[tid]["last_when"] = ensure_tz(r.when) or agg[tid]["last_when"]
                # último valor conocido gana
                agg[tid]["data"].update({k: v for k, v in d.items() if v is not None})
                agg[tid]["message"] = r.message or agg[tid]["message"]
                agg[tid]["last_event"] = r.event

        tasks: list[Task] = []
        for tid, a in agg.items():
            d = a["data"]

            title = d.get("title") or a.get("message") or tid
            mode = d.get("mode", Mode.USER)                 # Task.__post_init__ convierte str→Enum si hace falta
            due = d.get("due")                              # ya es dt o None
            priority = d.get("priority", Priority.MED)
            status = d.get("status", Status.PENDING)
            labels = _coerce_list(d.get("labels"))
            source = d.get("source", "activityLog")
            project_key = d.get("project_key")
            retries = d.get("retries", 0) or 0
            max_retries = d.get("max_retries", 0) or 0

            t = Task(
                title=title,
                mode=mode,
                due=due,
                priority=priority,
                status=status,
                id=tid,                                  # forzar el id del registro
                source=source,
                labels=labels,
                raw_line=d.get("raw_line"),
                project_key=project_key,
                retries=int(retries) if isinstance(retries, (int, str)) and str(retries).isdigit() else 0,
                max_retries=int(max_retries) if isinstance(max_retries, (int, str)) and str(max_retries).isdigit() else 0,
                created_at=a["first_when"],
                updated_at=a["last_when"],
            )
            tasks.append(t)

        print(tasks)
        return tasks


    def _routine(self, tasks: list[Task]) -> None:
        mlog.newLog("Scheduler ha recibido las tareas, iniciando rutina...")
        print()  # salto de línea

        forced_names = ("publicacionFbUmigus",)

        def _latest_by_title(name: str) -> Optional[Task]:
            # No la elimino, pero tampoo me estaba funcionando.
            # Propuestat por el GPT.
            cand = [t for t in tasks if (t.title or "").strip().lower() == name.lower()]
            if not cand:
                return None
            # Me quedo con la más reciente por updated_at (o created_at)
            return max(cand, key=lambda x: (x.updated_at or x.created_at))
        
        def _accion():
            try:
                    resultado = rapidaUmigus("")  # string
                    print(resultado)

                    # 3) Construye registro DONE (reutiliza task_id si existe; si no, crea uno)
                    tid= gen_id("tsk")

                    data = {
                        "title": name,
                        "mode": "AUTO",
                        "due": "DAILY",
                        "priority": "MED",
                        "status": "DONE",
                        "labels": ["forced", "umigus"],
                        "source":  "scheduler:forced",
                    }

                    reg = Registro(
                        kind=Kind.TASK,
                        event="DONE",
                        task_id=tid,
                        when=now_mx(),
                        message="DONE",
                        data=data,
                    )

                    # 4) Persistir en CSV exactamente con tu formato de fila
                    activityLog.addTopRow((
                        "",  # id vacío (tu parser ya lo tolera)
                        reg.kind.value,
                        reg.event,
                        reg.task_id,
                        reg.when.isoformat(),
                        reg.message,
                        json.dumps(reg.data, ensure_ascii=False),
                    ))

                    mlog.success("Propuesta de publicación Umigus ejecutada y registrada como DONE.")

                    if self._send_chat_message and self._channel_id:
                        asyncio.get_running_loop().create_task(
                            self._send_chat_message(resultado, self._channel_id)
                        )

            except Exception as e:
                mlog.error(f"Error ejecutando tarea forzada '{name}': {type(e).__name__}: {e}")

        flag: bool = False
        for name in forced_names:
            existing = activityLog.searchRows(name)
            print(f"Lista de tareas hechas: {existing}")

            # 1) Si ya hay una DONE hoy, omite
            if len(existing) == 0:
                # 2) Ejecuta acción forzada
                _accion()
            else:
                for r in existing:
                    if now_mx().date().isoformat() in r[4]:
                        flag = True
        
        if flag == False:
            _accion()
        else:
            mlog.info("- [x] Publicación FB Umigus ya realizada. Tarea ignorada")

                
    def _getRegistryRecord(self):
        pass


    def __init__(
            self,
            send_chat_message: Optional[Callable[[str, int], Awaitable[None]]] = None,
            channel_id: Optional[int] = None
        ) -> None:
        
        self._send_chat_message = send_chat_message
        self._channel_id = channel_id

        self.today = now_mx().date().isoformat()  # "YYYY-MM-DD"
        #print(self.today)
        registries = self._searchRegistries(self.today)
        tasks = self._getTasks(registries)
        self._routine(tasks)

    pass




# Private execution and testing
if __name__ == "__main__":
    
    # Ejecuta la prueba
    def test1():
        s = Scheduler()
    #test1()

    scheduler: Scheduler = Scheduler()
    pass
