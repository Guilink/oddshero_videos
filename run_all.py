"""
RUN ALL — Orquestrador Completo com Agendador Interno
======================================================
Fica rodando continuamente e executa o pipeline nos horários:
    09:00 BRT
    13:00 BRT
    17:00 BRT

Não precisa de cron externo no Railway.
Basta o Railway manter o container ativo (Start Command: python run_all.py).

Variáveis de ambiente necessárias na Railway:
    OPENAI_API_KEY
    API_KEY                  (apifootball)
    YOUTUBE_TOKEN            (gerado pelo auth_youtube.py)
    YOUTUBE_CLIENT_SECRETS   (conteúdo do client_secrets.json)
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

OUTPUT_DIR   = Path("output")
CONTROL_FILE = OUTPUT_DIR / "posted_today.json"
LOG_FILE     = OUTPUT_DIR / "run_log.txt"

OUTPUT_DIR.mkdir(exist_ok=True)

# Horários de execução (hora, minuto) no fuso de Brasília
SCHEDULE = [
    (11, 45),
    (13, 0),
    (17, 0),
]

MAX_POSTS_PER_DAY = 3


def log(msg: str):
    timestamp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def run_script(script: str) -> bool:
    log(f"Iniciando: {script}")
    result = subprocess.run(
        [sys.executable, script],
        capture_output=False,
    )
    if result.returncode != 0:
        log(f"ERRO em {script} — código {result.returncode}")
        return False
    log(f"OK: {script}")
    return True


def posts_today() -> int:
    today = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")
    if not CONTROL_FILE.exists():
        return 0
    try:
        data = json.loads(CONTROL_FILE.read_text(encoding="utf-8"))
        if data.get("date") != today:
            return 0
        return len(data.get("match_ids", []))
    except Exception:
        return 0


def run_pipeline():
    now_brt = datetime.now(ZoneInfo("America/Sao_Paulo"))
    log("=" * 54)
    log(f"RUN ALL — {now_brt.strftime('%Y-%m-%d %H:%M')} BRT")
    log("=" * 54)

    n_posts = posts_today()
    log(f"Posts realizados hoje: {n_posts}/{MAX_POSTS_PER_DAY}")

    if n_posts >= MAX_POSTS_PER_DAY:
        log("Limite de posts diários atingido. Pulando.")
        return

    log("--- FASE 1: Seleção do jogo + roteiro + áudio ---")
    if not run_script("video_pipeline_phase1.py"):
        log("FALHA na Fase 1. Abortando pipeline.")
        return

    log("--- FASE 2: Geração do vídeo ---")
    if not run_script("video_pipeline_phase2.py"):
        log("FALHA na Fase 2. Abortando pipeline.")
        return

    log("--- FASE 3: Upload para o YouTube ---")
    if not run_script("video_pipeline_phase3.py"):
        log("FALHA na Fase 3. Vídeo gerado mas não postado.")
        return

    log(f"Pipeline concluído. Posts hoje: {posts_today()}/{MAX_POSTS_PER_DAY}")
    log("=" * 54)


def already_ran_this_slot(hora: int, minuto: int) -> bool:
    """Verifica se já rodou neste slot hoje consultando o log."""
    today = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")
    slot_str = f"{today} {hora:02d}:{minuto:02d}"
    if not LOG_FILE.exists():
        return False
    content = LOG_FILE.read_text(encoding="utf-8")
    return slot_str in content


def main():
    log("Agendador iniciado. Aguardando horários: 11:35, 13:00, 17:00 BRT")

    # Rastreia qual slot já foi executado nesta sessão
    ran_slots = set()

    while True:
        now = datetime.now(ZoneInfo("America/Sao_Paulo"))
        hora_atual   = now.hour
        minuto_atual = now.minute
        hoje         = now.strftime("%Y-%m-%d")

        for (hora, minuto) in SCHEDULE:
            slot_key = f"{hoje}_{hora}_{minuto}"

            # Executa se estiver dentro de 1 minuto do horário e ainda não rodou
            if (hora_atual == hora and minuto_atual == minuto
                    and slot_key not in ran_slots):
                ran_slots.add(slot_key)
                run_pipeline()
                break

        # Limpa slots de dias anteriores do set
        ran_slots = {s for s in ran_slots if s.startswith(hoje)}

        # Verifica a cada 30 segundos
        time.sleep(30)


if __name__ == "__main__":
    main()