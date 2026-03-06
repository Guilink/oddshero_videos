"""
RUN ALL — Orquestrador Completo
================================
Executa as 3 fases em sequência e gerencia os 2 posts diários.

Usado pela Railway via cron job:
    09:00 BRT → python run_all.py
    13:00 BRT → python run_all.py

O script detecta automaticamente se é o 1º ou 2º post do dia
e garante que não repita o mesmo jogo.

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
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

OUTPUT_DIR   = Path("output")
CONTROL_FILE = OUTPUT_DIR / "posted_today.json"
LOG_FILE     = OUTPUT_DIR / "run_log.txt"

OUTPUT_DIR.mkdir(exist_ok=True)


def log(msg: str):
    timestamp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def run_script(script: str) -> bool:
    """Executa um script Python e retorna True se bem-sucedido."""
    log(f"Iniciando: {script}")
    result = subprocess.run(
        [sys.executable, script],
        capture_output=False,   # mostra output em tempo real no Railway
    )
    if result.returncode != 0:
        log(f"ERRO em {script} — código {result.returncode}")
        return False
    log(f"OK: {script}")
    return True


def posts_today() -> int:
    """Retorna quantos posts já foram feitos hoje."""
    today = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")
    if not CONTROL_FILE.exists():
        return 0
    data = json.loads(CONTROL_FILE.read_text(encoding="utf-8"))
    if data.get("date") != today:
        return 0
    return len(data.get("match_ids", []))


def main():
    now_brt = datetime.now(ZoneInfo("America/Sao_Paulo"))
    log("=" * 54)
    log(f"RUN ALL — {now_brt.strftime('%Y-%m-%d %H:%M')} BRT")
    log("=" * 54)

    n_posts = posts_today()
    log(f"Posts realizados hoje: {n_posts}/2")

    if n_posts >= 2:
        log("Limite de 2 posts diários atingido. Encerrando.")
        return

    # ── Fase 1 ──
    log("--- FASE 1: Seleção do jogo + roteiro + áudio ---")
    if not run_script("video_pipeline_phase1.py"):
        log("FALHA na Fase 1. Abortando pipeline.")
        sys.exit(1)

    # ── Fase 2 ──
    log("--- FASE 2: Geração do vídeo ---")
    if not run_script("video_pipeline_phase2.py"):
        log("FALHA na Fase 2. Abortando pipeline.")
        sys.exit(1)

    # ── Fase 3 ──
    log("--- FASE 3: Upload para o YouTube ---")
    if not run_script("video_pipeline_phase3.py"):
        log("FALHA na Fase 3. Vídeo gerado mas não postado.")
        sys.exit(1)

    n_agora = posts_today()
    log(f"Pipeline concluído. Posts hoje: {n_agora}/2")
    log("=" * 54)


if __name__ == "__main__":
    main()