"""
VIDEO PIPELINE — FASE 3
========================
Faz upload do vídeo gerado pela Fase 2 para o YouTube Shorts.

Funcionalidades:
- Título e descrição gerados automaticamente com dados do jogo
- Marcado como Short (#Shorts)
- Controle de jogos já postados hoje (evita repetição entre posts das 09h e 13h)
- Token OAuth2 lido de variável de ambiente (Railway) ou arquivo local

Variáveis de ambiente necessárias:
    YOUTUBE_TOKEN           → conteúdo do token.json (gerado pelo auth_youtube.py)
    YOUTUBE_CLIENT_SECRETS  → conteúdo do client_secrets.json

Uso:
    python video_pipeline_phase3.py
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ── Config ────────────────────────────────────────────────────────────────────

OUTPUT_DIR   = Path("output")
CONTROL_FILE = OUTPUT_DIR / "posted_today.json"   # controle de jogos já postados

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]


# ── Autenticação ──────────────────────────────────────────────────────────────

def get_credentials() -> Credentials:
    """
    Carrega credenciais OAuth2.
    Tenta primeiro variáveis de ambiente (Railway),
    depois arquivos locais (desenvolvimento).
    """
    token_json   = os.environ.get("YOUTUBE_TOKEN")
    secrets_json = os.environ.get("YOUTUBE_CLIENT_SECRETS")

    # Fallback para arquivos locais
    if not token_json and Path("token.json").exists():
        token_json = Path("token.json").read_text(encoding="utf-8")
        print("[AUTH] Usando token.json local")
    if not secrets_json and Path("client_secrets.json").exists():
        secrets_json = Path("client_secrets.json").read_text(encoding="utf-8")
        print("[AUTH] Usando client_secrets.json local")

    if not token_json:
        print("[ERRO] YOUTUBE_TOKEN não encontrado.")
        print("       Rode auth_youtube.py primeiro para gerar o token.")
        sys.exit(1)

    token_data   = json.loads(token_json)
    secrets_data = json.loads(secrets_json) if secrets_json else {}

    web = secrets_data.get("installed") or secrets_data.get("web", {})

    creds = Credentials(
        token         = token_data.get("token"),
        refresh_token = token_data.get("refresh_token"),
        token_uri     = token_data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id     = token_data.get("client_id") or web.get("client_id"),
        client_secret = token_data.get("client_secret") or web.get("client_secret"),
        scopes        = token_data.get("scopes", SCOPES),
    )

    # Renova automaticamente se expirado
    if creds.expired and creds.refresh_token:
        print("[AUTH] Renovando token expirado...")
        creds.refresh(Request())
        print("[AUTH] Token renovado com sucesso.")

    return creds


# ── Controle de jogos já postados hoje ───────────────────────────────────────

def load_posted_today() -> dict:
    """Carrega o controle de posts do dia. Reseta se for um novo dia."""
    today = datetime.now().strftime("%Y-%m-%d")
    if CONTROL_FILE.exists():
        data = json.loads(CONTROL_FILE.read_text(encoding="utf-8"))
        if data.get("date") == today:
            return data
    # Novo dia — reseta
    return {"date": today, "match_ids": [], "video_files": []}


def save_posted_today(data: dict):
    OUTPUT_DIR.mkdir(exist_ok=True)
    CONTROL_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def already_posted(match_id: str) -> bool:
    data = load_posted_today()
    return str(match_id) in [str(m) for m in data.get("match_ids", [])]


def mark_as_posted(match_id: str, video_file: str, youtube_id: str):
    data = load_posted_today()
    data["match_ids"].append(str(match_id))
    data["video_files"].append({
        "match_id":   str(match_id),
        "file":       video_file,
        "youtube_id": youtube_id,
        "posted_at":  datetime.now().isoformat(),
    })
    save_posted_today(data)


# ── Geração de título e descrição ────────────────────────────────────────────

def build_title(home_team: str, away_team: str, tip_display: str, league: str) -> str:
    """Gera título otimizado para YouTube Shorts (máx 100 chars)."""
    title = f"{home_team} x {away_team} | {tip_display} | {league}"
    if len(title) > 97:
        title = f"{home_team} x {away_team} | {tip_display}"
    if len(title) > 97:
        title = f"{home_team} x {away_team} | Análise IA"
    return title[:100]


def build_description(home_team: str, away_team: str, tip_display: str,
                       league: str, match_date: str, match_time: str) -> str:
    try:
        dt = datetime.strptime(match_date, "%Y-%m-%d")
        date_fmt = dt.strftime("%d/%m/%Y")
    except Exception:
        date_fmt = match_date

    return f"""⚽ {home_team} x {away_team}
🏆 {league}
📅 {date_fmt} às {match_time}

💡 TIP: {tip_display}

━━━━━━━━━━━━━━━━━━━
🔗 Acesse GRÁTIS no Telegram: @oddshero_bot
Nossa IA cruza estatísticas ao vivo com as melhores odds do momento.
━━━━━━━━━━━━━━━━━━━

#Futebol #ApostasEsportivas #Tips #Apostas #Betano #analisees #brasileirao
"""


# ── Upload para o YouTube ─────────────────────────────────────────────────────

def upload_video(video_path: Path, title: str, description: str,
                 creds: Credentials) -> str:
    """
    Faz upload do vídeo para o YouTube.
    Retorna o ID do vídeo publicado.
    """
    youtube = build("youtube", "v3", credentials=creds)

    body = {
        "snippet": {
            "title":       title,
            "description": description,
            "tags":        ["futebol", "apostas", "tips", "shorts", "IA",
                            "oddshero", "bet", "análise", "futebol ao vivo"],
            "categoryId":  "17",   # Sports
        },
        "status": {
            "privacyStatus":          "public",
            "selfDeclaredMadeForKids": False,
        },
    }

    media = MediaFileUpload(
        str(video_path),
        mimetype="video/mp4",
        resumable=True,
        chunksize=1024 * 1024 * 5,  # 5MB por chunk
    )

    print(f"[UPLOAD] Enviando: {video_path.name}")
    print(f"[UPLOAD] Título  : {title}")

    request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
    )

    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            pct = int(status.progress() * 100)
            print(f"  Progresso: {pct}%", end="\r")

    video_id = response["id"]
    print(f"\n[UPLOAD] Concluído!")
    print(f"[UPLOAD] URL: https://www.youtube.com/shorts/{video_id}")
    return video_id


# ── Pipeline principal ────────────────────────────────────────────────────────

def run_phase3():
    print("\n" + "=" * 52)
    print("  VIDEO PIPELINE — FASE 3 (YouTube Upload)")
    print("=" * 52 + "\n")

    # Carregar game_data
    gdp = OUTPUT_DIR / "game_data_latest.json"
    if not gdp.exists():
        gdp = OUTPUT_DIR / "game_data.json"
    if not gdp.exists():
        print("[ERRO] game_data não encontrado. Execute as Fases 1 e 2 primeiro.")
        sys.exit(1)

    with open(gdp, encoding="utf-8") as f:
        data = json.load(f)

    home_team   = data["home_team"]
    away_team   = data["away_team"]
    league      = data["league"]
    match_date  = data["match_date"]
    match_time  = data["match_time"]
    tip_display = data.get("tip_display") or data.get("tip", "")
    match_id    = data.get("match_id", data.get("run_ts", "unknown"))
    run_ts      = data.get("run_ts", "")

    print(f"  Jogo : {home_team} x {away_team}")
    print(f"  Tip  : {tip_display}\n")

    # Verificar se já foi postado hoje
    if already_posted(match_id):
        print(f"[AVISO] Jogo {match_id} já foi postado hoje. Abortando.")
        print("        O sistema de controle evitou duplicata.")
        sys.exit(0)

    # Encontrar o vídeo final mais recente
    videos = sorted(OUTPUT_DIR.glob("video_final_*.mp4"), reverse=True)
    if not videos:
        print("[ERRO] Nenhum video_final_*.mp4 encontrado em output/")
        print("       Execute a Fase 2 primeiro.")
        sys.exit(1)

    video_path = videos[0]
    size_mb = video_path.stat().st_size / (1024 * 1024)
    print(f"  Vídeo: {video_path.name} ({size_mb:.1f} MB)")

    # Verificar se vídeo do run atual bate com o game_data
    if run_ts and run_ts not in video_path.name:
        print(f"[AVISO] O vídeo mais recente ({video_path.name}) pode não corresponder")
        print(f"        ao game_data atual (run_ts: {run_ts}).")
        print(f"        Usando mesmo assim...\n")

    # Autenticar
    print("\n[AUTH] Autenticando com YouTube...")
    creds = get_credentials()
    print("[AUTH] OK\n")

    # Gerar metadados
    title       = build_title(home_team, away_team, tip_display, league)
    description = build_description(home_team, away_team, tip_display,
                                     league, match_date, match_time)

    # Upload
    youtube_id = upload_video(video_path, title, description, creds)

    # Marcar como postado
    mark_as_posted(match_id, video_path.name, youtube_id)

    print("\n" + "=" * 52)
    print("  FASE 3 CONCLUÍDA")
    print("=" * 52)
    print(f"\n  YouTube ID : {youtube_id}")
    print(f"  URL        : https://www.youtube.com/shorts/{youtube_id}")
    print(f"  Jogo       : {home_team} x {away_team}")
    print(f"  Tip        : {tip_display}\n")


if __name__ == "__main__":
    run_phase3()