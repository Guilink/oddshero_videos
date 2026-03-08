"""
VIDEO PIPELINE — FASE 1 (SIMPLIFICADO)
=======================================
Seleciona o melhor jogo do dia, gera análise + tip com GPT-4o
e salva o game_data_latest.json para a Fase 2.

Uso:
    python video_pipeline_phase1.py

Saída:
    output/game_data_latest.json  ← entrada para a Fase 2

Variáveis de ambiente necessárias:
    OPENAI_API_KEY
    API_KEY   (apifootball)
"""

import asyncio
import json
import os
import aiohttp
from datetime import datetime
from zoneinfo import ZoneInfo
from openai import OpenAI
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")

# ── Config ───────────────────────────────────────────────────────────────────
OPENAI_API_KEY   = os.environ["OPENAI_API_KEY"]
FOOTBALL_API_KEY = os.environ["API_KEY"]
FOOTBALL_API_BASE = "https://apiv3.apifootball.com/?"
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Ligas prioritárias — quanto menor o índice, maior a prioridade
PRIORITY_LEAGUES = [
    "302",  # UEFA Champions League
    "175",  # UEFA Europa League
    "152",  # Premier League
    "302",  # La Liga
    "207",  # Serie A
    "175",  # Bundesliga
    "168",  # Ligue 1
    "244",  # Brasileirão Série A
    "245",  # Brasileirão Série B
    "143",  # Copa do Brasil
]


# ── API Football ──────────────────────────────────────────────────────────────

async def fetch_football_data(params: dict) -> list:
    params["APIkey"] = FOOTBALL_API_KEY
    url = FOOTBALL_API_BASE + "&".join(f"{k}={v}" for k, v in params.items())
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, dict) and "error" in data:
                    return []
                return data if isinstance(data, list) else [data]
            return []


async def get_upcoming_games_today() -> list:
    now   = datetime.now(ZoneInfo("America/Sao_Paulo"))
    today = now.strftime("%Y-%m-%d")
    params = {
        "action":   "get_events",
        "from":     today,
        "to":       today,
        "timezone": "America/Sao_Paulo",
    }
    games = await fetch_football_data(params)
    upcoming = [
        g for g in games
        if isinstance(g, dict) and g.get("match_status", "") == "Not Started"
    ]
    print(f"[API] {len(upcoming)} jogos futuros encontrados hoje")
    return upcoming


async def get_predictions(match_id: str) -> dict | None:
    params = {"action": "get_predictions", "match_id": match_id}
    data = await fetch_football_data(params)
    if isinstance(data, list) and data:
        return data[0]
    return None


async def get_h2h(team1_id: str, team2_id: str) -> dict | None:
    params = {"action": "get_H2H", "firstTeamId": team1_id, "secondTeamId": team2_id}
    data = await fetch_football_data(params)
    if isinstance(data, list) and data:
        return data[0]
    if isinstance(data, dict):
        return data
    return None


async def get_team_badge_url(team_id: str) -> str | None:
    params = {"action": "get_teams", "team_id": team_id}
    data = await fetch_football_data(params)
    if isinstance(data, list) and data:
        return data[0].get("team_badge")
    return None


# ── Seleção do jogo ───────────────────────────────────────────────────────────

def score_game(game: dict) -> int:
    score = 0
    league_id = str(game.get("league_id", ""))
    if league_id in PRIORITY_LEAGUES:
        score += 100 - PRIORITY_LEAGUES.index(league_id) * 10

    try:
        match_dt = datetime.strptime(
            f"{game['match_date']} {game['match_time']}", "%Y-%m-%d %H:%M"
        ).replace(tzinfo=ZoneInfo("America/Sao_Paulo"))
        now = datetime.now(ZoneInfo("America/Sao_Paulo"))
        hours_away = (match_dt - now).total_seconds() / 3600
        if 0 < hours_away <= 12:
            score += 20
    except Exception:
        pass

    return score


async def pick_best_game(games: list):
    sorted_games = sorted(games, key=score_game, reverse=True)

    for game in sorted_games[:15]:
        match_id    = str(game.get("match_id", ""))
        predictions = await get_predictions(match_id)
        if predictions:
            print(f"[SELEÇÃO] {game['match_hometeam_name']} x {game['match_awayteam_name']}")
            print(f"          Liga: {game.get('league_name')} | {game['match_date']} {game['match_time']}")
            return game, predictions

    print("[SELEÇÃO] Nenhum jogo com predictions. Usando o primeiro disponível.")
    if sorted_games:
        return sorted_games[0], None
    return None, None


# ── Formatação de dados para o prompt ────────────────────────────────────────

def format_predictions(pred: dict) -> str:
    if not pred:
        return "Probabilidades não disponíveis."
    mapping = {
        "prob_HW":  "Vitória Casa",
        "prob_D":   "Empate",
        "prob_AW":  "Vitória Fora",
        "prob_O":   "Over 2.5 Gols",
        "prob_U":   "Under 2.5 Gols",
        "prob_bts": "Ambos Marcam (Sim)",
        "prob_ots": "Ambos Marcam (Não)",
    }
    lines = []
    for key, label in mapping.items():
        val = pred.get(key)
        if val:
            try:
                lines.append(f"  {label}: {float(val):.1f}%")
            except Exception:
                pass
    return "\n".join(lines) if lines else "Probabilidades não disponíveis."


def format_h2h(h2h: dict | None) -> str:
    if not h2h:
        return "Histórico não disponível."
    lines = []

    vs_games = h2h.get("firstTeam_VS_secondTeam", [])[:5]
    if vs_games:
        lines.append("Últimos confrontos diretos:")
        for g in vs_games:
            lines.append(
                f"  {g.get('match_hometeam_name')} {g.get('match_hometeam_score','?')} x "
                f"{g.get('match_awayteam_score','?')} {g.get('match_awayteam_name')} ({g.get('match_date','')})"
            )

    for key, label in [("firstTeam_lastResults", "Casa"), ("secondTeam_lastResults", "Fora")]:
        recent = h2h.get(key, [])[:3]
        if recent:
            lines.append(f"\nÚltimos resultados ({label}):")
            for g in recent:
                lines.append(
                    f"  {g.get('match_hometeam_name')} {g.get('match_hometeam_score','?')} x "
                    f"{g.get('match_awayteam_score','?')} {g.get('match_awayteam_name')}"
                )
    return "\n".join(lines)


# ── Geração da análise com GPT-4o ─────────────────────────────────────────────

def generate_analysis(
    home_team: str,
    away_team: str,
    league: str,
    match_date: str,
    match_time: str,
    predictions_text: str,
    h2h_text: str,
) -> dict:
    client = OpenAI(api_key=OPENAI_API_KEY)

    system_prompt = """Você é um analista esportivo que cria análises curtas e objetivas de futebol
para serem exibidas num slide estilo Telegram.

A análise deve ter entre 3 e 5 frases curtas cobrindo:
- Contexto rápido do jogo
- Dado estatístico ou de forma recente relevante
- Justificativa para a tip

REGRAS:
- Texto direto, sem enrolação
- Sem emojis no script
- Sem mencionar que foi gerado por IA
- Frases curtas e claras
- Números podem ser normais (ex: 2.5, 75%)

tip_display: tip curta para exibir no slide (máx 5 palavras, números normais)
Exemplos: "Under 2.5 Gols" / "Vitória do {home_team}" / "Ambos Marcam" / "Over 1.5 Gols"
"""

    user_prompt = f"""Analise esta partida e gere a análise + tip:

JOGO: {home_team} x {away_team}
LIGA: {league}
DATA: {match_date} às {match_time}

PROBABILIDADES:
{predictions_text}

HISTÓRICO:
{h2h_text}

Responda EXCLUSIVAMENTE com JSON válido:

{{
  "script": "análise curta de 3 a 5 frases para exibir no slide",
  "tip_display": "tip curta para o slide, máx 5 palavras"
}}"""

    print("[GPT-4o] Gerando análise...")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        temperature=0.7,
        max_tokens=300,
        response_format={"type": "json_object"},
    )

    result      = json.loads(response.choices[0].message.content.strip())
    script      = result.get("script", "").strip()
    tip_display = result.get("tip_display", "Confira a análise").strip()

    print(f"[GPT-4o] Script : {script[:80]}...")
    print(f"[GPT-4o] Tip    : {tip_display}")
    return {"script": script, "tip_display": tip_display}


# ── Pipeline principal ────────────────────────────────────────────────────────

async def run_pipeline():
    print("\n" + "=" * 50)
    print("  VIDEO PIPELINE — FASE 1 (SIMPLIFICADO)")
    print("=" * 50 + "\n")

    # 1. Buscar jogos do dia
    games = await get_upcoming_games_today()
    if not games:
        print("[ERRO] Nenhum jogo encontrado para hoje.")
        return

    # 2. Escolher o melhor jogo
    game, predictions = await pick_best_game(games)
    if not game:
        print("[ERRO] Não foi possível selecionar um jogo.")
        return

    home_team  = game["match_hometeam_name"]
    away_team  = game["match_awayteam_name"]
    home_id    = str(game.get("match_hometeam_id", ""))
    away_id    = str(game.get("match_awayteam_id", ""))
    league     = game.get("league_name", "")
    match_date = game.get("match_date", "")
    match_time = game.get("match_time", "")

    # 3. Buscar H2H e badges em paralelo
    print("[API] Buscando H2H e badges...")
    h2h, home_badge, away_badge = await asyncio.gather(
        get_h2h(home_id, away_id),
        get_team_badge_url(home_id),
        get_team_badge_url(away_id),
    )
    print(f"  Badge casa : {home_badge}")
    print(f"  Badge fora : {away_badge}")

    # 4. Gerar análise + tip
    predictions_text = format_predictions(predictions)
    h2h_text         = format_h2h(h2h)
    result           = generate_analysis(
        home_team, away_team, league,
        match_date, match_time,
        predictions_text, h2h_text
    )

    # 5. Montar e salvar game_data
    game_data = {
        "run_ts":         RUN_TS,
        "home_team":      home_team,
        "away_team":      away_team,
        "league":         league,
        "match_date":     match_date,
        "match_time":     match_time,
        "home_badge_url": home_badge,
        "away_badge_url": away_badge,
        "script":         result["script"],
        "tip_display":    result["tip_display"],
        "tip":            result["tip_display"],  # compatibilidade Fase 2
    }

    # Salva com timestamp + atalho latest
    ts_path     = OUTPUT_DIR / f"game_data_{RUN_TS}.json"
    latest_path = OUTPUT_DIR / "game_data_latest.json"
    for path in [ts_path, latest_path]:
        path.write_text(json.dumps(game_data, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 50)
    print("  FASE 1 CONCLUÍDA")
    print("=" * 50)
    print(f"\n  Jogo : {home_team} x {away_team}")
    print(f"  Liga : {league}  |  {match_date} {match_time}")
    print(f"  Tip  : {result['tip_display']}")
    print(f"\n  Arquivo : {latest_path}\n")


if __name__ == "__main__":
    asyncio.run(run_pipeline())