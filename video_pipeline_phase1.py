"""
VIDEO PIPELINE — FASE 1
========================
Seleciona o melhor jogo do dia, gera análise com GPT-4o,
cria o roteiro do vídeo e produz o áudio MP3 com OpenAI TTS.

Uso:
    python video_pipeline_phase1.py

Saída:
    output/audio_narracao.mp3
    output/roteiro.txt
    output/game_data.json   ← usado pela Fase 2 para gerar o vídeo

Variáveis de ambiente necessárias:
    OPENAI_API_KEY
    API_KEY   (apifootball)
"""

import asyncio
import json
import os
import aiohttp
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from openai import OpenAI
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Timestamp para nomear arquivos únicos por execução
RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")

# ── Config ──────────────────────────────────────────────────────────────────
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
FOOTBALL_API_KEY = os.environ["API_KEY"]
FOOTBALL_API_BASE = "https://apiv3.apifootball.com/?"
TTS_VOICE = "onyx"
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Ligas prioritárias (IDs da apifootball) — quanto menor o índice, maior a prioridade
PRIORITY_LEAGUES = [
    "302",   # UEFA Champions League
    "175",   # UEFA Europa League
    "152",   # Premier League
    "302",   # La Liga
    "207",   # Serie A
    "175",   # Bundesliga
    "168",   # Ligue 1
    "244",   # Brasileirão Série A
    "245",   # Brasileirão Série B
    "143",   # Copa do Brasil
]

# ── API Football ─────────────────────────────────────────────────────────────

async def fetch_football_data(params: dict) -> list | dict:
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
    """Busca todos os jogos de hoje com status 'Not Started'."""
    now = datetime.now(ZoneInfo("America/Sao_Paulo"))
    today = now.strftime("%Y-%m-%d")
    params = {
        "action": "get_events",
        "from": today,
        "to": today,
        "timezone": "America/Sao_Paulo",
    }
    games = await fetch_football_data(params)
    # Filtra apenas jogos que ainda não começaram
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
    """Retorna a URL do badge do time via get_teams."""
    params = {"action": "get_teams", "team_id": team_id}
    data = await fetch_football_data(params)
    if isinstance(data, list) and data:
        return data[0].get("team_badge")
    return None


# ── Seleção do jogo ──────────────────────────────────────────────────────────

def score_game(game: dict) -> int:
    """
    Pontua um jogo para decidir qual é mais relevante para o vídeo.
    Critérios: liga prioritária, horário próximo, tem predictions.
    """
    score = 0
    league_id = str(game.get("league_id", ""))
    if league_id in PRIORITY_LEAGUES:
        score += 100 - PRIORITY_LEAGUES.index(league_id) * 10

    # Preferir jogos nas próximas 12h
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


async def pick_best_game(games: list) -> dict | None:
    """Escolhe o jogo mais relevante que tenha predictions disponíveis."""
    sorted_games = sorted(games, key=score_game, reverse=True)

    for game in sorted_games[:15]:  # testa os 15 melhores candidatos
        match_id = str(game.get("match_id", ""))
        predictions = await get_predictions(match_id)
        if predictions:
            print(f"[SELEÇÃO] Jogo escolhido: {game['match_hometeam_name']} x {game['match_awayteam_name']}")
            print(f"          Liga: {game.get('league_name')} | {game['match_date']} {game['match_time']}")
            return game, predictions

    print("[SELEÇÃO] Nenhum jogo com predictions encontrado. Usando o primeiro disponível.")
    if sorted_games:
        game = sorted_games[0]
        return game, None
    return None, None


# ── Formatação de dados para o prompt ────────────────────────────────────────

def format_predictions_for_prompt(pred: dict) -> str:
    if not pred:
        return "Dados de probabilidades não disponíveis."

    lines = []
    mapping = {
        "prob_HW": "Vitória Casa",
        "prob_D": "Empate",
        "prob_AW": "Vitória Fora",
        "prob_O": "Over 2.5 Gols",
        "prob_U": "Under 2.5 Gols",
        "prob_bts": "Ambos Marcam (Sim)",
        "prob_ots": "Ambos Marcam (Não)",
    }
    for key, label in mapping.items():
        val = pred.get(key)
        if val:
            try:
                lines.append(f"  {label}: {float(val):.1f}%")
            except Exception:
                pass
    return "\n".join(lines) if lines else "Probabilidades não disponíveis."


def format_h2h_for_prompt(h2h: dict | None) -> str:
    if not h2h:
        return "Histórico de confrontos não disponível."
    
    lines = []
    vs_games = h2h.get("firstTeam_VS_secondTeam", [])[:5]
    if vs_games:
        lines.append("Últimos confrontos diretos:")
        for g in vs_games:
            lines.append(
                f"  {g.get('match_hometeam_name')} {g.get('match_hometeam_score','?')} x "
                f"{g.get('match_awayteam_score','?')} {g.get('match_awayteam_name')} ({g.get('match_date','')})"
            )

    team1_last = h2h.get("firstTeam_lastResults", [])[:3]
    team2_last = h2h.get("secondTeam_lastResults", [])[:3]

    if team1_last:
        lines.append(f"\nÚltimos resultados do time da casa:")
        for g in team1_last:
            lines.append(
                f"  {g.get('match_hometeam_name')} {g.get('match_hometeam_score','?')} x "
                f"{g.get('match_awayteam_score','?')} {g.get('match_awayteam_name')}"
            )

    if team2_last:
        lines.append(f"\nÚltimos resultados do time visitante:")
        for g in team2_last:
            lines.append(
                f"  {g.get('match_hometeam_name')} {g.get('match_hometeam_score','?')} x "
                f"{g.get('match_awayteam_score','?')} {g.get('match_awayteam_name')}"
            )

    return "\n".join(lines)
# ── Geração do roteiro com GPT-4o ────────────────────────────────────────────

def generate_script(
    home_team: str,
    away_team: str,
    league: str,
    match_date: str,
    match_time: str,
    predictions_text: str,
    h2h_text: str,
) -> dict:
    """
    Chama GPT-4o para gerar roteiro curto (20-30s) + dois campos de tip:
    - tip_display: para o SLIDE (curta, formatada, com números normais)
    - tip_narration: para o ÁUDIO (por extenso, natural para fala)
    """
    client = OpenAI(api_key=OPENAI_API_KEY)

    system_prompt = """Você cria roteiros curtos para YouTube Shorts sobre análise de futebol.
Os vídeos devem ser curtos, diretos e persuasivos, com aproximadamente 20 a 30 segundos de narração.

ESTRUTURA OBRIGATÓRIA (4 partes em sequência, texto corrido):

1. HOOK (primeira frase):
Explique rapidamente o jogo e chame atenção, sem exageros ou clichês.
Tom correto: "Esse jogo tem um claro favorito." / "Esse confronto tem números bem interessantes."
PROIBIDO: "Atenção pessoal", "Duelo explosivo", "Você não pode perder", "Jogão imperdível", "Fala galera"

2. ANÁLISE (2 a 4 frases curtas):
Entregue valor rapidamente: probabilidades, momento recente dos times, dado estatístico relevante.
Frases curtas. Sem linguagem técnica. Sem enrolação.

3. CONCLUSÃO + TIP:
Finalize com a tip principal e o motivo em uma frase.
Exemplo: "A melhor opção aqui é o under 2.5 gols, que aparece com 54% de probabilidade."

4. CALL TO ACTION (fixo, sempre igual):
"Você sabia que existe uma IA que analisa jogos de futebol para você gratuitamente? Acesse agora pelo link na bio e teste no Telegram."

REGRAS:
- Sem emojis (será narrado em voz alta)
- Sem linguagem exagerada ou sensacionalista
- Sem mencionar que foi criado por IA
- Sem frases genéricas de criador de conteúdo
- Frases curtas e naturais
- TODOS os números no script devem ser por extenso para soar natural na narração
  Exemplos: "2.5" → "dois vírgula cinco", "75%" → "setenta e cinco por cento"
- Nomes de times devem ser pronunciáveis"""

    user_prompt = f"""Crie o roteiro para esta partida:

JOGO: {home_team} x {away_team}
LIGA: {league}
DATA E HORA: {match_date} às {match_time}

PROBABILIDADES:
{predictions_text}

HISTÓRICO:
{h2h_text}

Responda EXCLUSIVAMENTE com JSON válido neste formato:

{{
  "script": "roteiro completo aqui, texto corrido, 20 a 30 segundos de narração",
  "tip_narration": "a tip para narrar, com números por extenso. Ex: under dois vírgula cinco gols",
  "tip_display": "a tip para exibir no slide, curta e clara. Ex: Under 2.5 Gols"
}}

tip_display deve ter no máximo 5 palavras e usar números normais (2.5, não por extenso).
Exemplos de tip_display: "Under 2.5 Gols" / "Vitória do {home_team}" / "Ambos Marcam" / "Over 1.5 Gols"

Exemplo de roteiro ideal:
"Esse jogo da V.League 2 tem um favorito claro. O {away_team} aparece com 58% de chance de vitória e vem de duas partidas fortes. Já o {home_team} vive um momento ruim. Com esse cenário, a melhor opção é o under dois vírgula cinco gols, que aparece com 54% de probabilidade. Você sabia que existe uma IA que analisa jogos de futebol para você gratuitamente? Acesse agora pelo link na bio e teste no Telegram." """

    print("[GPT-4o] Gerando roteiro...")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.7,
        max_tokens=500,
        response_format={"type": "json_object"},
    )

    raw    = response.choices[0].message.content.strip()
    result = json.loads(raw)

    script          = result.get("script", "").strip()
    tip_narration   = result.get("tip_narration", "").strip()
    tip_display     = result.get("tip_display", tip_narration).strip()

    # Fallback: se tip_display veio por extenso, usa tip_narration como display também
    if not tip_display:
        tip_display = tip_narration

    print(f"[GPT-4o] Roteiro gerado ({len(script)} caracteres)")
    print(f"[GPT-4o] Tip narração  : {tip_narration}")
    print(f"[GPT-4o] Tip display   : {tip_display}")
    return {"script": script, "tip_narration": tip_narration, "tip_display": tip_display}


# ── Geração do áudio TTS ─────────────────────────────────────────────────────

def generate_audio(script: str, output_path: Path) -> None:
    """Gera o arquivo MP3 de narração via OpenAI TTS."""
    client = OpenAI(api_key=OPENAI_API_KEY)

    print(f"[TTS] Gerando áudio com voz '{TTS_VOICE}'...")
    response = client.audio.speech.create(
        model="tts-1",
        voice=TTS_VOICE,
        input=script,
        speed=1.05,  # ligeiramente mais rápido para o formato Shorts
    )

    response.stream_to_file(output_path)
    size_kb = output_path.stat().st_size // 1024
    print(f"[TTS] Áudio salvo em: {output_path} ({size_kb} KB)")


# ── Extração de stats visuais para o slide ───────────────────────────────────

def extract_stats_for_slide(h2h: dict | None, home_team: str, away_team: str) -> dict:
    """
    Extrai do H2H os dados que aparecem no slide de estatísticas:
    - Últimos 2 resultados de cada time
    - Média de gols marcados e sofridos (últimos 5 jogos)
    - Histórico de confrontos diretos (últimos 2)
    Retorna dict simples pronto para o slide.
    """
    if not h2h:
        return {}

    def parse_games(games: list, team_name: str, limit: int = 5):
        results = []
        goals_scored = []
        goals_conceded = []
        for g in games[:limit]:
            hn = g.get("match_hometeam_name", "")
            an = g.get("match_awayteam_name", "")
            hs = g.get("match_hometeam_score", "")
            as_ = g.get("match_awayteam_score", "")
            try:
                hs_i = int(hs)
                as_i = int(as_)
            except Exception:
                continue

            # Descobre se o time é casa ou fora nesse jogo
            from difflib import SequenceMatcher
            def sim(a, b):
                return SequenceMatcher(None, a.lower(), b.lower()).ratio()

            is_home = sim(hn, team_name) > sim(an, team_name)
            gf = hs_i if is_home else as_i
            gc = as_i if is_home else hs_i

            goals_scored.append(gf)
            goals_conceded.append(gc)

            if gf > gc:
                result = "V"
            elif gf == gc:
                result = "E"
            else:
                result = "D"

            results.append({
                "home": hn, "away": an,
                "score": f"{hs_i} x {as_i}",
                "result": result,
                "date": g.get("match_date", "")[:10]
            })

        avg_scored    = round(sum(goals_scored) / len(goals_scored), 1) if goals_scored else None
        avg_conceded  = round(sum(goals_conceded) / len(goals_conceded), 1) if goals_conceded else None
        return results, avg_scored, avg_conceded

    team1_games = h2h.get("firstTeam_lastResults", [])
    team2_games = h2h.get("secondTeam_lastResults", [])
    vs_games    = h2h.get("firstTeam_VS_secondTeam", [])

    home_results, home_avg_scored, home_avg_conceded = parse_games(team1_games, home_team)
    away_results, away_avg_scored, away_avg_conceded = parse_games(team2_games, away_team)

    # Últimos 2 confrontos diretos
    h2h_direct = []
    for g in vs_games[:2]:
        hs = g.get("match_hometeam_score", "?")
        as_ = g.get("match_awayteam_score", "?")
        h2h_direct.append({
            "home": g.get("match_hometeam_name", ""),
            "away": g.get("match_awayteam_name", ""),
            "score": f"{hs} x {as_}",
            "date": g.get("match_date", "")[:10]
        })

    stats = {
        "home_last2":       home_results[:2],
        "away_last2":       away_results[:2],
        "home_avg_scored":  home_avg_scored,
        "home_avg_conceded": home_avg_conceded,
        "away_avg_scored":  away_avg_scored,
        "away_avg_conceded": away_avg_conceded,
        "h2h_direct":       h2h_direct,
    }

    print(f"[STATS] Casa — méd. gols: {home_avg_scored} marcados / {home_avg_conceded} sofridos")
    print(f"[STATS] Fora — méd. gols: {away_avg_scored} marcados / {away_avg_conceded} sofridos")
    print(f"[STATS] Confrontos diretos: {len(h2h_direct)} encontrados")
    return stats


# ── Pipeline principal ────────────────────────────────────────────────────────

async def run_pipeline():
    print("\n" + "="*50)
    print("  VIDEO PIPELINE — FASE 1")
    print("="*50 + "\n")

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

    home_team = game["match_hometeam_name"]
    away_team = game["match_awayteam_name"]
    home_id = str(game.get("match_hometeam_id", ""))
    away_id = str(game.get("match_awayteam_id", ""))
    league = game.get("league_name", "")
    match_date = game.get("match_date", "")
    match_time = game.get("match_time", "")

    # 3. Buscar H2H e badges em paralelo
    print("[API] Buscando H2H e badges dos times...")
    h2h_task = asyncio.create_task(get_h2h(home_id, away_id))
    home_badge_task = asyncio.create_task(get_team_badge_url(home_id))
    away_badge_task = asyncio.create_task(get_team_badge_url(away_id))

    h2h, home_badge, away_badge = await asyncio.gather(h2h_task, home_badge_task, away_badge_task)

    print(f"[API] Badge casa: {home_badge}")
    print(f"[API] Badge fora: {away_badge}")

    # 3b. Extrair stats visuais do H2H para o slide de estatísticas
    stats_visual = extract_stats_for_slide(h2h, home_team, away_team)

    # 4. Formatar dados para o prompt
    predictions_text = format_predictions_for_prompt(predictions)
    h2h_text = format_h2h_for_prompt(h2h)

    # 5. Gerar roteiro + tip
    result = generate_script(
        home_team, away_team, league,
        match_date, match_time,
        predictions_text, h2h_text
    )
    script        = result["script"]
    tip_narration = result["tip_narration"]
    tip_display   = result["tip_display"]

    # 6. Salvar arquivos com timestamp (evita sobrescrever)
    roteiro_path = OUTPUT_DIR / f"roteiro_{RUN_TS}.txt"
    roteiro_path.write_text(script, encoding="utf-8")
    print(f"[ROTEIRO] Salvo em: {roteiro_path}")

    # 7. Gerar áudio
    audio_path = OUTPUT_DIR / f"audio_narracao_{RUN_TS}.mp3"
    generate_audio(script, audio_path)

    # 8. Salvar game_data.json com timestamp para a Fase 2
    game_data = {
        "run_ts": RUN_TS,
        "home_team": home_team,
        "away_team": away_team,
        "league": league,
        "match_date": match_date,
        "match_time": match_time,
        "home_badge_url": home_badge,
        "away_badge_url": away_badge,
        "script": script,
        "tip_narration": tip_narration,
        "tip_display": tip_display,
        "tip": tip_display,          # compatibilidade com Fase 2 antiga
        "stats_visual": stats_visual,
        "audio_path": str(audio_path),
        "predictions": predictions,
    }
    game_data_path = OUTPUT_DIR / f"game_data_{RUN_TS}.json"
    game_data_path.write_text(json.dumps(game_data, ensure_ascii=False, indent=2), encoding="utf-8")

    # Também salva como "game_data_latest.json" para a Fase 2 encontrar fácil
    latest_path = OUTPUT_DIR / "game_data_latest.json"
    latest_path.write_text(json.dumps(game_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[JSON] Dados salvos em: {game_data_path}")
    print(f"[JSON] Atalho atualizado: {latest_path}")

    # 9. Resumo final
    print("\n" + "="*50)
    print("  FASE 1 CONCLUÍDA COM SUCESSO")
    print("="*50)
    print(f"\n  Jogo   : {home_team} x {away_team}")
    print(f"  Liga   : {league}")
    print(f"  Data   : {match_date} {match_time}")
    print(f"  Tip display   : {tip_display}")
    print(f"  Tip narração  : {tip_narration}")
    print(f"\n  Arquivos gerados:")
    print(f"    {roteiro_path}")
    print(f"    {audio_path}")
    print(f"    {game_data_path}")
    print(f"    output/game_data_latest.json  ← entrada para a Fase 2")
    print()
    print("  ROTEIRO GERADO:")
    print("  " + "-"*40)
    for line in script.split(". "):
        print(f"  {line.strip()}.")
    print()


if __name__ == "__main__":
    asyncio.run(run_pipeline())