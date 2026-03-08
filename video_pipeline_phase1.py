"""
VIDEO PIPELINE — FASE 1
========================
Seleciona o melhor jogo do dia, coleta predictions + H2H + standings,
gera análise completa com Claude (mesmo fluxo do OddsHero bot),
e salva o game_data_latest.json para a Fase 2.

Uso:
    python video_pipeline_phase1.py

Saída:
    output/game_data_latest.json  ← entrada para a Fase 2

Variáveis de ambiente necessárias:
    CLAUDE_API_KEY
    API_KEY   (apifootball)
"""

import asyncio
import json
import os
import re
import time
import aiohttp
import anthropic
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
load_dotenv()

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")

# ── Config ────────────────────────────────────────────────────────────────────
CLAUDE_API_KEY    = os.environ["CLAUDE_API_KEY"]
FOOTBALL_API_KEY  = os.environ["API_KEY"]
FOOTBALL_API_BASE = "https://apiv3.apifootball.com/?"
OUTPUT_DIR        = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Ligas prioritárias — quanto menor o índice, maior a prioridade
PRIORITY_LEAGUES = [
    "302",  # UEFA Champions League
    "175",  # UEFA Europa League
    "152",  # Premier League
    "233",  # La Liga
    "207",  # Serie A
    "168",  # Bundesliga
    "176",  # Ligue 1
    "244",  # Brasileirão Série A
    "245",  # Brasileirão Série B
    "143",  # Copa do Brasil
]

# Prompt do analista — idêntico ao prompt_pre_template.txt do OddsHero
PROMPT_TEMPLATE = """Você é o apostador profissional mais qualificado e habilidoso do mundo, especializado em identificar, com excelência, a melhor oportunidade de aposta em jogos de futebol antes de seu início. Sua missão agora é realizar uma análise minuciosa e estratégica dos dados disponíveis desta partida, entregando uma análise de altíssima qualidade, precisa, profissional e eficaz, com alto potencial de gerar lucros consistentes a longo prazo para seus leitores.

Esses são os dados que você tem disponíveis para fazer sua análise:

1 - Informações do Jogo:
[Base fundamental para iniciar sua análise]

Partida: {match}
Data: {date} Hora: {time}

2 - Previsões Avançadas:
[Peso: Alto - Probabilidades matemáticas calculadas por sistemas de inteligência, substitui os dados de odds e oferece os mercados disponíveis para o apostador]

{predictions}

3 - Histórico e Análise das Equipes:
[Peso: Muito Alto - Fornece dados históricos entre as equipes e padrões de desempenho individuais]

{h2h_data}

4 - Classificação da Liga:
[Peso: Alto - Oferece contexto do campeonato, posição atual e motivação das equipes]

{standings}

Caso a lista acima de Classificação da Liga não ofereça claramente uma tabela correta e organizada com as posições em ordem de cada time, desconsidere esses dados em sua avaliação.

PROCESSO DE ANÁLISE

1. Avalie o Contexto Pré-Jogo:
- Analise a importância do jogo para cada equipe (posição na tabela, objetivos na competição).
- Considere: sequência de resultados recentes, histórico de confrontos diretos, motivação baseada na classificação atual.

2. Análise Com Previsões Avançadas:
- Compare as previsões avançadas com o histórico das equipes.
- Identifique brechas significativas entre as expectativas e o desempenho histórico.

3. Análise de Confrontos Históricos:
- Examine o padrão de resultados nos jogos anteriores entre as equipes.
- Identifique tendências de marcação de gols, resultado e performance.

4. Contextualização da Liga:
- Analise a posição das equipes na tabela.
- Considere a importância do jogo para os objetivos de cada time.
- Avalie possíveis pressões competitivas.

5. Formulação de Recomendações:
- Desenvolva uma recomendação principal baseada na análise mais convincente.
- Considere recomendações secundárias com bom potencial de valor.

REGRAS PARA RECOMENDAÇÕES

1. Recomendação Principal:
- O tipo de aposta deve ser retirado das informações disponíveis em Previsões Avançadas.
- Odd deve estar entre 1.50 e 2.30.
- Justifique com base em dados concretos.
- Atribua um nível de confiança (de 0 a 10) baseado na convergência dos dados.

2. Recomendações Secundárias (até 2):
- Odds devem estar entre 1.50 e 4.00.
- Devem ser coerentes com a recomendação principal.

3. Critérios importantes:
- Todas as recomendações devem ser logicamente possíveis.
- Use apenas probabilidades e dados fornecidos.
- Evite contradições entre recomendações.

VERIFICAÇÕES INTERNAS OBRIGATÓRIAS

Antes de finalizar, confirme:
1. A odd da Recomendação Principal está entre 1.50 e 2.30.
2. As odds das Recomendações Secundárias estão entre 1.50 e 4.00.
3. Todas as recomendações são baseadas apenas nos dados fornecidos.
4. Não há contradições lógicas entre as recomendações.

FORMATO DE RESPOSTA

⚽ [Nome do Jogo]

📅 [Data do Jogo] ([Hora do Jogo])

Contexto Pré-Jogo: [Descrição breve da situação das equipes, motivações e importância do confronto. Máximo 1 frase]

💡 Recomendação Principal

[Aposta Específica]

Odd Mínima Esperada:

🧠 Justificativa

[Explicação detalhada baseada na análise completa - máximo 3 frases]

Confiança: [0-10]/10

🔍 Recomendações Secundárias:

1. [Tipo de aposta] (Odd Min: )
[Breve justificativa - máximo 1 frase]

2. [Tipo de aposta] (Odd Min: )
[Breve justificativa - máximo 1 frase]
[Fim da resposta, não é necessário enviar nada e nenhum comentário ou nota depois da última recomendação]"""


# ── API Football ──────────────────────────────────────────────────────────────

async def fetch_football_data(params: dict) -> list:
    params["APIkey"] = FOOTBALL_API_KEY
    url = FOOTBALL_API_BASE + "&".join(f"{k}={v}" for k, v in params.items())
    for attempt in range(2):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if isinstance(data, dict) and "error" in data:
                            return []
                        return data if isinstance(data, list) else [data]
                    elif resp.status == 429:
                        await asyncio.sleep(2)
                        continue
                    return []
        except Exception as e:
            print(f"  [API] Erro: {e}")
            if attempt < 1:
                await asyncio.sleep(1)
    return []


async def get_upcoming_games_today() -> list:
    now   = datetime.now(ZoneInfo("America/Sao_Paulo"))
    today = now.strftime("%Y-%m-%d")
    games = await fetch_football_data({
        "action": "get_events", "from": today, "to": today,
        "timezone": "America/Sao_Paulo",
    })
    upcoming = [g for g in games if isinstance(g, dict) and g.get("match_status", "") == "Not Started"]
    print(f"[API] {len(upcoming)} jogos futuros encontrados hoje")
    return upcoming


async def get_predictions(match_id: str) -> dict | None:
    data = await fetch_football_data({"action": "get_predictions", "match_id": match_id})
    return data[0] if isinstance(data, list) and data else None


async def get_h2h(team1_id: str, team2_id: str):
    data = await fetch_football_data({"action": "get_H2H", "firstTeamId": team1_id, "secondTeamId": team2_id})
    if isinstance(data, list) and data:
        return data[0]
    return data if isinstance(data, dict) else None


async def get_standings(league_id: str) -> list | None:
    data = await fetch_football_data({"action": "get_standings", "league_id": league_id})
    return data if data and isinstance(data, list) else None


async def get_team_badge_url(team_id: str) -> str | None:
    data = await fetch_football_data({"action": "get_teams", "team_id": team_id})
    return data[0].get("team_badge") if isinstance(data, list) and data else None


# ── Seleção do jogo ───────────────────────────────────────────────────────────

def score_game(game: dict) -> int:
    score     = 0
    league_id = str(game.get("league_id", ""))
    if league_id in PRIORITY_LEAGUES:
        score += 100 - PRIORITY_LEAGUES.index(league_id) * 10
    try:
        match_dt   = datetime.strptime(
            f"{game['match_date']} {game['match_time']}", "%Y-%m-%d %H:%M"
        ).replace(tzinfo=ZoneInfo("America/Sao_Paulo"))
        hours_away = (match_dt - datetime.now(ZoneInfo("America/Sao_Paulo"))).total_seconds() / 3600
        if 0 < hours_away <= 12:
            score += 20
    except Exception:
        pass
    return score


async def pick_best_game(games: list):
    for game in sorted(games, key=score_game, reverse=True)[:15]:
        predictions = await get_predictions(str(game.get("match_id", "")))
        if predictions:
            print(f"[SELEÇÃO] {game['match_hometeam_name']} x {game['match_awayteam_name']}")
            print(f"          Liga: {game.get('league_name')} | {game['match_date']} {game['match_time']}")
            return game, predictions
    print("[SELEÇÃO] Nenhum jogo com predictions. Usando o primeiro disponível.")
    games_sorted = sorted(games, key=score_game, reverse=True)
    return (games_sorted[0], None) if games_sorted else (None, None)


# ── Formatação de dados (idêntico ao OddsHero) ───────────────────────────────

def calculate_odd(prob) -> str:
    try:
        p = float(prob)
        return f"{round(100 / p, 2):.2f}" if p > 0 else "N/A"
    except Exception:
        return "N/A"


def is_valid_probability(value) -> bool:
    try:
        return 30 <= float(value) <= 70
    except Exception:
        return False


def format_predictions(pred: dict) -> str:
    if not pred:
        return "Probabilidades não disponíveis."

    mapping = {
        "prob_HW": "Vitória Casa", "prob_D": "Empate", "prob_AW": "Vitória Fora",
        "prob_HW_D": "Casa ou Empate", "prob_AW_D": "Fora ou Empate",
        "prob_HW_AW": "Casa ou Fora",
        "prob_O": "Mais de 2.5 Gols", "prob_U": "Menos de 2.5 Gols",
        "prob_O_1": "Mais de 1.5 Gols", "prob_U_1": "Menos de 1.5 Gols",
        "prob_O_3": "Mais de 3.5 Gols", "prob_U_3": "Menos de 3.5 Gols",
        "prob_bts": "Ambas Marcam - Sim", "prob_ots": "Ambas Marcam - Não",
    }
    lines = []
    for key, label in mapping.items():
        val = pred.get(key)
        if val and is_valid_probability(val):
            lines.append(f"{label}: {val}% (Odd: {calculate_odd(val)})")

    for v in [4.5, 3.5, 2.5, 1.5, 0.5]:
        for sign in ["+", "-"]:
            hk = f"prob_ah_h_{int(v*10)}" if sign == "+" else f"prob_ah_h_-{int(v*10)}"
            ak = f"prob_ah_a_{int(v*10)}" if sign == "+" else f"prob_ah_a_-{int(v*10)}"
            if hk in pred and is_valid_probability(pred[hk]):
                lines.append(f"Handicap Asiático Casa ({sign}{v}): {pred[hk]}% (Odd: {calculate_odd(pred[hk])})")
            if ak in pred and is_valid_probability(pred[ak]):
                lines.append(f"Handicap Asiático Fora ({sign}{v}): {pred[ak]}% (Odd: {calculate_odd(pred[ak])})")

    return "\n".join(lines) if lines else "Probabilidades não disponíveis."


def similar(a: str, b: str) -> bool:
    try:
        from unidecode import unidecode
        def norm(s):
            s = unidecode(str(s)).lower()
            for t in ["fc", "clube", "club", "sc"]:
                s = s.replace(t, "")
            return s.strip()
        return SequenceMatcher(None, norm(a), norm(b)).ratio() > 0.6
    except Exception:
        return False


def filter_h2h_data(h2h_data) -> str:
    """Formata H2H idêntico ao OddsHero — com médias, over/under e ambas marcam."""
    if not h2h_data or isinstance(h2h_data, str):
        return "Dados de confrontos diretos não disponíveis"

    def fmt_match(match, main_team=None):
        hn  = match.get("match_hometeam_name", "")
        an  = match.get("match_awayteam_name", "")
        hs  = match.get("match_hometeam_score", "")
        as_ = match.get("match_awayteam_score", "")
        dt  = match.get("match_date", "")
        try:
            dt = datetime.strptime(dt, "%Y-%m-%d").strftime("%d/%m/%Y")
        except Exception:
            pass
        formatted = f"{hn} {hs} - {as_} {an} | Data: {dt}"
        hsi  = int(hs)  if str(hs).isdigit()  else 0
        asi  = int(as_) if str(as_).isdigit() else 0
        if main_team:
            is_home = similar(hn, main_team)
            ms, os_ = (hsi, asi) if is_home else (asi, hsi)
        else:
            ms = os_ = None
        return {"formatted": formatted, "main_score": ms, "opponent_score": os_,
                "home_score": hsi, "away_score": asi}

    def find_main(games):
        counts = {}
        for g in games:
            for k in ["match_hometeam_name", "match_awayteam_name"]:
                t = g.get(k)
                if t:
                    counts[t] = counts.get(t, 0) + 1
        return max(counts, key=counts.get) if counts else None

    try:
        if isinstance(h2h_data, list) and h2h_data and isinstance(h2h_data[0], dict):
            h2h_raw, t1_raw, t2_raw = (
                h2h_data[0].get("firstTeam_VS_secondTeam", []),
                h2h_data[0].get("firstTeam_lastResults", []),
                h2h_data[0].get("secondTeam_lastResults", []),
            )
        elif isinstance(h2h_data, dict):
            h2h_raw  = h2h_data.get("firstTeam_VS_secondTeam", [])
            t1_raw   = h2h_data.get("firstTeam_lastResults", [])
            t2_raw   = h2h_data.get("secondTeam_lastResults", [])
        else:
            return "Formato de dados H2H não reconhecido"

        t1n = find_main(t1_raw) or "Time da Casa"
        t2n = find_main(t2_raw) or "Time Visitante"
        h2h_games = [fmt_match(m)        for m in h2h_raw][:3]
        t1_games  = [fmt_match(m, t1n)   for m in t1_raw][:5]
        t2_games  = [fmt_match(m, t2n)   for m in t2_raw][:5]

        def avgs(games):
            v = [g for g in games if g["main_score"] is not None]
            if not v:
                return 0, 0, 0, 0
            n = len(v)
            return (
                sum(g["main_score"] for g in v) / n,
                sum(g["opponent_score"] for g in v) / n,
                sum(1 for g in v if g["main_score"] > 0 and g["opponent_score"] > 0),
                sum(1 for g in v if g["main_score"] + g["opponent_score"] > 2.5),
            )

        t1_gf, t1_ga, t1_bts, t1_o25 = avgs(t1_games)
        t2_gf, t2_ga, t2_bts, t2_o25 = avgs(t2_games)

        out = ""
        if h2h_games:
            out += "#### Histórico de Confrontos (H2H):\n"
            for i, g in enumerate(h2h_games, 1):
                out += f"- Jogo {i}: {g['formatted']}\n"
            total = sum(g["home_score"] + g["away_score"] for g in h2h_games)
            bts   = sum(1 for g in h2h_games if g["home_score"] > 0 and g["away_score"] > 0)
            o25   = sum(1 for g in h2h_games if g["home_score"] + g["away_score"] > 2.5)
            out  += f"\n#### Análise do Histórico de Confrontos:\n"
            out  += f"Nos últimos {len(h2h_games)} jogos, houve {total} gols. "
            out  += f"Em {bts} jogo(s) Ambas Marcam e {o25} jogo(s) Over 2.5 gols.\n"
        else:
            out += "Não há histórico entre as equipes disponível.\n"

        if t1_games:
            out += f"\n#### Últimos 5 Jogos do Time da Casa ({t1n}):\n"
            for i, g in enumerate(t1_games, 1):
                out += f"- Jogo {i}: {g['formatted']}\n"
        if t2_games:
            out += f"\n#### Últimos 5 Jogos do Time Visitante ({t2n}):\n"
            for i, g in enumerate(t2_games, 1):
                out += f"- Jogo {i}: {g['formatted']}\n"

        out += "\n#### Médias de Gols e Estatísticas:\n\n"
        if t1_games:
            out += f"Time da Casa ({t1n}):\n"
            out += f"- Média gols marcados: {t1_gf:.2f} | sofridos: {t1_ga:.2f}\n"
            out += f"- {t1_bts} Ambas Marcam, {t1_o25} Over 2.5 nos últimos {len(t1_games)} jogos.\n\n"
        if t2_games:
            out += f"Time Visitante ({t2n}):\n"
            out += f"- Média gols marcados: {t2_gf:.2f} | sofridos: {t2_ga:.2f}\n"
            out += f"- {t2_bts} Ambas Marcam, {t2_o25} Over 2.5 nos últimos {len(t2_games)} jogos.\n"

        return out

    except Exception as e:
        print(f"  [H2H] Erro: {e}")
        return "Erro ao processar dados de confrontos diretos"


def format_standings(standings, home_team=None) -> str:
    """Formata tabela de classificação idêntico ao OddsHero."""
    if not standings:
        return "Dados de classificação não disponíveis"

    unique_stages = set(t.get("stage_name", "") for t in standings)
    home_stages   = set()
    if home_team:
        for t in standings:
            if similar(t.get("team_name", ""), home_team) and t.get("stage_name"):
                home_stages.add(t["stage_name"])

    if unique_stages:
        lines = []
        for stage in unique_stages:
            if home_team and stage not in home_stages:
                continue
            stage_teams = [
                t for t in standings
                if t.get("stage_name", "") == stage
                and int(t.get("overall_league_PTS", 0) or 0) >= 0
            ]
            lines.append(f"Tabela {stage}:")
            for t in stage_teams:
                lines.append(
                    f"Posição {t.get('overall_league_position','N/A')} - "
                    f"{t.get('team_name','N/A')}, Pontos: {t.get('overall_league_PTS','N/A')}"
                )
            lines.append("")
        result = "\n".join(lines).strip()
        return result if result else "Dados de classificação não disponíveis"
    else:
        lines = [
            f"Posição {t.get('overall_league_position','N/A')} - "
            f"{t.get('team_name','N/A')}, Pontos: {t.get('overall_league_PTS','N/A')}"
            for t in standings
        ]
        return "\n".join(lines) if lines else "Dados de classificação não disponíveis"


# ── Extração da tip da análise ────────────────────────────────────────────────

def extract_tip(analysis: str) -> str:
    """Extrai a aposta principal da análise do Claude para o tip_display."""
    # Linha logo após "💡 Recomendação Principal" (pula linhas em branco)
    match = re.search(
        r"Recomenda[çc][aã]o Principal\s*\n+\s*\n*(.+?)(?:\n|$)",
        analysis, re.IGNORECASE
    )
    if match:
        tip = re.sub(r"[^\w\s\.\,\/\(\)\-\+\%]", "", match.group(1)).strip()
        if tip:
            return tip[:50]

    # Fallback: linha entre "Recomendação Principal" e "Odd Mínima"
    match2 = re.search(r"Principal\n+(.+?)\n+Odd", analysis, re.IGNORECASE | re.DOTALL)
    if match2:
        lines = [l.strip() for l in match2.group(1).strip().splitlines() if l.strip()]
        if lines:
            return lines[-1][:50]

    return "Confira a análise"


# ── Claude API ────────────────────────────────────────────────────────────────

def call_claude(prompt: str) -> tuple[str, bool]:
    client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
    for attempt in range(3):
        try:
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )
            return msg.content[0].text, True
        except anthropic.RateLimitError:
            wait = 2 ** attempt
            print(f"  [CLAUDE] Rate limit — aguardando {wait}s...")
            time.sleep(wait)
        except anthropic.APIError as e:
            if getattr(e, "status_code", None) == 529:
                wait = 2 ** attempt
                print(f"  [CLAUDE] Sobrecarga — aguardando {wait}s...")
                time.sleep(wait)
            else:
                print(f"  [CLAUDE] Erro: {e}")
                return "Erro ao gerar análise.", False
        except Exception as e:
            print(f"  [CLAUDE] Erro inesperado: {e}")
            return "Erro ao gerar análise.", False
    return "Análise indisponível após várias tentativas.", False


# ── Pipeline principal ────────────────────────────────────────────────────────

async def run_pipeline():
    print("\n" + "=" * 54)
    print("  VIDEO PIPELINE — FASE 1")
    print("=" * 54 + "\n")

    # 1. Buscar jogos do dia
    games = await get_upcoming_games_today()
    if not games:
        print("[ERRO] Nenhum jogo encontrado para hoje.")
        return

    # 2. Escolher o melhor jogo com predictions
    game, predictions = await pick_best_game(games)
    if not game:
        print("[ERRO] Não foi possível selecionar um jogo.")
        return

    home_team  = game["match_hometeam_name"]
    away_team  = game["match_awayteam_name"]
    home_id    = str(game.get("match_hometeam_id", ""))
    away_id    = str(game.get("match_awayteam_id", ""))
    league     = game.get("league_name", "")
    league_id  = str(game.get("league_id", ""))
    match_date = game.get("match_date", "")
    match_time = game.get("match_time", "")
    match_id   = str(game.get("match_id", ""))

    # 3. Buscar H2H, badges e standings em paralelo
    print("[API] Buscando H2H, badges e standings...")
    h2h_data, home_badge, away_badge, standings_raw = await asyncio.gather(
        get_h2h(home_id, away_id),
        get_team_badge_url(home_id),
        get_team_badge_url(away_id),
        get_standings(league_id),
    )
    print(f"  Badge casa : {home_badge}")
    print(f"  Badge fora : {away_badge}")
    print(f"  H2H        : {'✓' if h2h_data else '✗'}")
    print(f"  Standings  : {'✓' if standings_raw else '✗'}")

    # 4. Formatar dados exatamente como o OddsHero faz
    try:
        date_fmt = datetime.strptime(match_date, "%Y-%m-%d").strftime("%d/%m")
    except Exception:
        date_fmt = match_date

    predictions_str = format_predictions(predictions) if predictions else "Probabilidades não disponíveis."
    h2h_str         = filter_h2h_data(h2h_data)       if h2h_data   else "Dados de histórico não disponíveis."
    standings_str   = format_standings(standings_raw, home_team=home_team) if standings_raw else "Dados de classificação não disponíveis."

    # 5. Montar prompt e chamar Claude
    prompt = PROMPT_TEMPLATE.format(
        match       = f"{home_team} vs {away_team}",
        date        = date_fmt,
        time        = match_time,
        predictions = predictions_str,
        h2h_data    = h2h_str,
        standings   = standings_str,
    )

    print(f"\n[CLAUDE] Gerando análise para {home_team} vs {away_team}...")
    analysis, success = call_claude(prompt)

    if not success:
        print(f"[ERRO] Falha na análise: {analysis}")
        return

    tip_display = extract_tip(analysis)
    print(f"[CLAUDE] Análise gerada ({len(analysis)} chars)")
    print(f"[CLAUDE] Tip extraída  : {tip_display}")

    # 6. Salvar game_data
    game_data = {
        "run_ts":         RUN_TS,
        "match_id":       match_id,
        "home_team":      home_team,
        "away_team":      away_team,
        "league":         league,
        "match_date":     match_date,
        "match_time":     match_time,
        "home_badge_url": home_badge,
        "away_badge_url": away_badge,
        "script":         analysis,
        "tip_display":    tip_display,
        "tip":            tip_display,  # compatibilidade Fase 2
    }

    ts_path     = OUTPUT_DIR / f"game_data_{RUN_TS}.json"
    latest_path = OUTPUT_DIR / "game_data_latest.json"
    for path in [ts_path, latest_path]:
        path.write_text(json.dumps(game_data, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 54)
    print("  FASE 1 CONCLUÍDA")
    print("=" * 54)
    print(f"\n  Jogo    : {home_team} x {away_team}")
    print(f"  Liga    : {league}  |  {match_date} {match_time}")
    print(f"  Tip     : {tip_display}")
    print(f"  Arquivo : {latest_path}\n")
    print("  ANÁLISE GERADA:")
    print("  " + "-" * 40)
    print(analysis)
    print()


if __name__ == "__main__":
    asyncio.run(run_pipeline())