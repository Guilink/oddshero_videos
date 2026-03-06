"""
VIDEO PIPELINE — FASE 2  (nova estrutura)
==========================================

Estrutura:
  [0s-6s]    video_base.mp4  + overlay topo (emblemas, times, data)
  [6s-12s]   video_base2.mp4 + overlay topo
  [12s-??s]  Slide STATS — historico recente + medias de gols
  [??s-??s]  Slide TIP  — tip limpa e elegante
  [??s-??s]  video_base3.mp4 + overlay CTA (mensagem da IA)
  [??s-fim]  video_base4.mp4 + overlay CTA

Assets necessarios:
  assets/video_base.mp4
  assets/video_base2.mp4
  assets/video_base3.mp4
  assets/video_base4.mp4

Entrada:
  output/game_data_latest.json  (gerado pela Fase 1)

Saida:
  output/video_final_<timestamp>.mp4
"""

import json
import subprocess
import sys
import urllib.request
from datetime import datetime
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

# ── Config ────────────────────────────────────────────────────────────────────
OUTPUT_DIR = Path("output")
ASSETS_DIR = Path("assets")
TEMP_DIR   = OUTPUT_DIR / "tmp"
OUTPUT_DIR.mkdir(exist_ok=True)
ASSETS_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)

W, H   = 1080, 1920
FPS    = 30

# Paleta
BG      = (10, 10, 12)
GOLD    = (212, 175, 55)
GREEN   = (0, 200, 100)
WHITE   = (255, 255, 255)
GRAY    = (155, 155, 155)
DGRAY   = (80, 80, 80)
CARD    = (20, 20, 24)
GOLD_D  = (130, 105, 28)

# Duracoes fixas (segundos)
DUR_V1      = 6
DUR_V2      = 6
DUR_STATS   = 10
DUR_TIP     = 8
DUR_V3      = 3
DUR_V4      = 3
# Duracao total minima = 42s — o audio determina o corte final via -shortest


# ── Helpers ───────────────────────────────────────────────────────────────────

def check_ffmpeg():
    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
        return True
    except Exception:
        return False

def audio_duration(path: Path) -> float:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True)
    try:
        return float(r.stdout.strip())
    except Exception:
        return 50.0

def dl(url: str, dest: Path):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            dest.write_bytes(r.read())
        return dest
    except Exception as e:
        print(f"  [badge] falhou: {e}")
        return None

def font(size, bold=False):
    cands = (
        ["arialbd.ttf", "Arial Bold.ttf", "DejaVuSans-Bold.ttf",
         "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"]
        if bold else
        ["arial.ttf", "Arial.ttf", "DejaVuSans.ttf",
         "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]
    )
    for p in cands:
        try: return ImageFont.truetype(p, size)
        except Exception: pass
    return ImageFont.load_default()

def ctext(draw, text, y, f, color, max_w=None, gap=8):
    """Texto centralizado com quebra de linha opcional."""
    if max_w:
        words, lines, cur = text.split(), [], ""
        for w in words:
            test = f"{cur} {w}".strip()
            if draw.textbbox((0,0), test, font=f)[2] > max_w and cur:
                lines.append(cur); cur = w
            else:
                cur = test
        if cur: lines.append(cur)
    else:
        lines = [text]
    lh = draw.textbbox((0,0), lines[0], font=f)[3] + gap
    for i, line in enumerate(lines):
        bx = draw.textbbox((0,0), line, font=f)
        draw.text(((W - bx[2]) // 2, y + i * lh), line, font=f, fill=color)
    return len(lines) * lh

def hline(draw, y, w=420, color=None):
    color = color or GOLD
    x0 = (W - w) // 2
    draw.rectangle([x0, y, x0 + w, y + 3], fill=color)

def badge_paste(img, path, cx, cy, size):
    from PIL import ImageDraw as ID
    if path and Path(path).exists():
        try:
            b = Image.open(path).convert("RGBA").resize((size, size), Image.LANCZOS)
            mask = Image.new("L", (size, size), 0)
            ID.Draw(mask).ellipse([0, 0, size-1, size-1], fill=255)
            bg = Image.new("RGBA", (size, size), (28, 28, 32, 220))
            bg.paste(b, mask=b.split()[3] if b.mode == "RGBA" else None)
            img.paste(bg, (cx - size//2, cy - size//2), mask)
            return
        except Exception:
            pass
    draw = ImageDraw.Draw(img)
    draw.ellipse([cx-size//2, cy-size//2, cx+size//2, cy+size//2],
                 fill=(35,35,40), outline=GOLD, width=3)


# ── FFmpeg helpers ────────────────────────────────────────────────────────────

def img2vid(img: Image.Image, dur: float, out: Path):
    png = out.with_suffix(".png")
    img.save(png, "PNG")
    r = subprocess.run([
        "ffmpeg", "-y", "-loop", "1", "-i", str(png),
        "-t", str(dur),
        "-vf", f"scale={W}:{H},fps={FPS}",
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p", "-an",
        str(out)
    ], capture_output=True)
    png.unlink(missing_ok=True)
    if r.returncode != 0:
        print("[ERRO img2vid]", r.stderr.decode(errors="replace")[-800:])
        raise RuntimeError("img2vid falhou")

def vid_overlay(src: Path, ov_png: Path, dur: float, out: Path):
    r = subprocess.run([
        "ffmpeg", "-y",
        "-stream_loop", "-1", "-i", str(src),
        "-i", str(ov_png),
        "-t", str(dur),
        "-filter_complex",
        (f"[0:v]scale={W}:{H}:force_original_aspect_ratio=increase,"
         f"crop={W}:{H},setsar=1,fps={FPS}[bg];"
         f"[1:v]scale={W}:{H},format=rgba[ov];"
         f"[bg][ov]overlay=0:0:format=auto[out]"),
        "-map", "[out]",
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p", "-an",
        str(out)
    ], capture_output=True)
    if r.returncode != 0:
        print("[ERRO vid_overlay]", r.stderr.decode(errors="replace")[-800:])
        raise RuntimeError(f"vid_overlay falhou para {src.name}")

def concat(parts: list, out: Path):
    lf = TEMP_DIR / "concat_list.txt"
    lf.write_text("\n".join(f"file '{p.resolve()}'" for p in parts))
    subprocess.run([
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", str(lf), "-c", "copy", str(out)
    ], capture_output=True, check=True)

def mux(vid: Path, aud: Path, out: Path):
    subprocess.run([
        "ffmpeg", "-y",
        "-i", str(vid), "-i", str(aud),
        "-map", "0:v:0", "-map", "1:a:0",
        "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
        "-shortest", str(out)
    ], capture_output=True, check=True)


# ── Overlay A: topo com emblemas (intro) ──────────────────────────────────────

def make_overlay_intro(home_team, away_team, league, match_date, match_time,
                       home_badge, away_badge) -> Path:
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    panel_h = 720
    ov.paste(Image.new("RGBA", (W, panel_h), (8, 8, 12, 210)), (0, 0))
    d = ImageDraw.Draw(ov)
    d.rectangle([0, panel_h - 4, W, panel_h], fill=GOLD)

    ctext(d, "ODDSHERO AI", 32, font(44, bold=True), GOLD)
    hline(d, 96, w=440)

    try:
        date_str = datetime.strptime(match_date, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        date_str = match_date

    ctext(d, league.upper(), 112, font(30), GRAY, max_w=900)
    ctext(d, f"{date_str}  •  {match_time}", 156, font(36, bold=True), WHITE)

    bsize, bcy = 230, 400
    badge_paste(ov, home_badge, W // 4, bcy, bsize)
    badge_paste(ov, away_badge, 3 * W // 4, bcy, bsize)
    ctext(d, "VS", bcy - 46, font(84, bold=True), GOLD)

    ny = bcy + bsize // 2 + 20
    fn = font(38, bold=True)

    def tlabel(name, cx):
        words, lines, cur = name.split(), [], ""
        for w in words:
            test = f"{cur} {w}".strip()
            if d.textbbox((0,0), test, font=fn)[2] > 420 and cur:
                lines.append(cur); cur = w
            else:
                cur = test
        if cur: lines.append(cur)
        lh = d.textbbox((0,0), lines[0], font=fn)[3] + 6
        for i, l in enumerate(lines):
            bx = d.textbbox((0,0), l, font=fn)
            d.text((cx - bx[2]//2, ny + i*lh), l, font=fn, fill=WHITE)

    tlabel(home_team, W // 4)
    tlabel(away_team, 3 * W // 4)

    out = TEMP_DIR / "overlay_intro.png"
    ov.save(out, "PNG")
    return out


# ── Overlay B: CTA final (fundo dos videos 3 e 4) ─────────────────────────────

def make_overlay_cta() -> Path:
    """
    Painel inferior semitransparente com a mensagem da IA.
    Ocupa a metade de baixo da tela para nao cobrir o video dos robos.
    """
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))

    panel_top = H // 2
    panel_h   = H - panel_top
    panel = Image.new("RGBA", (W, panel_h), (6, 6, 10, 225))
    ov.paste(panel, (0, panel_top))

    d = ImageDraw.Draw(ov)
    d.rectangle([0, panel_top, W, panel_top + 4], fill=GOLD)

    y = panel_top + 48

    ctext(d, "EXISTE UMA IA QUE FAZ ISSO", y, font(52, bold=True), GOLD, max_w=920)
    y += 130
    ctext(d, "POR VOCE, AO VIVO.", y, font(52, bold=True), WHITE, max_w=920)
    y += 100
    hline(d, y, w=500)
    y += 36

    ctext(d, "Cruza estatisticas em tempo real", y, font(36), WHITE, max_w=880)
    y += 54
    ctext(d, "com as melhores odds do momento", y, font(36), WHITE, max_w=880)
    y += 54
    ctext(d, "e gera a analise mais precisa possivel.", y, font(36), WHITE, max_w=880)
    y += 80

    # CTA box
    bx0, bx1 = 120, W - 120
    by0 = y
    by1 = by0 + 160
    d.rounded_rectangle([bx0, by0, bx1, by1], radius=20, fill=(15, 15, 20), outline=GOLD, width=3)
    ctext(d, "ACESSE GRATIS AGORA", by0 + 28, font(40, bold=True), GOLD)
    ctext(d, "Link na bio  •  Telegram: @oddshero_bot", by0 + 90, font(30), WHITE)

    out = TEMP_DIR / "overlay_cta.png"
    ov.save(out, "PNG")
    return out


# ── Slide STATS ───────────────────────────────────────────────────────────────

def make_slide_stats(home_team: str, away_team: str, stats: dict) -> Image.Image:
    img = Image.new("RGB", (W, H), BG)
    d   = ImageDraw.Draw(img)

    # fundo sutil
    for i in range(0, H, 55):
        d.line([(0, i), (W, i)], fill=tuple(min(255, v+5) for v in BG), width=1)

    ctext(d, "ODDSHERO AI", 72, font(42, bold=True), GOLD)
    hline(d, 132, w=430)
    ctext(d, "DADOS DO JOGO", 158, font(36, bold=True), GREEN)
    hline(d, 212, w=300, color=DGRAY)

    y = 250

    # ── Medias de gols ──
    home_s = stats.get("home_avg_scored")
    home_c = stats.get("home_avg_conceded")
    away_s = stats.get("away_avg_scored")
    away_c = stats.get("away_avg_conceded")

    if any(v is not None for v in [home_s, home_c, away_s, away_c]):
        ctext(d, "MEDIA DE GOLS (ultimos 5 jogos)", y, font(30, bold=True), GRAY, max_w=880)
        y += 52

        col_w = (W - 160) // 2
        lx = 80
        rx = W // 2 + 40

        fn_team = font(32, bold=True)
        fn_stat = font(28)
        fn_val  = font(52, bold=True)

        # Casa
        hn = home_team[:18]
        bx = d.textbbox((0,0), hn, font=fn_team)
        d.text((lx, y), hn, font=fn_team, fill=WHITE)
        if home_s is not None:
            d.text((lx, y + 46), f"Marcados:", font=fn_stat, fill=GRAY)
            d.text((lx, y + 80), f"{home_s}", font=fn_val, fill=GREEN)
        if home_c is not None:
            d.text((lx + 180, y + 46), f"Sofridos:", font=fn_stat, fill=GRAY)
            d.text((lx + 180, y + 80), f"{home_c}", font=fn_val, fill=(220, 80, 80))

        # Separador vertical
        mid = W // 2
        d.line([(mid, y - 8), (mid, y + 150)], fill=DGRAY, width=2)

        # Fora
        an = away_team[:18]
        d.text((rx, y), an, font=fn_team, fill=WHITE)
        if away_s is not None:
            d.text((rx, y + 46), f"Marcados:", font=fn_stat, fill=GRAY)
            d.text((rx, y + 80), f"{away_s}", font=fn_val, fill=GREEN)
        if away_c is not None:
            d.text((rx + 180, y + 46), f"Sofridos:", font=fn_stat, fill=GRAY)
            d.text((rx + 180, y + 80), f"{away_c}", font=fn_val, fill=(220, 80, 80))

        y += 180
        hline(d, y, w=800, color=DGRAY)
        y += 36

    # ── Ultimos 2 jogos de cada time ──
    home_last = stats.get("home_last2", [])
    away_last = stats.get("away_last2", [])

    fn_label = font(30, bold=True)
    fn_game  = font(27)
    fn_score = font(32, bold=True)

    def result_color(r):
        return {
            "V": (50, 200, 100),
            "E": (200, 180, 50),
            "D": (200, 70, 70),
        }.get(r, WHITE)

    if home_last or away_last:
        ctext(d, "ULTIMOS RESULTADOS", y, font(30, bold=True), GRAY, max_w=880)
        y += 50

        col_w = (W - 160) // 2
        lx = 80
        rx = W // 2 + 40

        d.text((lx, y), home_team[:16], font=fn_label, fill=WHITE)
        d.text((rx, y), away_team[:16], font=fn_label, fill=WHITE)
        y += 44

        max_rows = max(len(home_last), len(away_last))
        for i in range(max_rows):
            # Casa
            if i < len(home_last):
                g = home_last[i]
                rc = result_color(g["result"])
                d.rounded_rectangle([lx, y, lx + col_w, y + 72],
                                     radius=10, fill=(22, 22, 28))
                d.text((lx + 12, y + 8),
                       f"{g['home'][:10]} x {g['away'][:10]}", font=fn_game, fill=GRAY)
                d.text((lx + 12, y + 36), g["score"], font=fn_score, fill=WHITE)
                d.text((lx + col_w - 50, y + 22), g["result"], font=fn_score, fill=rc)

            # Fora
            if i < len(away_last):
                g = away_last[i]
                rc = result_color(g["result"])
                d.rounded_rectangle([rx, y, rx + col_w, y + 72],
                                     radius=10, fill=(22, 22, 28))
                d.text((rx + 12, y + 8),
                       f"{g['home'][:10]} x {g['away'][:10]}", font=fn_game, fill=GRAY)
                d.text((rx + 12, y + 36), g["score"], font=fn_score, fill=WHITE)
                d.text((rx + col_w - 50, y + 22), g["result"], font=fn_score, fill=rc)

            y += 88

        hline(d, y, w=800, color=DGRAY)
        y += 36

    # ── Confrontos diretos ──
    h2h = stats.get("h2h_direct", [])
    if h2h:
        ctext(d, "CONFRONTOS DIRETOS (H2H)", y, font(30, bold=True), GRAY, max_w=880)
        y += 50
        for g in h2h:
            d.rounded_rectangle([80, y, W - 80, y + 72], radius=10, fill=(22, 22, 28))
            label = f"{g['home'][:14]}  {g['score']}  {g['away'][:14]}"
            ctext(d, label, y + 18, font(30, bold=True), WHITE, max_w=860)
            d.text((90, y + 48), g.get("date", ""), font=font(24), fill=GRAY)
            y += 88

    # Rodape
    ctext(d, "Powered by ODDSHERO AI", H - 80, font(28), GOLD_D)
    return img


# ── Slide TIP ─────────────────────────────────────────────────────────────────

def make_slide_tip(home_team: str, away_team: str, tip_text: str) -> Image.Image:
    img = Image.new("RGB", (W, H), BG)
    d   = ImageDraw.Draw(img)

    # Gradiente sutil
    for y in range(H):
        v = int(y / H * 18)
        d.line([(0, y), (W, y)], fill=(10 + v, 10 + v, 14 + v))

    ctext(d, "ODDSHERO AI", 110, font(44, bold=True), GOLD)
    hline(d, 176, w=440)

    ctext(d, home_team, 230, font(46, bold=True), WHITE, max_w=900)
    ctext(d, "x", 314, font(38), DGRAY)
    ctext(d, away_team, 368, font(46, bold=True), WHITE, max_w=900)

    hline(d, 460, w=500, color=DGRAY)

    # Grande label TIP
    ctext(d, "TIP RECOMENDADA", 510, font(38, bold=True), GRAY)

    # Card da tip
    card_y = 590
    card_h = 260
    d.rounded_rectangle([80, card_y, W - 80, card_y + card_h],
                         radius=28, fill=CARD, outline=GOLD, width=4)

    # Texto da tip centralizado verticalmente no card
    fn_tip = font(58, bold=True)
    # quebra se longo
    words, lines, cur = tip_text.split(), [], ""
    for w in words:
        test = f"{cur} {w}".strip()
        if d.textbbox((0,0), test, font=fn_tip)[2] > 840 and cur:
            lines.append(cur); cur = w
        else:
            cur = test
    if cur: lines.append(cur)
    lh = d.textbbox((0,0), lines[0], font=fn_tip)[3] + 10
    total_h = len(lines) * lh
    start_y = card_y + (card_h - total_h) // 2
    for i, line in enumerate(lines):
        bx = d.textbbox((0,0), line, font=fn_tip)
        d.text(((W - bx[2]) // 2, start_y + i * lh), line, font=fn_tip, fill=WHITE)

    # Sublabel
    ctext(d, "com base em dados estatisticos", card_y + card_h + 32,
          font(30), GRAY, max_w=800)

    hline(d, card_y + card_h + 90, w=400, color=DGRAY)

    # Rodape
    ctext(d, "Jogue com responsabilidade  •  +18",
          H - 160, font(28), DGRAY, max_w=800)
    ctext(d, "Link na bio  •  @oddshero_bot",
          H - 108, font(34, bold=True), GOLD)
    ctext(d, "Telegram gratuito",
          H - 62, font(28), GRAY)

    return img


# ── Pipeline principal ─────────────────────────────────────────────────────────

def run_phase2():
    print("\n" + "=" * 54)
    print("  VIDEO PIPELINE — FASE 2")
    print("=" * 54 + "\n")

    if not check_ffmpeg():
        print("[ERRO] FFmpeg nao encontrado.")
        print("  Instale: https://www.gyan.dev/ffmpeg/builds/")
        sys.exit(1)

    # Carregar dados
    gdp = OUTPUT_DIR / "game_data_latest.json"
    if not gdp.exists(): gdp = OUTPUT_DIR / "game_data.json"
    if not gdp.exists():
        print("[ERRO] game_data nao encontrado. Execute a Fase 1.")
        sys.exit(1)

    with open(gdp, encoding="utf-8") as f:
        data = json.load(f)

    home_team  = data["home_team"]
    away_team  = data["away_team"]
    league     = data["league"]
    match_date = data["match_date"]
    match_time = data["match_time"]
    hb_url     = data.get("home_badge_url")
    ab_url     = data.get("away_badge_url")
    tip_text   = data.get("tip_display") or data.get("tip", "Confira a analise")
    stats      = data.get("stats_visual", {})
    run_ts     = data.get("run_ts", datetime.now().strftime("%Y%m%d_%H%M%S"))

    audio_path = Path(data.get("audio_path", ""))
    if not audio_path.exists():
        audio_path = OUTPUT_DIR / "audio_narracao.mp3"
    if not audio_path.exists():
        print("[ERRO] Audio nao encontrado. Execute a Fase 1.")
        sys.exit(1)

    total_dur = audio_duration(audio_path)

    print(f"  Jogo  : {home_team} x {away_team}")
    print(f"  Tip   : {tip_text}")
    print(f"  Audio : {audio_path}  ({total_dur:.1f}s)\n")

    # Duracoes fixas: 12s intro + 12s outro = 24s
    # O meio (stats + tip) divide o restante igualmente
    FIXED_INTRO = DUR_V1 + DUR_V2          # 12s
    FIXED_OUTRO = DUR_V3 + DUR_V4          # 12s
    meio        = max(4.0, total_dur - FIXED_INTRO - FIXED_OUTRO)
    dur_stats   = round(meio / 2, 2)
    dur_tip     = round(meio / 2, 2)
    dur_v4_real = DUR_V4                   # sem necessidade de estender: o outro cobre exato

    print(f"  Audio total : {total_dur:.1f}s")
    print(f"  Intro       : {FIXED_INTRO}s (fixo)")
    print(f"  Stats       : {dur_stats:.1f}s")
    print(f"  Tip         : {dur_tip:.1f}s")
    print(f"  Outro       : {FIXED_OUTRO}s (fixo)")
    print(f"  Total video : {FIXED_INTRO + dur_stats + dur_tip + FIXED_OUTRO:.1f}s\n")

    # Badges
    print("[BADGES] Baixando emblemas...")
    hb = dl(hb_url, TEMP_DIR / "badge_home.png") if hb_url else None
    ab = dl(ab_url, TEMP_DIR / "badge_away.png") if ab_url else None

    # Overlays
    print("[OVERLAY] Criando paineis...")
    ov_intro = make_overlay_intro(home_team, away_team, league,
                                   match_date, match_time, hb, ab)
    ov_cta   = make_overlay_cta()
    print(f"  Intro : {ov_intro}")
    print(f"  CTA   : {ov_cta}")

    # Assets
    vb = {i: ASSETS_DIR / f"video_base{'_' if i > 1 else ''}{'' if i == 1 else i}.mp4"
          for i in range(1, 5)}
    # Corrige paths: video_base.mp4, video_base_2.mp4 ... ou video_base2.mp4
    vb[1] = ASSETS_DIR / "video_base.mp4"
    vb[2] = ASSETS_DIR / "video_base2.mp4"
    vb[3] = ASSETS_DIR / "video_base3.mp4"
    vb[4] = ASSETS_DIR / "video_base4.mp4"

    segs = [TEMP_DIR / f"seg{i}.mp4" for i in range(1, 7)]

    print("\n[VIDEO] Gerando segmentos...")

    # Seg 1 e 2 — intro robos
    for i, (dur, vbi, seg) in enumerate(
        [(DUR_V1, vb[1], segs[0]), (DUR_V2, vb[2], segs[1])], 1
    ):
        if vbi.exists():
            print(f"  Seg {i} ({dur}s): {vbi.name} + overlay intro...")
            vid_overlay(vbi, ov_intro, dur, seg)
        else:
            print(f"  Seg {i}: {vbi.name} nao encontrado — slide escuro")
            img2vid(Image.new("RGB", (W, H), (10, 10, 16)), dur, seg)

    # Seg 3 — stats
    print(f"  Seg 3 ({dur_stats:.1f}s): slide de estatisticas...")
    img2vid(make_slide_stats(home_team, away_team, stats), dur_stats, segs[2])

    # Seg 4 — tip
    print(f"  Seg 4 ({dur_tip:.1f}s): slide da tip...")
    img2vid(make_slide_tip(home_team, away_team, tip_text), dur_tip, segs[3])

    # Seg 5 e 6 — outro robos + CTA
    for i, (dur, vbi, seg) in enumerate(
        [(DUR_V3, vb[3], segs[4]), (DUR_V4, vb[4], segs[5])], 5
    ):
        if vbi.exists():
            print(f"  Seg {i} ({dur}s): {vbi.name} + overlay CTA...")
            vid_overlay(vbi, ov_cta, dur, seg)
        elif vb[1].exists():
            print(f"  Seg {i}: {vbi.name} nao encontrado — reutilizando video_base.mp4")
            vid_overlay(vb[1], ov_cta, dur, seg)
        else:
            img2vid(Image.new("RGB", (W, H), (8, 8, 18)), dur, seg)

    # Concat
    print("\n[VIDEO] Concatenando todos os segmentos...")
    silent = TEMP_DIR / "video_mudo.mp4"
    concat(segs, silent)

    # Mux audio
    print("[VIDEO] Adicionando audio...")
    final = OUTPUT_DIR / f"video_final_{run_ts}.mp4"
    mux(silent, audio_path, final)

    size_mb = final.stat().st_size / (1024 * 1024)

    print("\n" + "=" * 54)
    print("  FASE 2 CONCLUIDA")
    print("=" * 54)
    print(f"\n  Arquivo  : {final}")
    print(f"  Tamanho  : {size_mb:.1f} MB")
    print(f"  Resolucao: {W}x{H}  |  FPS: {FPS}")
    print()
    print("  Estrutura do video:")
    t0 = 0
    print(f"    [{t0}s-{t0+DUR_V1}s]   video_base.mp4  + overlay emblemas")
    t0 += DUR_V1
    print(f"    [{t0}s-{t0+DUR_V2}s]  video_base2.mp4 + overlay emblemas")
    t0 += DUR_V2
    print(f"    [{t0}s-{t0+dur_stats:.0f}s]  Slide STATS ({dur_stats:.1f}s)")
    t0 += dur_stats
    print(f"    [{t0:.0f}s-{t0+dur_tip:.0f}s]  Slide TIP ({dur_tip:.1f}s)")
    t0 += dur_tip
    print(f"    [{t0:.0f}s-{t0+DUR_V3:.0f}s]  video_base3.mp4 + overlay CTA")
    t0 += DUR_V3
    print(f"    [{t0:.0f}s-{t0+DUR_V4:.0f}s]  video_base4.mp4 + overlay CTA")
    print(f"    Total: {total_dur:.1f}s\n")


if __name__ == "__main__":
    run_phase2()