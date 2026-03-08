"""
VIDEO PIPELINE — FASE 2 (SIMPLIFICADO)
=======================================
Entrada : assets/video_base_total.mp4   — vídeo completo com áudio e narração
          output/game_data_latest.json  — dados gerados pela Fase 1

Slots no vídeo base:
  [0s  – 5s ]  Chroma key verde → overlay dos emblemas dos times
  [5s  – 13s]  Vídeo normal (sem alteração)
  [13s – 19s]  Slide da análise Telegram (sobreposto ao fundo preto)
  [19s – fim]  Vídeo normal (sem alteração)

Saída : output/video_final_<timestamp>.mp4
"""

import os
os.environ["PATH"] += ":/usr/bin"

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

W, H = None, None  # detectado automaticamente do vídeo base
FPS  = 30

# Slots de tempo (segundos)
CHROMA_START = 0.0
CHROMA_END   = 5.0
SLIDE_START  = 13.0
SLIDE_END    = 19.4

# Chroma key
CHROMA_COLOR      = "0x00b140"
CHROMA_SIMILARITY = "0.13"
CHROMA_BLEND      = "0.02"

# Paleta Telegram
TG_BG      = (14, 22, 33)
TG_BUBBLE  = (23, 33, 43)
TG_BUBBLE2 = (30, 42, 55)
TG_GREEN   = (100, 200, 110)
TG_WHITE   = (230, 234, 238)
TG_GRAY    = (140, 152, 164)
TG_GOLD    = (212, 175, 55)


# ── Helpers ───────────────────────────────────────────────────────────────────

def check_ffmpeg():
    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
        return True
    except Exception:
        return False

def get_video_duration(path: Path) -> float:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True)
    try:
        return float(r.stdout.strip())
    except Exception:
        return 5.0

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
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            pass
    return ImageFont.load_default()

def wrap_text(draw, text, f, max_w):
    words = text.split()
    lines, cur = [], ""
    for w in words:
        test = f"{cur} {w}".strip()
        if draw.textbbox((0, 0), test, font=f)[2] > max_w and cur:
            lines.append(cur)
            cur = w
        else:
            cur = test
    if cur:
        lines.append(cur)
    return lines


# ── Overlay de emblemas estilo Sofascore ──────────────────────────────────────

def make_overlay_emblemas(
    home_team, away_team, league, match_date, match_time,
    home_badge, away_badge, video_base
) -> Path:
    r = subprocess.run([
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "csv=p=0", str(video_base)
    ], capture_output=True, text=True)
    try:
        vw, vh = map(int, r.stdout.strip().split(","))
    except Exception:
        vw, vh = 1080, 1920

    print(f"  [OVERLAY] Dimensões do vídeo: {vw}x{vh}")

    s = vw / 478

    C_BG       = (16, 20, 31)
    C_CARD     = (22, 28, 45)
    C_BORDER   = (38, 48, 72)
    C_YELLOW   = (255, 204, 0)
    C_YELLOW_D = (200, 158, 0)
    C_WHITE    = (240, 245, 255)
    C_GRAY     = (120, 140, 170)

    img = Image.new("RGB", (vw, vh), C_BG)
    d   = ImageDraw.Draw(img)

    d.rectangle([0, 0, vw, vh], fill=C_BG)

    card_x = int(10 * s)
    card_y = int(60 * s)
    card_w = vw - int(20 * s)
    card_h = vh - int(80 * s)
    d.rounded_rectangle(
        [card_x, card_y, card_x + card_w, card_y + card_h],
        radius=int(10 * s), fill=C_CARD
    )
    d.rectangle([card_x, card_y, card_x + card_w, card_y + int(3*s)], fill=C_YELLOW)

    fn_liga = font(max(10, int(12 * s)))
    liga_text = league.upper()[:36]
    bx_l = d.textbbox((0, 0), liga_text, font=fn_liga)
    d.text(((vw - bx_l[2]) // 2, card_y + int(10 * s)), liga_text, font=fn_liga, fill=C_GRAY)

    anchor_x = int(135 * s)
    anchor_y = int(350 * s)
    block_w  = int(260 * s)
    badge_s  = int(78 * s)
    home_cx  = anchor_x + int(38 * s)
    away_cx  = anchor_x + block_w - int(38 * s)
    badge_cy = anchor_y + int(55 * s)

    def paste_badge(badge_path, cx, cy, size, team_name):
        if badge_path and Path(str(badge_path)).exists():
            try:
                b = Image.open(str(badge_path)).convert("RGBA").resize((size, size), Image.LANCZOS)
                bg = Image.new("RGB", (size, size), C_CARD)
                bg.paste(b, mask=b.split()[3])
                img.paste(bg, (cx - size // 2, cy - size // 2))
                return
            except Exception:
                pass
        d.ellipse([cx-size//2, cy-size//2, cx+size//2, cy+size//2],
                  fill=C_BORDER, outline=C_YELLOW_D, width=int(2*s))
        fn_i = font(max(18, int(size * 0.4)), bold=True)
        letter = team_name[0]
        bx = d.textbbox((0, 0), letter, font=fn_i)
        d.text((cx - bx[2]//2, cy - bx[3]//2), letter, font=fn_i, fill=C_WHITE)

    paste_badge(home_badge, home_cx, badge_cy, badge_s, home_team)
    paste_badge(away_badge, away_cx, badge_cy, badge_s, away_team)

    fn_vs = font(max(20, int(24 * s)), bold=True)
    vs_cx = anchor_x + block_w // 2
    bx_vs = d.textbbox((0, 0), "VS", font=fn_vs)
    d.text((vs_cx - bx_vs[2]//2, badge_cy - bx_vs[3]//2 - int(10*s)),
           "VS", font=fn_vs, fill=C_YELLOW)

    fn_hora = font(max(14, int(16 * s)), bold=True)
    try:
        date_str = datetime.strptime(match_date, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        date_str = match_date
    bx_d = d.textbbox((0, 0), date_str, font=fn_hora)
    d.text((vs_cx - bx_d[2]//2, badge_cy + int(16*s)),
           date_str, font=fn_hora, fill=C_YELLOW)
    bx_t = d.textbbox((0, 0), match_time, font=fn_hora)
    d.text((vs_cx - bx_t[2]//2, badge_cy + int(34*s)),
           match_time, font=fn_hora, fill=C_YELLOW)

    fn_team = font(max(14, int(16 * s)), bold=True)
    name_y  = badge_cy + badge_s // 2 + int(10 * s)
    for name, cx in [(home_team, home_cx), (away_team, away_cx)]:
        short = name[:14]
        bx = d.textbbox((0, 0), short, font=fn_team)
        d.text((cx - bx[2]//2, name_y), short, font=fn_team, fill=C_WHITE)

    out = TEMP_DIR / "overlay_emblemas.png"
    img.save(out, "PNG")
    return out


# ── Slide Telegram ────────────────────────────────────────────────────────────

def make_slide_telegram(
    home_team, away_team, league, match_time, match_date,
    script_text, tip_display
) -> Path:
    """Gera o slide da análise e salva como PNG transparente (RGBA)."""

    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))  # fundo transparente
    d   = ImageDraw.Draw(img)

    now_str = datetime.now().strftime("%H:%M")
    try:
        date_str = datetime.strptime(match_date, "%Y-%m-%d").strftime("%d/%m")
    except Exception:
        date_str = match_date

    bubble_x   = 40
    bubble_w   = W - 80
    bubble_pad = 28
    y = 60

    # Bolha 1: cabeçalho do jogo
    lines_header = [
        f"⚽  {home_team}  x  {away_team}",
        f"🏆  {league}",
        f"🕐  {date_str}  •  {match_time}",
    ]
    fn_sub = font(30)
    b1_h = bubble_pad * 2 + 44 + len(lines_header) * 50
    b1_y = y
    d.rounded_rectangle([bubble_x, b1_y, bubble_x + bubble_w, b1_y + b1_h],
                        radius=18, fill=(*TG_BUBBLE2, 230))
    d.rounded_rectangle([bubble_x, b1_y, bubble_x + bubble_w, b1_y + 5],
                        radius=4, fill=(*TG_GOLD, 255))
    ty = b1_y + bubble_pad + 8
    d.text((bubble_x + bubble_pad, ty), "ANÁLISE AO VIVO",
           font=font(30, bold=True), fill=(*TG_GOLD, 255))
    ty += 46
    for line in lines_header:
        d.text((bubble_x + bubble_pad, ty), line, font=fn_sub, fill=(*TG_WHITE, 255))
        ty += 50
    y = b1_y + b1_h + 20

    # Bolha 2: texto da análise
    fn_body  = font(30)
    max_text_w = bubble_w - bubble_pad * 2
    all_lines = []
    for paragraph in script_text.split("\n"):
        if paragraph.strip():
            all_lines.extend(wrap_text(d, paragraph.strip(), fn_body, max_text_w))
            all_lines.append("")
    while all_lines and all_lines[-1] == "":
        all_lines.pop()

    line_h = 44
    b2_h   = bubble_pad * 2 + len(all_lines) * line_h + 20
    b2_y   = y
    d.rounded_rectangle([bubble_x, b2_y, bubble_x + bubble_w, b2_y + b2_h],
                        radius=18, fill=(*TG_BUBBLE, 230))
    ty = b2_y + bubble_pad
    for line in all_lines:
        if line == "":
            ty += 10
            continue
        d.text((bubble_x + bubble_pad, ty), line, font=fn_body, fill=(*TG_WHITE, 255))
        ty += line_h
    d.text((bubble_x + bubble_w - 90, b2_y + b2_h - 36),
           now_str, font=font(24), fill=(*TG_GRAY, 255))
    y = b2_y + b2_h + 20

    # Bolha 3: TIP
    fn_tip   = font(42, bold=True)
    tip_lines = wrap_text(d, f"💡 TIP: {tip_display}", fn_tip, bubble_w - bubble_pad * 2)
    b3_h = bubble_pad * 2 + len(tip_lines) * 58
    b3_y = y
    d.rounded_rectangle([bubble_x, b3_y, bubble_x + bubble_w, b3_y + b3_h],
                        radius=18, fill=(20, 40, 28, 230))
    d.rounded_rectangle([bubble_x, b3_y, bubble_x + bubble_w, b3_y + 5],
                        radius=4, fill=(*TG_GREEN, 255))
    ty = b3_y + bubble_pad
    for line in tip_lines:
        d.text((bubble_x + bubble_pad, ty), line, font=fn_tip, fill=(*TG_GREEN, 255))
        ty += 58

    out = TEMP_DIR / "slide_analise.png"
    img.save(out, "PNG")
    return out


# ── Pipeline principal ────────────────────────────────────────────────────────

def run_phase2():
    print("\n" + "=" * 54)
    print("  VIDEO PIPELINE — FASE 2 (SIMPLIFICADO)")
    print("=" * 54 + "\n")

    if not check_ffmpeg():
        print("[ERRO] FFmpeg não encontrado.")
        sys.exit(1)

    # Carregar dados da fase 1
    gdp = OUTPUT_DIR / "game_data_latest.json"
    if not gdp.exists():
        gdp = OUTPUT_DIR / "game_data.json"
    if not gdp.exists():
        print("[ERRO] game_data não encontrado. Execute a Fase 1.")
        sys.exit(1)

    with open(gdp, encoding="utf-8") as f:
        data = json.load(f)

    home_team   = data["home_team"]
    away_team   = data["away_team"]
    league      = data["league"]
    match_date  = data["match_date"]
    match_time  = data["match_time"]
    hb_url      = data.get("home_badge_url")
    ab_url      = data.get("away_badge_url")
    tip_display = data.get("tip_display") or data.get("tip", "Confira a análise")
    script_text = data.get("script", "")
    run_ts      = data.get("run_ts", datetime.now().strftime("%Y%m%d_%H%M%S"))

    print(f"  Jogo : {home_team} x {away_team}")
    print(f"  Tip  : {tip_display}\n")

    # Verificar asset principal
    video_base = ASSETS_DIR / "video_base_total.mp4"
    if not video_base.exists():
        print("[ERRO] Asset não encontrado: assets/video_base_total.mp4")
        sys.exit(1)

    dur_total = get_video_duration(video_base)

    # Detectar dimensões reais do vídeo
    global W, H
    r_probe = subprocess.run([
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "csv=p=0", str(video_base)
    ], capture_output=True, text=True)
    try:
        W, H = map(int, r_probe.stdout.strip().split(","))
    except Exception:
        W, H = 1064, 1920
    print(f"  Vídeo base     : {dur_total:.2f}s  ({W}x{H})")
    print(f"  Slot chroma    : {CHROMA_START}s – {CHROMA_END}s")
    print(f"  Slot análise   : {SLIDE_START}s – {SLIDE_END}s\n")

    # Baixar emblemas
    print("[BADGES] Baixando emblemas...")
    hb = dl(hb_url, TEMP_DIR / "badge_home.png") if hb_url else None
    ab = dl(ab_url, TEMP_DIR / "badge_away.png") if ab_url else None

    # Gerar overlay dos emblemas
    print("[OVERLAY] Gerando overlay de emblemas...")
    overlay_png = make_overlay_emblemas(
        home_team, away_team, league, match_date, match_time,
        hb, ab, video_base
    )

    # Gerar slide da análise
    print("[SLIDE] Gerando slide da análise Telegram...")
    slide_png = make_slide_telegram(
        home_team, away_team, league, match_time, match_date,
        script_text, tip_display
    )

    slide_dur = SLIDE_END - SLIDE_START

    # ── FFmpeg: tudo em um único comando ──────────────────────────────────────
    # Lógica:
    #   - [0s–5s]   : chroma key do vídeo base + overlay dos emblemas
    #   - [5s–13s]  : vídeo base intacto
    #   - [13s–19s] : vídeo base + slide sobreposto (alpha blend)
    #   - [19s–fim] : vídeo base intacto
    #   - Áudio     : original do vídeo base (cópia direta)

    print("[VIDEO] Processando vídeo final (único passo)...")

    final = OUTPUT_DIR / f"video_final_{run_ts}.mp4"

    filter_complex = (
        # Normaliza o vídeo base (dimensões reais detectadas)
        f"[0:v]setsar=1,fps={FPS}[base];"

        # Overlay dos emblemas escalado para dimensões reais
        f"[1:v]scale={W}:{H}[ovr];"

        # Slide da análise escalado para dimensões reais
        f"[2:v]scale={W}:{H}[slide];"

        # Aplica chroma key no trecho 0s–5s:
        # Separa o segmento com chroma key e o resto
        f"[base]split=2[base_ck][base_full];"
        f"[base_ck]trim=start={CHROMA_START}:end={CHROMA_END},setpts=PTS-STARTPTS[seg_ck_raw];"
        f"[seg_ck_raw]chromakey={CHROMA_COLOR}:{CHROMA_SIMILARITY}:{CHROMA_BLEND}[seg_ck_fg];"
        f"[ovr]trim=start={CHROMA_START}:end={CHROMA_END},setpts=PTS-STARTPTS[ovr_trim];"
        f"[ovr_trim][seg_ck_fg]overlay=0:0[seg1];"

        # Segmento do meio (5s–13s) — intacto
        f"[base_full]trim=start={CHROMA_END}:end={SLIDE_START},setpts=PTS-STARTPTS[seg2];"

        # Segmento da análise (13s–19s) — slide sobreposto
        f"[base_full]trim=start={SLIDE_START}:end={SLIDE_END},setpts=PTS-STARTPTS[seg3_base];"
        f"[slide]trim=start=0:end={slide_dur},setpts=PTS-STARTPTS[slide_trim];"
        f"[seg3_base][slide_trim]overlay=0:0:format=auto[seg3];"

        # Segmento final (19s–fim) — intacto
        f"[base_full]trim=start={SLIDE_END},setpts=PTS-STARTPTS[seg4];"

        # Concatena tudo
        f"[seg1][seg2][seg3][seg4]concat=n=4:v=1:a=0[vout]"
    )

    r = subprocess.run([
        "ffmpeg", "-y",
        "-i", str(video_base),        # [0] vídeo base total
        "-i", str(overlay_png),       # [1] overlay emblemas (PNG estático)
        "-i", str(slide_png),         # [2] slide análise (PNG estático — loop)
        "-loop", "1",                 # faz o PNG do slide loopar
        "-filter_complex", filter_complex,
        "-map", "[vout]",
        "-map", "0:a:0",              # áudio original do vídeo base
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        str(final)
    ], capture_output=True)

    if r.returncode != 0:
        print("[ERRO ffmpeg]", r.stderr.decode(errors="replace")[-1200:])
        raise RuntimeError("Processamento do vídeo falhou")

    size_mb = final.stat().st_size / (1024 * 1024)

    print("\n" + "=" * 54)
    print("  FASE 2 CONCLUÍDA")
    print("=" * 54)
    print(f"\n  Arquivo   : {final}")
    print(f"  Tamanho   : {size_mb:.1f} MB")
    print(f"  Resolução : {W}x{H}  |  FPS: {FPS}")
    print(f"\n  Estrutura do vídeo:")
    print(f"    [0s – {CHROMA_END}s]    Chroma key + emblemas")
    print(f"    [{CHROMA_END}s – {SLIDE_START}s]   Vídeo normal")
    print(f"    [{SLIDE_START}s – {SLIDE_END}s]   Slide análise Telegram")
    print(f"    [{SLIDE_END}s – {dur_total:.1f}s]  Vídeo normal até o fim")
    print(f"    Áudio: original do vídeo base\n")


if __name__ == "__main__":
    run_phase2()