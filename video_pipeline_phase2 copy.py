"""
VIDEO PIPELINE — FASE 2
=======================
Estrutura do vídeo final:
  [SEG 1]  video_base_inicio.mp4  — chroma key + overlay emblemas  (duração original)
  [SEG 2]  video_meio1.mp4        — chroma key + overlay emblemas  (duração original)
  [SEG 3]  Slide Telegram         — análise da IA                  (preenche o meio)
  [SEG 4]  video_base_fim.mp4     — tutorial Telegram              (últimos 16s fixos)
  Áudio:   narracao.mp3           — narração genérica              (duração total)

Assets necessários em assets/:
  video_base_inicio.mp4
  video_meio1.mp4
  video_base_fim.mp4
  narracao.mp3
  logo.jpeg  (opcional — avatar do bot no slide Telegram)

Entrada:  output/game_data_latest.json  (gerado pela Fase 1)
Saída:    output/video_final_<timestamp>.mp4
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

W, H = 1080, 1920
FPS  = 30

DUR_FIM = 16.0  # video_base_fim sempre ocupa os últimos 16s

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

def concat(parts: list, out: Path):
    lf = TEMP_DIR / "concat_list.txt"
    lf.write_text("\n".join(f"file '{p.resolve()}'" for p in parts))
    r = subprocess.run([
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", str(lf), "-c", "copy", str(out)
    ], capture_output=True)
    if r.returncode != 0:
        print("[ERRO concat]", r.stderr.decode(errors="replace")[-800:])
        raise RuntimeError("concat falhou")

def mux(vid: Path, aud: Path, out: Path):
    r = subprocess.run([
        "ffmpeg", "-y",
        "-i", str(vid), "-i", str(aud),
        "-map", "0:v:0", "-map", "1:a:0",
        "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
        "-shortest", str(out)
    ], capture_output=True)
    if r.returncode != 0:
        print("[ERRO mux]", r.stderr.decode(errors="replace")[-800:])
        raise RuntimeError("mux falhou")


# ── Overlay de emblemas estilo Sofascore ──────────────────────────────────────

def make_overlay_emblemas(
    home_team, away_team, league, match_date, match_time,
    home_badge, away_badge, video_inicio
) -> Path:
    # Detecta dimensões reais do vídeo
    r = subprocess.run([
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "csv=p=0", str(video_inicio)
    ], capture_output=True, text=True)
    try:
        vw, vh = map(int, r.stdout.strip().split(","))
    except Exception:
        vw, vh = 1080, 1920

    print(f"  [OVERLAY] Dimensões do vídeo início: {vw}x{vh}")

    s = vw / 478

    # Paleta Sofascore
    C_BG     = (16, 20, 31)
    C_CARD   = (22, 28, 45)
    C_BORDER = (38, 48, 72)
    C_YELLOW = (255, 204, 0)
    C_YELLOW_D = (200, 158, 0)
    C_WHITE  = (240, 245, 255)
    C_GRAY   = (120, 140, 170)

    img = Image.new("RGB", (vw, vh), C_BG)
    d   = ImageDraw.Draw(img)

    # Fundo total
    d.rectangle([0, 0, vw, vh], fill=C_BG)

    # Card central
    card_x = int(10 * s)
    card_y = int(60 * s)
    card_w = vw - int(20 * s)
    card_h = vh - int(80 * s)
    d.rounded_rectangle(
        [card_x, card_y, card_x + card_w, card_y + card_h],
        radius=int(10 * s), fill=C_CARD
    )
    # Linha amarela no topo
    d.rectangle([card_x, card_y, card_x + card_w, card_y + int(3*s)], fill=C_YELLOW)

    # Liga
    fn_liga = font(max(10, int(12 * s)))
    liga_text = league.upper()[:36]
    bx_l = d.textbbox((0, 0), liga_text, font=fn_liga)
    d.text(((vw - bx_l[2]) // 2, card_y + int(10 * s)), liga_text, font=fn_liga, fill=C_GRAY)

    # ── Posição do confronto ──
    anchor_x = int(135 * s)
    anchor_y = int(350 * s)

    block_w  = int(260 * s)
    badge_s  = int(78 * s)
    home_cx  = anchor_x + int(38 * s)
    away_cx  = anchor_x + block_w - int(38 * s)
    badge_cy = anchor_y + int(55 * s)

    def paste_badge(badge_path, cx, cy, size):
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
        letter = home_team[0] if cx < anchor_x + block_w // 2 else away_team[0]
        bx = d.textbbox((0, 0), letter, font=fn_i)
        d.text((cx - bx[2]//2, cy - bx[3]//2), letter, font=fn_i, fill=C_WHITE)

    paste_badge(home_badge, home_cx, badge_cy, badge_s)
    paste_badge(away_badge, away_cx, badge_cy, badge_s)

    # VS
    fn_vs = font(max(20, int(24 * s)), bold=True)
    vs_cx = anchor_x + block_w // 2
    bx_vs = d.textbbox((0, 0), "VS", font=fn_vs)
    d.text((vs_cx - bx_vs[2]//2, badge_cy - bx_vs[3]//2 - int(10*s)),
           "VS", font=fn_vs, fill=C_YELLOW)

    # Data (linha 1) e hora (linha 2)
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

    # Nomes dos times
    fn_team = font(max(14, int(16 * s)), bold=True)
    name_y  = badge_cy + badge_s // 2 + int(10 * s)
    for name, cx in [(home_team, home_cx), (away_team, away_cx)]:
        short = name[:14]
        bx = d.textbbox((0, 0), short, font=fn_team)
        d.text((cx - bx[2]//2, name_y), short, font=fn_team, fill=C_WHITE)

    out = TEMP_DIR / "overlay_emblemas.png"
    img.save(out, "PNG")
    return out


# ── Chroma key ────────────────────────────────────────────────────────────────

def make_seg_chromakey(video: Path, overlay: Path, out: Path):
    dur = get_video_duration(video)
    print(f"  [CHROMA] {video.name} ({dur:.1f}s)...")
    r = subprocess.run([
        "ffmpeg", "-y",
        "-i", str(video),
        "-i", str(overlay),
        "-filter_complex",
        (
            f"[1:v]scale={W}:{H}[bg];"
            f"[0:v]scale={W}:{H}:force_original_aspect_ratio=decrease,"
            f"pad={W}:{H}:(ow-iw)/2:(oh-ih)/2,setsar=1,fps={FPS}[cam];"
            f"[cam]chromakey={CHROMA_COLOR}:{CHROMA_SIMILARITY}:{CHROMA_BLEND}[ck];"
            f"[bg][ck]overlay=0:0[out]"
        ),
        "-map", "[out]",
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p", "-an",
        "-t", str(dur),
        str(out)
    ], capture_output=True)
    if r.returncode != 0:
        print("[ERRO chroma]", r.stderr.decode(errors="replace")[-800:])
        raise RuntimeError(f"chroma key falhou em {video.name}")


# ── Slide Telegram ────────────────────────────────────────────────────────────

def make_slide_telegram(
    home_team, away_team, league, match_time, match_date,
    script_text, tip_display, home_badge_path, away_badge_path
) -> Image.Image:

    img = Image.new("RGB", (W, H), TG_BG)
    d   = ImageDraw.Draw(img)

    # Header
    header_h = 160
    d.rectangle([0, 0, W, header_h], fill=(19, 30, 43))
    d.rectangle([0, header_h, W, header_h + 2], fill=(30, 45, 60))

    # Avatar circular — usa logo.jpeg se existir
    avatar_r  = 52
    avatar_cx = 80
    avatar_cy = header_h // 2
    logo_path = ASSETS_DIR / "logo.jpeg"
    if logo_path.exists():
        try:
            logo = Image.open(logo_path).convert("RGBA").resize((avatar_r*2, avatar_r*2), Image.LANCZOS)
            mask = Image.new("L", (avatar_r*2, avatar_r*2), 0)
            ImageDraw.Draw(mask).ellipse([0, 0, avatar_r*2-1, avatar_r*2-1], fill=255)
            img.paste(logo, (avatar_cx - avatar_r, avatar_cy - avatar_r), mask)
        except Exception:
            d.ellipse([avatar_cx-avatar_r, avatar_cy-avatar_r,
                       avatar_cx+avatar_r, avatar_cy+avatar_r], fill=(41, 182, 100))
    else:
        d.ellipse([avatar_cx-avatar_r, avatar_cy-avatar_r,
                   avatar_cx+avatar_r, avatar_cy+avatar_r], fill=(41, 182, 100))

    d.text((152, 42), "OddsHero Bot", font=font(40, bold=True), fill=TG_WHITE)
    d.text((152, 96), "● online", font=font(28), fill=TG_GREEN)

    # Área de mensagens
    y = header_h + 30
    now_str = datetime.now().strftime("%H:%M")
    try:
        date_str = datetime.strptime(match_date, "%Y-%m-%d").strftime("%d/%m")
    except Exception:
        date_str = match_date

    bubble_x   = 40
    bubble_w   = W - 80
    bubble_pad = 28

    # Bolha 1: cabeçalho do jogo
    lines_header = [
        f"⚽  {home_team}  x  {away_team}",
        f"🏆  {league}",
        f"🕐  {date_str}  •  {match_time}",
    ]
    fn_sub = font(30)
    b1_h = bubble_pad * 2 + 44 + len(lines_header) * 50
    b1_y = y
    d.rounded_rectangle([bubble_x, b1_y, bubble_x + bubble_w, b1_y + b1_h], radius=18, fill=TG_BUBBLE2)
    d.rounded_rectangle([bubble_x, b1_y, bubble_x + bubble_w, b1_y + 5], radius=4, fill=TG_GOLD)
    ty = b1_y + bubble_pad + 8
    d.text((bubble_x + bubble_pad, ty), "ANÁLISE AO VIVO", font=font(30, bold=True), fill=TG_GOLD)
    ty += 46
    for line in lines_header:
        d.text((bubble_x + bubble_pad, ty), line, font=fn_sub, fill=TG_WHITE)
        ty += 50
    y = b1_y + b1_h + 20

    # Bolha 2: texto da análise
    fn_body = font(30)
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
    d.rounded_rectangle([bubble_x, b2_y, bubble_x + bubble_w, b2_y + b2_h], radius=18, fill=TG_BUBBLE)
    ty = b2_y + bubble_pad
    for line in all_lines:
        if line == "":
            ty += 10
            continue
        d.text((bubble_x + bubble_pad, ty), line, font=fn_body, fill=TG_WHITE)
        ty += line_h
    d.text((bubble_x + bubble_w - 90, b2_y + b2_h - 36), now_str, font=font(24), fill=TG_GRAY)
    y = b2_y + b2_h + 20

    # Bolha 3: TIP
    fn_tip   = font(42, bold=True)
    tip_lines = wrap_text(d, f"💡 TIP: {tip_display}", fn_tip, bubble_w - bubble_pad * 2)
    b3_h = bubble_pad * 2 + len(tip_lines) * 58
    b3_y = y
    d.rounded_rectangle([bubble_x, b3_y, bubble_x + bubble_w, b3_y + b3_h], radius=18, fill=(20, 40, 28))
    d.rounded_rectangle([bubble_x, b3_y, bubble_x + bubble_w, b3_y + 5], radius=4, fill=TG_GREEN)
    ty = b3_y + bubble_pad
    for line in tip_lines:
        d.text((bubble_x + bubble_pad, ty), line, font=fn_tip, fill=TG_GREEN)
        ty += 58

    # Rodapé
    d.rectangle([0, H - 100, W, H], fill=(19, 30, 43))
    d.text((W // 2 - 180, H - 72), "⬆ Responder  •  @oddshero_bot", font=font(28), fill=TG_GRAY)

    return img


# ── Pipeline principal ─────────────────────────────────────────────────────────

def run_phase2():
    print("\n" + "=" * 54)
    print("  VIDEO PIPELINE — FASE 2")
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

    # Verificar assets
    video_inicio = ASSETS_DIR / "video_base_inicio.mp4"
    video_meio1  = ASSETS_DIR / "video_meio1.mp4"
    video_fim    = ASSETS_DIR / "video_base_fim.mp4"
    audio_narr   = ASSETS_DIR / "narracao.mp3"

    for asset, name in [
        (video_inicio, "video_base_inicio.mp4"),
        (video_meio1,  "video_meio1.mp4"),
        (video_fim,    "video_base_fim.mp4"),
        (audio_narr,   "narracao.mp3"),
    ]:
        if not asset.exists():
            print(f"[ERRO] Asset não encontrado: assets/{name}")
            sys.exit(1)

    # Calcular durações
    dur_audio   = get_video_duration(audio_narr)
    dur_inicio  = get_video_duration(video_inicio)
    dur_meio1   = get_video_duration(video_meio1)
    dur_analise = max(2.0, dur_audio - dur_inicio - dur_meio1 - DUR_FIM)

    print(f"  Áudio total    : {dur_audio:.1f}s")
    print(f"  SEG1 início    : {dur_inicio:.1f}s  (duração original)")
    print(f"  SEG2 meio1     : {dur_meio1:.1f}s  (duração original)")
    print(f"  SEG3 análise   : {dur_analise:.1f}s  (calculado automaticamente)")
    print(f"  SEG4 fim       : {DUR_FIM:.1f}s  (fixo — últimos 16s)")
    print(f"  Total          : {dur_inicio + dur_meio1 + dur_analise + DUR_FIM:.1f}s\n")

    # Baixar emblemas
    print("[BADGES] Baixando emblemas...")
    hb = dl(hb_url, TEMP_DIR / "badge_home.png") if hb_url else None
    ab = dl(ab_url, TEMP_DIR / "badge_away.png") if ab_url else None

    # Gerar overlay
    print("[OVERLAY] Gerando overlay de emblemas...")
    overlay = make_overlay_emblemas(
        home_team, away_team, league, match_date, match_time,
        hb, ab, video_inicio
    )

    # SEG 1: início + chroma key
    seg1 = TEMP_DIR / "seg1_inicio.mp4"
    make_seg_chromakey(video_inicio, overlay, seg1)
    print(f"  ✓ SEG1: {seg1.name} ({dur_inicio:.1f}s)")

    # SEG 2: meio1 + chroma key
    seg2 = TEMP_DIR / "seg2_meio1.mp4"
    make_seg_chromakey(video_meio1, overlay, seg2)
    print(f"  ✓ SEG2: {seg2.name} ({dur_meio1:.1f}s)")

    # SEG 3: slide análise Telegram
    print(f"\n[SLIDE] Gerando slide análise Telegram ({dur_analise:.1f}s)...")
    slide_img = make_slide_telegram(
        home_team, away_team, league, match_time, match_date,
        script_text, tip_display, hb, ab
    )
    seg3 = TEMP_DIR / "seg3_analise.mp4"
    img2vid(slide_img, dur_analise, seg3)
    print(f"  ✓ SEG3: {seg3.name} ({dur_analise:.1f}s)")

    # SEG 4: vídeo fim — corta em 16s, sem áudio
    print(f"\n[FIM] Preparando vídeo tutorial ({DUR_FIM:.0f}s)...")
    seg4 = TEMP_DIR / "seg4_fim.mp4"
    r = subprocess.run([
        "ffmpeg", "-y", "-i", str(video_fim),
        "-t", str(DUR_FIM),
        "-vf", f"scale={W}:{H}:force_original_aspect_ratio=decrease,"
               f"pad={W}:{H}:(ow-iw)/2:(oh-ih)/2,setsar=1,fps={FPS}",
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p", "-an",
        str(seg4)
    ], capture_output=True)
    if r.returncode != 0:
        print("[ERRO seg4]", r.stderr.decode(errors="replace")[-400:])
        raise RuntimeError("seg4 falhou")
    print(f"  ✓ SEG4: {seg4.name} ({DUR_FIM:.0f}s)")

    # Concatenar
    print("\n[VIDEO] Concatenando segmentos...")
    silent = TEMP_DIR / "video_mudo.mp4"
    concat([seg1, seg2, seg3, seg4], silent)
    total = dur_inicio + dur_meio1 + dur_analise + DUR_FIM
    print(f"  ✓ Vídeo mudo concatenado ({total:.1f}s)")

    # Adicionar áudio
    print("[AUDIO] Adicionando narração...")
    final = OUTPUT_DIR / f"video_final_{run_ts}.mp4"
    mux(silent, audio_narr, final)

    size_mb = final.stat().st_size / (1024 * 1024)

    print("\n" + "=" * 54)
    print("  FASE 2 CONCLUÍDA")
    print("=" * 54)
    print(f"\n  Arquivo   : {final}")
    print(f"  Tamanho   : {size_mb:.1f} MB")
    print(f"  Resolução : {W}x{H}  |  FPS: {FPS}")
    t = 0
    print(f"\n  Estrutura do vídeo:")
    print(f"    [0s – {t+dur_inicio:.1f}s]   SEG1 — início + chroma key")
    t += dur_inicio
    print(f"    [{t:.1f}s – {t+dur_meio1:.1f}s]  SEG2 — meio1 + chroma key")
    t += dur_meio1
    print(f"    [{t:.1f}s – {t+dur_analise:.1f}s]  SEG3 — slide análise Telegram")
    t += dur_analise
    print(f"    [{t:.1f}s – {t+DUR_FIM:.1f}s]  SEG4 — tutorial (últimos 16s)")
    print(f"    Áudio: narracao.mp3 ({dur_audio:.1f}s)\n")


if __name__ == "__main__":
    run_phase2()