"""
AUTH YOUTUBE — Rode este script UMA VEZ no seu PC
==================================================
Abre o navegador para você logar no canal do YouTube.
Gera o arquivo 'token.json' que será usado pela Fase 3 para sempre.

Uso:
    pip install google-auth-oauthlib google-auth-httplib2 google-api-python-client
    python auth_youtube.py

Coloque o client_secrets.json na mesma pasta antes de rodar.
"""

import json
from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
SECRETS_FILE = "client_secrets.json"
TOKEN_FILE   = "token.json"

def main():
    print("\n" + "=" * 50)
    print("  AUTENTICAÇÃO YOUTUBE — ODDSHERO")
    print("=" * 50 + "\n")

    if not Path(SECRETS_FILE).exists():
        print(f"[ERRO] {SECRETS_FILE} não encontrado.")
        print("       Coloque o arquivo na mesma pasta e tente novamente.")
        return

    print("[INFO] Abrindo navegador para autenticação...")
    print("[INFO] Faça login com a conta do canal OddsHero.\n")

    flow = InstalledAppFlow.from_client_secrets_file(SECRETS_FILE, SCOPES)
    creds = flow.run_local_server(port=0)

    # Salva o token
    token_data = {
        "token":         creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri":     creds.token_uri,
        "client_id":     creds.client_id,
        "client_secret": creds.client_secret,
        "scopes":        list(creds.scopes),
    }
    Path(TOKEN_FILE).write_text(json.dumps(token_data, indent=2), encoding="utf-8")

    print(f"\n[OK] Token salvo em: {TOKEN_FILE}")
    print("\nPróximos passos:")
    print("  1. Copie o conteúdo do token.json")
    print("  2. No Railway: Settings → Variables → adicione:")
    print("     YOUTUBE_TOKEN = <conteúdo do token.json>")
    print("  3. Faça o mesmo com o client_secrets.json:")
    print("     YOUTUBE_CLIENT_SECRETS = <conteúdo do client_secrets.json>")
    print("\nPronto! A Fase 3 usará essas variáveis para sempre.\n")

if __name__ == "__main__":
    main()