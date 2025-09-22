# Telegram VIP Funnel Bot — Railway Ready (Python 3.11)

Este pacote está configurado para rodar no **Railway** usando **Python 3.11.9**, 
que é compatível com `aiogram 2.25.2` e `aiohttp 3.8.6`.

## Passo a passo

1. Suba este projeto no GitHub.
2. No Railway, crie um novo projeto → Deploy from GitHub.
3. Railway detecta `runtime.txt` e usa Python 3.11.9.
4. Configure as variáveis de ambiente no Railway (Project → Variables):
   - `TG_BOT_TOKEN`
   - `PREVIEWS_GROUP_ID`
   - `PREVIEWS_GROUP_INVITE_LINK`
   - `PURCHASE_LINK`
   - `DISCOUNT_LINK` (opcional)
   - `ADMINS`
   - `VIDEO_URL`
   - etc.
5. Deploy → veja os logs → deve aparecer: **Bot iniciado e agendador ativado**.

Obs.: O banco SQLite (`vip_funnel_async.db`) é efêmero no Railway. 
Se quiser persistência real, adapte para Postgres (Railway já oferece).

---

## Comandos locais (opcional)
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
