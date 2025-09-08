# Serviço de Mensagens Distribuídas (Consistência Eventual) + Autenticação Básica

## Resumo
Projeto didático que implementa um mural de mensagens replicado entre vários nós (mínimo 3),
com replicação assíncrona, autenticação simples (login token) e simulação de falhas. Quando um nó
fica "offline" para replicação e depois volta, ele reconcilia suas mensagens com os peers garantindo
consistência eventual.

## Tecnologias
- Python 3.8+ (testado em 3.10)
- Flask (API HTTP)
- requests (para comunicação entre nós via HTTP)
- Threading para replicação assíncrona

## Arquivos
- `app.py` : código-fonte do nó (servidor)
- `messages_<node_id>.json` : arquivo gerado automaticamente para persistência simples por nó
- `README.md` : este arquivo

## Instalação (dependências)
Crie um virtualenv (opcional) e instale dependências:

```bash
python -m venv venv
source venv/bin/activate   # Linux/Mac
venv\Scripts\activate      # Windows

pip install Flask requests
