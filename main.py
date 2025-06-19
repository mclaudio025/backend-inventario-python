
import os # Adicione esta linha no topo
from pydantic import BaseModel
from typing import Optional # Adicione esta linha
from fastapi import FastAPI
from supabase import create_client, Client # Adicione esta linha

class Produto(BaseModel):
    codigo: str
    descricao: str
    preco: float
    loja: str
    estado: str
    cod_barra: Optional[str] = None
    sloja: int
    sestoque: int
    sminimo: int
    smaximo: int

# --- Configuração do Supabase ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
# --------------------------------

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Olá, Mundo! Meu backend profissional está no ar."}

@app.get("/produtos")
def get_produtos():
    response = supabase.table('produtos').select("*").limit(5).execute()
    # Acessamos os dados da resposta
    dados = response.data
    return {"produtos": dados}

# ... (código anterior) ...

@app.post("/produtos")
def criar_produto(produto: Produto):
    # O método dict() converte o objeto Produto para um dicionário
    # que o Supabase entende
    response = supabase.table('produtos').insert(produto.dict()).execute()
    
    # Verifica se a inserção deu certo
    if len(response.data) > 0:
        return {"status": "sucesso", "dados_inseridos": response.data}
    else:
        # Se der erro, a API do Supabase geralmente retorna um erro na propriedade 'error'
        # Adicionamos uma verificação para não causar um novo erro aqui
        erro_msg = response.error.message if response.error else "Erro desconhecido"
        return {"status": "falha", "erro": erro_msg}    

# ... (código anterior, incluindo a classe Produto e a rota "/")

@app.post("/produtos/sincronizar")
def sincronizar_produto(produto: Produto):
    # 1. VERIFICA SE O PRODUTO JÁ EXISTE
    # Busca na tabela 'produtos' usando o 'codigo' e a 'loja' do produto recebido
    response = supabase.table('produtos').select("id").eq('codigo', produto.codigo).eq('loja', produto.loja).execute()

    # 2. DECIDE SE VAI ATUALIZAR OU INSERIR
    if response.data:
        # SE response.data NÃO ESTÁ VAZIO, O PRODUTO EXISTE. VAMOS ATUALIZAR.
        print(f"Produto encontrado. Atualizando ID: {response.data[0]['id']}")
        
        # O método update() precisa saber qual 'id' atualizar
        update_response = supabase.table('produtos').update(produto.dict()).eq('codigo', produto.codigo).eq('loja', produto.loja).execute()

        if len(update_response.data) > 0:
            return {"status": "sucesso", "operacao": "produto_atualizado", "dados": update_response.data}
        else:
            return {"status": "falha", "operacao": "atualizar", "erro": update_response.error.message if update_response.error else "Erro desconhecido"}
    else:
        # SE response.data ESTÁ VAZIO, O PRODUTO É NOVO. VAMOS INSERIR.
        print("Produto não encontrado. Inserindo novo produto.")
        
        insert_response = supabase.table('produtos').insert(produto.dict()).execute()
        
        if len(insert_response.data) > 0:
            return {"status": "sucesso", "operacao": "produto_inserido", "dados": insert_response.data}
        else:
            return {"status": "falha", "operacao": "inserir", "erro": insert_response.error.message if insert_response.error else "Erro desconhecido"}        

