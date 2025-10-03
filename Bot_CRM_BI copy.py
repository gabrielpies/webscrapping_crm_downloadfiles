"""
Bot_CRM_BI_paralelo.py

Este script automatiza o download de relatórios diários do portal CRM IPASGO usando Playwright
de forma paralela com dois workers (um para datas pares, outro para ímpares).
Ele também converte arquivos .xls (HTML) para .xlsx e renomeia os arquivos de forma sequencial.
Uso: Execute diretamente para baixar, converter e organizar os relatórios do período desejado.
"""

import os #biblioteca para manipulação de arquivos e diretórios
import re #biblioteca para manipulação de expressões regulares
from concurrent.futures import ThreadPoolExecutor, as_completed #biblioteca para execução paralela
from playwright.sync_api import sync_playwright #biblioteca para automação de navegador
from datetime import date, timedelta #biblioteca para manipulação de datas
import pandas as pd #biblioteca para manipulação de dados
import time #biblioteca para manipulação de tempo
import shutil #biblioteca para operações de arquivos
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
ENV_PATH = os.path.join(os.path.dirname(__file__), "Bot_CRM_BI copy.env")
load_dotenv(ENV_PATH)

# Variáveis de ambiente com valores padrão (fallback)
CRM_USER = os.getenv("CRM_USER") or "04950891170"
CRM_PASSWORD = os.getenv("CRM_PASSWORD") or "Cenoura2!"
CRM_LOGIN_URL = os.getenv("CRM_LOGIN_URL") or "https://facportalipasgo.facilinformatica.com.br/account/login?returnUrl="
DEST_DIR = os.getenv("DEST_DIR") or r'C:\Users\04950891170\OneDrive - IPASGO\IPASGO_GERAT - Documentos\3.1 - CRM AUTOMAÇÃO PYTHON\DAILY'
BACKUP_DIR = os.getenv("BACKUP_DIR") or r'C:\Users\04950891170\OneDrive - IPASGO\IPASGO_GERAT - Documentos\3.1 - CRM AUTOMAÇÃO PYTHON\BKP'

data_inicio1 = date(2025,4,26)
#data_fim1 = date(2025, 9, 19)
data_fim1 = date.today()

caminho_pasta_destino = DEST_DIR
caminho_pasta_BKP = BACKUP_DIR

#teste de commit pro v2 - agora desta vez vai confia


# Converte os arquivos em xls para xlsx
def converter_xls_para_xlsx(caminho_arquivo_xls, pasta_destino):
    try:
        nome_arquivo = os.path.basename(caminho_arquivo_xls) # Extrai o nome do arquivo do caminho completo
        print(f'Convertendo {nome_arquivo}...')
        
        tabelas = pd.read_html(caminho_arquivo_xls, encoding='utf-8') # Lê todas as tabelas do arquivo .xls
        
        if tabelas:
            df = tabelas[0] # Pega a primeira tabela encontrada
            nome_base = os.path.splitext(nome_arquivo)[0] # Remove a extensão .xls
            novo_caminho_xlsx = os.path.join(pasta_destino, nome_base + '.xlsx') # Novo caminho com extensão .xlsx
            df.to_excel(novo_caminho_xlsx, index=False) # Salva como .xlsx
            print(f'{nome_arquivo} convertido com sucesso para .xlsx!') #   Mensagem de sucesso
            os.remove(caminho_arquivo_xls) # Remove o arquivo .xls original
            print(f'{nome_arquivo} original deletado.') # Mensagem de remoção
        else:
            print(f'Erro: Nenhum dado tabular encontrado no arquivo {nome_arquivo}.') # Mensagem de erro se nenhuma tabela for encontrada
    except Exception as e:
        print(f'Erro ao converter {caminho_arquivo_xls}: {e}') # Mensagem de erro geral

# Renomeia os arquivos de forma sequencial
def renomear_arquivos_sequencialmente(diretorio): 
    print("-" * 50)
    print("Iniciando a renomeação sequencial dos arquivos .xlsx...")
    
    arquivos_xlsx = [f for f in os.listdir(diretorio) if f.endswith('.xlsx')] # Lista todos os arquivos .xlsx no diretório
    arquivos_xlsx.sort() # Garante a ordem correta antes de renomear
    
    for i, nome_antigo in enumerate(arquivos_xlsx, 1): # Começa a enumeração a partir de 1
        novo_nome = f'FilaChamados-{i}.xlsx'
        caminho_antigo = os.path.join(diretorio, nome_antigo)
        caminho_novo = os.path.join(diretorio, novo_nome)
        
        try:
            os.rename(caminho_antigo, caminho_novo)
            print(f'Renomeado: {nome_antigo} -> {novo_nome}')
        except OSError as e:
            print(f"Erro ao renomear o arquivo {nome_antigo}: {e}")
            
    print("Renomeação sequencial concluída!")


# CAso sucesso, copia os arquivos para a nova pasta
def copiar_para_backup(caminho_origem, caminho_destino):
    """
    Copia todos os arquivos .xlsx de um diretório de origem para um de destino.
    Se o diretório de destino não existir, ele é criado.
    """
    print("-" * 50)
    print("Iniciando a cópia dos arquivos para a pasta de backup...")
    try:
        os.makedirs(caminho_destino, exist_ok=True) # Garante que a pasta de backup existe

        arquivos_para_copiar = [f for f in os.listdir(caminho_origem) if f.endswith('.xlsx')]
        
        if not arquivos_para_copiar:
            print("Nenhum arquivo .xlsx encontrado para copiar.")
            return

        for arquivo in arquivos_para_copiar:
            caminho_origem_completo = os.path.join(caminho_origem, arquivo)
            caminho_destino_completo = os.path.join(caminho_destino, arquivo)
            shutil.copy2(caminho_origem_completo, caminho_destino_completo)
            print(f"Copiado: {arquivo}")

        print("Cópia de backup concluída com sucesso!")
        
    except Exception as e:
        print(f"Erro durante a cópia de backup: {e}")

#Função Worker para processar uma lista de datas

def processar_datas(lista_de_datas, caminho_pasta, worker_id):
    """
    Função alvo para cada thread.
    Ela inicializa uma instância do Playwright e processa sua lista designada de datas.
    
    Args:
        lista_de_datas (list): A lista de objetos `date` que este worker deve processar.
        caminho_pasta (str): O diretório para salvar os arquivos.
        worker_id (str): Um identificador para o worker (ex: "PAR", "ÍMPAR") para logging.
    """
    MAX_TENTATIVAS = 3
    TEMPO_ESPERA_SEGUNDOS = 10

    # Cada worker (thread) deve ter sua própria instância do Playwright e do Browser

    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        def abrir_navegador():
            nonlocal browser, context, page
            browser.close()
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            print(f"[{worker_id}] Navegador reiniciado.")
            # Login e filtros
            page.goto(CRM_LOGIN_URL)
            page.get_by_role("textbox", name="Usuário").fill(CRM_USER)
            page.get_by_role("textbox", name="Senha").fill(CRM_PASSWORD)
            page.get_by_role("button", name="Entrar").click()
            page.get_by_role("button", name=" CRM").click()
            page.locator("xpath=/html/body/div[1]/div/div[1]/div[1]/div/div[2]/div/div/div[1]/div/div/div[1]/div[1]/div[1]/form/div[1]/span[1]").click()
            page.locator("div").filter(has_text=re.compile(r"^Aguardando Posição×$")).get_by_role("link").click()
            page.get_by_role("link", name="×").click()
            page.locator("div").filter(has_text=re.compile(r"^Não Resolvido$")).click()
            page.locator("div").filter(has_text=re.compile(r"^Resolvido$")).click()
            page.locator("div").filter(has_text=re.compile(r"^Em Resolução$")).click()
            page.locator("div").filter(has_text=re.compile(r"^Resolvido por falta de retorno$")).click()
            page.get_by_text("Aguardando Posição").first.click()
            page.get_by_role("button", name="").click()
            page.get_by_role("checkbox", name="Somente a minha fila").uncheck()
            page.get_by_role("link", name="Fechar").click()
            print(f"[{worker_id}] Filtros configurados.")

        print(f"[{worker_id}] Iniciando login e configuração dos filtros...")
        abrir_navegador()

        for data_atual in lista_de_datas:
            data_str = data_atual.strftime('%d/%m/%Y')
            print("-" * 50)
            print(f"[{worker_id}] Processando relatório para o dia: {data_str}")
            sucesso_na_data = False
            for tentativa in range(1, MAX_TENTATIVAS + 1):
                try:
                    print(f"[{worker_id}] Tentativa {tentativa} de {MAX_TENTATIVAS}...")
                    page.locator("#dataInicio > .form-control").first.fill(data_str)
                    page.locator("#dataFim > .form-control").first.fill(data_str)
                    page.locator("body").click()
                    print(f"[{worker_id}] Aguardando o relatório ser gerado...")
                    excel_button = page.get_by_role("button", name=" Excel")
                    excel_button.wait_for(state="visible", timeout=450000)
                    print(f"[{worker_id}] Botão habilitado! Iniciando o download.")
                    with page.expect_download(timeout=450000) as download_info:
                        excel_button.click()
                    download = download_info.value
                    nome_sugerido = download.suggested_filename
                    nome_base, extensao = os.path.splitext(nome_sugerido)
                    data_formatada_nome = data_atual.strftime('%Y-%m-%d')
                    novo_nome_arquivo = f"{nome_base}_{data_formatada_nome}{extensao}"
                    caminho_salvar_xls = os.path.join(caminho_pasta, novo_nome_arquivo)
                    download.save_as(caminho_salvar_xls)
                    print(f"[{worker_id}] Arquivo salvo com sucesso em")
                    converter_xls_para_xlsx(caminho_salvar_xls, caminho_pasta)
                    sucesso_na_data = True
                    break
                except Exception as e:
                    print(f"[{worker_id}] FALHA na tentativa {tentativa} para a data {data_str}. Erro: {e}")
                    if tentativa < MAX_TENTATIVAS:
                        print(f"[{worker_id}] Aguardando {TEMPO_ESPERA_SEGUNDOS} segundos...")
                        time.sleep(TEMPO_ESPERA_SEGUNDOS)
                    elif tentativa == MAX_TENTATIVAS:
                        print(f"[{worker_id}] Fechando e reabrindo navegador após 3 falhas...")
                        abrir_navegador()
            if not sucesso_na_data:
                print(f"[{worker_id}] ERRO FINAL: Todas as {MAX_TENTATIVAS} tentativas falharam para a data {data_str}.")
        print(f"[{worker_id}] Finalizou todas as suas tarefas. Fechando o navegador.")
        context.close()
        browser.close()

# --- Bloco Principal de Execução ---


if __name__ == "__main__":
    tempo_inicial_total = time.time()

    os.makedirs(caminho_pasta_destino, exist_ok=True)

    data_inicio = data_inicio1
    data_fim = data_fim1

    # 2. Gerar e Dividir a Lista de Datas
    print("Gerando e dividindo a lista de datas entre os workers...")
    datas_pares = []
    datas_impares = []
    data_atual_loop = data_inicio
    while data_atual_loop <= data_fim:
        if data_atual_loop.day % 2 == 0:
            datas_pares.append(data_atual_loop)
        else:
            datas_impares.append(data_atual_loop)
        data_atual_loop += timedelta(days=1)

    print(f"Worker de dias PARES processará {len(datas_pares)} datas.")
    print(f"Worker de dias ÍMPARES processará {len(datas_impares)} datas.")
    print("-" * 50)

    # 3. Processamento paralelo usando ThreadPoolExecutor
    workers = [
        (datas_pares, caminho_pasta_destino, "WORKER-PAR"),
        (datas_impares, caminho_pasta_destino, "WORKER-ÍMPAR")
    ]
    print("Iniciando os workers em paralelo...")
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(processar_datas, *args) for args in workers]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"[ERRO] Worker terminou com exceção: {exc}")

    print("-" * 50)
    print("Ambos os workers concluíram o download e a conversão dos arquivos!")

    # 5. Executar a Renomeação (apenas após ambos os workers terminarem)
    renomear_arquivos_sequencialmente(caminho_pasta_destino)

    # 6. Finalizar
    print("-" * 50)
    # Verificação de quantidade de arquivos salvos
    arquivos_xlsx = [f for f in os.listdir(caminho_pasta_destino) if f.endswith('.xlsx')]
    total_arquivos = len(arquivos_xlsx)
    total_datas = len(datas_pares) + len(datas_impares)
    if total_arquivos == total_datas:
        print("Todos os relatórios no período especificado foram baixados e processados!")
        copiar_para_backup(caminho_pasta_destino, caminho_pasta_BKP)
    else:
        faltando = total_datas - total_arquivos
        print(f"ATENÇÃO: Faltam {faltando} arquivo(s) para o período solicitado! ({total_arquivos} baixados de {total_datas} datas)")
    tempo_final_total = time.time()
    print(f"\nTempo total de execução do script: {(tempo_final_total - tempo_inicial_total) / 60:.2f} minutos.")