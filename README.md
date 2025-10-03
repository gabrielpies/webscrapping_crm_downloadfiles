# webscrapping_crm_downloadfiles

# Bot_CRM_BI - Automação de Download de Relatórios CRM IPASGO

Este projeto automatiza o download de relatórios diários do portal CRM IPASGO utilizando Playwright, com execução paralela para maior eficiência. O bot realiza:

- Download automático dos relatórios em datas especificadas.
- Conversão dos arquivos .xls (HTML) para .xlsx.
- Renomeação sequencial dos arquivos baixados.
- Verificação automática da quantidade de arquivos baixados.
- Cópia dos arquivos para uma pasta de backup, garantindo segurança dos dados.

## Como usar

1. Configure as variáveis de ambiente no arquivo `.env` (usuário, senha, URLs e caminhos de pastas).
2. Defina o intervalo de datas desejado no início do script.
3. Execute o script Python.
4. Os relatórios serão baixados, convertidos, renomeados e copiados para a pasta de backup automaticamente.

## Requisitos

- Python 3.8+
- Playwright
- Pandas
- Dotenv

## Observações

- O script faz o download em paralelo, utilizando dois workers (um para dias pares e outro para ímpares).
- Caso algum relatório não seja baixado, o script informa a quantidade faltante.
- Todos os arquivos baixados são copiados para a pasta de backup ao final do processo.

---

Adapte conforme necessário para o seu contexto!