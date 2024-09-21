import time
import requests
import logging
import os
from datetime import datetime, timedelta
from prometheus_client import start_http_server, Gauge, Counter
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Configurações do GitLab
GITLAB_URL = "https://gitlab.com"  # URL da API do GitLab
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GROUP_ID = os.getenv("GITLAB_GROUP")  # ID do grupo GitLab que queremos monitorar

# Métricas Prometheus (usadas para monitorar o estado dos pipelines)
pipeline_status_gauge = Gauge('gitlab_pipeline_status', 'Pipeline status', ['group', 'project', 'ref', 'id', 'status'])
pipeline_duration_gauge = Gauge('gitlab_pipeline_duration_seconds', 'Duração do pipeline em segundos', ['group', 'project', 'ref', 'id', 'status'])
pipeline_run_count = Counter('gitlab_pipeline_run_count', 'Pipeline run count', ['group', 'project', 'ref'])
pipeline_queue_time = Gauge('gitlab_pipeline_queue_time_seconds', 'Pipeline queue time in seconds', ['group', 'project', 'ref', 'id'])
pipeline_id_gauge = Gauge('gitlab_pipeline_id', 'ID do pipeline', ['group', 'project', 'ref', 'id'])
pipeline_timestamp_gauge = Gauge('gitlab_pipeline_timestamp', 'Last update pipeline timestamp', ['group', 'project', 'ref', 'id'])

# Métricas Prometheus (usadas para monitorar o estado dos jobs)
job_id_gauge = Gauge('gitlab_job_id', 'ID do job', ['group', 'project', 'ref', 'pipeline_id', 'job_id', 'job_name'])
job_duration_gauge = Gauge('gitlab_job_duration_seconds', 'Job duration in seconds', ['group', 'project', 'ref', 'pipeline_id', 'job_id', 'status', 'job_name'])
job_queue_time = Gauge('gitlab_job_queue_time_secondes', 'Pipeline queue time in seconds', ['group', 'project', 'ref'])
job_run_count = Counter('gitlab_job_run_count', 'Job run count', ['group', 'project', 'ref', 'job_name'])
job_status_gauge = Gauge('gitlab_job_status', 'Job status', ['group', 'project', 'ref', 'pipeline_id', 'job_id', 'status', 'job_name'])
job_timestamp_gauge = Gauge('gitlab_job_timestamp', 'Last update job timestamp', ['group', 'project', 'ref', 'pipeline_id', 'job_id', 'job_name'])

# Configuração de logging (registro de eventos e erros)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuração de requisições com retry (tenta novamente em caso de falha temporária) | Define como as requisições HTTP devem ser tratadas e feitas
session = requests.Session()
retry = Retry(
    total=3,  # Número total de tentativas antes de desistir
    backoff_factor=1,  # Tempo de espera entre tentativas, que aumenta exponencialmente
    status_forcelist=[500, 502, 503, 504],  # Códigos de status HTTP que acionam a nova tentativa
    allowed_methods=frozenset(['GET'])  # Apenas o método GET tem retry
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)  # Configuração do pool de conexões HTTP
session.mount('https://', adapter)  # Aplica essas configurações para conexões HTTPS
session.mount('http://', adapter)  # Aplica essas configurações para conexões HTTP

# Cache de IDs de pipelines processados
processed_pipelines_run_count = set()  # Usamos um conjunto para armazenar IDs únicos de pipelines processados
processed_pipelines = set()  # Usamos um conjunto para armazenar IDs únicos de pipelines processados
cache_lock = Lock()  # Lock para evitar condições de corrida ao acessar o cache
cache_key_pipeline_status = set()  # Dicionário para armazenar o status do pipeline em cache

def get_projects(group_id):
    """Obtém a lista de projetos do grupo e seus subgrupos no GitLab."""
    retry_delay = 10
    projects = []  # Lista para armazenar os projetos
    PAGE = 1  # Começa na primeira página de resultados
    PER_PAGE = 100  # Número de projetos por página (limite máximo)

    while True:
        # URL da API para obter projetos do grupo (inclui subgrupos)
        url = f"{GITLAB_URL}/api/v4/groups/{group_id}/projects?include_subgroups=true&page={PAGE}&per_page={PER_PAGE}"
        try:
            response = session.get(url, headers={"PRIVATE-TOKEN": GITLAB_TOKEN})  # Faz a requisição GET
            response.raise_for_status()  # Gera uma exceção se o status da resposta não for 200 (OK)
            page_projects = response.json()  # Converte a resposta JSON para um objeto Python
            
            if not page_projects:  # Se não houver mais projetos na página
                logging.info('No more projects to fetch.')  # Informa que não há mais projetos a buscar
                break  # Sai do loop
            
            logging.info(f'Fetched {len(page_projects)} projects from page {PAGE}.')  # Informa quantos projetos foram obtidos nesta página
            projects.extend(page_projects)  # Adiciona os projetos desta página à lista total
            PAGE += 1  # Passa para a próxima página de resultados
        
        except requests.RequestException as e:
            logging.error(f'Error fetching projects: {e}')  # Registra um erro se a requisição falhar
            time.sleep(retry_delay)  # Espera 10 segundos antes de tentar novamente
            retry_delay *= 2

    logging.info(f'Total projects fetched: {len(projects)}')  # Informa o total de projetos obtidos
    return projects  # Retorna a lista de projetos

def get_pipelines(project_id, project_name):
    """Obtém a lista de pipelines para um projeto específico, paginando se necessário."""
    retry_delay = 10
    pipelines = []  # Lista para armazenar os pipelines
    PAGE = 1  # Começa na primeira página de resultados
    PER_PAGE = 100  # Número de pipelines por página
    max_errors = 3  # Número máximo de erros permitidos antes de desistir
    error_count = 0  # Contador de erros
    updated_after = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')

    while True:
        # URL da API para obter pipelines do projeto
        url = f"{GITLAB_URL}/api/v4/projects/{project_id}/pipelines?sort=asc&page={PAGE}&per_page={PER_PAGE}&updated_after={updated_after}"
        try:
            response = session.get(url, headers={"PRIVATE-TOKEN": GITLAB_TOKEN})  # Faz a requisição GET
            response.raise_for_status()  # Gera uma exceção se o status da resposta não for 200 (OK)
            page_pipelines = response.json()  # Converte a resposta JSON para um objeto Python

            if not page_pipelines:  # Se não houver mais pipelines na página
                logging.info(f'No more pipelines to fetch for project {project_name} ID {project_id}.')  # Informa que não há mais pipelines a buscar
                break  # Sai do loop

            logging.info(f'Fetched {len(page_pipelines)} pipelines from page {PAGE} for project: {project_name} - {project_id}.')  # Informa quantos pipelines foram obtidos nesta página
            pipelines.extend(page_pipelines)  # Adiciona os pipelines desta página à lista total
            PAGE += 1  # Passa para a próxima página de resultados
            error_count = 0  # Reinicia o contador de erros após uma requisição bem-sucedida
        
        except requests.RequestException as e:
            error_count += 1  # Incrementa o contador de erros
            logging.error(f'Error fetching pipelines for project {project_name} ID {project_id}: {e}')  # Registra um erro se a requisição falhar
            if error_count >= max_errors:  # Se o número de erros ultrapassar o máximo permitido
                logging.error(f'Too many errors fetching pipelines for project {project_name} ID {project_id}. Skipping to the next project.')  # Informa que está pulando para o próximo projeto
                break  # Sai do loop
            time.sleep(retry_delay)  # Espera 10 segundos antes de tentar novamente
            retry_delay *= 2

    logging.info(f'Total pipelines fetched for project {project_name} ID {project_id}: {len(pipelines)}')  # Informa o total de pipelines obtidos para este projeto
    return pipelines  # Retorna a lista de pipelines

def get_jobs(project_id, pipeline_id):
    """Obtém detalhes de um pipeline específico."""
    url = f"{GITLAB_URL}/api/v4/projects/{project_id}/pipelines/{pipeline_id}/jobs"  # URL da API para obter detalhes de jobs de um pipeline específico
    try:
        response = session.get(url, headers={"PRIVATE-TOKEN": GITLAB_TOKEN})  # Faz a requisição GET
        response.raise_for_status()  # Gera uma exceção se o status da resposta não for 200 (OK)
        return response.json()  # Converte a resposta JSON para um objeto Python e a retorna
    except requests.RequestException as e:
        logging.error(f'Error fetching job details for pipeline ID {pipeline_id} in project ID {project_id}: {e}')  # Registra um erro se a requisição falhar
        return {}  # Retorna um dicionário vazio em caso de erro

def get_pipeline_details(project_id, pipeline_id):
    """Obtém detalhes de um pipeline específico."""
    url = f"{GITLAB_URL}/api/v4/projects/{project_id}/pipelines/{pipeline_id}"  # URL da API para obter detalhes de um pipeline específico
    try:
        response = session.get(url, headers={"PRIVATE-TOKEN": GITLAB_TOKEN})  # Faz a requisição GET
        response.raise_for_status()  # Gera uma exceção se o status da resposta não for 200 (OK)
        return response.json()  # Converte a resposta JSON para um objeto Python e a retorna
    except requests.RequestException as e:
        logging.error(f'Error fetching details for pipeline ID {pipeline_id} in project ID {project_id}: {e}')  # Registra um erro se a requisição falhar
        return {}  # Retorna um dicionário vazio em caso de erro

def proccess_job(jobs, project_id, pipeline_id, ref, project_name, group_name):
    for job in jobs:
        try:
            job_id = job['id']
            job_duration = job['duration']
            job_status = job['status']
            job_queue_duration = job['queued_duration']
            job_timestamp_str = job['created_at']
            job_name = job['name']
            #job_stage = job['stage']

            if job_id is not None:
                job_id_gauge.labels(group=group_name, project=project_name, ref=ref, pipeline_id=pipeline_id, job_id=job_id, job_name=job_name).set(job_id)
            if job_duration is not None:
                job_duration_gauge.labels(group=group_name, project=project_name, ref=ref, pipeline_id=pipeline_id, job_id=job_id, status=job_status, job_name=job_name).set(job_duration)
            if job_status is not None:
                job_status_gauge.labels(group=group_name, project=project_name, ref=ref, pipeline_id=pipeline_id, job_id=job_id, status=job_status, job_name=job_name).set(1)
            if job_queue_duration is not None:
                job_queue_time.labels(group=group_name, project=project_name, ref=ref).set(job_queue_duration)
            if job_timestamp_str is not None:
                job_timestamp_dt = datetime.strptime(job_timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
                job_timestamp = job_timestamp_dt.timestamp()
                job_timestamp_gauge.labels(group=group_name, project=project_name, ref=ref, pipeline_id=pipeline_id, job_id=job_id, job_name=job_name).set(job_timestamp)
            
            job_run_count.labels(group=group_name, project=project_name, ref=ref, job_name=job_name).inc()  # Incrementa o contador de execuções do pipeline
            
        except Exception as e:
            logging.error(f'Error processing job {job_id} for project {project_name} - pipeline ID {pipeline_id}: {e}')  # Registra um erro se algo falhar ao processar o job


def process_pipeline(project, pipeline):
    """Processa um pipeline específico para coletar métricas e expô-las imediatamente."""
    project_id = project['id']  # ID do projeto
    project_name = project['path']  # Nome do projeto
    group_name = project['namespace']['full_path']  # Caminho completo do grupo ao qual o projeto pertence
    pipeline_id = pipeline['id']  # ID do pipeline
    ref = pipeline['ref']  # Nome da referência (branch ou tag)
    try:
        pipeline_details = get_pipeline_details(project_id, pipeline_id)  # Obtém detalhes do pipeline
        pipeline_duration = pipeline_details.get('duration')  # Duração do pipeline em segundos
        pipeline_status = pipeline_details.get('status')  # Status do pipeline (sucesso, falha, etc.)
        pipeline_queued_duration = pipeline_details.get('queued_duration')  # Tempo em fila do pipeline em segundos
        pipeline_timestamp_str = pipeline_details.get('updated_at')  # Timestamp da última atualização do pipeline
        statuses_to_reset = ["success", "failed", "created", "pending", "running", "canceled", "waiting_for_resource", "preparing", "skipped", "manual", "scheduled"]

        if pipeline_id is not None:
            pipeline_id_gauge.labels(group=group_name, project=project_name, ref=ref, id=pipeline_id).set(pipeline_id)  # Define o valor do ID do pipeline na métrica

        if pipeline_status is not None:
            pipeline_status_gauge.labels(group=group_name, project=project_name, ref=ref, id=pipeline_id, status=pipeline_status).set(1)  # Define status com 1
            for status in statuses_to_reset:
                if status != pipeline_status:
                    if f"{pipeline_id}-{status}" in cache_key_pipeline_status:
                        pipeline_status_gauge.labels(group=group_name, project=project_name, ref=ref, id=pipeline_id, status=status).set(0) # Se a pipeline for reiniciada e mudar seu status os outros status dessa pipeline será definido como 0
            cache_key_pipeline_status.add(f"{pipeline_id}-{pipeline_status}")
        if pipeline_duration is not None:
            pipeline_duration_gauge.labels(group=group_name, project=project_name, ref=ref, id=pipeline_id,  status=pipeline_status).set(pipeline_duration)  # Define o valor da métrica de duração
        if pipeline_queued_duration is not None:
            pipeline_queue_time.labels(group=group_name, project=project_name, ref=ref, id=pipeline_id).set(pipeline_queued_duration)  # Define o valor da métrica de tempo em fila
        if pipeline_timestamp_str is not None:
            pipeline_timestamp_dt = datetime.strptime(pipeline_timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')  # Converte o timestamp para um objeto datetime
            pipeline_timestamp = pipeline_timestamp_dt.timestamp()  # Converte o objeto datetime para um timestamp (float)
            pipeline_timestamp_gauge.labels(group=group_name, project=project_name, ref=ref, id=pipeline_id).set(pipeline_timestamp)
    
        # Adiciona ID da pipeline na lista de pipelines processadas (geral)
        if pipeline_id not in processed_pipelines_run_count:
            pipeline_run_count.labels(group=group_name, project=project_name, ref=ref).inc()  # Incrementa o contador de execuções do pipeline

        jobs = get_jobs(project_id, pipeline_id)  # Obtém a lista de jobs da pipeline
        proccess_job(jobs, project_id, pipeline_id, ref, project_name, group_name)

        with cache_lock:  # Adquire o lock antes de modificar o conjunto de pipelines processados
            datetime_now = datetime.utcnow()
            # Adiciona ID da pipeline na lista de pipelines processadas com sucesso
            if pipeline_status == "success" or datetime_now - pipeline_timestamp_dt > timedelta(weeks=1):
                processed_pipelines.add(pipeline_id)  # Adiciona o pipeline ao conjunto de pipelines que não serão processadas novamente
            
            processed_pipelines_run_count.add(pipeline_id)  # Adiciona o pipeline ao conjunto de processados
        
    except Exception as e:
        logging.error(f'Error processing pipeline {pipeline_id} for project {project_name}: {e}')  # Registra um erro se algo falhar ao processar o pipeline

def collect_pipeline_metrics(initial_run=False):
    """Coleta métricas de pipelines e as expõe em intervalos regulares."""
    logging.info('Starting to collect pipeline metrics.')  # Informa que a coleta de métricas foi iniciada
    # Define a variável como global para acessá-la em todo o script   
    global processed_pipelines_run_count 
    global processed_pipelines
    
    projects = get_projects(GROUP_ID)  # Obtém a lista de projetos do grupo

    with ThreadPoolExecutor(max_workers=10) as executor:  # Inicia um pool de threads para processar pipelines em paralelo
        futures = []  # Lista para armazenar objetos futuros
        for project in projects:  # Para cada projeto obtido
            if all(substring not in project['path_with_namespace'] for substring in ["library-pipeline", "LIVINFRACLOUD", "/ba/"]): # não coleta métricas de pipelines de projetos que se encontram no subgrupo library-pipeline, INFRACLOUD e ba
                pipelines = get_pipelines(project['id'], project['name'])  # Obtém a lista de pipelines do projeto
                for pipeline in pipelines:  # Para cada pipeline obtido
                    if pipeline['id'] not in processed_pipelines:  # Verifica se o pipeline já foi processado
                        futures.append(executor.submit(process_pipeline, project, pipeline))  # Envia a tarefa para ser processada por uma thread
        
        for future in as_completed(futures):  # Espera até que cada tarefa seja concluída
            try:
                future.result(timeout=60)  # Obtém o resultado da tarefa para capturar exceções
            except Exception as e:
                logging.error(f'Error in future task: {e}')  # Registra um erro se uma tarefa falhar
    
    logging.info('Pipeline metrics collection completed.')  # Informa que a coleta de métricas foi concluída

if __name__ == "__main__":
    start_http_server(8000)  # Inicia o servidor HTTP para expor as métricas Prometheus
    logging.info('Init Pull...')  # Informa que a primeira coleta de métricas foi iniciada
    collect_pipeline_metrics(initial_run=True)  # Faz a primeira coleta de métricas
    logging.info('Sleep 5 minutes...')  # Informa que o script vai dormir por 5 minutos
    time.sleep(300)  # Aguarda 5 minutos antes de iniciar o loop de coletas regulares
    
    while True:
        try:
            collect_pipeline_metrics(initial_run=False)  # Faz a coleta de métricas em intervalos regulares
            logging.info('Sleeping 5 minutes...')  # Informa que o script vai dormir por 5 minutos
            time.sleep(300)  # Aguarda 5 minutos antes de repetir a coleta
        except Exception as e:
            logging.error(f'Error in collect_pipeline_metrics loop: {e}')  # Registra um erro se algo falhar no loop de coleta
            time.sleep(60)  # Aguarda 1 minuto antes de tentar novamente em caso de erro
