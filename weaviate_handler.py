import os
import json
import weaviate
from weaviate.auth import AuthApiKey
from weaviate.classes.query import Filter
import csv
from dotenv import load_dotenv
import spacy
import chardet

# Carrega o modelo spaCy para português
nlp = spacy.load('pt_core_news_sm')

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

weaviate_host = os.getenv("WEAVIATE_HOST")
weaviate_api_key = os.getenv("WEAVIATE_API_KEY")
openai_api_key = os.getenv("OPENAI_API_KEY")

if not weaviate_host or not weaviate_api_key or not openai_api_key:
    raise ValueError("As variáveis WEAVIATE_HOST, WEAVIATE_API_KEY, e OPENAI_API_KEY devem estar configuradas no arquivo .env.")

auth_client = AuthApiKey(api_key=weaviate_api_key)
client = weaviate.Client(
    url=weaviate_host,
    auth_client_secret=auth_client,
    additional_headers={"X-OpenAI-Api-Key": openai_api_key}
)

def generate_chunked_text(text, chunk_size):
    # Processa o texto com o spaCy para segmentação de sentenças
    doc = nlp(text)
    
    chunks = []
    current_chunk = ""
    
    # Itera sobre as sentenças detectadas pelo spaCy
    for sent in doc.sents:
        current_chunk += sent.text + " "  # Adiciona a sentença ao chunk atual
        if len(current_chunk) > chunk_size:
            chunks.append(current_chunk.strip())
            current_chunk = ""
    
    # Adiciona o último chunk se ainda houver texto restante
    if current_chunk:
        chunks.append(current_chunk.strip())
    
    return chunks

def schema_exists(class_name):
    try:
        client.schema.get(class_name)
        return True
    except weaviate.exceptions.UnexpectedStatusCodeException:
        return False


def import_file(file, chunk_size, doc_name, doc_type, collection_name):
    os.makedirs('data/upload', exist_ok=True)
    file_path = os.path.join('data/upload', file.name)
    with open(file_path, 'wb') as f:
        f.write(file.getbuffer())
    if file.name.endswith('.txt'):
        return import_txt_file(file, chunk_size, doc_name, doc_type, collection_name)
    
    return import_csv_file(file, doc_name, doc_type, collection_name, file_path)


def import_txt_file(file, chunk_size, doc_name, doc_type, collection_name):
    content = file.read().decode('utf-8')
    chunks = generate_chunked_text(content, chunk_size)
    
    with client.batch as batch:
        for chunk in chunks:
            batch.add_data_object(
                {
                    "content": chunk,
                    "doc_name": doc_name,
                    "doc_type": doc_type
                },
                class_name=collection_name
            )
    return f"Arquivo '{doc_name}' vetorizado com sucesso em {len(chunks)} chunks!"

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read(50000)  # Leia os primeiros 50.000 bytes para adivinhar a codificação
    result = chardet.detect(raw_data)
    return result['encoding']

def import_csv_file(file, doc_name, doc_type, collection_name, file_path):
    with open(file_path, mode='r', encoding=detect_encoding(file_path)) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        with client.batch as batch:
            for row in csv_reader:
                data_object = {"content": row, "doc_name": doc_name, "doc_type": doc_type}
                batch.add_data_object(data_object, class_name=collection_name)
    return f"Arquivo CSV '{doc_name}' importado com sucesso!"


def remove_document_from_weaviate(doc_name, collection_name):
    try:
        # Define o filtro para deletar todos os objetos cujo 'doc_name' corresponde ao documento especificado
        where_filter = {
            "path": ["doc_name"],
            "operator": "Equal",
            "valueText": doc_name
        }

        # Executa a exclusão de todos os objetos que correspondem ao filtro
        client.batch.delete_objects(
            class_name=collection_name,
            where=where_filter
        )

        return f"Documento '{doc_name}' e todos os seus chunks removidos com sucesso da coleção '{collection_name}'."
    except Exception as e:
        return f"Erro ao remover o documento e seus chunks: {e}"



def load_schema(schema_path='schemas/default.json'):
    if os.path.exists(schema_path):
        with open(schema_path, 'r') as schema_file:
            schema = json.load(schema_file)
            collection_name = schema.get("class", "PositiveLibraryDocument")
        return schema, collection_name
    return None, "PositiveLibraryDocument"

def get_vectorized_files(collection_name):
    try:
        objects = client.data_object.get(class_name=collection_name)
        return objects.get('objects', [])
    except Exception as e:
        return f"Erro ao buscar a lista de arquivos: {e}"

def get_chunks_by_file_name(doc_name, collection_name):
    try:
        # Realiza a consulta para buscar todos os objetos cujo 'doc_name' corresponde ao documento especificado
        response = (
            client.query
            .get(collection_name, ["content"])
            .with_where({
                "path": ["doc_name"],
                "operator": "Equal",
                "valueText": doc_name
            }).do()
        )

        # Extrai o conteúdo de todos os chunks
        
        chunks = [obj['content'] for obj in response.get('data', {}).get('Get', {}).get(collection_name, [])]

        return chunks
    except Exception as e:
        return f"Erro ao carregar os chunks do arquivo: {e}"

