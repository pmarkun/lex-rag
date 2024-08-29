import os
import json
import streamlit as st
import weaviate
from weaviate.auth import AuthApiKey
from textblob import TextBlob
from textblob import download_corpora
from dotenv import load_dotenv

# Configura a interface para ocupar a tela wide
st.set_page_config(layout="wide")

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

weaviate_host = os.getenv("WEAVIATE_HOST")
weaviate_api_key = os.getenv("WEAVIATE_API_KEY")
openai_api_key = os.getenv("OPENAI_API_KEY")

if not weaviate_host or not weaviate_api_key or not openai_api_key:
    st.error("As variáveis WEAVIATE_HOST, WEAVIATE_API_KEY, e OPENAI_API_KEY devem estar configuradas no arquivo .env.")
else:
    # Usando AuthApiKey para autenticação via chave API
    auth_client = AuthApiKey(api_key=weaviate_api_key)

    client = weaviate.Client(
        url=weaviate_host,
        auth_client_secret=auth_client,
        additional_headers={
            "X-OpenAI-Api-Key": openai_api_key
        }
    )

    def generate_chunked_text(text, chunk_size):
        download_corpora.download_all()
        blob = TextBlob(text)
        chunks = []
        current_chunk = ""
        for sentence in blob.sentences:
            current_chunk += sentence
            if len(current_chunk) > chunk_size:
                chunks.append(current_chunk)
                current_chunk = ""

        return chunks

    def schema_exists(class_name):
        try:
            client.schema.get(class_name)
            return True
        except weaviate.exceptions.UnexpectedStatusCodeException:
            return False

    schema_path = 'schemas/default.json'
    if os.path.exists(schema_path):
        with open(schema_path, 'r') as schema_file:
            schema = json.load(schema_file)
            collection_name = schema.get("class", "PositiveLibraryDocument")
    else:
        st.error("Arquivo de esquema não encontrado. Verifique o caminho e o nome do arquivo.")
        schema = None
        collection_name = "PositiveLibraryDocument"

    if schema:
        schema_already_exists = schema_exists(collection_name)
        if not schema_already_exists:
            if st.button("Criar Schema"):
                try:
                    client.schema.create_class(schema)
                    st.success(f"Coleção '{collection_name}' criada com sucesso!")
                except weaviate.exceptions.UnexpectedStatusCodeException as e:
                    st.error(f"Erro ao criar a coleção: {e}")
        else:
            st.info(f"O schema para a coleção '{collection_name}' já existe.")

    def import_txt_file(file, chunk_size, doc_name, doc_type, collection_name):
        try:
            content = file.read().decode('utf-8')
            chunks = generate_chunked_text(content, chunk_size)
            
            with client.batch as batch:
                for chunk in chunks:
                    id = batch.add_data_object(
                        {
                            "content": chunk,
                            "doc_name": doc_name,
                            "doc_type": doc_type
                        },
                        class_name=collection_name
                    )
            st.success(f"Arquivo '{doc_name}' vetorizado com sucesso em {len(chunks)} chunks!")
        except Exception as e:
            st.error(f"Erro ao vetorizar o arquivo: {e}")

    st.title("Vetorizador de Arquivos TXT para a Biblioteca Positiva da Lex")

    collection_name_input = st.text_input("Nome da Coleção no Weaviate", value=collection_name)

    uploaded_file = st.file_uploader("Escolha um arquivo TXT para vetorizar", type=["txt"])
    chunk_size = st.number_input("Tamanho dos chunks (número de caracteres)", min_value=100, max_value=5000, value=1000)
    doc_name = st.text_input("Nome do Documento")
    doc_type = st.text_input("Tipo do Documento (configurável)")

    if uploaded_file and doc_name and doc_type and collection_name_input:
        if st.button("Vetorizar Arquivo"):
            import_txt_file(uploaded_file, chunk_size, doc_name, doc_type, collection_name_input)

    # Layout de duas colunas
    col1, col2 = st.columns([1, 2])

    with col1:
        st.header("Arquivos Vetorizados")
        selected_file = None
        if collection_name_input:
            try:
                objects = client.data_object.get(class_name=collection_name_input)
                files = objects['objects']

                if files:
                    file_names = {file['properties'].get('doc_name', 'Sem nome'): file['id'] for file in files}
                    selected_file = st.selectbox("Selecione um arquivo", options=list(file_names.keys()))

                else:
                    st.write("Nenhum arquivo vetorizado encontrado.")
            except Exception as e:
                st.error(f"Erro ao buscar a lista de arquivos: {e}")

    if selected_file:
        with col2:
            st.header(f"Chunks de '{selected_file}'")
            try:
                selected_file_id = file_names[selected_file]
                file_chunks = client.data_object.get_by_id(selected_file_id)['properties']['content']
                for i, chunk in enumerate(file_chunks.split('\n')):
                    st.write(f"Chunk {i + 1}:")
                    st.write(chunk)
            except Exception as e:
                st.error(f"Erro ao carregar os chunks do arquivo: {e}")
