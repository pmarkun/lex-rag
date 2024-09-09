import streamlit as st
from weaviate_handler import (
    import_file,
    remove_document_from_weaviate,
    load_schema,
    get_vectorized_files,
    get_chunks_by_file_name,
    schema_exists
)


# Configura a interface para ocupar a tela wide
st.set_page_config(layout="wide")

schema, collection_name = load_schema()

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

st.title("Vetorizador de Arquivos TXT para a Biblioteca Positiva da Lex")

collection_name_input = st.text_input("Nome da Coleção no Weaviate", value=collection_name)

uploaded_file = st.file_uploader("Escolha um arquivo TXT para vetorizar", type=["txt", "csv"])
chunk_size = st.number_input("Tamanho dos chunks (número de caracteres)", min_value=1000, max_value=50000, value=10000)
doc_name = st.text_input("Nome do Documento")
doc_type = st.text_input("Tipo do Documento (configurável)")

if uploaded_file and doc_name and doc_type and collection_name_input:
    if st.button("Vetorizar Arquivo"):
        message = import_file(uploaded_file, chunk_size, doc_name, doc_type, collection_name_input)
        st.success(message)

# Layout de duas colunas
col1, col2 = st.columns([1, 2])

with col1:
    st.header("Arquivos Vetorizados")
    selected_file = None
    if collection_name_input:
        files = get_vectorized_files(collection_name_input)
        if files:
            file_names = {file['properties'].get('doc_name', 'Sem nome'): file['id'] for file in files}
            selected_file = st.selectbox("Selecione um arquivo", options=list(file_names.keys()))

            if selected_file:
                if st.button("Remover Documento"):
                    message = remove_document_from_weaviate(selected_file, collection_name_input)
                    st.success(message)
        else:
            st.write("Nenhum arquivo vetorizado encontrado.")

if selected_file:
    with col2:
        st.header(f"Chunks de '{selected_file}'")
        chunks = get_chunks_by_file_name(selected_file, collection_name_input)
        if isinstance(chunks, list):
            for i, chunk in enumerate(chunks):
                st.write(f"Chunk {i + 1}:")
                st.write(chunk[0:511] + "...")
        else:
            st.error(chunks)
