from streamlit.web import cli

SL_HOST = "0.0.0.0"
SL_PORT = 8501

if __name__ == "__main__":
     
    cli.main_run(["client_node.py", "--server.address", SL_HOST, "--server.port", SL_PORT])        




   


